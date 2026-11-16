"""检查点恢复业务规则：状态流转、字段校验与筛选口径都收在这里。

状态只能按 已创建 → 已恢复 → 已过期 → 已清理 单向推进，清理动作写入「已清理」终态，
绝不回写「已恢复」。每条检查点记录带一份只追加、已确认的「版本数据」台账：
列表页与详情接口、作业运维等跨模块消费侧都读同一份版本数据，保证终态与路径释放结果可核对。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "checkpoint"
JOBOPS_MODULE = "jobops"
REQUIRED_FIELDS = ["记录编号", "流作业名", "检查点ID"]
STATUS_ORDER = ["已创建", "已恢复", "已过期", "已清理"]
CREATED, RESTORED, EXPIRED, CLEANED = STATUS_ORDER
TERMINAL_STATUS = CLEANED
LEDGER_KEY = "版本数据"

# 动作 -> (允许的前置状态, 动作确认后写入的目标状态)。
# 旧实现把前置状态当成目标状态回写，清理记录才会把记录打回「已恢复」。
ACTION_RULES: dict[str, tuple[tuple[str, ...], str]] = {
    "恢复状态": ((CREATED,), RESTORED),
    "归档记录": ((RESTORED,), EXPIRED),
    "清理记录": ((RESTORED, EXPIRED), CLEANED),
}
# 作业实例上引用检查点保存路径的字段，清理终态确认后需要释放。
JOBO_PATH_FIELD = "引用检查点路径"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class CheckpointService:
    """检查点记录的状态流转与版本数据都由本服务统一裁决。"""

    # 锁注册表放在类级别：数据仓库 store 是进程级单例，不同入口（路由、作业运维
    # 跨模块消费、核对接口）即便各自持有服务实例，也必须在同一把记录锁上裁决，
    # 保证清理回执与恢复事件并发时只落一份确认版本。
    _locks_guard = threading.Lock()
    _entry_locks: dict[int, threading.Lock] = {}

    @classmethod
    def _lock_for(cls, entry_id: int) -> threading.Lock:
        with cls._locks_guard:
            lock = cls._entry_locks.get(entry_id)
            if lock is None:
                lock = threading.Lock()
                cls._entry_locks[entry_id] = lock
            return lock

    # ------------------------------------------------------------------ 读取
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("记录编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def confirmed_version(self, entry_id: int) -> dict[str, Any] | None:
        """跨模块消费侧入口：在记录锁内读取已确认的最新版本数据。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        with self._lock_for(entry_id):
            self._ensure_ledger(entry)
            return dict(entry[LEDGER_KEY][-1])

    def resolve_by_path(self, save_path: str | None) -> dict[str, Any] | None:
        """按保存路径定位检查点记录，返回其已确认最新版本，供作业实例核对引用。"""
        if not save_path:
            return None
        for entry in store.rows(MODULE):
            if entry.get("保存路径") == save_path:
                return self.confirmed_version(int(entry["id"]))
        return None

    # -------------------------------------------------------------- 版本台账
    def _ensure_ledger(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """历史检查点（种子数据）没有台账时，按当前快照补登 v1，保持可追溯。"""
        ledger = entry.get(LEDGER_KEY)
        if not isinstance(ledger, list) or not ledger:
            ledger = []
            entry[LEDGER_KEY] = ledger
            ledger.append(self._build_version(
                entry,
                version=1,
                action="初始登记",
                from_status=CREATED,
                to_status=str(entry.get("status") or CREATED),
                path_released=bool(entry.get("路径释放")),
                released_refs=[],
                note="按历史快照补登的检查点记录版本数据",
            ))
        return ledger

    def _build_version(
        self,
        entry: dict[str, Any],
        *,
        version: int,
        action: str,
        from_status: str,
        to_status: str,
        path_released: bool,
        released_refs: list[str],
        note: str = "",
    ) -> dict[str, Any]:
        # 恢复耗时、保存路径沿用记录上的历史取值，清理动作不改写这些业务字段。
        return {
            "version": version,
            "action": action,
            "from_status": from_status,
            "to_status": to_status,
            "记录状态": to_status,
            "保存路径": entry.get("保存路径"),
            "恢复耗时": entry.get("恢复耗时"),
            "路径释放": path_released,
            "释放引用": list(released_refs),
            "确认时间": _now(),
            "备注": note,
        }

    def _commit(self, entry: dict[str, Any], version: dict[str, Any]) -> dict[str, Any]:
        """只追加已确认版本，并把记录当前字段对齐到该版本（单一事实来源）。"""
        ledger = self._ensure_ledger(entry)
        ledger.append(version)
        entry["status"] = version["to_status"]
        entry["记录状态"] = version["to_status"]
        entry["pending"] = version["to_status"] != TERMINAL_STATUS
        entry["abnormal"] = False
        entry["路径释放"] = version["路径释放"]
        return entry

    def _release_path_references(self, save_path: Any) -> list[str]:
        """释放作业实例对该检查点保存路径的引用，返回被释放的作业实例编号。"""
        released: list[str] = []
        if not save_path:
            return released
        for job in store.rows(JOBOPS_MODULE):
            if job.get(JOBO_PATH_FIELD) == save_path:
                job[JOBO_PATH_FIELD] = None
                code = job.get("实例编号")
                if code:
                    released.append(str(code))
        return released

    # ------------------------------------------------------------------ 写入
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["路径释放"] = False
        rows.append(entry)
        with self._lock_for(int(entry["id"])):
            self._ensure_ledger(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"检查点记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于检查点恢复可执行范围"

        preconditions, target = ACTION_RULES[action]
        with self._lock_for(entry_id):
            ledger = self._ensure_ledger(entry)
            current = str(entry.get("status") or CREATED)

            # 已清理是终态：重复清理幂等返回、不重复落版本；其它动作一律拒绝，
            # 终态不能被恢复事件回写。
            if current == TERMINAL_STATUS:
                if action == "清理记录":
                    return entry, "检查点记录已处于已清理终态，重复清理不重复生效"
                return None, "检查点记录已清理，终态不可回退到恢复或归档状态"

            # 校验清理（及其它动作）的前置状态，状态只能沿 STATUS_ORDER 单向推进。
            if current not in preconditions:
                return None, (
                    f"检查点记录当前为「{current}」，不满足动作「{action}」的前置状态"
                    f"（需为「{'、'.join(preconditions)}」）；记录状态只能按"
                    "已创建 → 已恢复 → 已过期 → 已清理 推进"
                )

            released_refs: list[str] = []
            path_released = False
            note = ""
            if action == "清理记录":
                # 清理终态：先释放作业实例上的路径引用，再把终态与释放结果写入版本数据。
                released_refs = self._release_path_references(entry.get("保存路径"))
                path_released = True
                note = f"保存路径 {entry.get('保存路径')} 已释放，作业实例引用已解除"

            version = self._build_version(
                entry,
                version=len(ledger) + 1,
                action=action,
                from_status=current,
                to_status=target,
                path_released=path_released,
                released_refs=released_refs,
                note=note,
            )
            self._commit(entry, version)
            return entry, f"检查点记录已{action}"

    # -------------------------------------------------------------- 异常核对
    def reconcile(self, entry_id: int) -> tuple[dict[str, Any] | None, list[str], str]:
        """按已确认的版本数据重新核对记录，恢复原终态与路径释放结果。

        处理清理后出现的状态错位、恢复数据残留、路径引用丢失：版本台账是唯一事实来源，
        核对只对齐当前字段、不新增业务版本。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, [], f"检查点记录 {entry_id} 不存在或已归档"

        drift: list[str] = []
        with self._lock_for(entry_id):
            ledger = self._ensure_ledger(entry)
            latest = ledger[-1]
            terminal = latest["to_status"]

            if entry.get("status") != terminal:
                drift.append(f"记录状态错位：{entry.get('status')} → {terminal}")
                entry["status"] = terminal
            if entry.get("记录状态") != terminal:
                drift.append(f"记录状态展示值错位，已对齐为「{terminal}」")
                entry["记录状态"] = terminal

            expected_pending = terminal != TERMINAL_STATUS
            if bool(entry.get("pending")) != expected_pending:
                drift.append("待处理标记与终态不一致，已按版本数据校正")
                entry["pending"] = expected_pending

            if bool(entry.get("路径释放")) != bool(latest.get("路径释放")):
                drift.append("路径释放标记与版本数据不一致，已按版本数据校正")
                entry["路径释放"] = bool(latest.get("路径释放"))

            # 版本数据确认路径已释放，但作业实例又挂回了该路径：重新解除引用。
            if latest.get("路径释放"):
                save_path = latest.get("保存路径")
                for job in store.rows(JOBOPS_MODULE):
                    if job.get(JOBO_PATH_FIELD) == save_path:
                        job[JOBO_PATH_FIELD] = None
                        code = job.get("实例编号")
                        drift.append(f"路径引用残留：{code} 重新解除对 {save_path} 的引用")

            entry["最近核对时间"] = _now()
            message = "检查点记录版本数据核对完成，终态与路径释放结果一致" if not drift else "；".join(drift)
            return entry, drift, message
