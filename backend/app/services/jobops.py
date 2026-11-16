"""流作业运维业务规则：状态流转、字段校验与筛选口径都收在这里。

作业实例通过「引用检查点路径」与检查点恢复模块联动。所有跨模块判断都读取
CheckpointService 已确认的检查点记录版本数据（同一份事实来源）：检查点清理并释放路径后，
作业既不能继续引用该路径，也不能基于该路径再发起恢复/启动，避免版本冲突下
「路径已释放、恢复又在准备」的双写。
"""
from __future__ import annotations

from typing import Any

from app.services.checkpoint import JOBO_PATH_FIELD, CheckpointService
from app.store import store

MODULE = "jobops"
REQUIRED_FIELDS = ["实例编号", "作业名称", "运行节点"]
STATUS_ORDER = ["待启动", "运行中", "已暂停", "已停止"]
ACTION_RULES = {"启动作业": "待启动", "暂停作业": "运行中", "停止作业": "已暂停"}
NEGATIVE_ACTIONS = []

checkpoint_service = CheckpointService()


class JobopsService:
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
            rows = [row for row in rows if keyword in str(row.get("实例编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is not None:
            self._sync_checkpoint_ref(entry)
        return entry

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
        if values.get(JOBO_PATH_FIELD):
            entry[JOBO_PATH_FIELD] = values.get(JOBO_PATH_FIELD)
        rows.append(entry)
        return entry, []

    def _checkpoint_version_for(self, job: dict[str, Any]) -> dict[str, Any] | None:
        """沿用检查点记录版本数据：优先按路径解析；路径已释放为空时，
        按作业实例与检查点记录的同号对应关系兜底读取同一份版本数据，
        确保已清理终态在引用字段被释放后仍能拦住新的恢复。
        """
        save_path = job.get(JOBO_PATH_FIELD)
        version = checkpoint_service.resolve_by_path(save_path)
        if version is not None:
            return version
        entry = checkpoint_service.get_entry(int(job.get("id", 0)))
        if entry is not None:
            return checkpoint_service.confirmed_version(int(entry["id"]))
        return None

    def _sync_checkpoint_ref(self, job: dict[str, Any]) -> None:
        """消费侧沿用检查点记录版本数据：已清理并释放的路径不得继续被作业引用。"""
        save_path = job.get(JOBO_PATH_FIELD)
        version = self._checkpoint_version_for(job)
        if version is not None and version.get("路径释放"):
            if save_path:
                job[JOBO_PATH_FIELD] = None
            job["检查点引用状态"] = "检查点已清理，保存路径引用已释放"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"作业实例 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流作业运维可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        # 版本冲突裁决：启动/恢复前读取已确认的检查点记录版本数据，
        # 若检查点已清理且路径已释放，拒绝再准备新的恢复，不能既释放路径又拉起恢复。
        if action == "启动作业":
            save_path = entry.get(JOBO_PATH_FIELD)
            version = self._checkpoint_version_for(entry)
            if version is not None and version.get("路径释放"):
                blocked_path = save_path or version.get("保存路径")
                return None, (
                    f"引用的检查点记录已处于「{version.get('记录状态')}」终态，"
                    f"保存路径 {blocked_path} 已释放，不能再基于该检查点启动恢复"
                )

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        self._sync_checkpoint_ref(entry)
        return entry, f"作业实例已{action}"
