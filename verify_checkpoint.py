"""检查点清理终态 / 版本数据 / 路径释放 验收脚本（不依赖第三方测试框架）。"""
from __future__ import annotations

import sys
import threading

sys.path.insert(0, "/workspace/backend")

from app.services.checkpoint import CheckpointService, LEDGER_KEY
from app.services.jobops import JobopsService
from app.store import store

cp = CheckpointService()
jo = JobopsService()
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


# ---- 验收样例：CHEC-0003（id=3）清理 ------------------------------------------------
entry3 = cp.get_entry(3)
check("CHEC-0003 清理前状态为已过期", entry3["status"] == "已过期", f"实际 {entry3['status']}")
check("CHEC-0003 保存路径为 /ckpt/03", entry3["保存路径"] == "/ckpt/03")
check("CHEC-0003 恢复耗时沿用历史值", entry3["恢复耗时"] == "18秒")

job3 = store.find("jobops", 3)
check("清理前 JOBO-0003 引用 /ckpt/03", job3["引用检查点路径"] == "/ckpt/03")

e, msg = cp.run_action(3, "清理记录")
check("清理动作返回成功", e is not None, msg)
check("清理后记录停在已清理终态", e["status"] == "已清理", f"实际 {e['status']}")
check("展示字段 记录状态 同步为已清理", e["记录状态"] == "已清理")
check("已清理后 pending=False", e["pending"] is False)
check("路径释放标记写入记录", e["路径释放"] is True)
check("保存路径字段保留历史值可追溯", e["保存路径"] == "/ckpt/03")
check("恢复耗时未被清理动作改写", e["恢复耗时"] == "18秒")

job3 = store.find("jobops", 3)
check("JOBO-0003 不再引用 /ckpt/03", job3.get("引用检查点路径") is None)

ledger = e[LEDGER_KEY]
last = ledger[-1]
check("版本数据追加了已清理终态版本", last["to_status"] == "已清理")
check("版本数据保留路径释放结果", last["路径释放"] is True)
check("版本数据记录释放的作业引用", "JOBO-0003" in last["释放引用"], str(last["释放引用"]))
check("版本数据保留历史保存路径", last["保存路径"] == "/ckpt/03")
check("版本数据保留恢复耗时", last["恢复耗时"] == "18秒")

# ---- 重复清理只生效一次、只落一个终态 ----------------------------------------------
versions_before = len(ledger)
e2, msg2 = cp.run_action(3, "清理记录")
check("重复清理幂等不报错", e2 is not None, msg2)
check("重复清理不新增版本", len(ledger) == versions_before, f"版本数 {len(ledger)} vs {versions_before}")
check("终态版本只有一个", sum(1 for v in ledger if v["to_status"] == "已清理") == 1)
check("终态仍停在已清理", e2["status"] == "已清理")

# ---- 清理动作不能回写恢复状态 ------------------------------------------------------
e_restore, msg_restore = cp.run_action(3, "恢复状态")
check("已清理记录拒绝恢复动作", e_restore is None, msg_restore)
check("拒绝恢复后仍是已清理", cp.get_entry(3)["status"] == "已清理")
e_archive, _ = cp.run_action(3, "归档记录")
check("已清理记录拒绝归档动作", e_archive is None)

# ---- 已恢复记录保持原值 ------------------------------------------------------------
entry2 = cp.get_entry(2)
check("CHEC-0002 仍为已恢复原值", entry2["status"] == "已恢复" and entry2["恢复耗时"] == "12秒")
check("CHEC-0002 保存路径原值保留", entry2["保存路径"] == "/ckpt/02")

# ---- 状态只能按顺序推进：已创建不能直接清理 ----------------------------------------
e_bad, msg_bad = cp.run_action(1, "清理记录")
check("已创建记录不允许直接清理（前置状态校验）", e_bad is None, msg_bad)
check("前置状态校验后原状态不变", cp.get_entry(1)["status"] == "已创建")

# 正常推进：已创建 -> 已恢复 -> 已过期 -> 已清理
e1, _ = cp.run_action(1, "恢复状态")
check("CHEC-0001 已创建→已恢复", e1["status"] == "已恢复")
e1, _ = cp.run_action(1, "归档记录")
check("CHEC-0001 已恢复→已过期", e1["status"] == "已过期")
job1 = store.find("jobops", 1)
check("清理前 JOBO-0001 引用 /ckpt/01", job1["引用检查点路径"] == "/ckpt/01")
e1, _ = cp.run_action(1, "清理记录")
check("CHEC-0001 已过期→已清理", e1["status"] == "已清理")
check("JOBO-0001 引用已释放", store.find("jobops", 1).get("引用检查点路径") is None)
check("CHEC-0001 版本链可追溯（v1 起多版本）", len(e1[LEDGER_KEY]) >= 4)

# ---- 并发清理：只落一个终态 --------------------------------------------------------
# 取一条未清理的已恢复记录：CHEC-0002
results: list[tuple[dict | None, str]] = []
def worker() -> None:
    results.append(cp.run_action(2, "清理记录"))

threads = [threading.Thread(target=worker) for _ in range(8)]
for t in threads:
    t.start()
for t in threads:
    t.join()
ok_count = sum(1 for r, _ in results if r is not None and r["status"] == "已清理")
ledger2 = cp.get_entry(2)[LEDGER_KEY]
check("并发清理全部得到已清理视图", ok_count == 8, f"成功 {ok_count}/8")
check("并发清理只落一个已清理终态版本", sum(1 for v in ledger2 if v["to_status"] == "已清理") == 1)
check("JOBO-0002 引用已释放", store.find("jobops", 2).get("引用检查点路径") is None)

# ---- 并发版本冲突：清理回执与恢复事件同时到达同一条记录 ------------------------------
# 新建一条已创建记录，再恢复到已恢复，然后并发「清理」与「恢复」
created, _ = cp.create_entry({"记录编号": "CHEC-0099", "流作业名": "冲突流作业", "检查点ID": "ckpt-x"})
new_id = created["id"]
created["保存路径"] = "/ckpt/99"
cp.run_action(new_id, "恢复状态")
outcomes: list[tuple[str, dict | None, str]] = []
def do_clean() -> None:
    outcomes.append(("清理记录",) + cp.run_action(new_id, "清理记录"))
def do_restore() -> None:
    outcomes.append(("恢复状态",) + cp.run_action(new_id, "恢复状态"))

t1 = threading.Thread(target=do_clean)
t2 = threading.Thread(target=do_restore)
t1.start(); t2.start()
t1.join(); t2.join()
final = cp.get_entry(new_id)
check("版本冲突后状态唯一确定（已清理 或 仍已恢复）", final["status"] in {"已清理", "已恢复"})
if final["status"] == "已清理":
    check("冲突裁决为清理终态时恢复事件被拒绝",
          any(a == "恢复状态" and r is None for a, r, _ in outcomes))
    check("终态版本只有一个", sum(1 for v in final[LEDGER_KEY] if v["to_status"] == "已清理") == 1)
else:
    check("冲突裁决为恢复保留时清理未写入",
          all(r is None or r["status"] != "已清理" for a, r, _ in outcomes if a == "清理记录"))

# ---- 跨模块：作业消费侧沿用同一份版本数据，不能对已释放路径再恢复 --------------------
j3, jmsg = jo.run_action(3, "启动作业")
check("JOBO-0003 不能基于已释放路径启动恢复", j3 is None, jmsg)
# 作业详情读取时同步核对引用
job3_detail = jo.get_entry(3)
check("作业详情读取已无路径引用", job3_detail.get("引用检查点路径") is None)

# ---- 异常核对：人为制造状态错位/路径引用丢失后按版本数据恢复原终态 ------------------
tampered = cp.get_entry(3)
tampered["status"] = "已恢复"          # 状态错位
tampered["记录状态"] = "已恢复"
tampered["路径释放"] = False           # 释放标记丢失
store.find("jobops", 3)["引用检查点路径"] = "/ckpt/03"  # 路径引用残留
reconciled, drift, rmsg = cp.reconcile(3)
check("核对后恢复已清理终态", reconciled["status"] == "已清理", rmsg)
check("核对发现并纠正错位", len(drift) >= 1, str(drift))
check("核对后路径释放标记恢复", reconciled["路径释放"] is True)
check("核对后残留引用被重新解除", store.find("jobops", 3).get("引用检查点路径") is None)
check("核对不新增业务版本", len(reconciled[LEDGER_KEY]) == len(ledger))

# ---- 列表与详情同一根因：接口层读取的就是对齐后的同一份数据 --------------------------
items, total = cp.list_entries(status="已清理")
check("列表过滤已清理可查到全部终态记录", {i["记录编号"] for i in items} >= {"CHEC-0001", "CHEC-0002", "CHEC-0003"})
check("列表项均带版本数据且终态一致", all(i["status"] == "已清理" and i[LEDGER_KEY][-1]["to_status"] == "已清理" for i in items))
detail = cp.get_entry(3)
check("详情与列表读到同一终态", detail["status"] == "已清理")
check("详情版本数据保留已清理终态与路径释放", detail[LEDGER_KEY][-1]["路径释放"] is True)

print()
if failures:
    print(f"共 {len(failures)} 项失败：{failures}")
    sys.exit(1)
print("全部验收项通过")
