"""用逻辑步骤演示缺口补采、410 快照恢复、失败重试与重启幂等。

只使用 ``source_site`` 的合成资料，不连接真实消息平台。运行：

    python -m event_bridge.demo
"""
from __future__ import annotations

import tempfile

from source_site.source import Source

from .bridge import EventBridge
from .source_client import DirectSourceClient


def _fmt_records(records):
    if not records:
        return "（空）"
    parts = []
    for record in records:
        body = record["body"]
        parts.append(
            "{rid}@{epoch} rev{rev} {op} {body}".format(
                rid=record["record_id"],
                epoch=record["epoch"],
                rev=record["revision"],
                op=record["operation"],
                body=body,
            )
        )
    return "; ".join(parts)


def _fmt_log(entries):
    lines = []
    for entry in entries:
        if entry["kind"] == "change":
            lines.append(
                "  #{seq} change {op} {rid} rev{rev} epoch={epoch}".format(
                    seq=entry["seq"],
                    op=entry["operation"],
                    rid=entry["record_id"],
                    rev=entry["revision"],
                    epoch=entry["epoch"],
                )
            )
        else:
            lines.append(
                "  #{seq} {kind} applied={applied} detail={detail}".format(
                    seq=entry["seq"],
                    kind=entry["kind"],
                    applied=entry["applied"],
                    detail=entry["detail"],
                )
            )
    return "\n".join(lines)


def run_demo(echo=print):
    with tempfile.TemporaryDirectory(prefix="event-bridge-demo-") as tmp:
        db_path = tmp + "/bridge.sqlite3"
        source = Source()
        client = DirectSourceClient(source)
        bridge = EventBridge(client, db_path)

        echo("1) 接收乱序推送（offset 3,1,3），offset 2 先留缺口")
        pushed = client.request("GET", "/push-sequence")[2]
        result = bridge.receive_push(pushed)
        echo(
            "   接收 {} 条；连续确认 offset={}，已见最大 offset={}".format(
                result["received"],
                result["ack"]["contiguous_offset"],
                result["ack"]["max_seen_offset"],
            )
        )
        assert result["ack"]["contiguous_offset"] == 1
        assert result["ack"]["max_seen_offset"] == 3
        echo("   当前资料: " + _fmt_records(bridge.read_records(include_deleted=True)))

        echo("2) 主动补采：从连续确认 offset=1 开始，而不是最大 offset=3")
        result = bridge.backfill()
        echo(
            "   补采 after={}，处理 {} 条，连续进度推进到 {}".format(
                result["after"], len(result["processed"]), result["ack"]["contiguous_offset"]
            )
        )
        assert result["ack"]["contiguous_offset"] == 3
        echo("   当前资料: " + _fmt_records(bridge.read_records(include_deleted=True)))

        echo("3) 重启桥接，确认日志不重复")
        bridge.close()
        bridge = EventBridge(client, db_path)
        log_before = bridge.read_log(0, 100)["entries"]
        echo("   重启后日志 {} 条；当前资料: {}".format(len(log_before), _fmt_records(bridge.read_records(include_deleted=True))))
        bridge.receive_push(pushed)
        assert bridge.read_log(0, 100)["entries"] == log_before
        echo("   重复推送后日志仍为 {} 条".format(len(bridge.read_log(0, 100)["entries"])))

        echo("4) 来源轮换 epoch，补采游标过期返回 410")
        source.phase = "snapshot-fail"
        failed = bridge.backfill()
        echo(
            "   快照第 {} 页失败：{}".format(failed["failed_page"], failed["error"])
        )
        assert failed["status"] == "snapshot_failed"
        echo(
            "   失败未切换视图：current_epoch={}，旧视图: {}".format(
                bridge.status()["current_epoch"],
                _fmt_records(bridge.read_records(include_deleted=True)),
            )
        )
        assert bridge.status()["current_epoch"] == "e1"

        echo("5) 修复快照后重试：完整落盘两页快照，再原子切换并接 barrier 后事件")
        source.phase = "rotated"
        switched = bridge.backfill()
        echo(
            "   切换到 epoch={}，barrier={}，快照记录 {} 条".format(
                switched["epoch"], switched["barrier"], switched["snapshot_records"]
            )
        )
        assert switched["status"] == "switched"
        assert switched["barrier"] == 10
        echo("   切换后资料: " + _fmt_records(bridge.read_records(include_deleted=True)))
        b_record = bridge.get_record("b")
        assert b_record is not None and b_record["operation"] == "delete"
        echo("   barrier 后的 delete 已保留：b rev{} {}".format(b_record["revision"], b_record["operation"]))

        echo("6) 旧 epoch 迟到推送只留证，不改新状态")
        late = {
            "epoch": "e1",
            "offset": 4,
            "event_id": "x4",
            "record_id": "a",
            "revision": 9,
            "operation": "upsert",
            "body": {"place": "迟到写入"},
        }
        before = bridge.read_records(include_deleted=True)
        bridge.receive_push([late])
        assert bridge.read_records(include_deleted=True) == before
        echo("   迟到推送后视图不变: " + _fmt_records(before))

        echo("7) 再次重启，验证日志游标稳定且不重复通知")
        bridge.close()
        bridge = EventBridge(client, db_path)
        final_log = bridge.read_log(0, 100)["entries"]
        echo("   最终下游日志 {} 条:".format(len(final_log)))
        echo(_fmt_log(final_log))
        page = bridge.read_log(0, 3)
        assert bridge.read_log(0, 3) == page
        bridge.receive_push(pushed)
        assert bridge.read_log(0, 100)["entries"] == final_log
        echo("   重放同一页内容一致；重复推送未新增日志")
        final_records = bridge.read_records(include_deleted=True)
        bridge.close()
        return {"log_entries": len(final_log), "records": final_records}


if __name__ == "__main__":
    run_demo()
