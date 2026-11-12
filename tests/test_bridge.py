"""事件采集桥接的回归检查。"""
import tempfile
import unittest

from event_bridge import EventBridge
from event_bridge.source_client import DirectSourceClient
from source_site.source import Source


def event(epoch, offset, event_id, record_id, revision, operation, body=None):
    return {
        "epoch": epoch,
        "offset": offset,
        "event_id": event_id,
        "record_id": record_id,
        "revision": revision,
        "operation": operation,
        "body": body,
    }


class BridgeTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = self.tmp.name + "/bridge.sqlite3"
        self.source = Source()
        self.client = DirectSourceClient(self.source)
        self.bridge = EventBridge(self.client, self.db)
        self.addCleanup(self.bridge.close)

    def push_sequence(self):
        return self.client.request("GET", "/push-sequence")[2]

    def log_entries(self):
        return self.bridge.read_log(0, 1000)["entries"]


class PushAndBackfillTests(BridgeTestCase):
    def test_push_gap_then_backfill_from_contiguous_offset(self):
        result = self.bridge.receive_push(self.push_sequence())
        self.assertEqual(3, result["received"])
        self.assertEqual(1, result["ack"]["contiguous_offset"])
        self.assertEqual(3, result["ack"]["max_seen_offset"])
        self.assertEqual(
            [{"record_id": "a", "revision": 1}],
            [
                {"record_id": r["record_id"], "revision": r["revision"]}
                for r in self.bridge.read_records()
            ],
        )

        result = self.bridge.backfill()
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["after"])
        self.assertEqual(3, result["ack"]["contiguous_offset"])
        self.assertEqual(3, result["ack"]["max_seen_offset"])

        records = self.bridge.read_records(include_deleted=True)
        self.assertEqual(1, len(records))
        self.assertEqual("delete", records[0]["operation"])
        self.assertEqual(3, records[0]["revision"])
        self.assertEqual([], self.bridge.read_records())

        changes = [e for e in self.log_entries() if e["kind"] == "change"]
        self.assertEqual(
            [("upsert", 1), ("delete", 3)],
            [(e["operation"], e["revision"]) for e in changes],
        )
        ignored = [e for e in self.log_entries() if e["kind"] == "ignored"]
        self.assertEqual(1, len(ignored))
        self.assertFalse(ignored[0]["applied"])
        self.assertEqual("x3", ignored[0]["event_id"])

    def test_duplicate_push_is_idempotent(self):
        sequence = self.push_sequence()
        self.bridge.receive_push(sequence)
        before = self.log_entries()
        again = self.bridge.receive_push(sequence)
        self.assertEqual(3, again["received"])
        self.assertEqual(3, again["counts"].get("duplicate_event_id", 0))
        self.assertEqual(before, self.log_entries())

    def test_conflict_is_evidence_not_business_change(self):
        self.bridge.receive_push(
            [event("e1", 1, "c1", "r", 1, "upsert", {"place": "一号"})]
        )
        self.bridge.receive_push(
            [event("e1", 2, "c2", "r", 1, "upsert", {"place": "二号"})]
        )
        records = self.bridge.read_records()
        self.assertEqual([{"place": "一号"}], [r["body"] for r in records])
        conflicts = [e for e in self.log_entries() if e["kind"] == "conflict"]
        self.assertEqual(1, len(conflicts))
        self.assertFalse(conflicts[0]["applied"])
        self.assertEqual(1, self.bridge.status()["conflicts"])

    def test_same_content_different_event_id_does_not_renotify(self):
        self.bridge.receive_push(
            [event("e1", 1, "d1", "r", 1, "upsert", {"place": "同一处"})]
        )
        self.bridge.receive_push(
            [event("e1", 2, "d2", "r", 1, "upsert", {"place": "同一处"})]
        )
        changes = [e for e in self.log_entries() if e["kind"] == "change"]
        self.assertEqual(1, len(changes))
        self.assertEqual(2, self.bridge.contiguous_offset())


class SnapshotRotationTests(BridgeTestCase):
    def prepare_e1(self):
        self.bridge.receive_push(self.push_sequence())
        self.bridge.backfill()

    def test_snapshot_failure_keeps_old_view_then_retry_switches(self):
        self.prepare_e1()
        old_view = self.bridge.read_records(include_deleted=True)

        self.source.phase = "snapshot-fail"
        failed = self.bridge.backfill()
        self.assertEqual("snapshot_failed", failed["status"])
        self.assertEqual(2, failed["failed_page"])
        self.assertEqual("e1", self.bridge.status()["current_epoch"])
        self.assertEqual(old_view, self.bridge.read_records(include_deleted=True))

        self.source.phase = "rotated"
        switched = self.bridge.backfill()
        self.assertEqual("switched", switched["status"])
        self.assertEqual("e1", switched["old_epoch"])
        self.assertEqual("e2", switched["epoch"])
        self.assertEqual(10, switched["barrier"])
        self.assertEqual("e2", self.bridge.status()["current_epoch"])

    def test_barrier_delete_survives_snapshot_switch(self):
        self.prepare_e1()
        self.source.phase = "rotated"
        result = self.bridge.backfill()
        self.assertEqual("switched", result["status"])

        live = self.bridge.read_records()
        self.assertEqual(
            [("a", 1, {"place": "新场地"})],
            [(r["record_id"], r["revision"], r["body"]) for r in live],
        )
        everything = self.bridge.read_records(include_deleted=True)
        tombstones = [r for r in everything if r["operation"] == "delete"]
        self.assertEqual([("b", 2)], [(r["record_id"], r["revision"]) for r in tombstones])

        kinds = [(e["kind"], e["operation"], e["record_id"]) for e in self.log_entries()]
        self.assertIn(("snapshot_switch", None, None), kinds)
        self.assertIn(("change", "delete", "b"), kinds)
        switch_seq = next(e["seq"] for e in self.log_entries() if e["kind"] == "snapshot_switch")
        delete_seq = next(
            e["seq"]
            for e in self.log_entries()
            if e["kind"] == "change" and e["record_id"] == "b"
        )
        self.assertLess(switch_seq, delete_seq)

    def test_late_old_epoch_push_is_evidence_only(self):
        self.prepare_e1()
        self.source.phase = "rotated"
        self.bridge.backfill()
        before = self.bridge.read_records(include_deleted=True)

        late = event("e1", 4, "x4", "a", 9, "upsert", {"place": "迟到"})
        result = self.bridge.receive_push([late])
        self.assertEqual(1, result["counts"].get("retired_stale", 0))
        self.assertEqual(before, self.bridge.read_records(include_deleted=True))
        late_entries = [e for e in self.log_entries() if e["kind"] == "late_epoch"]
        self.assertEqual(1, len(late_entries))
        self.assertFalse(late_entries[0]["applied"])

    def test_new_epoch_accepts_low_revision_again(self):
        self.prepare_e1()
        self.source.phase = "rotated"
        self.bridge.backfill()
        record = self.bridge.get_record("a")
        self.assertEqual("e2", record["epoch"])
        self.assertEqual(1, record["revision"])


class DurabilityTests(BridgeTestCase):
    def test_restart_does_not_duplicate_notifications(self):
        self.bridge.receive_push(self.push_sequence())
        self.bridge.backfill()
        self.bridge.close()

        reopened = EventBridge(self.client, self.db)
        self.addCleanup(reopened.close)
        self.bridge = reopened
        before = self.log_entries()
        self.assertEqual(3, len(before))

        reopened.receive_push(self.push_sequence())
        reopened.backfill()
        self.assertEqual(before, self.log_entries())
        self.assertEqual(3, reopened.contiguous_offset())
        self.assertEqual(3, reopened.max_seen_offset())

    def test_log_cursor_replay_is_stable(self):
        self.bridge.receive_push(self.push_sequence())
        self.bridge.backfill()
        page = self.bridge.read_log(0, 2)
        replay = self.bridge.read_log(0, 2)
        self.assertEqual(page, replay)
        self.assertTrue(page["has_more"])
        self.assertEqual([1, 2], [e["seq"] for e in page["entries"]])
        rest = self.bridge.read_log(page["next_cursor"], 2)
        self.assertEqual([3], [e["seq"] for e in rest["entries"]])
        self.assertFalse(rest["has_more"])
        self.assertEqual(rest["next_cursor"], 3)


if __name__ == "__main__":
    unittest.main()
