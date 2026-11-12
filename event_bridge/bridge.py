"""来源推送与补采接口到可靠本地变更日志的桥接。

设计要点：

* 推送先写入持久化收件箱（SQLite 事务提交）再返回确认；
* 同一 epoch 内 event_id 幂等，offset 唯一；
* 只有从连续确认 offset 开始的事件才会按序应用到当前资料视图；
* 同一 record_id 只接受更高 revision，同 revision 不同内容进入冲突证据；
* delete 是带 revision 的墓碑，旧 revision 的 upsert 不会复活资料；
* 来源切换 epoch 时，只有完整保存的分页快照才能原子切换可读视图；
* 下游变更日志使用独立的本地自增游标，按游标分页且只追加。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .source_client import normalize_query

MAX_SNAPSHOT_PAGES = 10000


class BridgeError(RuntimeError):
    """桥接层可预期的协议或状态错误。"""


class SnapshotError(BridgeError):
    def __init__(self, message, page=None):
        super().__init__(message)
        self.page = page


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _json_dumps(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _body_hash(operation, body):
    payload = _json_dumps({"operation": operation, "body": body})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _extract_events(payload):
    if isinstance(payload, dict):
        events = payload.get("events")
        if events is None:
            events = payload.get("push_sequence")
        if events is None:
            raise BridgeError("推送载荷缺少 events 列表")
        payload = events
    if not isinstance(payload, (list, tuple)):
        raise BridgeError("推送载荷必须是事件列表")
    return list(payload)


def _normalize_event(raw):
    if not isinstance(raw, dict):
        raise BridgeError("事件必须是 JSON 对象")
    missing = [
        key
        for key in ("epoch", "offset", "event_id", "record_id", "revision", "operation")
        if key not in raw
    ]
    if missing:
        raise BridgeError("事件缺少字段: " + ", ".join(missing))
    epoch = raw["epoch"]
    event_id = raw["event_id"]
    record_id = raw["record_id"]
    if not isinstance(epoch, str) or not epoch:
        raise BridgeError("epoch 必须是非空字符串")
    if not isinstance(event_id, str) or not event_id:
        raise BridgeError("event_id 必须是非空字符串")
    if not isinstance(record_id, str) or not record_id:
        raise BridgeError("record_id 必须是非空字符串")
    if type(raw["offset"]) is not int or raw["offset"] < 0:
        raise BridgeError("offset 必须是非负整数")
    if type(raw["revision"]) is not int or raw["revision"] < 0:
        raise BridgeError("revision 必须是非负整数")
    operation = raw["operation"]
    if operation not in ("upsert", "delete"):
        raise BridgeError("operation 必须是 upsert 或 delete")
    body = raw.get("body")
    if operation == "delete" and body is not None:
        raise BridgeError("delete 事件的 body 必须为 null")
    try:
        body_json = None if body is None else _json_dumps(body)
    except (TypeError, ValueError) as exc:
        raise BridgeError("body 必须是可 JSON 序列化的内容") from exc
    return {
        "epoch": epoch,
        "offset": raw["offset"],
        "event_id": event_id,
        "record_id": record_id,
        "revision": raw["revision"],
        "operation": operation,
        "body": body,
        "body_json": body_json,
        "operation_body_hash": _body_hash(operation, body),
    }


class EventBridge:
    """把来源推送和补采落到同一个 SQLite 变更日志。"""

    def __init__(self, source, db_path=":memory:", *, initial_epoch=None):
        """
        source: 带 ``request(method, path, query=None, ...)`` 的对象；可以直接
                传入 ``source_site.source.Source``，也可以传入
                ``HttpSourceClient`` / ``DirectSourceClient``。
        db_path: SQLite 文件路径；``:memory:`` 仅用于临时测试。
        initial_epoch: 首次接触来源前可选的当前 epoch；未提供时由第一条推送确定。
        """
        self._lock = threading.RLock()
        self.source = source
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            db_path, isolation_level=None, timeout=30, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        with self._tx() as conn:
            if initial_epoch and self._current_epoch(conn) is None:
                self._adopt_epoch(conn, initial_epoch)
            current = self._current_epoch(conn)
            if current is not None:
                self._drain_contiguous(conn, current)

    # ------------------------------------------------------------------ 基础
    def _init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS epochs (
                epoch TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS epoch_progress (
                epoch TEXT PRIMARY KEY,
                contiguous_offset INTEGER NOT NULL DEFAULT 0,
                max_seen_offset INTEGER NOT NULL DEFAULT 0,
                barrier INTEGER,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inbox (
                epoch TEXT NOT NULL,
                event_id TEXT NOT NULL,
                offset INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                operation TEXT NOT NULL,
                body_json TEXT,
                operation_body_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                logged INTEGER NOT NULL DEFAULT 0,
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                detail_json TEXT,
                PRIMARY KEY (epoch, event_id),
                UNIQUE (epoch, offset)
            );
            CREATE INDEX IF NOT EXISTS idx_inbox_epoch_offset
                ON inbox(epoch, offset);
            CREATE INDEX IF NOT EXISTS idx_inbox_epoch_status
                ON inbox(epoch, status);
            CREATE TABLE IF NOT EXISTS records (
                record_id TEXT PRIMARY KEY,
                epoch TEXT NOT NULL,
                revision INTEGER NOT NULL,
                operation TEXT NOT NULL,
                body_json TEXT,
                operation_body_hash TEXT NOT NULL,
                event_id TEXT,
                offset INTEGER,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                epoch TEXT NOT NULL,
                event_id TEXT,
                offset INTEGER,
                record_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                incoming_hash TEXT NOT NULL,
                existing_hash TEXT NOT NULL,
                incoming_body_json TEXT,
                existing_body_json TEXT,
                detected_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS downstream_log (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                epoch TEXT,
                event_id TEXT,
                offset INTEGER,
                record_id TEXT,
                revision INTEGER,
                operation TEXT,
                body_json TEXT,
                applied INTEGER NOT NULL DEFAULT 0,
                detail_json TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshot_runs (
                run_id TEXT PRIMARY KEY,
                epoch TEXT NOT NULL,
                barrier INTEGER,
                expected_old_epoch TEXT,
                status TEXT NOT NULL,
                pages INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                switched_at TEXT
            );
            CREATE TABLE IF NOT EXISTS snapshot_pages (
                run_id TEXT NOT NULL,
                page INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (run_id, page)
            );
            CREATE TABLE IF NOT EXISTS snapshot_records (
                run_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                body_json TEXT,
                PRIMARY KEY (run_id, record_id)
            );
            CREATE TABLE IF NOT EXISTS evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                kind TEXT NOT NULL,
                epoch TEXT,
                event_id TEXT,
                offset INTEGER,
                detail_json TEXT
            );
            """
        )

    @contextmanager
    def _tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    @staticmethod
    def _request(source, method, path, query=None, headers=None, body=b""):
        status, extra, payload = source.request(
            method, path, normalize_query(query), headers or {}, body
        )
        if isinstance(payload, (bytes, bytearray)):
            payload = json.loads(payload.decode("utf-8"))
        return status, extra, payload

    def close(self):
        with self._lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    # -------------------------------------------------------------- 元数据
    @staticmethod
    def _get_meta(conn, key):
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    @staticmethod
    def _set_meta(conn, key, value):
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def _current_epoch(self, conn):
        return self._get_meta(conn, "current_epoch")

    def _ensure_epoch(self, conn, epoch, state):
        now = _now()
        row = conn.execute("SELECT state FROM epochs WHERE epoch=?", (epoch,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO epochs(epoch, state, created_at, updated_at) VALUES(?,?,?,?)",
                (epoch, state, now, now),
            )
        elif state and row["state"] != state:
            conn.execute(
                "UPDATE epochs SET state=?, updated_at=? WHERE epoch=?",
                (state, now, epoch),
            )

    def _ensure_progress(self, conn, epoch, start=0, barrier=None):
        row = conn.execute(
            "SELECT * FROM epoch_progress WHERE epoch=?", (epoch,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO epoch_progress"
                "(epoch, contiguous_offset, max_seen_offset, barrier, updated_at)"
                " VALUES(?,?,?,?,?)",
                (epoch, start, start, barrier, _now()),
            )
            return start
        return row["contiguous_offset"]

    def _adopt_epoch(self, conn, epoch):
        state = conn.execute("SELECT state FROM epochs WHERE epoch=?", (epoch,)).fetchone()
        if state is not None and state["state"] == "retired":
            raise BridgeError("不能把已退役的 epoch 重新设为当前 epoch")
        self._ensure_epoch(conn, epoch, "current")
        self._set_meta(conn, "current_epoch", epoch)
        self._ensure_progress(conn, epoch, start=0)
        return epoch

    def _progress(self, conn, epoch):
        return conn.execute(
            "SELECT * FROM epoch_progress WHERE epoch=?", (epoch,)
        ).fetchone()

    def _note_offset(self, conn, epoch, offset):
        self._ensure_progress(conn, epoch, start=0)
        conn.execute(
            "UPDATE epoch_progress SET max_seen_offset=?, updated_at=? "
            "WHERE epoch=? AND max_seen_offset<?",
            (offset, _now(), epoch, offset),
        )

    # ---------------------------------------------------------------- 证据
    def _evidence(self, conn, kind, *, epoch=None, event_id=None, offset=None, detail=None):
        conn.execute(
            "INSERT INTO evidence(created_at, kind, epoch, event_id, offset, detail_json)"
            " VALUES(?,?,?,?,?,?)",
            (
                _now(),
                kind,
                epoch,
                event_id,
                offset,
                None if detail is None else _json_dumps(detail),
            ),
        )

    def _append_log(
        self, conn, kind, *, epoch=None, event_id=None, offset=None, record_id=None,
        revision=None, operation=None, body_json=None, applied=0, detail=None
    ):
        cursor = conn.execute(
            "INSERT INTO downstream_log"
            "(kind, epoch, event_id, offset, record_id, revision, operation,"
            " body_json, applied, detail_json, created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                kind,
                epoch,
                event_id,
                offset,
                record_id,
                revision,
                operation,
                body_json,
                applied,
                None if detail is None else _json_dumps(detail),
                _now(),
            ),
        ).lastrowid
        return cursor

    # ---------------------------------------------------------------- 收件箱
    def _persist_event(self, conn, event, current):
        """把一条事件持久化到收件箱，返回本次接收的分类。"""
        existing = conn.execute(
            "SELECT * FROM inbox WHERE epoch=? AND event_id=?",
            (event["epoch"], event["event_id"]),
        ).fetchone()
        if existing is not None:
            if existing["operation_body_hash"] == event["operation_body_hash"]:
                conn.execute(
                    "UPDATE inbox SET duplicate_count=duplicate_count+1 "
                    "WHERE epoch=? AND event_id=?",
                    (event["epoch"], event["event_id"]),
                )
                return "duplicate_event_id"
            self._evidence(
                conn, "event_id_reused",
                epoch=event["epoch"], event_id=event["event_id"],
                offset=event["offset"],
                detail={"existing_offset": existing["offset"]},
            )
            return "event_id_conflict"

        same_offset = conn.execute(
            "SELECT * FROM inbox WHERE epoch=? AND offset=?",
            (event["epoch"], event["offset"]),
        ).fetchone()
        if same_offset is not None:
            if same_offset["operation_body_hash"] == event["operation_body_hash"]:
                self._evidence(
                    conn, "duplicate_at_offset",
                    epoch=event["epoch"], event_id=event["event_id"],
                    offset=event["offset"],
                    detail={"existing_event_id": same_offset["event_id"]},
                )
                return "duplicate_offset"
            self._evidence(
                conn, "offset_conflict",
                epoch=event["epoch"], event_id=event["event_id"],
                offset=event["offset"],
                detail={"existing_event_id": same_offset["event_id"]},
            )
            return "offset_conflict"

        if current is None or event["epoch"] == current:
            progress = self._progress(conn, event["epoch"])
            if progress is not None and event["offset"] <= progress["contiguous_offset"]:
                status = "behind_cursor"
                self._evidence(
                    conn, "event_behind_contiguous_cursor",
                    epoch=event["epoch"], event_id=event["event_id"],
                    offset=event["offset"],
                    detail={"contiguous_offset": progress["contiguous_offset"]},
                )
            else:
                status = "pending"
        else:
            state = conn.execute(
                "SELECT state FROM epochs WHERE epoch=?", (event["epoch"],)
            ).fetchone()
            if state is not None and state["state"] == "retired":
                status = "retired_stale"
            else:
                status = "held"

        conn.execute(
            "INSERT INTO inbox"
            "(epoch, event_id, offset, record_id, revision, operation, body_json,"
            " operation_body_hash, received_at, status)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                event["epoch"],
                event["event_id"],
                event["offset"],
                event["record_id"],
                event["revision"],
                event["operation"],
                event["body_json"],
                event["operation_body_hash"],
                _now(),
                status,
            ),
        )
        self._note_offset(conn, event["epoch"], event["offset"])

        if status == "retired_stale":
            # 旧 epoch 的迟到推送只留证，不能影响新 epoch 的任何资料。
            self._append_log(
                conn, "late_epoch",
                epoch=event["epoch"], event_id=event["event_id"],
                offset=event["offset"], record_id=event["record_id"],
                revision=event["revision"], operation=event["operation"],
                body_json=event["body_json"], applied=0,
                detail={"reason": "retired_epoch_push"},
            )
        elif status == "held":
            self._evidence(
                conn, "held_for_other_epoch",
                epoch=event["epoch"], event_id=event["event_id"],
                offset=event["offset"], detail={"current_epoch": current},
            )
        return status

    # ------------------------------------------------------------ 应用与排空
    def _write_record(self, conn, event, logged):
        operation = event["operation"]
        if logged:
            seq = self._append_log(
                conn, "change", epoch=event["epoch"], event_id=event["event_id"],
                offset=event["offset"], record_id=event["record_id"],
                revision=event["revision"], operation=operation,
                body_json=event["body_json"], applied=1,
            )
        else:
            seq = None
        conn.execute(
            "INSERT INTO records"
            "(record_id, epoch, revision, operation, body_json, operation_body_hash,"
            " event_id, offset, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(record_id) DO UPDATE SET"
            " epoch=excluded.epoch, revision=excluded.revision,"
            " operation=excluded.operation, body_json=excluded.body_json,"
            " operation_body_hash=excluded.operation_body_hash,"
            " event_id=excluded.event_id, offset=excluded.offset,"
            " updated_at=excluded.updated_at",
            (
                event["record_id"],
                event["epoch"],
                event["revision"],
                operation,
                None if operation == "delete" else event["body_json"],
                event["operation_body_hash"],
                event["event_id"],
                event["offset"],
                _now(),
            ),
        )
        return seq

    def _apply_event(self, conn, row, epoch):
        existing = conn.execute(
            "SELECT * FROM records WHERE record_id=?", (row["record_id"],)
        ).fetchone()
        incoming_hash = row["operation_body_hash"]

        if existing is not None and row["revision"] < existing["revision"]:
            detail = {
                "reason": "lower_revision",
                "existing_revision": existing["revision"],
                "existing_operation": existing["operation"],
                "incoming_revision": row["revision"],
            }
            seq = self._append_log(
                conn, "ignored", epoch=epoch, event_id=row["event_id"],
                offset=row["offset"], record_id=row["record_id"],
                revision=row["revision"], operation=row["operation"],
                body_json=row["body_json"], applied=0, detail=detail,
            )
            conn.execute(
                "UPDATE inbox SET status='ignored', detail_json=? "
                "WHERE epoch=? AND event_id=?",
                (_json_dumps(detail), epoch, row["event_id"]),
            )
            return {"outcome": "ignored", "log_seq": seq}

        if existing is not None and row["revision"] == existing["revision"]:
            if incoming_hash == existing["operation_body_hash"]:
                conn.execute(
                    "UPDATE inbox SET status='duplicate_content', duplicate_count=duplicate_count+1 "
                    "WHERE epoch=? AND event_id=?",
                    (epoch, row["event_id"]),
                )
                return {"outcome": "duplicate_content", "log_seq": None}
            detail = {
                "reason": "same_revision_different_content",
                "existing_operation": existing["operation"],
                "existing_body": None
                if existing["body_json"] is None
                else json.loads(existing["body_json"]),
                "incoming_operation": row["operation"],
                "incoming_body": None
                if row["body_json"] is None
                else json.loads(row["body_json"]),
                "existing_hash": existing["operation_body_hash"],
                "incoming_hash": incoming_hash,
            }
            conn.execute(
                "INSERT INTO conflicts"
                "(epoch, event_id, offset, record_id, revision, incoming_hash,"
                " existing_hash, incoming_body_json, existing_body_json, detected_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    epoch,
                    row["event_id"],
                    row["offset"],
                    row["record_id"],
                    row["revision"],
                    incoming_hash,
                    existing["operation_body_hash"],
                    row["body_json"],
                    existing["body_json"],
                    _now(),
                ),
            )
            seq = self._append_log(
                conn, "conflict", epoch=epoch, event_id=row["event_id"],
                offset=row["offset"], record_id=row["record_id"],
                revision=row["revision"], operation=row["operation"],
                body_json=row["body_json"], applied=0, detail=detail,
            )
            self._evidence(
                conn, "revision_conflict", epoch=epoch,
                event_id=row["event_id"], offset=row["offset"], detail=detail,
            )
            conn.execute(
                "UPDATE inbox SET status='conflict', detail_json=? "
                "WHERE epoch=? AND event_id=?",
                (_json_dumps(detail), epoch, row["event_id"]),
            )
            return {"outcome": "conflict", "log_seq": seq}

        # 没有旧资料，或收到严格更高的 revision：正常应用。
        event = {
            "epoch": epoch,
            "event_id": row["event_id"],
            "offset": row["offset"],
            "record_id": row["record_id"],
            "revision": row["revision"],
            "operation": row["operation"],
            "body_json": row["body_json"],
            "operation_body_hash": incoming_hash,
        }
        seq = self._write_record(conn, event, logged=not bool(row["logged"]))
        if not row["logged"]:
            conn.execute(
                "UPDATE inbox SET logged=1 WHERE epoch=? AND event_id=?",
                (epoch, row["event_id"]),
            )
        conn.execute(
            "UPDATE inbox SET status='applied', detail_json=NULL "
            "WHERE epoch=? AND event_id=?",
            (epoch, row["event_id"]),
        )
        return {"outcome": "applied", "log_seq": seq}

    def _drain_contiguous(self, conn, epoch):
        """从连续游标之后按 offset 顺序应用到当前资料。"""
        progress = self._progress(conn, epoch)
        if progress is None:
            return []
        next_offset = progress["contiguous_offset"] + 1
        processed = []
        while True:
            row = conn.execute(
                "SELECT * FROM inbox WHERE epoch=? AND offset=?", (epoch, next_offset)
            ).fetchone()
            if row is None:
                break
            result = self._apply_event(conn, row, epoch)
            conn.execute(
                "UPDATE epoch_progress SET contiguous_offset=?, updated_at=? WHERE epoch=?",
                (next_offset, _now(), epoch),
            )
            processed.append(
                {
                    "epoch": epoch,
                    "event_id": row["event_id"],
                    "offset": next_offset,
                    "record_id": row["record_id"],
                    "revision": row["revision"],
                    "operation": row["operation"],
                    "outcome": result["outcome"],
                    "log_seq": result["log_seq"],
                }
            )
            next_offset += 1
        return processed

    # ---------------------------------------------------------------- 推送
    def receive_push(self, payload):
        """持久化一批推送后返回确认；应用在持久化提交之后进行。

        返回值中的 ``ack.contiguous_offset`` 是连续确认进度，``max_seen_offset``
        可能更大——存在缺口时补采必须从连续进度开始。
        """
        events = [_normalize_event(raw) for raw in _extract_events(payload)]
        with self._lock:
            current = None
            counts = {}
            with self._tx() as conn:
                current = self._current_epoch(conn)
                if current is None and events:
                    current = self._adopt_epoch(conn, events[0]["epoch"])
                for event in events:
                    label = self._persist_event(conn, event, current)
                    counts[label] = counts.get(label, 0) + 1
            # 收件箱已经提交，以下失败也不会丢掉已确认接收的事件。
            processing_error = None
            processed = []
            try:
                with self._tx() as conn:
                    current = self._current_epoch(conn)
                    if current is not None:
                        processed = self._drain_contiguous(conn, current)
            except Exception as exc:  # 保留已持久化事件，供重启后继续处理
                processing_error = repr(exc)

            progress = self._progress_snapshot(current)
            result = {
                "received": len(events),
                "counts": counts,
                "processed": processed,
                "ack": {
                    "epoch": current,
                    "contiguous_offset": progress["contiguous_offset"],
                    "max_seen_offset": progress["max_seen_offset"],
                },
            }
            if processing_error:
                result["processing_error"] = processing_error
            return result

    # ---------------------------------------------------------------- 补采
    def backfill(self, after=None):
        """主动从连续确认 offset 开始补采。

        410 时自动进入快照恢复；快照或 barrier 事件获取失败时，当前可读视图
        保持不变，返回 ``status='snapshot_failed'``，可再次调用重试。
        """
        with self._lock:
            current = self._current_epoch_ro()
            if after is None:
                progress = self._progress_snapshot(current)
                after = progress["contiguous_offset"]
            if type(after) is not int or after < 0:
                raise BridgeError("after 必须是非负整数")
            query = {"after": after}
            if current is not None:
                query["epoch"] = current
            try:
                status, _extra, payload = self._request(
                    self.source, "GET", "/events", query
                )
            except Exception as exc:
                return {
                    "status": "source_error",
                    "error": repr(exc),
                    "epoch": current,
                    "after": after,
                }

            if status == 410:
                new_epoch = payload.get("epoch") if isinstance(payload, dict) else None
                if not isinstance(new_epoch, str) or not new_epoch:
                    return {
                        "status": "cursor_expired",
                        "error": "来源未返回新 epoch",
                        "epoch": current,
                        "after": after,
                    }
                return self._recover_snapshot(new_epoch)

            if status != 200 or not isinstance(payload, dict):
                return {
                    "status": "source_error",
                    "http_status": status,
                    "payload": payload,
                    "epoch": current,
                    "after": after,
                }

            response_epoch = payload.get("epoch")
            if not isinstance(response_epoch, str) or not response_epoch:
                return {
                    "status": "source_error",
                    "error": "补采响应缺少 epoch",
                    "http_status": status,
                }
            if current is not None and response_epoch != current:
                # 来源换了 epoch 却没有返回 410：同样只能走完整快照。
                return self._recover_snapshot(response_epoch)

            try:
                events = [_normalize_event(raw) for raw in payload.get("events", [])]
            except BridgeError as exc:
                return {"status": "invalid_event", "error": str(exc), "epoch": response_epoch}

            with self._tx() as conn:
                if self._current_epoch(conn) is None:
                    self._adopt_epoch(conn, response_epoch)
                current = self._current_epoch(conn)
                counts = {}
                for event in events:
                    label = self._persist_event(conn, event, current)
                    counts[label] = counts.get(label, 0) + 1
            with self._tx() as conn:
                current = self._current_epoch(conn)
                processed = self._drain_contiguous(conn, current) if current else []
            progress = self._progress_snapshot(current)
            return {
                "status": "ok",
                "epoch": current,
                "after": after,
                "received": len(events),
                "counts": counts,
                "processed": processed,
                "ack": {
                    "epoch": current,
                    "contiguous_offset": progress["contiguous_offset"],
                    "max_seen_offset": progress["max_seen_offset"],
                },
            }

    def poll(self, after=None):
        """``backfill`` 的同义入口。"""
        return self.backfill(after=after)

    # ------------------------------------------------------------ 快照恢复
    def _fetch_snapshot_page(self, run_id, epoch, page, expected_barrier):
        try:
            status, _extra, payload = self._request(
                self.source, "GET", "/snapshot", {"epoch": epoch, "page": page}
            )
        except Exception as exc:
            raise SnapshotError(repr(exc), page=page) from exc
        if status != 200 or not isinstance(payload, dict):
            error = payload.get("error") if isinstance(payload, dict) else None
            raise SnapshotError(
                "快照第 {} 页获取失败: {}".format(page, error or status), page=page
            )
        if payload.get("epoch") != epoch:
            raise SnapshotError("快照 epoch 与请求不一致", page=page)
        barrier = payload.get("barrier")
        if type(barrier) is not int or barrier < 0:
            raise SnapshotError("快照缺少有效 barrier", page=page)
        if expected_barrier is not None and barrier != expected_barrier:
            raise SnapshotError("快照分页的 barrier 不一致", page=page)
        records = payload.get("records", [])
        if not isinstance(records, list):
            raise SnapshotError("快照 records 不是列表", page=page)
        next_page = payload.get("next")
        if next_page is not None and (type(next_page) is not int or next_page <= page):
            raise SnapshotError("快照 next 页号无效", page=page)
        return barrier, records, next_page, payload

    def _stage_snapshot_page(self, run_id, page, barrier, records, payload):
        seen = []
        for record in records:
            if not isinstance(record, dict):
                raise SnapshotError("快照记录必须是 JSON 对象", page=page)
            record_id = record.get("record_id")
            revision = record.get("revision")
            if not isinstance(record_id, str) or not record_id:
                raise SnapshotError("快照记录缺少 record_id", page=page)
            if type(revision) is not int or revision < 0:
                raise SnapshotError("快照记录 revision 无效", page=page)
            body = record.get("body")
            try:
                body_json = None if body is None else _json_dumps(body)
            except (TypeError, ValueError) as exc:
                raise SnapshotError("快照记录 body 不可序列化", page=page) from exc
            seen.append((record_id, revision, body_json))

        with self._tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO snapshot_pages(run_id, page, payload_json)"
                " VALUES(?,?,?)",
                (run_id, page, _json_dumps(payload)),
            )
            for record_id, revision, body_json in seen:
                existing = conn.execute(
                    "SELECT revision, body_json FROM snapshot_records"
                    " WHERE run_id=? AND record_id=?",
                    (run_id, record_id),
                ).fetchone()
                if existing is not None:
                    if existing["revision"] != revision or existing["body_json"] != body_json:
                        raise SnapshotError(
                            "快照分页对 record_id={} 给出不一致的内容".format(record_id),
                            page=page,
                        )
                    continue
                conn.execute(
                    "INSERT INTO snapshot_records(run_id, record_id, revision, body_json)"
                    " VALUES(?,?,?,?)",
                    (run_id, record_id, revision, body_json),
                )
            conn.execute(
                "UPDATE snapshot_runs SET pages=MAX(pages, ?), barrier=? WHERE run_id=?",
                (page, barrier, run_id),
            )

    def _fail_snapshot(self, run_id, error, *, page=None, stage="snapshot"):
        try:
            with self._tx() as conn:
                conn.execute(
                    "UPDATE snapshot_runs SET status='failed', error=?, finished_at=?"
                    " WHERE run_id=?",
                    (str(error), _now(), run_id),
                )
                self._append_log(
                    conn, "snapshot_failure", epoch=self._current_epoch(conn),
                    applied=0,
                    detail={
                        "run_id": run_id,
                        "stage": stage,
                        "page": page,
                        "error": str(error),
                    },
                )
        except Exception:
            # 证据写入失败不应该掩盖原始快照错误。
            pass

    def _recover_snapshot(self, new_epoch):
        """取得完整分页快照后原子替换视图，再应用 barrier 之后的事件。"""
        run_id = uuid.uuid4().hex
        current = self._current_epoch_ro()
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO snapshot_runs"
                "(run_id, epoch, expected_old_epoch, status, started_at)"
                " VALUES(?,?,?,?,?)",
                (run_id, new_epoch, current, "fetching", _now()),
            )

        barrier = None
        page = 1
        pages = 0
        try:
            while page is not None:
                if pages >= MAX_SNAPSHOT_PAGES:
                    raise SnapshotError("快照页数超过安全上限", page=page)
                barrier, records, next_page, payload = self._fetch_snapshot_page(
                    run_id, new_epoch, page, barrier
                )
                self._stage_snapshot_page(run_id, page, barrier, records, payload)
                pages += 1
                page = next_page
        except SnapshotError as exc:
            self._fail_snapshot(run_id, exc, page=exc.page, stage="snapshot_page")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": str(exc),
                "failed_page": exc.page,
                "current_epoch": current,
            }
        except Exception as exc:
            self._fail_snapshot(run_id, exc, page=page, stage="snapshot_page")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": repr(exc),
                "failed_page": page,
                "current_epoch": current,
            }

        with self._tx() as conn:
            conn.execute(
                "UPDATE snapshot_runs SET status='complete', barrier=?, pages=?,"
                " finished_at=? WHERE run_id=?",
                (barrier, pages, _now(), run_id),
            )

        # 完整快照已经落盘，现在才去取 barrier 之后的事件。
        try:
            status, _extra, payload = self._request(
                self.source, "GET", "/events",
                {"epoch": new_epoch, "after": barrier},
            )
        except Exception as exc:
            self._fail_snapshot(run_id, exc, stage="barrier_events")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": repr(exc),
                "current_epoch": current,
            }
        if status != 200 or not isinstance(payload, dict):
            error = payload.get("error") if isinstance(payload, dict) else status
            self._fail_snapshot(run_id, error, stage="barrier_events")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": str(error),
                "current_epoch": current,
            }
        if payload.get("epoch") != new_epoch:
            error = "barrier 事件响应的 epoch 不一致"
            self._fail_snapshot(run_id, error, stage="barrier_events")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": error,
                "current_epoch": current,
            }
        try:
            barrier_events = [
                _normalize_event(raw) for raw in payload.get("events", [])
            ]
        except BridgeError as exc:
            self._fail_snapshot(run_id, exc, stage="barrier_events")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": str(exc),
                "current_epoch": current,
            }

        # 把 barrier 事件先持久化到收件箱；它们属于尚未成为当前的新 epoch。
        with self._tx() as conn:
            current_now = self._current_epoch(conn)
            for event in barrier_events:
                self._persist_event(conn, event, current_now)
            conn.execute(
                "UPDATE snapshot_runs SET status='complete_unswitched' WHERE run_id=?",
                (run_id,),
            )

        try:
            with self._tx() as conn:
                current_now = self._current_epoch(conn)
                if current_now != current:
                    raise BridgeError("快照切换期间当前 epoch 已变化")
                snapshot_rows = conn.execute(
                    "SELECT record_id, revision, body_json FROM snapshot_records"
                    " WHERE run_id=?",
                    (run_id,),
                ).fetchall()
                old_record_count = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]

                # 只有此事务提交后，新快照才替换当前可读视图。
                conn.execute("DELETE FROM records")
                for row in snapshot_rows:
                    body = None if row["body_json"] is None else json.loads(row["body_json"])
                    conn.execute(
                        "INSERT INTO records"
                        "(record_id, epoch, revision, operation, body_json,"
                        " operation_body_hash, event_id, offset, updated_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            row["record_id"],
                            new_epoch,
                            row["revision"],
                            "upsert",
                            row["body_json"],
                            _body_hash("upsert", body),
                            None,
                            None,
                            _now(),
                        ),
                    )

                conn.execute(
                    "UPDATE epochs SET state='retired', updated_at=? WHERE state='current'",
                    (_now(),),
                )
                if current is not None and current != new_epoch:
                    # 旧 epoch 尚未应用完的事件随轮换退役，只保留为证据。
                    conn.execute(
                        "UPDATE inbox SET status='retired_unprocessed'"
                        " WHERE epoch=? AND status IN ('pending','held','behind_cursor')",
                        (current,),
                    )
                self._ensure_epoch(conn, new_epoch, "current")
                self._set_meta(conn, "current_epoch", new_epoch)

                max_held = conn.execute(
                    "SELECT MAX(offset) AS m FROM inbox WHERE epoch=?", (new_epoch,)
                ).fetchone()["m"]
                max_seen = max(barrier, max_held or barrier)
                conn.execute(
                    "INSERT INTO epoch_progress"
                    "(epoch, contiguous_offset, max_seen_offset, barrier, updated_at)"
                    " VALUES(?,?,?,?,?)"
                    " ON CONFLICT(epoch) DO UPDATE SET"
                    " contiguous_offset=excluded.contiguous_offset,"
                    " max_seen_offset=excluded.max_seen_offset,"
                    " barrier=excluded.barrier, updated_at=excluded.updated_at",
                    (new_epoch, barrier, max_seen, barrier, _now()),
                )

                # barrier 覆盖的旧事件只作为快照证据保留；barrier 之后的事件
                # 重新排队，由新快照作为基线按 offset 顺序重放。
                conn.execute(
                    "UPDATE inbox SET status='covered_by_snapshot'"
                    " WHERE epoch=? AND offset<=? AND status NOT IN ('applied','ignored','conflict')",
                    (new_epoch, barrier),
                )
                conn.execute(
                    "UPDATE inbox SET status='pending' WHERE epoch=? AND offset>?",
                    (new_epoch, barrier),
                )

                self._append_log(
                    conn, "snapshot_switch", epoch=new_epoch, applied=0,
                    detail={
                        "run_id": run_id,
                        "old_epoch": current,
                        "new_epoch": new_epoch,
                        "barrier": barrier,
                        "snapshot_records": len(snapshot_rows),
                        "previous_records": old_record_count,
                    },
                )
                conn.execute(
                    "UPDATE snapshot_runs SET status='switched', switched_at=?"
                    " WHERE run_id=?",
                    (_now(), run_id),
                )
                processed = self._drain_contiguous(conn, new_epoch)
        except Exception as exc:
            self._fail_snapshot(run_id, exc, stage="switch")
            return {
                "status": "snapshot_failed",
                "epoch": current,
                "new_epoch": new_epoch,
                "run_id": run_id,
                "error": repr(exc),
                "current_epoch": self._current_epoch_ro(),
            }

        progress = self._progress_snapshot(new_epoch)
        return {
            "status": "switched",
            "epoch": new_epoch,
            "old_epoch": current,
            "barrier": barrier,
            "run_id": run_id,
            "snapshot_records": len(snapshot_rows),
            "processed": processed,
            "ack": {
                "epoch": new_epoch,
                "contiguous_offset": progress["contiguous_offset"],
                "max_seen_offset": progress["max_seen_offset"],
            },
        }

    # ---------------------------------------------------------------- 读取
    def _current_epoch_ro(self):
        with self._lock:
            return self._get_meta(self.conn, "current_epoch")

    def _progress_snapshot(self, epoch):
        if epoch is None:
            return {"contiguous_offset": 0, "max_seen_offset": 0}
        row = self._progress(self.conn, epoch)
        if row is None:
            return {"contiguous_offset": 0, "max_seen_offset": 0}
        return {
            "contiguous_offset": row["contiguous_offset"],
            "max_seen_offset": row["max_seen_offset"],
        }

    def contiguous_offset(self, epoch=None):
        """连续确认进度；补采应从这里开始。"""
        with self._lock:
            epoch = epoch or self._get_meta(self.conn, "current_epoch")
            return self._progress_snapshot(epoch)["contiguous_offset"]

    def max_seen_offset(self, epoch=None):
        """已持久化事件中的最大 offset，可能因缺口大于连续进度。"""
        with self._lock:
            epoch = epoch or self._get_meta(self.conn, "current_epoch")
            return self._progress_snapshot(epoch)["max_seen_offset"]

    def read_records(self, include_deleted=False):
        """读取当前 epoch 的资料视图。

        默认只返回仍然存在的资料；``include_deleted=True`` 时也会返回带
        revision 的墓碑，便于证据核对。
        """
        with self._lock:
            if include_deleted:
                rows = self.conn.execute(
                    "SELECT * FROM records ORDER BY record_id"
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM records WHERE operation='upsert' ORDER BY record_id"
                ).fetchall()
            result = []
            for row in rows:
                result.append(
                    {
                        "record_id": row["record_id"],
                        "epoch": row["epoch"],
                        "revision": row["revision"],
                        "operation": row["operation"],
                        "body": None
                        if row["body_json"] is None
                        else json.loads(row["body_json"]),
                    }
                )
            return result

    def get_record(self, record_id, include_deleted=True):
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM records WHERE record_id=?", (record_id,)
            ).fetchone()
            if row is None:
                return None
            if row["operation"] == "delete" and not include_deleted:
                return None
            return {
                "record_id": row["record_id"],
                "epoch": row["epoch"],
                "revision": row["revision"],
                "operation": row["operation"],
                "body": None if row["body_json"] is None else json.loads(row["body_json"]),
            }

    def read_log(self, cursor=0, limit=50):
        """按本地游标读取下游变更日志。

        日志只追加，``seq`` 单调递增；用同一个 ``cursor`` 重放一页得到相同内容。
        ``kind`` 为 ``change`` 的条目才是已应用的业务变更；``conflict``、
        ``ignored``、``snapshot_switch``、``snapshot_failure``、``late_epoch``
        是证据，``applied`` 为 0。
        """
        if type(cursor) is not int or cursor < 0:
            raise BridgeError("cursor 必须是非负整数")
        if type(limit) is not int or limit <= 0:
            raise BridgeError("limit 必须是正整数")
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM downstream_log WHERE seq>? ORDER BY seq LIMIT ?",
                (cursor, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            rows = rows[:limit]
            entries = []
            for row in rows:
                entries.append(
                    {
                        "seq": row["seq"],
                        "kind": row["kind"],
                        "epoch": row["epoch"],
                        "event_id": row["event_id"],
                        "offset": row["offset"],
                        "record_id": row["record_id"],
                        "revision": row["revision"],
                        "operation": row["operation"],
                        "body": None
                        if row["body_json"] is None
                        else json.loads(row["body_json"]),
                        "applied": bool(row["applied"]),
                        "detail": None
                        if row["detail_json"] is None
                        else json.loads(row["detail_json"]),
                        "created_at": row["created_at"],
                    }
                )
            next_cursor = entries[-1]["seq"] if entries else cursor
            return {
                "entries": entries,
                "next_cursor": next_cursor,
                "has_more": has_more,
            }

    def status(self):
        """当前视图、连续游标、收件箱和冲突的汇总。"""
        with self._lock:
            current = self._get_meta(self.conn, "current_epoch")
            progress = self._progress_snapshot(current)
            statuses = {
                row["status"]: row["n"]
                for row in self.conn.execute(
                    "SELECT status, COUNT(*) AS n FROM inbox GROUP BY status"
                ).fetchall()
            }
            records = self.conn.execute(
                "SELECT operation, COUNT(*) AS n FROM records GROUP BY operation"
            ).fetchall()
            record_count = {row["operation"]: row["n"] for row in records}
            conflicts = self.conn.execute(
                "SELECT COUNT(*) AS n FROM conflicts"
            ).fetchone()["n"]
            cursor = self.conn.execute(
                "SELECT MAX(seq) AS m FROM downstream_log"
            ).fetchone()["m"] or 0
            last_run = self.conn.execute(
                "SELECT * FROM snapshot_runs ORDER BY started_at DESC, run_id DESC LIMIT 1"
            ).fetchone()
            epochs = [
                {"epoch": row["epoch"], "state": row["state"]}
                for row in self.conn.execute(
                    "SELECT epoch, state FROM epochs ORDER BY created_at, epoch"
                ).fetchall()
            ]
            return {
                "current_epoch": current,
                "contiguous_offset": progress["contiguous_offset"],
                "max_seen_offset": progress["max_seen_offset"],
                "inbox": statuses,
                "records": {
                    "live": record_count.get("upsert", 0),
                    "tombstones": record_count.get("delete", 0),
                },
                "conflicts": conflicts,
                "log_cursor": cursor,
                "epochs": epochs,
                "last_snapshot_run": None
                if last_run is None
                else {
                    "run_id": last_run["run_id"],
                    "epoch": last_run["epoch"],
                    "barrier": last_run["barrier"],
                    "status": last_run["status"],
                    "pages": last_run["pages"],
                    "error": last_run["error"],
                },
            }
