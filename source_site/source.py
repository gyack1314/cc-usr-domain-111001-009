"""合成来源行为，可直接调用或通过本地 HTTP 访问。"""
import hashlib
import json
import threading
from pathlib import Path
from urllib.parse import parse_qs

DATA = Path(__file__).resolve().parents[1] / "sources"


def json_file(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


class Source:
    def __init__(self):
        self.lock = threading.RLock()
        self.calls = []
        self.phase = "base"
        self.state = {}

    def request(self, method, path, query=None, headers=None, body=b""):
        query, headers = query or {}, headers or {}
        self.calls.append({"method": method, "path": path, "query": query})
        if path == "/__control" and method == "POST":
            control = json.loads(body or b"{}")
            self.phase = control.get("phase", self.phase)
            self.state.update(control.get("state", {}))
            return 200, {}, {"phase": self.phase}
        if path == "/__calls":
            return 200, {}, self.calls[:-1]
        return self.dispatch(method, path, query, headers, body)

    def dispatch(self, method, path, q, h, body):
        events = json_file("events.json")
        epoch = "e2" if self.phase in ("rotated", "snapshot-fail") else "e1"
        if path == "/push-sequence":
            return 200, {}, [events[2], events[0], events[2]]
        if path == "/events":
            after = int(q.get("after", ["0"])[0])
            if q.get("epoch", [epoch])[0] != epoch or self.phase in ("rotated", "snapshot-fail") and after<10:
                return 410, {}, {"error": "游标保留期已过", "epoch": epoch}
            rows = [e for e in events if e["epoch"] == epoch and e["offset"]>after]
            return 200, {}, {"epoch": epoch, "events": rows}
        if path == "/snapshot":
            page = int(q.get("page", ["1"])[0])
            if self.phase == "snapshot-fail" and page == 2:
                return 503, {}, {"error": "快照第二页暂不可用"}
            if q.get("epoch", [epoch])[0] != epoch:
                return 410, {}, {"error": "快照版本已变化"}
            rows = [{"record_id": "a", "revision": 1, "body": {"place": "新场地"}}, {"record_id": "b", "revision": 1, "body": {"place": "北门"}}]
            return 200, {}, {"epoch": epoch, "barrier": 10, "records": rows[page-1:page], "next": page+1 if page<2 else None}
        return 404, {}, {"error": "接口不存在"}
