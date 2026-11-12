"""把来源的 request 形态适配成桥接需要的 GET 调用。

两个适配器返回 ``(status, headers, payload)``，其中 payload 已经解码成
Python 对象。来源站既可以在进程内直接调用，也可以通过本机 HTTP 访问。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


def normalize_query(query):
    """来源接口统一接收值为列表的查询参数。"""
    result = {}
    for key, value in (query or {}).items():
        if isinstance(value, (list, tuple)):
            result[key] = [str(item) for item in value]
        else:
            result[key] = [str(value)]
    return result


def _decode(payload):
    if isinstance(payload, (bytes, bytearray)):
        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("来源响应不是有效 JSON") from exc
    return payload


class DirectSourceClient:
    """直接调用 ``source_site.source.Source.request``，不经过网络。"""

    def __init__(self, source):
        self.source = source

    def request(self, method, path, query=None, headers=None, body=b""):
        status, extra, payload = self.source.request(
            method, path, normalize_query(query), headers or {}, body
        )
        return status, extra, _decode(payload)


class HttpSourceClient:
    """通过本机 HTTP 访问来源站，仅使用标准库。"""

    def __init__(self, base_url, timeout=5):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method, path, query=None, headers=None, body=b""):
        url = self.base_url + path
        normalized = normalize_query(query)
        if normalized:
            url += "?" + urllib.parse.urlencode(normalized, doseq=True)
        data = body if method != "GET" else None
        request = urllib.request.Request(
            url, data=data, method=method, headers=headers or {}
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = response.status
                extra = {k.lower(): v for k, v in response.headers.items()}
                payload = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            extra = {k.lower(): v for k, v in exc.headers.items()}
            payload = exc.read()
        return status, extra, _decode(payload)
