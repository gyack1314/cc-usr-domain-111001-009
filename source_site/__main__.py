"""本地资料来源的 HTTP 适配层。"""
import argparse
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .source import Source


def make_server(source, host="127.0.0.1", port=0):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            parsed = urlsplit(self.path)
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            headers = {k.lower(): v for k, v in self.headers.items()}
            try:
                with source.lock:
                    status, extra, payload = source.request(
                        self.command, parsed.path, parse_qs(parsed.query), headers, body)
            except (ValueError, KeyError, TypeError) as exc:
                status, extra, payload = 400, {}, {"error": str(exc)}
            if not isinstance(payload, bytes):
                payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                extra = {"Content-Type": "application/json; charset=utf-8", **extra}
            self.send_response(status)
            for key, value in extra.items():
                self.send_header(key, str(value))
            if "Content-Length" not in extra:
                self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()
            if int(extra.get("Content-Length", len(payload))) > len(payload):
                self.close_connection = True
    return ThreadingHTTPServer((host, port), Handler)


@contextmanager
def running(source=None):
    source = source or Source()
    server = make_server(source)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), source
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def main():
    parser = argparse.ArgumentParser(description="启动本地合成资料来源")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()
    server = make_server(Source(), port=args.port)
    print("本地来源已启动", server.server_address, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
