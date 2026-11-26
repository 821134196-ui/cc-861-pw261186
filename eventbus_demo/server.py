"""HTTP server for the demo UI and JSON control API."""

from __future__ import annotations

import argparse
import json
import threading
import webbrowser
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .engine import DemoActionError, DemoEngine
from .idempotency import IdempotentProcessor, ProcessorClient, start_processor_server

STATIC_DIR = Path(__file__).resolve().parent / "static"


def build_handler(engine: DemoEngine) -> type[BaseHTTPRequestHandler]:
    class DemoHandler(BaseHTTPRequestHandler):
        server_version = "EventBusDemo/1.0"

        def log_message(self, format: str, *args: Any) -> None:
            # Compact server log; detailed demo events are available in the UI.
            print(f"[web] {self.address_string()} - {format % args}")

        def _send_json(self, status: int, data: dict[str, Any]) -> None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length == 0:
                return {}
            raw = self.rfile.read(length).decode("utf-8")
            data = json.loads(raw or "{}")
            if not isinstance(data, dict):
                raise ValueError("JSON body must be an object")
            return data

        def _serve_static(self, relative_path: str) -> None:
            if relative_path in {"", "/"}:
                file_path = STATIC_DIR / "index.html"
            else:
                static_root = STATIC_DIR.resolve()
                file_path = (static_root / relative_path.lstrip("/")).resolve()
                try:
                    file_path.relative_to(static_root)
                except ValueError:
                    self.send_error(403)
                    return

            if not file_path.is_file():
                self.send_error(404)
                return

            content_types = {
                ".html": "text/html; charset=utf-8",
                ".css": "text/css; charset=utf-8",
                ".js": "application/javascript; charset=utf-8",
                ".svg": "image/svg+xml",
                ".json": "application/json; charset=utf-8",
            }
            body = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_types.get(file_path.suffix, "application/octet-stream"))
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/api/state":
                    self._send_json(200, engine.snapshot())
                    return
                if path == "/api/health":
                    self._send_json(200, {"ok": True})
                    return
                if path.startswith("/api/"):
                    self._send_json(404, {"ok": False, "error": "unknown API route"})
                    return
                self._serve_static(path)
            except Exception as exc:  # noqa: BLE001 - boundary returns JSON
                self._send_json(500, {"ok": False, "error": repr(exc)})

        def do_POST(self) -> None:  # noqa: N802, C901
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                body = self._read_json()
                if path == "/api/reset":
                    state = engine.reset()
                    self._send_json(200, {"ok": True, "state": state})
                    return

                if path == "/api/consumers/consumer-1/stop":
                    result = engine.stop_member("consumer-1")
                    self._send_json(200, {"ok": True, "result": result, "state": engine.snapshot()})
                    return

                if path == "/api/rebalance/next":
                    result = engine.rebalance_next()
                    self._send_json(200, {"ok": True, "result": result, "state": engine.snapshot()})
                    return

                if path == "/api/events":
                    result = engine.produce_event(body.get("partition"))
                    self._send_json(200, {"ok": True, "result": result, "state": engine.snapshot()})
                    return

                if path == "/api/deliveries/process-next":
                    partition = int(body.get("partition", 0))
                    result = engine.process_next(
                        partition,
                        lose_ack=bool(body.get("loseAck", False)),
                        commit=bool(body.get("commit", True)),
                    )
                    status = 200 if result.get("ok") else 409
                    self._send_json(status, {"ok": bool(result.get("ok")), "result": result, "state": engine.snapshot()})
                    return

                if path == "/api/deliveries/retry":
                    key = str(body.get("key", ""))
                    result = engine.retry_delivery(key, commit=bool(body.get("commit", True)))
                    self._send_json(200, {"ok": True, "result": result, "state": engine.snapshot()})
                    return

                self._send_json(404, {"ok": False, "error": "unknown API route"})
            except DemoActionError as exc:
                self._send_json(
                    exc.status,
                    {"ok": False, "error": exc.message, "code": exc.code, "state": engine.snapshot()},
                )
            except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
                self._send_json(400, {"ok": False, "error": f"bad request: {exc}"})
            except Exception as exc:  # noqa: BLE001
                self._send_json(500, {"ok": False, "error": repr(exc)})

    return DemoHandler


def create_servers(
    host: str = "127.0.0.1",
    web_port: int = 8080,
    processor_port: int = 8091,
    processor_host: str = "127.0.0.1",
) -> tuple[ThreadingHTTPServer, ThreadingHTTPServer, DemoEngine]:
    processor = IdempotentProcessor()
    processor_server, _ = start_processor_server(processor, host=processor_host, port=processor_port)
    actual_processor_port = processor_server.server_address[1]
    client = ProcessorClient(f"http://{processor_host}:{actual_processor_port}")
    engine = DemoEngine(client, processor=processor)
    handler_class = build_handler(engine)
    web_server = ThreadingHTTPServer((host, web_port), handler_class)
    web_server.daemon_threads = True
    return web_server, processor_server, engine


def main() -> None:
    parser = argparse.ArgumentParser(description="Local event bus + consumer coordinator demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080, help="Web UI/API port (default 8080)")
    parser.add_argument(
        "--processor-port",
        type=int,
        default=8091,
        help="Local idempotent processing service port (default 8091; 0 picks an ephemeral port)",
    )
    parser.add_argument("--no-browser", action="store_true", help="Do not open a browser")
    args = parser.parse_args()

    try:
        web_server, processor_server, _ = create_servers(
            host=args.host,
            web_port=args.port,
            processor_port=args.processor_port,
        )
    except OSError as exc:
        raise SystemExit(
            f"启动失败：{exc}\n可使用 --port 和 --processor-port 指定其他端口。"
        ) from exc

    web_port = web_server.server_address[1]
    processor_port = processor_server.server_address[1]
    processor_url = f"http://127.0.0.1:{processor_port}/health"
    url = f"http://{args.host}:{web_port}/"
    print("本地事件总线与消费者协调器演示已启动")
    print(f"  控制台页面: {url}")
    print(f"  幂等处理服务: {processor_url}")
    print("按 Ctrl+C 停止。")

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        web_server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在停止...")
    finally:
        web_server.shutdown()
        web_server.server_close()
        processor_server.shutdown()
        processor_server.server_close()


if __name__ == "__main__":
    main()
