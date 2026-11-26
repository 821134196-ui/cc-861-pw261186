"""Local HTTP processing service with idempotency-key deduplication.

The service intentionally knows nothing about Kafka/rebalancing. It exposes a
small ``POST /process`` API. If the caller asks for simulated acknowledgement
loss, the operation is durably recorded first and the HTTP response is dropped
by closing the socket without sending one.
"""

from __future__ import annotations

import hashlib
import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest


class IdempotencyConflict(Exception):
    """Same key reused with a different request payload."""


class AckLostError(Exception):
    """The simulated response/ack was dropped after service-side commit."""


class ProcessorUnavailableError(Exception):
    pass


def _canonical_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


class IdempotentProcessor:
    """In-memory ledger standing in for a local side-effecting service."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.records: dict[str, dict[str, Any]] = {}
        self.service_log: list[dict[str, Any]] = []
        self.side_effects: list[dict[str, Any]] = []
        self.execution_count = 0
        self.started_wall_time = time.time()

    def _log(self, level: str, event: str, key: str | None = None, **detail: Any) -> None:
        entry = {
            "atWall": round((time.time() - self.started_wall_time) * 1000),
            "level": level,
            "event": event,
            "key": key,
            **detail,
        }
        self.service_log.append(entry)
        self.service_log[:] = self.service_log[-100:]

    def process(
        self,
        key: str,
        payload: dict[str, Any],
        *,
        virtual_clock: int = 0,
        lose_ack: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        if not key or not isinstance(key, str):
            raise ValueError("idempotency key is required")
        request_hash = _canonical_hash(payload)

        with self.lock:
            existing = self.records.get(key)
            if existing is not None:
                if existing["requestHash"] != request_hash:
                    self._log(
                        "error",
                        "idempotency_conflict",
                        key,
                        requestHash=request_hash,
                        storedHash=existing["requestHash"],
                    )
                    raise IdempotencyConflict(
                        f"幂等键 {key} 已用于不同请求体；拒绝执行。"
                    )

                existing["duplicateHits"] += 1
                attempt_no = len(existing["attempts"]) + 1
                existing["attempts"].append(
                    {
                        "attempt": attempt_no,
                        "at": virtual_clock,
                        "outcome": "deduplicated",
                        "httpResponse": "cached_result",
                    }
                )
                existing["lastSeenAt"] = virtual_clock
                self._log(
                    "info",
                    "duplicate_delivery_deduplicated",
                    key,
                    duplicateHits=existing["duplicateHits"],
                    attempt=attempt_no,
                )
                return dict(existing["result"]), True

            # The side effect happens exactly once per idempotency key.
            self.execution_count += 1
            side_effect = {
                "sequence": self.execution_count,
                "key": key,
                "orderId": payload.get("orderId"),
                "action": "charge_and_fulfill",
                "amount": payload.get("amount"),
                "at": virtual_clock,
            }
            self.side_effects.append(side_effect)
            result = {
                "processed": True,
                "deduped": False,
                "orderId": payload.get("orderId"),
                "chargedAmount": payload.get("amount"),
                "sideEffectSequence": self.execution_count,
                "message": "外部服务已执行扣款/履约。",
            }
            record = {
                "idempotencyKey": key,
                "requestHash": request_hash,
                "status": "EXECUTED",
                "firstSeenAt": virtual_clock,
                "lastSeenAt": virtual_clock,
                "executionCount": 1,
                "duplicateHits": 0,
                "loseAckRequested": lose_ack,
                "result": result,
                "sideEffect": side_effect,
                "attempts": [
                    {
                        "attempt": 1,
                        "at": virtual_clock,
                        "outcome": "executed",
                        "httpResponse": "will_be_dropped" if lose_ack else "200_ok",
                    }
                ],
            }
            self.records[key] = record
            self._log(
                "success",
                "side_effect_executed_once",
                key,
                sequence=self.execution_count,
                loseAck=lose_ack,
            )
            return dict(result), False

    def mark_ack_dropped(self, key: str) -> None:
        with self.lock:
            record = self.records[key]
            record["attempts"][-1]["httpResponse"] = "dropped_before_headers"
            record["ackState"] = "executed_but_response_dropped"
            self._log("warning", "response_ack_dropped_by_simulator", key)

    def reset(self) -> None:
        with self.lock:
            self.records.clear()
            self.service_log.clear()
            self.side_effects.clear()
            self.execution_count = 0

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "name": "idempotent-processing-service",
                "ledgerSize": len(self.records),
                "executionCount": self.execution_count,
                "duplicateHitCount": sum(r["duplicateHits"] for r in self.records.values()),
                "sideEffects": list(reversed(self.side_effects[-20:])),
                "records": [
                    dict(record)
                    for record in sorted(
                        self.records.values(), key=lambda r: r["firstSeenAt"], reverse=True
                    )
                ],
                "serviceLog": list(reversed(self.service_log[-30:])),
            }


class ProcessorClient:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def process(
        self,
        *,
        key: str,
        event_id: str,
        order_id: str,
        amount: int,
        partition: int,
        offset: int,
        virtual_clock: int,
        lose_ack: bool = False,
    ) -> dict[str, Any]:
        body = json.dumps(
            {
                "idempotencyKey": key,
                "eventId": event_id,
                "partition": partition,
                "offset": offset,
                "virtualClock": virtual_clock,
                "loseAck": lose_ack,
                "payload": {"orderId": order_id, "amount": amount},
            }
        ).encode("utf-8")
        req = urlrequest.Request(
            f"{self.base_url}/process",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urlerror.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = {"error": raw}
            if exc.code == 409:
                raise IdempotencyConflict(data.get("error", "idempotency conflict")) from exc
            raise ProcessorUnavailableError(f"HTTP {exc.code}: {raw}") from exc
        except (urlerror.URLError, TimeoutError, OSError) as exc:
            # The simulator closes the socket after committing the operation.
            # Only the request that asked for a dropped ack interprets a
            # transport failure as the known ambiguous outcome.
            if lose_ack:
                raise AckLostError(
                    "外部服务已执行，但响应/确认在返回途中丢失："
                    f"{type(exc).__name__}"
                ) from exc
            raise ProcessorUnavailableError(str(exc)) from exc


def make_processor_handler(processor: IdempotentProcessor) -> type[BaseHTTPRequestHandler]:
    class ProcessorHandler(BaseHTTPRequestHandler):
        server_version = "IdempotentLocalService/1.0"

        def log_message(self, format: str, *args: Any) -> None:
            # Keep test/server output quiet; the UI reads the service ledger.
            return

        def _send_json(self, status: int, data: dict[str, Any]) -> None:
            encoded = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/health":
                self._send_json(200, {"ok": True, "service": "idempotent-processor"})
            elif path == "/records":
                self._send_json(200, processor.snapshot())
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/reset":
                processor.reset()
                self._send_json(200, {"ok": True})
                return
            if path != "/process":
                self._send_json(404, {"error": "not found"})
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
                request_data = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                key = request_data["idempotencyKey"]
                payload = request_data.get("payload", {})
                virtual_clock = int(request_data.get("virtualClock", 0))
                lose_ack = bool(request_data.get("loseAck", False))
                result, deduped = processor.process(
                    key,
                    payload,
                    virtual_clock=virtual_clock,
                    lose_ack=lose_ack,
                )

                if lose_ack:
                    # Commit is already in the ledger. Deliberately send no HTTP
                    # response and close the connection. The client sees EOF.
                    # This also works on a duplicate request when the operator
                    # explicitly asks to drop that attempt's acknowledgement.
                    processor.mark_ack_dropped(key)
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                    try:
                        self.connection.close()
                    except OSError:
                        pass
                    return

                self._send_json(
                    200,
                    {"ok": True, "deduped": deduped, "result": result, "service": processor.snapshot()},
                )
            except IdempotencyConflict as exc:
                self._send_json(409, {"ok": False, "error": str(exc)})
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                self._send_json(400, {"ok": False, "error": f"bad request: {exc}"})

    return ProcessorHandler


def start_processor_server(
    processor: IdempotentProcessor,
    host: str = "127.0.0.1",
    port: int = 8091,
) -> tuple[ThreadingHTTPServer, threading.Thread]:
    httpd = ThreadingHTTPServer((host, port), make_processor_handler(processor))
    httpd.daemon_threads = True
    actual_port = httpd.server_address[1]
    thread = threading.Thread(
        target=httpd.serve_forever,
        name=f"idempotent-processor-{actual_port}",
        daemon=True,
    )
    thread.start()
    return httpd, thread
