from __future__ import annotations

import json
import threading
import unittest
from urllib import request as urlrequest

from eventbus_demo.engine import DemoEngine
from eventbus_demo.idempotency import (
    IdempotentProcessor,
    ProcessorClient,
    start_processor_server,
)
from eventbus_demo.server import create_servers


class DemoEngineTest(unittest.TestCase):
    processor_server = None
    processor_httpd = None
    processor: IdempotentProcessor
    base_url: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.processor = IdempotentProcessor()
        cls.processor_httpd, _ = start_processor_server(cls.processor, port=0)
        port = cls.processor_httpd.server_address[1]
        cls.base_url = f"http://127.0.0.1:{port}"

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.processor_httpd is not None:
            cls.processor_httpd.shutdown()
            cls.processor_httpd.server_close()

    def setUp(self) -> None:
        self.engine = DemoEngine(ProcessorClient(self.base_url), self.processor)

    def test_checkpoint_rejects_regression(self) -> None:
        self.assertEqual(self.engine.bus.checkpoints[0], 1)

        backwards = self.engine.bus.commit_checkpoint(
            0,
            0,
            member="test",
            generation=1,
            source="test",
            clock=1,
        )
        self.assertFalse(backwards["accepted"])
        self.assertEqual(backwards["reason"], "checkpoint_regression_rejected")
        self.assertEqual(self.engine.bus.checkpoints[0], 1)

        advance = self.engine.bus.commit_checkpoint(
            0, 3, member="consumer-2", generation=2, source="test", clock=2
        )
        self.assertTrue(advance["accepted"])
        self.assertEqual(self.engine.bus.checkpoints[0], 3)

        stale = self.engine.bus.commit_checkpoint(
            0, 2, member="consumer-1", generation=1, source="test", clock=3
        )
        self.assertFalse(stale["accepted"])
        self.assertEqual(stale["reason"], "checkpoint_regression_rejected")
        self.assertEqual(self.engine.bus.checkpoints[0], 3)

    def test_successful_processing_with_lost_ack_is_deduped_on_retry(self) -> None:
        self.assertEqual(self.engine.bus.checkpoints[2], -1)

        lost = self.engine.process_next(2, lose_ack=True, commit=True)
        self.assertFalse(lost["ok"])
        self.assertTrue(lost["ackLost"])
        key = "idem:orders:2:0"
        self.assertEqual(lost["key"], key)

        # The service committed its side effect before dropping the response.
        self.assertEqual(self.processor.execution_count, 1)
        self.assertIn(key, self.processor.records)
        self.assertEqual(self.engine.bus.checkpoints[2], -1)
        self.assertEqual(self.engine.deliveries[key]["status"], "ack_lost")

        retried = self.engine.retry_delivery(key, commit=True)
        self.assertTrue(retried["ok"])
        self.assertTrue(retried["deduped"])
        self.assertTrue(retried["commit"]["accepted"])
        self.assertEqual(self.engine.bus.checkpoints[2], 0)

        # Redelivery did not execute the side effect again.
        self.assertEqual(self.processor.execution_count, 1)
        self.assertEqual(self.processor.records[key]["duplicateHits"], 1)

    def test_retry_after_failover_is_performed_by_new_owner(self) -> None:
        lost = self.engine.process_next(0, lose_ack=True, commit=True)
        key = "idem:orders:0:2"
        self.assertTrue(lost["ackLost"])
        self.assertEqual(self.engine.bus.checkpoints[0], 1)

        self.engine.stop_member("consumer-1")
        for _ in range(3):  # detect, fence, assign
            self.engine.rebalance_next()

        self.assertEqual(self.engine.coordinator.owners[0], "consumer-2")
        retried = self.engine.retry_delivery(key, commit=True)
        self.assertTrue(retried["ok"])
        self.assertTrue(retried["deduped"])
        self.assertEqual(retried["commit"]["member"], "consumer-2")
        self.assertEqual(retried["commit"]["generation"], 2)
        self.assertEqual(self.engine.bus.checkpoints[0], 2)
        self.assertEqual(self.processor.execution_count, 1)

    def test_failover_overlap_and_stale_commit_cannot_regress_checkpoint(self) -> None:
        self.engine.stop_member("consumer-1")
        self.assertIsNotNone(self.engine.rebalance)

        for expected_index in range(7):
            result = self.engine.rebalance_next()
            self.assertTrue(result["ok"])
            self.assertEqual(result["stepIndex"], expected_index)

        snapshot = self.engine.snapshot()
        self.assertEqual(snapshot["coordinator"]["generation"], 2)
        self.assertEqual(snapshot["coordinator"]["owners"]["0"], "consumer-2")
        self.assertEqual(snapshot["coordinator"]["owners"]["1"], "consumer-2")

        # Initial p0 CP=1; overlap redelivery commits 2, then new owner commits 3.
        self.assertEqual(snapshot["bus"]["checkpoints"][0], 3)

        stale = snapshot["rebalance"]["staleResults"]
        self.assertFalse(stale["coordinator"]["accepted"])
        self.assertEqual(stale["coordinator"]["reason"], "fenced_stale_generation")
        self.assertFalse(stale["storage"]["accepted"])
        self.assertEqual(stale["storage"]["reason"], "checkpoint_regression_rejected")
        self.assertEqual(stale["storage"]["currentOffset"], 3)

        # Offset 2 was executed by old C1 once and deduped for new C2.
        key = "idem:orders:0:2"
        record = self.processor.records[key]
        self.assertEqual(record["executionCount"], 1)
        self.assertGreaterEqual(record["duplicateHits"], 1)
        self.assertEqual(self.processor.execution_count, 2)

        # The completed plan disallows further stepping until reset.
        with self.assertRaises(Exception):
            self.engine.rebalance_next()

    def test_reset_clears_demo_and_processor_ledger(self) -> None:
        self.engine.process_next(2, lose_ack=False, commit=True)
        self.assertEqual(self.processor.execution_count, 1)

        state = self.engine.reset()
        self.assertEqual(state["bus"]["checkpoints"][0], 1)
        self.assertEqual(state["coordinator"]["generation"], 1)
        self.assertIsNone(state["rebalance"])
        self.assertEqual(self.processor.execution_count, 0)
        self.assertEqual(state["external"]["ledgerSize"], 0)


class HttpServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.web_server, cls.processor_server, cls.engine = create_servers(
            host="127.0.0.1", web_port=0, processor_port=0
        )
        cls.port = cls.web_server.server_address[1]
        cls.thread = threading.Thread(target=cls.web_server.serve_forever, daemon=True)
        cls.thread.start()

    def setUp(self) -> None:
        # HTTP tests share one server; reset before each case for isolation.
        self.engine.reset()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.web_server.shutdown()
        cls.web_server.server_close()
        cls.processor_server.shutdown()
        cls.processor_server.server_close()
        cls.thread.join(timeout=2)

    def get(self, path: str):
        with urlrequest.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as response:
            return response.status, response.read().decode("utf-8"), response.headers.get("Content-Type", "")

    def post(self, path: str, payload=None):
        body = json.dumps(payload or {}).encode()
        request = urlrequest.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode())
        except Exception as exc:  # urllib raises HTTPError for 409
            if hasattr(exc, "code"):
                return exc.code, json.loads(exc.read().decode())
            raise

    def test_ui_state_and_static_assets(self) -> None:
        status, html, content_type = self.get("/")
        self.assertEqual(status, 200)
        self.assertIn("本地事件总线", html)
        self.assertIn("text/html", content_type)

        for asset in ("/styles.css", "/app.js"):
            status, body, content_type = self.get(asset)
            self.assertEqual(status, 200)
            self.assertTrue(body)

        status, state_text, _ = self.get("/api/state")
        self.assertEqual(status, 200)
        state = json.loads(state_text)
        self.assertEqual(state["coordinator"]["generation"], 1)

    def test_http_step_rebalance(self) -> None:
        status, data = self.post("/api/consumers/consumer-1/stop")
        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])

        for _ in range(7):
            status, data = self.post("/api/rebalance/next")
            self.assertEqual(status, 200)
            self.assertTrue(data["ok"], data)

        self.assertEqual(data["state"]["bus"]["checkpoints"][0], 3)
        self.assertTrue(data["state"]["rebalance"]["finished"])


if __name__ == "__main__":
    unittest.main()
