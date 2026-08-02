import json
import unittest

from producer.normalize_event import normalize_event
from producer.websocket_client import MarketWebSocketClient


class Phase1Tests(unittest.TestCase):
    def test_normalize_event_adds_metadata(self):
        payload = {
            "symbol": "BTCUSDT",
            "price": 118420.52,
            "volume": 0.42,
            "timestamp": "2026-08-01T10:20:34Z",
        }

        event = normalize_event(payload, source="binance")

        self.assertEqual(event["symbol"], "BTCUSDT")
        self.assertEqual(event["source"], "binance")
        self.assertIn("idempotency_key", event)
        self.assertTrue(event["idempotency_key"].startswith("BTCUSDT-"))

    def test_websocket_client_normalizes_and_forwards_event(self):
        received = []

        client = MarketWebSocketClient(source="binance", handler=received.append)
        payload = {
            "symbol": "ETHUSDT",
            "price": 3200.75,
            "volume": 12.5,
            "timestamp": "2026-08-01T10:21:00Z",
        }

        event = client.process_message(payload)

        self.assertEqual(event["symbol"], "ETHUSDT")
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["symbol"], "ETHUSDT")

    def test_websocket_client_rejects_invalid_payload(self):
        client = MarketWebSocketClient(source="binance")

        with self.assertRaises(ValueError):
            client.process_message({"symbol": "BTCUSDT", "price": -1, "volume": 0.1})

    def test_client_reconnects_after_initial_connection_error(self):
        received = []

        class FakeTransport:
            def __init__(self, fail_connect=False):
                self.fail_connect = fail_connect

            def connect(self):
                if self.fail_connect:
                    raise ConnectionError("temporary outage")

            def receive(self, timeout=None):
                return json.dumps(
                    {
                        "symbol": "BTCUSDT",
                        "price": 118500.0,
                        "volume": 1.5,
                        "timestamp": "2026-08-01T10:22:00Z",
                    }
                )

            def send_ping(self):
                return None

            def close(self):
                return None

        transport_factory = []

        def factory(source):
            transport_factory.append(source)
            if len(transport_factory) == 1:
                return FakeTransport(fail_connect=True)
            return FakeTransport()

        client = MarketWebSocketClient(source="binance", handler=received.append, transport_factory=factory, sleep_func=lambda _: None)
        client.listen(max_messages=1)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["source"], "binance")

    def test_client_fails_over_to_secondary_source_on_stale_connection(self):
        received = []

        class StaleTransport:
            def __init__(self, should_fail=False):
                self.should_fail = should_fail

            def connect(self):
                if self.should_fail:
                    raise ConnectionError("stale feed")

            def receive(self, timeout=None):
                raise ConnectionError("stale feed")

            def send_ping(self):
                return None

            def close(self):
                return None

        class HealthyTransport:
            def connect(self):
                return None

            def receive(self, timeout=None):
                return json.dumps(
                    {
                        "symbol": "ETHUSDT",
                        "price": 3200.0,
                        "volume": 5.0,
                        "timestamp": "2026-08-01T10:23:00Z",
                    }
                )

            def send_ping(self):
                return None

            def close(self):
                return None

        def factory(source):
            if source == "binance":
                return StaleTransport(should_fail=True)
            return HealthyTransport()

        client = MarketWebSocketClient(
            source="binance",
            sources=["finnhub"],
            handler=received.append,
            transport_factory=factory,
            sleep_func=lambda _: None,
        )
        client.listen(max_messages=1)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["symbol"], "ETHUSDT")
        self.assertEqual(client.current_source, "finnhub")


if __name__ == "__main__":
    unittest.main()
