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


if __name__ == "__main__":
    unittest.main()
