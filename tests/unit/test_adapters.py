import unittest

from producer.adapters import (
    adapt_payload,
    binance_to_market_event,
    finnhub_to_market_event,
)


class AdapterTests(unittest.TestCase):
    def test_binance_payload_is_adapted(self):
        raw = {"e": "trade", "s": "BTCUSDT", "p": "118420.52", "q": "0.42", "T": 1690886434000}
        adapted = binance_to_market_event(raw)
        self.assertEqual(adapted["symbol"], "BTCUSDT")
        self.assertEqual(adapted["price"], 118420.52)
        self.assertEqual(adapted["volume"], 0.42)
        self.assertIn("T", adapted["timestamp"])

    def test_finnhub_payload_is_adapted(self):
        raw = {"type": "trade", "data": [{"s": "BINANCE:BTCUSDT", "p": 118420.52, "v": 0.42, "t": 1690886434000}]}
        adapted = finnhub_to_market_event(raw)
        self.assertEqual(adapted["symbol"], "BTCUSDT")
        self.assertEqual(adapted["price"], 118420.52)
        self.assertEqual(adapted["volume"], 0.42)
        self.assertIn("T", adapted["timestamp"])

    def test_adapt_payload_uses_source_specific_adapter(self):
        raw = {"e": "trade", "s": "ETHUSDT", "p": "3200.75", "q": "12.5", "T": 1690886434000}
        adapted = adapt_payload(raw, "binance")
        self.assertEqual(adapted["symbol"], "ETHUSDT")


if __name__ == "__main__":
    unittest.main()
