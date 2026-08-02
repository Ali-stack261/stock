import unittest

from producer.transports import BinanceTransport, FinnhubTransport


class TransportTests(unittest.TestCase):
    def test_binance_transport_uses_expected_defaults(self):
        transport = BinanceTransport()
        self.assertEqual(transport.source, "binance")
        self.assertIn("binance", transport.url)

    def test_finnhub_transport_uses_expected_defaults(self):
        transport = FinnhubTransport()
        self.assertEqual(transport.source, "finnhub")
        self.assertIn("finnhub", transport.url)


if __name__ == "__main__":
    unittest.main()
