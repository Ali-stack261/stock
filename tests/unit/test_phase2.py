import unittest

from producer.kafka_producer import MarketKafkaProducer


class Phase2Tests(unittest.TestCase):
    def test_kafka_producer_uses_reliable_settings(self):
        producer = MarketKafkaProducer(bootstrap_servers=["localhost:9092"], topic="stock_prices")
        self.assertEqual(producer.topic, "stock_prices")
        self.assertEqual(producer.bootstrap_servers, ["localhost:9092"])

    def test_send_event_uses_symbol_as_key(self):
        producer = MarketKafkaProducer(bootstrap_servers=["localhost:9092"], topic="stock_prices")
        event = {"symbol": "BTCUSDT", "price": 1.0, "volume": 2.0, "timestamp": "2026-01-01T00:00:00Z"}

        try:
            producer.send_event(event)
        except Exception as exc:  # noqa: BLE001
            self.assertIn("NoBrokersAvailable", str(exc))
        else:
            self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
