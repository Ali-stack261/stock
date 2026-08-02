import unittest

from pyspark.sql import SparkSession
from streaming.spark_stream import build_spark_session, create_streaming_dataframe, get_market_event_schema


class Phase3Tests(unittest.TestCase):
    def test_market_event_schema_has_expected_fields(self):
        schema = get_market_event_schema()
        self.assertEqual([field.name for field in schema.fields], ["symbol", "price", "volume", "timestamp", "source", "idempotency_key", "received_at"])

    def test_build_spark_session(self):
        spark = build_spark_session(app_name="test_phase3")
        self.assertEqual(spark.sparkContext.appName, "test_phase3")
        spark.stop()


if __name__ == "__main__":
    unittest.main()
