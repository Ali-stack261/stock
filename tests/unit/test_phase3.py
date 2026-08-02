import unittest

from pyspark.sql import SparkSession
from streaming.feature_engineering import compute_features
from streaming.spark_stream import build_spark_session, get_market_event_schema


class Phase3Tests(unittest.TestCase):
    def test_market_event_schema_has_expected_fields(self):
        schema = get_market_event_schema()
        self.assertEqual(
            [field.name for field in schema.fields],
            ["symbol", "price", "volume", "timestamp", "source", "idempotency_key", "received_at"],
        )

    def test_build_spark_session(self):
        spark = build_spark_session(app_name="test_phase3")
        self.assertEqual(spark.sparkContext.appName, "test_phase3")
        spark.stop()

    def test_compute_features_streaming_pipeline(self):
        spark = build_spark_session(app_name="test_phase3_features")
        rate_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

        event_df = rate_df.selectExpr(
            "CAST(value % 3 AS STRING) as symbol",
            "CAST(value * 1.0 AS DOUBLE) as price",
            "CAST(value * 10.0 AS DOUBLE) as volume",
            "timestamp",
            "CAST('binance' AS STRING) as source",
            "CAST(value AS STRING) as idempotency_key",
            "CAST(timestamp AS STRING) as received_at",
        )

        features_df = compute_features(event_df, window_duration="1 minute", watermark_duration="1 minute")
        expected_columns = [
            "window",
            "symbol",
            "first_price",
            "last_price",
            "avg_price",
            "max_price",
            "min_price",
            "volume_sum",
            "vwap",
            "price_change",
            "price_range",
        ]

        self.assertEqual([field.name for field in features_df.schema.fields], expected_columns)
        spark.stop()


if __name__ == "__main__":
    unittest.main()
