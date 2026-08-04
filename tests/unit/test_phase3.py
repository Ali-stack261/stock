import sys
import unittest

import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streaming.feature_engineering import compute_features
from streaming.spark_stream import build_spark_session, get_market_event_schema


class Phase3Tests(unittest.TestCase):
    # Share one SparkSession for the whole file — avoids paying JVM startup
    # cost once per test method (several seconds each).
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark_session(app_name="test_phase3")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_market_event_schema_has_expected_fields(self):
        schema = get_market_event_schema()
        self.assertEqual(
            [field.name for field in schema.fields],
            ["symbol", "price", "volume", "timestamp", "source", "idempotency_key", "received_at"],
        )

    def test_build_spark_session(self):
        self.assertEqual(self.spark.sparkContext.appName, "test_phase3")

    # ------------------------------------------------------------------
    # Test: complete feature computation pipeline (Streaming Mode)
    # ------------------------------------------------------------------
    @pytest.mark.skipif(sys.platform == "win32", reason="Structured streaming in memory sink hangs on Windows")
    def test_compute_features_streaming_pipeline(self):
        rate_df = self.spark.readStream.format("rate").option("rowsPerSecond", 1).load()

        event_df = rate_df.selectExpr(
            "CAST(value % 3 AS STRING) as symbol",
            "CAST(value * 1.0 AS DOUBLE) as price",
            "CAST(value * 10.0 AS DOUBLE) as volume",
            "timestamp",
            "CAST('binance' AS STRING) as source",
            "CAST(value AS STRING) as idempotency_key",
            "CAST(timestamp AS STRING) as received_at",
        )

        features_df = compute_features(event_df, mode="streaming", window_duration="1 minute", watermark_duration="1 minute")
        expected_columns = [
            "symbol",
            "event_ts",
            "price",
            "volume",
            "price_change",
            "price_return",
            "volume_change",
            "ma5",
            "ma20",
            "vwap",
            "price_range",
            "ma5_ratio",
            "ma20_ratio",
            "vwap_ratio",
            "price_range_ratio",
        ]

        self.assertEqual([field.name for field in features_df.schema.fields], expected_columns)

        query = (
            features_df
            .writeStream
            .format("memory")
            .queryName("phase4_features_test")
            .outputMode("append")
            .start()
        )
        query.processAllAvailable()
        query.stop()

    def test_compute_batch_features_pipeline(self):
        schema = StructType(
            [
                StructField("symbol", StringType(), nullable=False),
                StructField("price", DoubleType(), nullable=False),
                StructField("volume", DoubleType(), nullable=False),
                StructField("timestamp", StringType(), nullable=False),
                StructField("source", StringType(), nullable=False),
                StructField("idempotency_key", StringType(), nullable=False),
                StructField("received_at", StringType(), nullable=False),
            ]
        )
        rows = [
            ("BTCUSDT", 100.0, 10.0, "2026-08-01T00:00:00", "binance", "1", "2026-08-01T00:00:02"),
            ("BTCUSDT", 102.0, 8.0, "2026-08-01T00:01:00", "binance", "2", "2026-08-01T00:01:02"),
            ("ETHUSDT", 50.0, 5.0, "2026-08-01T00:00:00", "binance", "3", "2026-08-01T00:00:02"),
        ]
        batch_df = self.spark.createDataFrame(rows, schema=schema)
        features_df = compute_features(batch_df, mode="batch")

        self.assertTrue("ma5" in [field.name for field in features_df.schema.fields])
        self.assertTrue("price_return" in [field.name for field in features_df.schema.fields])
        self.assertTrue("volume_change" in [field.name for field in features_df.schema.fields])
        self.assertEqual(features_df.count(), 3)


if __name__ == "__main__":
    unittest.main()
