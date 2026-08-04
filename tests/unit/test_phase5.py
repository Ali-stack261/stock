import os
import tempfile
import unittest
from datetime import date
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from streaming.feature_engineering import compute_features
from streaming.spark_stream import build_spark_session
from streaming.storage import (
    apply_retention_policy,
    compact_partition,
    write_features_batch,
    write_raw_batch,
)


def _market_schema():
    return StructType(
        [
            StructField("symbol",          StringType(),    nullable=False),
            StructField("price",           DoubleType(),    nullable=False),
            StructField("volume",          DoubleType(),    nullable=False),
            StructField("timestamp",       StringType(),    nullable=False),
            StructField("source",          StringType(),    nullable=False),
            StructField("idempotency_key", StringType(),    nullable=False),
            StructField("received_at",     StringType(),    nullable=False),
        ]
    )


def _sample_rows():
    return [
        ("BTCUSDT", 100.0,  10.0, "2026-08-01T00:00:00", "binance", "1", "2026-08-01T00:00:02"),
        ("BTCUSDT", 102.0,   8.0, "2026-08-01T00:01:00", "binance", "2", "2026-08-01T00:01:02"),
        ("ETHUSDT",  50.0,   5.0, "2026-08-01T00:00:00", "binance", "3", "2026-08-01T00:00:02"),
        ("ETHUSDT",  51.0,   6.0, "2026-08-01T00:02:00", "binance", "4", "2026-08-01T00:02:02"),
    ]


class Phase5StorageTests(unittest.TestCase):
    # Share one SparkSession for the whole file — avoids paying JVM startup
    # cost once per test method (several seconds each).
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark_session(app_name="test_phase5")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ------------------------------------------------------------------
    # Test: write_raw_batch creates partitioned Parquet files
    # ------------------------------------------------------------------
    def test_write_raw_batch_creates_partitioned_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = os.path.join(tmp, "raw")
            df = self.spark.createDataFrame(_sample_rows(), schema=_market_schema())
            write_raw_batch(df, raw_path)

            # Year / month / day partitions must exist
            year_dirs = [p for p in Path(raw_path).iterdir() if p.name.startswith("year=")]
            self.assertTrue(len(year_dirs) > 0, "Expected at least one year= partition")

            # Re-read and count rows
            result = self.spark.read.parquet(raw_path)
            self.assertEqual(result.count(), 4)

            # Partition columns present
            cols = [f.name for f in result.schema.fields]
            self.assertIn("year",  cols)
            self.assertIn("month", cols)
            self.assertIn("day",   cols)
            self.assertIn("symbol", cols)

    # ------------------------------------------------------------------
    # Test: write_features_batch stores feature output correctly
    # ------------------------------------------------------------------
    def test_write_features_batch_creates_partitioned_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            feat_path = os.path.join(tmp, "features")
            df = self.spark.createDataFrame(_sample_rows(), schema=_market_schema())
            features = compute_features(df, mode="batch")
            write_features_batch(features, feat_path)

            result = self.spark.read.parquet(feat_path)
            self.assertEqual(result.count(), 4)

            cols = [f.name for f in result.schema.fields]
            self.assertIn("ma5",          cols)
            self.assertIn("ma20",         cols)
            self.assertIn("price_return", cols)
            self.assertIn("year",         cols)
            self.assertIn("symbol",       cols)

    # ------------------------------------------------------------------
    # Test: symbol partitioning — each symbol lands in its own directory
    # ------------------------------------------------------------------
    def test_symbol_partition_isolation(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = os.path.join(tmp, "raw")
            df = self.spark.createDataFrame(_sample_rows(), schema=_market_schema())
            write_raw_batch(df, raw_path)

            # Navigate into year/month/day and find symbol= dirs
            symbol_dirs = list(Path(raw_path).rglob("symbol=*"))
            symbols_found = {p.name.split("=")[1] for p in symbol_dirs if p.is_dir()}
            self.assertIn("BTCUSDT", symbols_found)
            self.assertIn("ETHUSDT", symbols_found)

    # ------------------------------------------------------------------
    # Test: compact_partition merges small files into one
    # ------------------------------------------------------------------
    def test_compact_partition_reduces_file_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = os.path.join(tmp, "raw")
            df = self.spark.createDataFrame(_sample_rows(), schema=_market_schema())

            # Write same partition twice (simulates two micro-batches)
            write_raw_batch(df.filter("symbol = 'BTCUSDT'"), raw_path, mode="append")
            write_raw_batch(df.filter("symbol = 'BTCUSDT'"), raw_path, mode="append")

            partition_path = Path(raw_path) / "year=2026" / "month=8" / "day=1" / "symbol=BTCUSDT"
            parquet_before = list(partition_path.glob("*.parquet"))

            row_count = compact_partition(
                self.spark, raw_path, date(2026, 8, 1), "BTCUSDT"
            )

            parquet_after = list(partition_path.glob("*.parquet"))
            self.assertEqual(row_count, 4, "Expected 4 rows (2 writes × 2 rows)")
            self.assertLessEqual(
                len(parquet_after),
                len(parquet_before),
                "Compaction should not increase the number of Parquet files",
            )

    # ------------------------------------------------------------------
    # Test: apply_retention_policy removes old partitions
    # ------------------------------------------------------------------
    def test_apply_retention_policy_removes_old_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Create fake old partition (100 days ago) and recent one (1 day ago)
            old_date  = date(2026, 4, 23)  # ~100 days before 2026-08-01
            new_date  = date(2026, 7, 31)  # 1 day before reference

            for d in (old_date, new_date):
                p = Path(tmp) / f"year={d.year}" / f"month={d.month}" / f"day={d.day}"
                p.mkdir(parents=True, exist_ok=True)
                (p / "part-00000.parquet").write_bytes(b"")  # placeholder file

            deleted = apply_retention_policy(
                tmp,
                retention_days=90,
                reference_date=date(2026, 8, 1),
            )

            self.assertEqual(len(deleted), 1, "Expected exactly one old partition deleted")
            self.assertFalse(
                (Path(tmp) / f"year={old_date.year}" / f"month={old_date.month}" / f"day={old_date.day}").exists(),
                "Old partition directory should have been removed",
            )
            self.assertTrue(
                (Path(tmp) / f"year={new_date.year}" / f"month={new_date.month}" / f"day={new_date.day}").exists(),
                "Recent partition directory should be kept",
            )

    # ------------------------------------------------------------------
    # Test: dry_run mode lists without deleting
    # ------------------------------------------------------------------
    def test_apply_retention_policy_dry_run_does_not_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_date = date(2026, 4, 1)
            p = Path(tmp) / f"year={old_date.year}" / f"month={old_date.month}" / f"day={old_date.day}"
            p.mkdir(parents=True, exist_ok=True)

            deleted = apply_retention_policy(
                tmp,
                retention_days=90,
                reference_date=date(2026, 8, 1),
                dry_run=True,
            )

            self.assertEqual(len(deleted), 1)
            self.assertTrue(p.exists(), "Dry-run must not actually delete the directory")


if __name__ == "__main__":
    unittest.main()
