"""streaming/storage.py – Data Lake storage layer for Phase 5.

Responsibilities
----------------
- Write raw validated market events to a partitioned Parquet lake (streaming + batch).
- Write computed features to a separate partitioned Parquet lake (streaming + batch).
- Compact small Parquet files left behind by streaming micro-batches.
- Enforce a configurable retention policy by removing data older than N days.

Partition layout
----------------
Both lakes share the same partition scheme:

    <base_path>/year=YYYY/month=MM/day=DD/symbol=<SYMBOL>/part-*.parquet

Partitioning by date *and* symbol means:
- Training jobs that target a single ticker read only their partition (no full scan).
- Date-range queries skip irrelevant months without listing every file.

Small-file problem
------------------
Spark Structured Streaming writes one file per micro-batch per partition.  With
many symbols and frequent micro-batches this creates thousands of tiny files that
are slow to list and read.  ``compact_partition`` re-reads a single date+symbol
partition and rewrites it as a single file, and should be scheduled (e.g. daily
via Airflow) after the previous day's streaming is complete.

Retention policy
----------------
``apply_retention_policy`` deletes partition directories whose date is older
than ``retention_days`` (default 90).  Run on a schedule to bound data-lake
storage cost as history grows.
"""

from __future__ import annotations

import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofmonth, month, to_timestamp, year

if TYPE_CHECKING:
    import pyspark

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _with_date_partitions(df: DataFrame) -> DataFrame:
    """Add year / month / day columns derived from timestamp for partitioning."""
    if "event_ts" in df.columns:
        ts_col = col("event_ts")
    else:
        ts_col = to_timestamp(col("timestamp"))
        
    return (
        df
        .withColumn("year",  year(ts_col))
        .withColumn("month", month(ts_col))
        .withColumn("day",   dayofmonth(ts_col))
    )


# ---------------------------------------------------------------------------
# Streaming writes
# ---------------------------------------------------------------------------

def write_raw_stream(
    df: DataFrame,
    base_path: str,
    checkpoint_path: str,
    trigger_seconds: int | None = 30,
) -> pyspark.sql.streaming.StreamingQuery:
    """Write validated raw market events to the data lake via Structured Streaming.

    Parameters
    ----------
    df:
        A streaming DataFrame containing validated market events.  Must include
        ``event_ts`` (TimestampType), ``symbol`` (StringType), ``price``, and
        ``volume`` columns.
    base_path:
        Root directory for the raw-events lake, e.g. ``"data/raw"``.
    checkpoint_path:
        Durable checkpoint location for exactly-once delivery.
    trigger_seconds:
        Micro-batch interval in seconds.  ``None`` means continuous trigger
        (process as fast as possible).

    Returns
    -------
    pyspark.sql.streaming.StreamingQuery
    """
    partitioned = _with_date_partitions(df)

    writer = (
        partitioned
        .writeStream
        .outputMode("append")
        .format("parquet")
        .partitionBy("year", "month", "day", "symbol")
        .option("path", base_path)
        .option("checkpointLocation", checkpoint_path)
    )

    if trigger_seconds is not None:
        writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")

    return writer.start()


def write_features_stream(
    df: DataFrame,
    base_path: str,
    checkpoint_path: str,
    trigger_seconds: int | None = 30,
) -> pyspark.sql.streaming.StreamingQuery:
    """Write computed feature rows to the features lake via Structured Streaming.

    Parameters
    ----------
    df:
        A streaming DataFrame produced by ``compute_features(mode="streaming")``.
        Must include ``event_ts`` and ``symbol`` columns.
    base_path:
        Root directory for the features lake, e.g. ``"data/features"``.
    checkpoint_path:
        Durable checkpoint location.
    trigger_seconds:
        Micro-batch interval in seconds.

    Returns
    -------
    pyspark.sql.streaming.StreamingQuery
    """
    partitioned = _with_date_partitions(df)

    writer = (
        partitioned
        .writeStream
        .outputMode("append")
        .format("parquet")
        .partitionBy("year", "month", "day", "symbol")
        .option("path", base_path)
        .option("checkpointLocation", checkpoint_path)
    )

    if trigger_seconds is not None:
        writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")

    return writer.start()


# ---------------------------------------------------------------------------
# Batch writes (for back-fills and testing)
# ---------------------------------------------------------------------------

def write_raw_batch(df: DataFrame, base_path: str, mode: str = "append") -> None:
    """Write a static (batch) DataFrame of raw events to the data lake.

    Parameters
    ----------
    df:
        A batch DataFrame with an ``event_ts`` column.
    base_path:
        Root directory for the raw-events lake.
    mode:
        Spark write mode — ``"append"`` (default) or ``"overwrite"``.
    """
    partitioned = _with_date_partitions(df)
    (
        partitioned
        .write
        .mode(mode)
        .partitionBy("year", "month", "day", "symbol")
        .parquet(base_path)
    )


def write_features_batch(df: DataFrame, base_path: str, mode: str = "append") -> None:
    """Write a static (batch) DataFrame of feature rows to the features lake.

    Parameters
    ----------
    df:
        A batch DataFrame produced by ``compute_features(mode="batch")``.
    base_path:
        Root directory for the features lake.
    mode:
        Spark write mode.
    """
    partitioned = _with_date_partitions(df)
    (
        partitioned
        .write
        .mode(mode)
        .partitionBy("year", "month", "day", "symbol")
        .parquet(base_path)
    )


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

def compact_partition(
    spark: SparkSession,
    base_path: str,
    partition_date: date,
    symbol: str,
    output_mode: str = "overwrite",
) -> int:
    """Compact all small Parquet files in one date+symbol partition into a single file.

    Streaming micro-batches each produce a separate Parquet file per partition.
    Over time this creates many small files that degrade read performance.
    This function re-reads the target partition, coalesces to a single file,
    and overwrites it in place.

    Parameters
    ----------
    spark:
        An active SparkSession.
    base_path:
        Root directory of the lake (same value passed to ``write_raw_batch`` /
        ``write_features_batch``).
    partition_date:
        The calendar date whose partition should be compacted.
    symbol:
        The ticker symbol whose partition should be compacted.
    output_mode:
        Spark write mode for the compacted output (default ``"overwrite"``).

    Returns
    -------
    int
        Row count of the compacted partition (useful for logging/validation).
    """
    partition_path = (
        f"{base_path}"
        f"/year={partition_date.year}"
        f"/month={partition_date.month}"
        f"/day={partition_date.day}"
        f"/symbol={symbol}"
    )
    tmp_path = partition_path + "_tmp"

    df = spark.read.parquet(partition_path)
    row_count = df.count()

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(tmp_path)
    )

    # Replace the old partition directory with the compacted one
    sc = spark.sparkContext
    assert sc._jvm is not None, "SparkContext JVM gateway is not initialized"
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    PathClass = sc._jvm.org.apache.hadoop.fs.Path
    
    fs.delete(PathClass(partition_path), True)
    fs.rename(PathClass(tmp_path), PathClass(partition_path))

    return row_count


# ---------------------------------------------------------------------------
# Retention policy
# ---------------------------------------------------------------------------

def apply_retention_policy(
    base_path: str,
    retention_days: int = 90,
    reference_date: date | None = None,
    dry_run: bool = False,
) -> list[str]:
    """Delete partition directories older than ``retention_days``.

    Walks the lake directory tree looking for directories named
    ``year=YYYY/month=MM/day=DD`` and removes any whose date falls outside
    the retention window.

    Parameters
    ----------
    base_path:
        Root directory of the lake.
    retention_days:
        Number of days to keep.  Partitions older than this are deleted.
        Default is 90 days.
    reference_date:
        The date to count backwards from.  Defaults to today (UTC).
    dry_run:
        If ``True``, log which directories *would* be deleted without actually
        removing them.  Useful for testing the policy before running for real.

    Returns
    -------
    list[str]
        Paths that were deleted (or would have been deleted in dry-run mode).
    """
    if reference_date is None:
        reference_date = datetime.now(timezone.utc).date()

    cutoff = reference_date - timedelta(days=retention_days)
    deleted: list[str] = []

    root = Path(base_path)
    if not root.exists():
        return deleted

    for year_dir in sorted(root.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.startswith("year="):
            continue
        try:
            y = int(year_dir.name.split("=")[1])
        except ValueError:
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                continue
            try:
                m = int(month_dir.name.split("=")[1])
            except ValueError:
                continue

            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir() or not day_dir.name.startswith("day="):
                    continue
                try:
                    d = int(day_dir.name.split("=")[1])
                    partition_date = date(y, m, d)
                except (ValueError, OverflowError):
                    continue

                if partition_date < cutoff:
                    deleted.append(str(day_dir))
                    if not dry_run:
                        shutil.rmtree(day_dir)

    return deleted
