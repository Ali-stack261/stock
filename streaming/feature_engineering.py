from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    avg,
    col,
    expr,
    first,
    lag,
    last,
    max as max_,
    min as min_,
    sum as sum_,
    to_timestamp,
    window,
)

FEATURE_COLUMNS = [
    "symbol",
    "price_change",
    "price_return",
    "volume_change",
    "ma5",
    "ma20",
    "vwap",
    "price_range",
]


def validate_market_events(df: DataFrame) -> DataFrame:
    """Reject invalid market events before feature computation."""
    return (
        df.withColumn("event_ts", to_timestamp(col("timestamp")))
          .filter((col("price") > 0.0) & (col("volume") >= 0.0) & col("event_ts").isNotNull())
    )


def compute_batch_features(df: DataFrame) -> DataFrame:
    """Compute batch-safe features with row-ordering windows for training pipelines."""
    validated = validate_market_events(df)
    symbol_window = Window.partitionBy("symbol").orderBy(col("event_ts"))
    ma5_window = symbol_window.rowsBetween(-4, 0)
    ma20_window = symbol_window.rowsBetween(-19, 0)
    previous_price = lag(col("price"), 1).over(symbol_window)
    previous_volume = lag(col("volume"), 1).over(symbol_window)

    return (
        validated
        .withColumn("previous_price", previous_price)
        .withColumn("previous_volume", previous_volume)
        .withColumn("price_change", col("price") - col("previous_price"))
        .withColumn(
            "price_return",
            expr(
                "CASE WHEN previous_price > 0 THEN (price - previous_price) / previous_price ELSE NULL END"
            ),
        )
        .withColumn("volume_change", col("volume") - col("previous_volume"))
        .withColumn("ma5", avg(col("price")).over(ma5_window))
        .withColumn("ma20", avg(col("price")).over(ma20_window))
        .withColumn(
            "vwap",
            sum_(col("price") * col("volume")).over(ma20_window)
            / sum_(col("volume")).over(ma20_window),
        )
        .withColumn("max_price", max_(col("price")).over(ma20_window))
        .withColumn("min_price", min_(col("price")).over(ma20_window))
        .withColumn("price_range", col("max_price") - col("min_price"))
        .drop("previous_price", "previous_volume", "max_price", "min_price")
    )


def compute_stream_features(df: DataFrame, window_duration: str = "1 minute", watermark_duration: str = "2 minutes") -> DataFrame:
    """Compute streaming-safe features using time-window aggregation and watermarking."""
    validated = validate_market_events(df)
    return (
        validated
        .withWatermark("event_ts", watermark_duration)
        .groupBy(window(col("event_ts"), window_duration), col("symbol"))
        .agg(
            first(col("price")).alias("first_price"),
            last(col("price")).alias("last_price"),
            avg(col("price")).alias("avg_price"),
            max_(col("price")).alias("max_price"),
            min_(col("price")).alias("min_price"),
            sum_(col("volume")).alias("volume_sum"),
            (sum_(col("price") * col("volume")) / sum_(col("volume"))).alias("vwap"),
        )
        .withColumn("price_change", col("last_price") - col("first_price"))
        .withColumn(
            "price_return",
            expr(
                "CASE WHEN first_price > 0 THEN (last_price - first_price) / first_price ELSE NULL END"
            ),
        )
        .withColumn("price_range", col("max_price") - col("min_price"))
    )


def compute_features(
    df: DataFrame,
    mode: str = "streaming",
    window_duration: str = "1 minute",
    watermark_duration: str = "2 minutes",
) -> DataFrame:
    if mode == "batch":
        return compute_batch_features(df)
    return compute_stream_features(df, window_duration=window_duration, watermark_duration=watermark_duration)
