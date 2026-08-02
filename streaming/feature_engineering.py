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


def compute_time_window_features(
    df: DataFrame,
    window_duration: str = "1 minute",
    watermark_duration: str | None = "2 minutes",
) -> DataFrame:
    """Compute features using shared time-window aggregation for batch and streaming."""
    validated = validate_market_events(df)
    windowed = validated

    if watermark_duration is not None:
        windowed = windowed.withWatermark("event_ts", watermark_duration)

    base_window = (
        windowed
        .groupBy(window(col("event_ts"), window_duration).alias("window"), col("symbol"))
        .agg(
            first(col("price")).alias("first_price"),
            last(col("price")).alias("last_price"),
            avg(col("price")).alias("avg_price"),
            max_(col("price")).alias("max_price"),
            min_(col("price")).alias("min_price"),
            first(col("volume")).alias("first_volume"),
            last(col("volume")).alias("last_volume"),
            sum_(col("volume")).alias("volume_sum"),
            (sum_(col("price") * col("volume")) / sum_(col("volume"))).alias("vwap"),
        )
        .alias("base")
    )

    ma5_window = (
        windowed
        .groupBy(window(col("event_ts"), "5 minutes", window_duration).alias("ma5_window"), col("symbol"))
        .agg(avg(col("price")).alias("ma5"))
        .alias("ma5")
    )

    ma20_window = (
        windowed
        .groupBy(window(col("event_ts"), "20 minutes", window_duration).alias("ma20_window"), col("symbol"))
        .agg(avg(col("price")).alias("ma20"))
        .alias("ma20")
    )

    joined = (
        base_window
        .join(
            ma5_window,
            (col("base.symbol") == col("ma5.symbol"))
            & (col("base.window").getField("end") == col("ma5.ma5_window").getField("end"))
            & (col("base.window").getField("start") == col("ma5.ma5_window").getField("start")),
            how="left",
        )
        .join(
            ma20_window,
            (col("base.symbol") == col("ma20.symbol"))
            & (col("base.window").getField("end") == col("ma20.ma20_window").getField("end"))
            & (col("base.window").getField("start") == col("ma20.ma20_window").getField("start")),
            how="left",
        )
    )

    return (
        joined
        .select(
            col("base.window").alias("window"),
            col("base.symbol"),
            col("base.first_price"),
            col("base.last_price"),
            col("base.avg_price"),
            col("base.max_price"),
            col("base.min_price"),
            col("base.first_volume"),
            col("base.last_volume"),
            col("base.volume_sum"),
            col("base.vwap"),
            col("ma5.ma5"),
            col("ma20.ma20"),
        )
        .withColumn("price_change", col("last_price") - col("first_price"))
        .withColumn(
            "price_return",
            expr(
                "CASE WHEN first_price > 0 THEN (last_price - first_price) / first_price ELSE NULL END"
            ),
        )
        .withColumn("volume_change", col("last_volume") - col("first_volume"))
        .withColumn("price_range", col("max_price") - col("min_price"))
    )


def compute_batch_features(df: DataFrame) -> DataFrame:
    """Compute batch-safe features using the same time-window aggregation as streaming."""
    return compute_time_window_features(df, watermark_duration=None)


def compute_stream_features(df: DataFrame, window_duration: str = "1 minute", watermark_duration: str = "2 minutes") -> DataFrame:
    """Compute streaming-safe features using time-window aggregation and watermarking."""
    return compute_time_window_features(df, window_duration=window_duration, watermark_duration=watermark_duration)


def compute_features(
    df: DataFrame,
    mode: str = "streaming",
    window_duration: str = "1 minute",
    watermark_duration: str = "2 minutes",
) -> DataFrame:
    if mode == "batch":
        return compute_batch_features(df)
    return compute_stream_features(df, window_duration=window_duration, watermark_duration=watermark_duration)
