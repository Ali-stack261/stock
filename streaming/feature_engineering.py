from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, first, last, sum as sum_, avg, max as max_, min as min_, to_timestamp, window


def compute_features(df: DataFrame, window_duration: str = "1 minute", watermark_duration: str = "2 minutes") -> DataFrame:
    """Compute streaming-compatible time-window features for market events.

    Spark Structured Streaming does not support row-ordering window functions such as
    LAG/LEAD on unbounded data. This implementation uses a time-window aggregation
    with watermarking so the pipeline remains compatible with real-time streaming.
    """

    event_df = (
        df.withColumn("event_ts", to_timestamp(col("timestamp")))
          .withWatermark("event_ts", watermark_duration)
    )

    windowed = (
        event_df
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
        .withColumn("price_range", col("max_price") - col("min_price"))
    )

    return windowed
