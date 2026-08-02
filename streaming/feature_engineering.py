from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
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
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import ArrayType, TimestampType

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


def _stream_state_schema() -> StructType:
    return StructType(
        [
            StructField("last_price", DoubleType(), nullable=True),
            StructField("last_volume", DoubleType(), nullable=True),
            StructField("price_history", ArrayType(DoubleType()), nullable=False),
            StructField("volume_history", ArrayType(DoubleType()), nullable=False),
        ]
    )


def _stateful_feature_fn(key, pdf_iter, state):
    if isinstance(key, tuple):
        symbol = key[0]
    else:
        symbol = key

    last_price = None
    last_volume = None
    price_history: list[float] = []
    volume_history: list[float] = []

    if state.exists:
        stored = state.get()
        last_price = stored[0]
        last_volume = stored[1]
        price_history = list(stored[2] or [])
        volume_history = list(stored[3] or [])

    rows = []

    for pdf in pdf_iter:
        pdf = pdf.sort_values(by="event_ts")
        for _, row in pdf.iterrows():
            price = float(row["price"])
            volume = float(row["volume"])
            event_ts = row["event_ts"]

            price_change = float(price - last_price) if last_price is not None else None
            price_return = float((price - last_price) / last_price) if last_price and last_price > 0 else None
            volume_change = float(volume - last_volume) if last_volume is not None else None

            price_history.append(price)
            volume_history.append(volume)
            if len(price_history) > 20:
                price_history = price_history[-20:]
                volume_history = volume_history[-20:]

            recent_prices = price_history[-5:]
            ma5 = float(sum(recent_prices) / len(recent_prices)) if recent_prices else None
            ma20 = float(sum(price_history) / len(price_history)) if price_history else None
            vwap = float(sum(p * v for p, v in zip(price_history, volume_history)) / sum(volume_history)) if sum(volume_history) > 0 else None
            price_range = float(max(price_history) - min(price_history)) if price_history else None

            rows.append(
                {
                    "symbol": symbol,
                    "event_ts": event_ts,
                    "price": price,
                    "volume": volume,
                    "price_change": price_change,
                    "price_return": price_return,
                    "volume_change": volume_change,
                    "ma5": ma5,
                    "ma20": ma20,
                    "vwap": vwap,
                    "price_range": price_range,
                }
            )

            last_price = price
            last_volume = volume

    if rows:
        state.update((last_price, last_volume, price_history, volume_history))
        yield pd.DataFrame(rows)


def compute_stream_features(df: DataFrame, window_duration: str = "1 minute", watermark_duration: str = "2 minutes") -> DataFrame:
    """Compute streaming-safe features using stateful per-symbol updates."""
    validated = validate_market_events(df)
    grouped = validated.groupby("symbol")
    output_schema = StructType(
        [
            StructField("symbol", StringType(), nullable=False),
            StructField("event_ts", TimestampType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("volume", DoubleType(), nullable=False),
            StructField("price_change", DoubleType(), nullable=True),
            StructField("price_return", DoubleType(), nullable=True),
            StructField("volume_change", DoubleType(), nullable=True),
            StructField("ma5", DoubleType(), nullable=True),
            StructField("ma20", DoubleType(), nullable=True),
            StructField("vwap", DoubleType(), nullable=True),
            StructField("price_range", DoubleType(), nullable=True),
        ]
    )

    return grouped.applyInPandasWithState(
        _stateful_feature_fn,
        output_schema,
        _stream_state_schema(),
        outputMode="append",
        timeoutConf=GroupStateTimeout.NoTimeout,
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
