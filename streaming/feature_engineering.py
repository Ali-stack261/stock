from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr


def compute_features(df: DataFrame) -> DataFrame:
    window_cols = ["symbol", "timestamp"]
    return (
        df.withColumn("price", col("price"))
          .withColumn("volume", col("volume"))
          .withColumn("price_change", expr("price - lag(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)"))
    )
