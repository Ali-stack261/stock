from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, TimestampType


def build_spark_session(app_name: str = "real_time_stock_stream") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_market_event_schema() -> StructType:
    return StructType(
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


def create_streaming_dataframe(spark: SparkSession, kafka_bootstrap_servers: str, kafka_topic: str):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    json_schema = get_market_event_schema()
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), json_schema).alias("data"))
        .select("data.*")
    )
    return parsed_stream


def run_streaming_query(parsed_stream, output_path: str):
    return (
        parsed_stream
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", f"{output_path}/_checkpoints")
        .option("path", output_path)
        .start()
    )
