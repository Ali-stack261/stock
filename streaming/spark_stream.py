from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, StringType, DoubleType

from streaming.feature_engineering import compute_features


def build_spark_session(app_name: str = "real_time_stock_stream") -> SparkSession:
    # JDK 17+ restricts reflective access to internal java.nio/sun.misc classes
    # that Arrow's memory layer (DirectByteBuffer) requires.  Setting
    # --add-opens here means the flags are applied whether PySpark is launched
    # via pytest, spark-submit, or any other entry point — no external
    # SPARK_SUBMIT_OPTS / PYSPARK_SUBMIT_ARGS needed.
    java17_opens = " ".join([
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ])
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.extraJavaOptions", java17_opens)
        .config("spark.executor.extraJavaOptions", java17_opens)
        # Disable the web UI — saves real startup time in tests and CI.
        .config("spark.ui.enabled", "false")
        # Spark defaults to 200 shuffle partitions; for tiny local test data
        # that means 200 tasks doing trivial work.  2 is enough and fast.
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


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


def create_feature_stream(
    spark: SparkSession,
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    window_duration: str = "1 minute",
    watermark_duration: str = "2 minutes",
):
    parsed_stream = create_streaming_dataframe(spark, kafka_bootstrap_servers, kafka_topic)
    return compute_features(parsed_stream, window_duration=window_duration, watermark_duration=watermark_duration)


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
