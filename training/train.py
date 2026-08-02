"""training/train.py – Phase 6 Model Training Pipeline

Responsibilities
----------------
- Load raw historical data from the data lake.
- Re-run the batch feature engineering pipeline to ensure train/serve parity.
- Generate the prediction target (e.g. next tick's price).
- Perform a strict chronological train/val/test split (no random splitting).
- Evaluate a Naive Persistence Baseline (predicting that the next price equals
  the current price) to sanity-check model performance.
- Train a Gradient Boosted Trees (GBT) model using PySpark MLlib.
- Evaluate the model against the baseline.
"""

from typing import Tuple

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lag, percent_rank
from pyspark.sql.window import Window

from streaming.feature_engineering import compute_features


def prepare_training_data(raw_df: DataFrame) -> DataFrame:
    """Run feature engineering and generate the target label.

    Parameters
    ----------
    raw_df:
        A DataFrame containing raw market events.

    Returns
    -------
    DataFrame
        A DataFrame with features and a ``target_price`` column representing
        the price at the *next* tick for each symbol.
    """
    # 1. Compute all features (guaranteed parity with streaming)
    features_df = compute_features(raw_df, mode="batch")

    # 2. Generate target variable (next tick's price)
    # We use lag(..., -1) which looks 1 row *ahead* over the time window.
    window_spec = Window.partitionBy("symbol").orderBy("event_ts")
    
    prepared_df = features_df.withColumn(
        "target_price", lag(col("price"), -1).over(window_spec)
    )

    # Drop the last row for each symbol, since its next-tick price is unknown (null)
    prepared_df = prepared_df.filter(col("target_price").isNotNull())
    
    return prepared_df


def chronological_split(
    df: DataFrame, train_ratio: float = 0.7, val_ratio: float = 0.15
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Split the dataset chronologically to prevent look-ahead leakage.

    Parameters
    ----------
    df:
        The prepared dataset with a timestamp column (``event_ts``).
    train_ratio:
        Proportion of earliest data to use for training.
    val_ratio:
        Proportion of subsequent data to use for validation.
        (The remainder is used for testing).

    Returns
    -------
    Tuple[DataFrame, DataFrame, DataFrame]
        (train_df, val_df, test_df)
    """
    # Calculate a percent rank across the entire dataset ordered by time
    window_spec = Window.orderBy("event_ts")
    df_with_rank = df.withColumn("rank", percent_rank().over(window_spec))

    # Split using the rank boundaries
    train_df = df_with_rank.filter(col("rank") <= train_ratio).drop("rank")
    
    val_upper_bound = train_ratio + val_ratio
    val_df = df_with_rank.filter(
        (col("rank") > train_ratio) & (col("rank") <= val_upper_bound)
    ).drop("rank")
    
    test_df = df_with_rank.filter(col("rank") > val_upper_bound).drop("rank")

    return train_df, val_df, test_df


def evaluate_naive_baseline(df: DataFrame, metric_name: str = "rmse") -> float:
    """Evaluate a naive persistence baseline (prediction = current price).

    If our complex ML model cannot beat this simple baseline, it is not
    adding value.

    Parameters
    ----------
    df:
        A DataFrame with ``price`` (current) and ``target_price`` (actual next).
    metric_name:
        Metric to evaluate (``rmse``, ``mae``, etc.).

    Returns
    -------
    float
        The baseline metric value.
    """
    # In naive persistence, the prediction for the next tick is simply the current price.
    baseline_df = df.withColumn("prediction", col("price"))
    
    evaluator = RegressionEvaluator(
        labelCol="target_price",
        predictionCol="prediction",
        metricName=metric_name
    )
    
    return evaluator.evaluate(baseline_df)


def train_gbt_model(
    train_df: DataFrame,
    val_df: DataFrame,
    feature_cols: list[str]
) -> Tuple["pyspark.ml.PipelineModel", float, float]:
    """Train a Gradient Boosted Trees model using Spark MLlib.

    Parameters
    ----------
    train_df:
        Chronological training split.
    val_df:
        Chronological validation split.
    feature_cols:
        List of column names to use as features.

    Returns
    -------
    Tuple[PipelineModel, float, float]
        The trained Pipeline model, training RMSE, and validation RMSE.
    """
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip" # skip nulls if any snuck through
    )

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="target_price",
        predictionCol="prediction",
        maxIter=20,     # small for speed in starter implementation
        maxDepth=5,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, gbt])
    
    # Train
    model = pipeline.fit(train_df)
    
    # Evaluate
    evaluator = RegressionEvaluator(
        labelCol="target_price",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    train_preds = model.transform(train_df)
    val_preds = model.transform(val_df)
    
    train_rmse = evaluator.evaluate(train_preds)
    val_rmse = evaluator.evaluate(val_preds)
    
    return model, train_rmse, val_rmse
