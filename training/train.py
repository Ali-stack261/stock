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

import mlflow
import mlflow.spark
from typing import Optional, Tuple

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lag, percent_rank
from pyspark.sql.window import Window

from streaming.feature_engineering import compute_features


def validate_training_data(df: DataFrame) -> DataFrame:
    """Validate and clean raw training data before feature engineering.

    Applies the same business-rule checks used by ``normalize_event()`` in the
    streaming path so that bad records that somehow made it into the data lake
    cannot silently corrupt model training.

    Checks performed:
    - ``price > 0`` — non-positive prices are impossible and indicate corrupt
      data or pipeline bugs.
    - ``volume >= 0`` — negative volumes are impossible.
    - ``timestamp`` is non-null/non-empty — rows without a timestamp cannot be
      ordered chronologically and must be dropped.
    - Duplicate ``idempotency_key`` values — keeps only the first occurrence so
      replayed events do not inflate the training set.

    Parameters
    ----------
    df:
        Raw DataFrame of market events as loaded from the data lake.

    Returns
    -------
    DataFrame
        A cleaned DataFrame ready for feature engineering.
    """
    from pyspark.sql.functions import row_number
    from pyspark.sql.window import Window as _Window

    # 1. Drop rows with impossible price / volume values or missing timestamps.
    cleaned = df.filter(
        (col("price") > 0.0)
        & (col("volume") >= 0.0)
        & col("timestamp").isNotNull()
        & (col("timestamp") != "")
    )

    # 2. Deduplicate on idempotency_key — keep the first arrival per key.
    dedup_window = _Window.partitionBy("idempotency_key").orderBy("timestamp")
    cleaned = (
        cleaned
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return cleaned


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
    # 1. Validate and clean raw data (drop impossible values, deduplicate keys)
    validated_df = validate_training_data(raw_df)

    # 2. Compute all features (parity with streaming via batch path)
    features_df = compute_features(validated_df, mode="batch")

    # 3. Generate target variable (next tick's price)
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
    
    # Start MLflow run
    with mlflow.start_run():
        # Log hyperparameters
        mlflow.log_param("maxIter", 20)
        mlflow.log_param("maxDepth", 5)
        
        # Log feature list
        mlflow.log_param("features", ",".join(feature_cols))
        
        # Train
        model = pipeline.fit(train_df)
        
        # Evaluate RMSE
        evaluator_rmse = RegressionEvaluator(
            labelCol="target_price",
            predictionCol="prediction",
            metricName="rmse"
        )
        
        train_preds = model.transform(train_df)
        val_preds = model.transform(val_df)
        
        train_rmse = evaluator_rmse.evaluate(train_preds)
        val_rmse = evaluator_rmse.evaluate(val_preds)
        
        # Evaluate MAE
        evaluator_mae = RegressionEvaluator(
            labelCol="target_price",
            predictionCol="prediction",
            metricName="mae"
        )
        val_mae = evaluator_mae.evaluate(val_preds)
        
        # Log metrics
        mlflow.log_metric("train_rmse", train_rmse)
        mlflow.log_metric("val_rmse", val_rmse)
        mlflow.log_metric("val_mae", val_mae)
        
        # Log the model
        mlflow.spark.log_model(model, "gbt_model")
        
    return model, train_rmse, val_rmse


def should_promote_challenger(
    challenger_rmse: float,
    production_rmse: Optional[float],
    min_improvement_pct: float = 0.0,
) -> bool:
    """Return True if the challenger model should replace the production model.

    The challenger wins when it improves on the production model's validation
    RMSE by at least ``min_improvement_pct`` percentage points.  If there is no
    production model yet (``production_rmse is None``) the challenger is always
    promoted — it's the first deployment.

    Parameters
    ----------
    challenger_rmse:
        Validation RMSE of the newly trained model.
    production_rmse:
        Validation RMSE of the currently deployed production model, or ``None``
        if no model is in production yet.  This value can be loaded from
        MLflow's model registry (Phase 7) or from a simple persisted metrics
        file in the interim.
    min_improvement_pct:
        Minimum required improvement expressed as a percentage of the
        production RMSE.  Defaults to ``0.0`` (any improvement wins).  Pass
        e.g. ``1.0`` to require the challenger to be at least 1 % better.

    Returns
    -------
    bool
        ``True`` if the challenger should be promoted to production.

    Examples
    --------
    >>> should_promote_challenger(0.95, 1.0)          # 5 % better → promote
    True
    >>> should_promote_challenger(1.01, 1.0)          # 1 % worse → reject
    False
    >>> should_promote_challenger(0.99, None)         # first deploy → promote
    True
    >>> should_promote_challenger(0.99, 1.0, min_improvement_pct=2.0)
    False  # only 1 % better, threshold is 2 %
    """
    if production_rmse is None:
        # No model in production — always promote the first challenger.
        return True
    improvement_pct = (production_rmse - challenger_rmse) / production_rmse * 100
    return improvement_pct >= min_improvement_pct
