"""training/train.py – Phase 6 Model Training Pipeline

Responsibilities
----------------
- Load raw historical data from the data lake.
- Re-run the batch feature engineering pipeline to ensure train/serve parity.
- Generate the prediction target (next tick's *return*).
- Perform a strict chronological train/val/test split (no random splitting).
- Evaluate a Naive Persistence Baseline (predicting that the next price equals
  the current price) to sanity-check model performance.
- Evaluate a Zero-Return Baseline (predicting no change) against the return target.
- Train a Gradient Boosted Trees (GBT) model using PySpark MLlib.
- Evaluate the model against the baselines and enforce a promotion gate.
"""

import mlflow
import mlflow.spark
from typing import Optional, Tuple

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lag, lit, percent_rank
from pyspark.sql.window import Window

from streaming.feature_engineering import compute_features

# ---------------------------------------------------------------------------
# Feature set — scale-invariant only.
#
# Tree-based models (GBT) cannot extrapolate past the value ranges they saw
# during training.  Feeding raw price levels (or absolute deltas) makes the
# model structurally wrong whenever the test period's price drifts outside the
# training range.  All features here are relative/ratio-based so the model sees
# a stationary, scale-invariant representation of the market.
# ---------------------------------------------------------------------------
MODEL_FEATURE_COLS = [
    "price_return",        # (price - prev_price) / prev_price
    "volume_change",       # volume - prev_volume
    "ma5_ratio",           # ma5 / price
    "ma20_ratio",          # ma20 / price
    "vwap_ratio",          # vwap / price
    "price_range_ratio",   # price_range / price
]

TARGET_RETURN_COL = "target_return"


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
    """Run feature engineering and generate the target labels.

    Produces both ``target_price`` (the price at the *next* tick, kept for
    reporting/downstream uses) and ``target_return`` — the scale-invariant
    target the model is actually trained on:
    ``target_return = (next_price - price) / price``.

    Parameters
    ----------
    raw_df:
        A DataFrame containing raw market events.

    Returns
    -------
    DataFrame
        A DataFrame with features and ``target_price`` / ``target_return``
        columns representing the price and return at the *next* tick for each
        symbol.
    """
    # 1. Validate and clean raw data (drop impossible values, deduplicate keys)
    validated_df = validate_training_data(raw_df)

    # 2. Compute all features (parity with streaming via batch path)
    features_df = compute_features(validated_df, mode="batch")

    # 3. Generate target variables (next tick's price / return)
    # We use lag(..., -1) which looks 1 row *ahead* over the time window.
    window_spec = Window.partitionBy("symbol").orderBy("event_ts")

    prepared_df = features_df.withColumn(
        "target_price", lag(col("price"), -1).over(window_spec)
    )

    # Return-based target — scale-invariant so tree models never have to
    # extrapolate to unseen absolute price levels.
    prepared_df = prepared_df.withColumn(
        TARGET_RETURN_COL,
        (col("target_price") - col("price")) / col("price"),
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
        metricName=metric_name,
    )

    return evaluator.evaluate(baseline_df)


def evaluate_naive_return_baseline(df: DataFrame, metric_name: str = "rmse") -> float:
    """Evaluate a zero-return baseline (prediction = 0.0 return).

    This is the return-space analogue of ``evaluate_naive_baseline``: with the
    model now predicting the *next return* rather than the next raw price, the
    simplest sensible baseline is "no change", i.e. a predicted return of 0.
    If the model cannot beat this on the holdout, it is not adding value.

    Parameters
    ----------
    df:
        A DataFrame with a ``target_return`` column (the actual next return).
    metric_name:
        Metric to evaluate (``rmse``, ``mae``, etc.).

    Returns
    -------
    float
        The baseline metric value.
    """
    baseline_df = df.withColumn("prediction", lit(0.0))

    evaluator = RegressionEvaluator(
        labelCol=TARGET_RETURN_COL,
        predictionCol="prediction",
        metricName=metric_name,
    )

    return evaluator.evaluate(baseline_df)


def predicted_return_to_price(current_price: float, predicted_return: float) -> float:
    """Convert a model's predicted return back into an absolute price.

    The GBT model is trained on return space (``target_return``) so it never
    has to extrapolate to unseen price levels.  Anything downstream that expects
    a price (dashboard, API response, prediction storage) must convert:

        predicted_price = current_price * (1 + predicted_return)

    Parameters
    ----------
    current_price:
        The current (latest observed) price for the symbol.
    predicted_return:
        The model's predicted next-tick return, e.g. ``0.001`` for +0.1 %.

    Returns
    -------
    float
        The predicted next-tick price.

    Examples
    --------
    >>> predicted_return_to_price(100.0, 0.01)
    101.0
    >>> predicted_return_to_price(100.0, -0.005)
    99.5
    """
    return current_price * (1.0 + predicted_return)


def train_gbt_model(
    train_df: DataFrame,
    val_df: DataFrame,
    feature_cols: Optional[list[str]] = None,
    label_col: str = TARGET_RETURN_COL,
) -> Tuple["pyspark.ml.PipelineModel", float, float, str]:
    """Train a Gradient Boosted Trees model using Spark MLlib.

    Parameters
    ----------
    train_df:
        Chronological training split.
    val_df:
        Chronological validation split.
    feature_cols:
        List of column names to use as features.  Defaults to
        :data:`MODEL_FEATURE_COLS` — the scale-invariant feature set.
        Raw price levels should NOT be used (tree models cannot extrapolate).
    label_col:
        Column to predict.  Defaults to ``target_return`` (the scale-invariant
        next-tick return).

    Returns
    -------
    Tuple[PipelineModel, float, float, str]
        The trained Pipeline model, training RMSE, validation RMSE, and the
        MLflow run ID (used to register the model in Phase 8).
    """
    if feature_cols is None:
        feature_cols = list(MODEL_FEATURE_COLS)

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",  # skip nulls if any snuck through
    )

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=label_col,
        predictionCol="prediction",
        maxIter=20,  # small for speed in starter implementation
        maxDepth=5,
        seed=42,
    )

    pipeline = Pipeline(stages=[assembler, gbt])

    # Start MLflow run
    with mlflow.start_run() as run:
        # Log hyperparameters
        mlflow.log_param("maxIter", 20)
        mlflow.log_param("maxDepth", 5)
        mlflow.log_param("labelCol", label_col)

        # Log feature list
        mlflow.log_param("features", ",".join(feature_cols))

        # Train
        model = pipeline.fit(train_df)

        # Evaluate RMSE
        evaluator_rmse = RegressionEvaluator(
            labelCol=label_col,
            predictionCol="prediction",
            metricName="rmse",
        )

        train_preds = model.transform(train_df)
        val_preds = model.transform(val_df)

        train_rmse = evaluator_rmse.evaluate(train_preds)
        val_rmse = evaluator_rmse.evaluate(val_preds)

        # Evaluate MAE
        evaluator_mae = RegressionEvaluator(
            labelCol=label_col,
            predictionCol="prediction",
            metricName="mae",
        )
        val_mae = evaluator_mae.evaluate(val_preds)

        # Log metrics
        mlflow.log_metric("train_rmse", train_rmse)
        mlflow.log_metric("val_rmse", val_rmse)
        mlflow.log_metric("val_mae", val_mae)

        # Log the model
        mlflow.spark.log_model(model, "gbt_model")

        run_id = run.info.run_id

    return model, train_rmse, val_rmse, run_id


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


def train_and_evaluate(
    raw_df: DataFrame,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    production_rmse: Optional[float] = None,
    baseline_min_improvement_pct: float = 0.0,
) -> dict:
    """Run the full training pipeline with an enforced model-quality gate.

    This wires together the previously-unused ``evaluate_naive_baseline`` /
    ``evaluate_naive_return_baseline`` and ``should_promote_challenger`` so that
    an overfit model can never silently reach production:

    1. Prepare features + return target from the raw data.
    2. Chronologically split into train / val / test.
    3. Train GBT on scale-invariant, return-space features.
    4. Evaluate the model on the holdout **test** split.
    5. Compare against the zero-return baseline on the same test split.
    6. Apply ``should_promote_challenger()`` — the model is only promotable if
       it beats the baseline (and, when an existing production RMSE is
       supplied, beats that too).
    7. Save a reference feature sample for Phase 12 drift detection.

    Parameters
    ----------
    raw_df:
        Raw market-event DataFrame (same shape as ``prepare_training_data``).
    train_ratio:
        Chronological train split fraction.
    val_ratio:
        Chronological validation split fraction (remainder is test).
    production_rmse:
        Validation RMSE of the currently deployed production model, or ``None``
        if none exists yet.  Passed to ``should_promote_challenger()``.
    baseline_min_improvement_pct:
        Minimum improvement over the test baseline required for promotion.

    Returns
    -------
    dict
        A report with all metrics and the promotion decision.
    """
    mlflow.set_experiment(
        mlflow.get_experiment_by_name("stock_training")
        .experiment_id
        if mlflow.get_experiment_by_name("stock_training")
        else mlflow.create_experiment("stock_training")
    )

    prepared_df = prepare_training_data(raw_df)
    train_df, val_df, test_df = chronological_split(
        prepared_df, train_ratio=train_ratio, val_ratio=val_ratio
    )

    model, train_rmse, val_rmse, run_id = train_gbt_model(train_df, val_df)

    # --- Enforced gate: evaluate on the holdout test split -----------------
    test_preds = model.transform(test_df)
    evaluator = RegressionEvaluator(
        labelCol=TARGET_RETURN_COL,
        predictionCol="prediction",
        metricName="rmse",
    )
    test_rmse = evaluator.evaluate(test_preds)

    # Baselines on the same test split.
    zero_return_test_rmse = evaluate_naive_return_baseline(test_df)
    price_persistence_test_rmse = evaluate_naive_baseline(test_df)

    # The model must beat the zero-return baseline on the test holdout to be
    # promotable, otherwise it is fitting noise / extrapolating poorly.
    beats_baseline = test_rmse < zero_return_test_rmse * (
        1 - baseline_min_improvement_pct / 100.0
    )

    # Production-vs-challenger gate (only meaningful if a prod model exists).
    promotion_decision = should_promote_challenger(
        test_rmse, production_rmse, min_improvement_pct=0.0
    )

    # A model that loses to "predict no change" must never be promoted,
    # regardless of the production comparison.
    promotable = beats_baseline and promotion_decision

    with mlflow.start_run(run_name="training_gate", nested=True):
        mlflow.log_metric("test_rmse", test_rmse)
        mlflow.log_metric("zero_return_baseline_test_rmse", zero_return_test_rmse)
        mlflow.log_metric("price_persistence_baseline_test_rmse", price_persistence_test_rmse)
        mlflow.log_metric("train_rmse", train_rmse)
        mlflow.log_metric("val_rmse", val_rmse)
        mlflow.log_param("promotable", str(promotable))
        mlflow.log_param("beats_baseline", str(beats_baseline))

    # Phase 12 — save a reference feature sample for live drift detection.
    feature_cols = list(MODEL_FEATURE_COLS)
    reference_sample = train_df.select(feature_cols).sample(fraction=0.1, seed=42).toPandas()
    ref_path = "reference_features.parquet"
    reference_sample.to_parquet(ref_path, index=False)
    mlflow.log_artifact(ref_path)
    mlflow.log_param("reference_sample_rows", len(reference_sample))

    return {
        "model": model,
        "run_id": run_id,
        "train_rmse": train_rmse,
        "val_rmse": val_rmse,
        "test_rmse": test_rmse,
        "zero_return_baseline_test_rmse": zero_return_test_rmse,
        "price_persistence_baseline_test_rmse": price_persistence_test_rmse,
        "beats_baseline": beats_baseline,
        "should_promote_challenger": promotion_decision,
        "promotable": promotable,
        "reference_sample_path": ref_path,
    }


def main(raw_df: DataFrame) -> None:
    """CLI-friendly entry point that enforces the promotion gate."""
    report = train_and_evaluate(raw_df)
    print("=== Training gate report ===")
    print(f"train RMSE: {report['train_rmse']:.6f}")
    print(f"val RMSE:   {report['val_rmse']:.6f}")
    print(f"test RMSE:  {report['test_rmse']:.6f}")
    print(f"zero-return baseline test RMSE: {report['zero_return_baseline_test_rmse']:.6f}")
    print(f"beats baseline: {report['beats_baseline']}")
    print(f"promotable: {report['promotable']}")