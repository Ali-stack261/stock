"""airflow/dags/retrain_pipeline.py – Phase 13 Automated Retraining DAG.

Trigger: drift detected OR scheduled interval (every 15 min by default).

Flow
----
1. check_drift_trigger — poll for drift signal (lightweight).
2. pull_training_data — read latest features from the data lake.
3. validate_training_data — schema + range checks.
4. train_and_evaluate — call the existing Phase 6 training pipeline.
5. check_promotable — branch on the promotion gate.
6. register_and_promote — register to MLflow, promote if promotable, save
   reference_features.parquet ONLY after successful promotion.
7. log_and_notify_only — log the failure, skip promotion.
8. reload_serving_model — touch a signal file the serving layer watches.

Design notes
------------
- **Polling, not push:** the DAG checks drift state itself rather than
  receiving a webhook from the serving layer.  This keeps ``serving/app.py``
  decoupled from Airflow.
- **Single cooldown owner:** ``DriftDetector`` owns the cooldown timer.  The
  DAG does not add a second independent cooldown.
- **Reference sample saved on promotion only:** ``reference_features.parquet``
  is overwritten only when a model actually reaches production, so the drift
  reference always tracks the deployed model, not just the most recent
  training attempt.
- **Testable without Airflow:** each task function is a plain Python function
  that can be called directly in unit tests.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Airflow's __init__.py lacks static stubs exposing DAG for mypy; safe to ignore.
from airflow import DAG  # type: ignore[attr-defined]
from airflow.operators.python import PythonOperator
from monitoring.drift import DriftDetector
from serving.prediction_store import PredictionStore
from training.register_model import run_registry_gate
from training.train import (
    MODEL_FEATURE_COLS,
    prepare_training_data,
)
from training.train import (
    validate_training_data as validate_training_dataframe,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DRIFT_CHECK_SYMBOLS = ["BTCUSDT", "ETHUSDT", "AAPL"]
DATA_LAKE_PATH = "data/features"
REFERENCE_PARQUET = "reference_features.parquet"
MODEL_RELOAD_SIGNAL = "model_reload_signal"

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1, tzinfo=timezone.utc),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task functions (plain Python — testable without Airflow)
# ---------------------------------------------------------------------------

def check_drift_trigger(**context: Any) -> bool:
    """Return True if any tracked symbol has triggered a drift retrain.

    Loads the reference distribution, queries recent live features and
    prediction errors from the prediction store, and runs ``DriftDetector.check()``.
    The detector's own cooldown timer is the single source of truth for
    "should we retrain right now".

    Cooldown state is persisted in ``PredictionStore.drift_state`` so it
    survives across separate DAG runs.
    """
    reference = _load_reference_data()
    if reference is None:
        logger.warning("No %s found — skipping drift check", REFERENCE_PARQUET)
        return False

    store = PredictionStore(db_path="predictions.db")

    any_triggered = False
    for symbol in DRIFT_CHECK_SYMBOLS:
        last_trigger = store.get_last_drift_trigger_time(symbol)
        detector = DriftDetector(
            reference_data=reference,
            cooldown_minutes=30,
            initial_last_trigger_time=last_trigger,
        )

        recent_features = store.get_recent_feature_rows(symbol, limit=500)
        recent_errors = store.get_recent_return_errors(symbol, limit=500)
        if recent_features.empty:
            logger.info("No feature rows for %s yet", symbol)
            continue

        report = detector.check(recent_features, prediction_errors=recent_errors)
        logger.info(
            "Drift check %s: feature=%s concept=%s triggered=%s cooldown_active=%s",
            symbol,
            report.feature_drift_detected,
            report.concept_drift_detected,
            report.triggered,
            report.cooldown_active,
        )
        if report.triggered:
            any_triggered = True
            assert detector.last_trigger_time is not None
            store.set_last_drift_trigger_time(symbol, detector.last_trigger_time)
            context["ti"].xcom_push(
                key=f"drift_report_{symbol}",
                value=_drift_report_to_dict(report),
            )

    return any_triggered


def pull_training_data(**context: Any) -> str:
    """Read the latest feature data from the data lake and return the output path.

    Returns
    -------
    str
        Path to the written Parquet file containing the latest features.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("retrain_pull_data").getOrCreate()

    df = spark.read.parquet(f"{DATA_LAKE_PATH}/**/*.parquet")
    row_count = df.count()
    logger.info("Loaded %d feature rows from data lake", row_count)

    output_path = "tmp/retrain_pull_training_data.parquet"
    os.makedirs("tmp", exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def validate_training_data(**context: Any) -> str:
    """Validate the pulled data and write cleaned output to a temp Parquet.

    Returns
    -------
    str
        Path to the validated Parquet file.
    """
    from pyspark.sql import SparkSession

    input_path = context["ti"].xcom_pull(key="return_value", task_ids="pull_training_data")
    spark = SparkSession.builder.appName("retrain_validate").getOrCreate()
    raw_df = spark.read.parquet(input_path)

    validated_df = validate_training_dataframe(raw_df)
    validated_count = validated_df.count()
    logger.info("Validation passed: %d rows after cleaning", validated_count)

    output_path = "tmp/retrain_validated_data.parquet"
    validated_df.write.mode("overwrite").parquet(output_path)
    return output_path


def train_and_evaluate_task(**context: Any) -> dict:
    """Run the full training pipeline on the validated data.

    Returns
    -------
    dict
        The training report from ``train_and_evaluate``.
    """
    from pyspark.sql import SparkSession

    input_path = context["ti"].xcom_pull(key="return_value", task_ids="validate_training_data")
    spark = SparkSession.builder.appName("retrain_train").getOrCreate()
    validated_df = spark.read.parquet(input_path)

    # Compute features + target (parity with streaming path)
    prepared_df = prepare_training_data(validated_df)
    prepared_count = prepared_df.count()
    logger.info("Prepared %d training rows", prepared_count)

    # Run the existing training + evaluation pipeline
    from training.train import train_and_evaluate
    report = train_and_evaluate(prepared_df)

    context["ti"].xcom_push(key="training_report", value=report)
    logger.info(
        "Training complete: test_rmse=%.6f promotable=%s",
        report["test_rmse"],
        report["promotable"],
    )
    return report


def check_promotable(**context: Any) -> bool:
    """Read the training report and return whether the model passed the gate."""
    report = context["ti"].xcom_pull(key="training_report", task_ids="train_and_evaluate")
    promotable = report.get("promotable", False)
    logger.info("Promotion gate result: %s", promotable)
    return promotable


def register_and_promote(**context: Any) -> dict:
    """Register the model and promote it to production if it passed the gate.

    Also saves ``reference_features.parquet`` ONLY after successful promotion,
    so the drift reference tracks the deployed model, not every training attempt.
    """
    report = context["ti"].xcom_pull(key="training_report", task_ids="train_and_evaluate")
    result = run_registry_gate(report)

    if result["status"] == "promoted":
        logger.info("Model promoted to production: version %s", result["version"])
        _save_reference_sample()
    else:
        logger.info("Model stayed in staging: version %s", result["version"])

    return result


def log_and_notify_only(**context: Any) -> dict:
    """Log that the model was not promoted; no further action."""
    report = context["ti"].xcom_pull(key="training_report", task_ids="train_and_evaluate")
    logger.warning(
        "Model not promotable — staying in staging. test_rmse=%.6f beats_baseline=%s",
        report.get("test_rmse"),
        report.get("beats_baseline"),
    )
    return {"status": "not_promoted", "reason": "failed_gate"}


def reload_serving_model(**context: Any) -> dict:
    """Signal the serving layer to reload the latest production model.

    In production this would call a model-management API; for the starter
    implementation we touch a signal file that the serving layer watches.
    """
    Path(MODEL_RELOAD_SIGNAL).touch()
    logger.info("Model reload signal written to %s", MODEL_RELOAD_SIGNAL)
    return {"reloaded": True}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_reference_data() -> pd.DataFrame | None:
    path = Path(REFERENCE_PARQUET)
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _save_reference_sample() -> None:
    """Save a fresh reference feature sample from the latest training data.

    This replaces the previous ``reference_features.parquet`` so the drift
    reference always reflects the currently deployed model's training
    distribution.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("retrain_save_reference").getOrCreate()
    df = spark.read.parquet(f"{DATA_LAKE_PATH}/**/*.parquet")

    sample = df.select(MODEL_FEATURE_COLS).sample(fraction=0.1, seed=42).toPandas()
    sample.to_parquet(REFERENCE_PARQUET, index=False)  # type: ignore[attr-defined]  # pyspark's toPandas() stub return type is imprecise
    logger.info("Reference sample saved (%d rows)", len(sample))


def _drift_report_to_dict(report: Any) -> dict:
    return {
        "feature_drift_detected": report.feature_drift_detected,
        "feature_details": report.feature_details,
        "concept_drift_detected": report.concept_drift_detected,
        "concept_details": report.concept_details,
        "triggered": report.triggered,
        "cooldown_active": report.cooldown_active,
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="retrain_pipeline",
    default_args=default_args,
    description="Automated model retraining triggered by drift detection",
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=["mlops", "retraining"],
    max_active_runs=1,
) as dag:

    check_drift = PythonOperator(
        task_id="check_drift_trigger",
        python_callable=check_drift_trigger,
    )

    pull_data = PythonOperator(
        task_id="pull_training_data",
        python_callable=pull_training_data,
    )

    validate = PythonOperator(
        task_id="validate_training_data",
        python_callable=validate_training_data,
    )

    train = PythonOperator(
        task_id="train_and_evaluate",
        python_callable=train_and_evaluate_task,
    )

    promotable = PythonOperator(
        task_id="check_promotable",
        python_callable=check_promotable,
    )

    promote = PythonOperator(
        task_id="register_and_promote",
        python_callable=register_and_promote,
    )

    notify = PythonOperator(
        task_id="log_and_notify_only",
        python_callable=log_and_notify_only,
    )

    reload = PythonOperator(
        task_id="reload_serving_model",
        python_callable=reload_serving_model,
    )

    # Linear flow up to the gate, then branch.
    check_drift >> pull_data >> validate >> train >> promotable
    promotable >> promote >> reload
    promotable >> notify
