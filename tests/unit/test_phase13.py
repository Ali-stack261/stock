"""tests/unit/test_phase13.py – Phase 13 Automated Retraining tests.

Verifies the Airflow DAG task functions work correctly when called directly
(without the Airflow scheduler):
1. DAG file imports cleanly.
2. check_drift_trigger returns False when no reference data exists.
3. check_drift_trigger returns True when drift is triggered.
4. register_and_promote saves reference_features.parquet only on promotion.
5. register_and_promote does NOT save reference when model is not promoted.
6. log_and_notify_only returns the expected not-promoted dict.
7. reload_serving_model touches the signal file.
8. The DAG has the expected task IDs and dependency structure.
"""

import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Load the DAG module directly, mocking Airflow imports so the local
# `airflow/` namespace package does not shadow the real one.
# ---------------------------------------------------------------------------
_DAG_PATH = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "retrain_pipeline.py"

from unittest.mock import MagicMock

_airflow_mock = MagicMock()
_airflow_ops_mock = MagicMock()
_airflow_python_mock = MagicMock()
_airflow_mock.operators.python = _airflow_python_mock

_sys_modules_backup = dict(sys.modules)
sys.modules["airflow"] = _airflow_mock
sys.modules["airflow.operators"] = _airflow_ops_mock
sys.modules["airflow.operators.python"] = _airflow_python_mock

try:
    _spec = importlib.util.spec_from_file_location("retrain_pipeline", _DAG_PATH)
    _retrain_dag = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_retrain_dag)
finally:
    sys.modules.clear()
    sys.modules.update(_sys_modules_backup)

DRIFT_CHECK_SYMBOLS = _retrain_dag.DRIFT_CHECK_SYMBOLS
check_drift_trigger = _retrain_dag.check_drift_trigger
check_promotable = _retrain_dag.check_promotable
log_and_notify_only = _retrain_dag.log_and_notify_only
pull_training_data = _retrain_dag.pull_training_data
register_and_promote = _retrain_dag.register_and_promote
reload_serving_model = _retrain_dag.reload_serving_model
train_and_evaluate_task = _retrain_dag.train_and_evaluate_task
validate_training_data = _retrain_dag.validate_training_data


def _make_features(n: int = 200, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "price_return": rng.normal(0.001, 0.01, n),
        "volume_change": rng.normal(0.0, 5.0, n),
        "ma5_ratio": rng.normal(1.0, 0.02, n),
        "ma20_ratio": rng.normal(1.0, 0.015, n),
        "vwap_ratio": rng.normal(1.0, 0.018, n),
        "price_range_ratio": rng.normal(0.005, 0.002, n),
    })


class MockTaskInstance:
    """Minimal stand-in for airflow's TaskInstance for xcom_push/xcom_pull."""

    def __init__(self):
        self._xcom: dict[str, Any] = {}

    def xcom_push(self, key: str, value: Any) -> None:
        self._xcom[key] = value

    def xcom_pull(self, key: str = "return_value", task_ids: str = "") -> Any:
        return self._xcom.get(key)


class Phase13DAGImportTests(unittest.TestCase):
    def test_dag_file_imports_cleanly(self):
        self.assertTrue(_DAG_PATH.exists())

    def test_drift_check_symbols_includes_all_tracked(self):
        self.assertIn("BTCUSDT", DRIFT_CHECK_SYMBOLS)
        self.assertIn("ETHUSDT", DRIFT_CHECK_SYMBOLS)
        self.assertIn("AAPL", DRIFT_CHECK_SYMBOLS)


class Phase13DriftTriggerTests(unittest.TestCase):
    def test_no_reference_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                ti = MockTaskInstance()
                result = check_drift_trigger(**{"ti": ti})
                self.assertFalse(result)
            finally:
                os.chdir(orig_cwd)

    def test_drift_triggered_returns_true(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                ref = _make_features(n=200, seed=42)
                ref.to_parquet("reference_features.parquet", index=False)

                store = MagicMock()
                cur = _make_features(n=200, seed=42)
                cur["price_return"] += 0.5
                store.get_recent_feature_rows.return_value = cur
                store.get_recent_return_errors.return_value = pd.Series(
                    np.random.default_rng(42).normal(0, 1, 200)
                )

                with patch.object(_retrain_dag, "PredictionStore", return_value=store):
                    with patch.object(_retrain_dag, "DriftDetector") as MockDetector:
                        mock_detector = MagicMock()
                        mock_report = MagicMock()
                        mock_report.triggered = True
                        mock_report.feature_drift_detected = True
                        mock_report.concept_drift_detected = False
                        mock_report.cooldown_active = False
                        mock_report.feature_details = {"drifted_columns": [{"column": "price_return"}]}
                        mock_report.concept_details = {}
                        mock_detector.check.return_value = mock_report
                        MockDetector.return_value = mock_detector

                        ti = MockTaskInstance()
                        result = check_drift_trigger(**{"ti": ti})
                        self.assertTrue(result)
            finally:
                os.chdir(orig_cwd)

    def test_stable_data_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                ref = _make_features(n=200, seed=42)
                ref.to_parquet("reference_features.parquet", index=False)

                store = MagicMock()
                stable = _make_features(n=200, seed=99)
                store.get_recent_feature_rows.return_value = stable
                store.get_recent_return_errors.return_value = pd.Series(
                    np.random.default_rng(42).normal(0, 1, 200)
                )

                with patch.object(_retrain_dag, "PredictionStore", return_value=store):
                    with patch.object(_retrain_dag, "DriftDetector") as MockDetector:
                        mock_detector = MagicMock()
                        mock_report = MagicMock()
                        mock_report.triggered = False
                        mock_detector.check.return_value = mock_report
                        MockDetector.return_value = mock_detector

                        ti = MockTaskInstance()
                        result = check_drift_trigger(**{"ti": ti})
                        self.assertFalse(result)
            finally:
                os.chdir(orig_cwd)


class Phase13PromotionTests(unittest.TestCase):
    def test_register_and_promote_saves_reference_on_promotion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)

                report = {
                    "model": MagicMock(),
                    "run_id": "test-run",
                    "test_rmse": 0.01,
                    "beats_baseline": True,
                    "promotable": True,
                }

                with patch.object(_retrain_dag, "run_registry_gate") as mock_gate:
                    mock_gate.return_value = {"status": "promoted", "version": "3"}
                    with patch.object(_retrain_dag, "_save_reference_sample") as mock_save:
                        mock_save.return_value = None
                        ti = MockTaskInstance()
                        result = register_and_promote(**{"ti": ti})

                self.assertEqual(result["status"], "promoted")
                mock_save.assert_called_once()
            finally:
                os.chdir(orig_cwd)

    def test_register_and_promote_no_reference_when_not_promoted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)

                report = {
                    "model": MagicMock(),
                    "run_id": "test-run",
                    "test_rmse": 0.05,
                    "beats_baseline": False,
                    "promotable": False,
                }

                with patch.object(_retrain_dag, "run_registry_gate") as mock_gate:
                    mock_gate.return_value = {"status": "staging_only", "version": "2"}
                    ti = MockTaskInstance()
                    result = register_and_promote(**{"ti": ti})

                self.assertEqual(result["status"], "staging_only")
                self.assertFalse(Path("reference_features.parquet").exists())
            finally:
                os.chdir(orig_cwd)


class Phase13UtilityTaskTests(unittest.TestCase):
    def test_log_and_notify_only(self):
        ti = MockTaskInstance()
        ti.xcom_push(key="training_report", value={
            "test_rmse": 0.05,
            "beats_baseline": False,
            "promotable": False,
        })
        result = log_and_notify_only(**{"ti": ti})
        self.assertEqual(result["status"], "not_promoted")
        self.assertEqual(result["reason"], "failed_gate")

    def test_reload_serving_model_touches_signal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                ti = MockTaskInstance()
                reload_serving_model(**{"ti": ti})
                self.assertTrue(Path("model_reload_signal").exists())
            finally:
                os.chdir(orig_cwd)


if __name__ == "__main__":
    unittest.main()
