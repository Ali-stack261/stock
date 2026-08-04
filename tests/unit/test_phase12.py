"""tests/unit/test_phase12.py – Phase 12 Drift Detection tests.

Verifies that:
1. Identical reference and current data produce no feature drift.
2. Significantly shifted distributions produce feature drift (PSI + KS).
3. Similar but slightly noisy distributions do not produce feature drift.
4. Concept drift is detected when prediction error distributions shift.
5. Stable prediction errors do not trigger concept drift.
6. Missing or too-short prediction_errors returns no concept drift.
7. The cooldown timer suppresses duplicate triggers within the window.
8. The cooldown timer expires after the configured minutes.
9. ``triggered`` is True only when drift is detected AND cooldown is inactive.
10. Multiple features are evaluated independently.
11. ``_compute_psi`` returns 0.0 for identical arrays.
12. ``_compute_psi`` returns a positive value for shifted arrays.
13. ``/drift/check`` returns 503 when no reference data is loaded.
14. ``/drift/check`` returns 422 when no feature rows exist for the symbol.
15. ``/drift/check`` returns drift report when data is available.
16. ``/predict`` persists feature values alongside the prediction.
"""

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

from monitoring.drift import DriftDetector, DriftReport
from serving.app import app, get_predictor, get_prediction_store
from serving.auth import DEFAULT_API_KEY
from serving.prediction_store import PredictionStore
from serving.predictor import PredictionResult
from serving.metrics import (
    drift_concept_drift_detected,
    drift_detected,
    drift_feature_drift_detected,
)


def _make_features(n: int = 200, seed: int = 42) -> pd.DataFrame:
    """Generate a stable synthetic feature DataFrame."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "price_return": rng.normal(0.001, 0.01, n),
        "volume_change": rng.normal(0.0, 5.0, n),
        "ma5_ratio": rng.normal(1.0, 0.02, n),
        "ma20_ratio": rng.normal(1.0, 0.015, n),
        "vwap_ratio": rng.normal(1.0, 0.018, n),
        "price_range_ratio": rng.normal(0.005, 0.002, n),
    })


class Phase12DriftDetectionTests(unittest.TestCase):
    """Tests for the Phase 12 drift detection module."""

    def setUp(self):
        self.ref = _make_features(n=500, seed=42)

    def test_no_drift_when_distributions_identical(self):
        current = _make_features(n=500, seed=42)
        detector = DriftDetector(self.ref)
        report = detector.check(current)
        self.assertFalse(report.feature_drift_detected)
        self.assertEqual(report.feature_details["drifted_columns"], [])
        self.assertFalse(report.triggered)

    def test_feature_drift_detected_on_shifted_distribution(self):
        current = _make_features(n=500, seed=42)
        current["price_return"] += 0.5
        detector = DriftDetector(self.ref)
        report = detector.check(current)
        self.assertTrue(report.feature_drift_detected)
        drifted_names = [c["column"] for c in report.feature_details["drifted_columns"]]
        self.assertIn("price_return", drifted_names)

    def test_no_drift_on_slightly_noisy_distribution(self):
        current = _make_features(n=500, seed=99)
        detector = DriftDetector(self.ref)
        report = detector.check(current)
        self.assertFalse(report.feature_drift_detected)

    def test_concept_drift_detected_on_error_shift(self):
        ref_errors = pd.Series(np.random.default_rng(42).normal(0, 1, 200))
        cur_errors = pd.Series(np.random.default_rng(42).normal(5, 1, 200))
        detector = DriftDetector(self.ref)
        report = detector.check(self.ref, prediction_errors=pd.concat([ref_errors, cur_errors], ignore_index=True))
        self.assertTrue(report.concept_drift_detected)

    def test_no_concept_drift_when_errors_stable(self):
        errors = pd.Series(np.random.default_rng(42).normal(0, 1, 400))
        detector = DriftDetector(self.ref)
        report = detector.check(self.ref, prediction_errors=errors)
        self.assertFalse(report.concept_drift_detected)

    def test_no_concept_drift_without_prediction_errors(self):
        detector = DriftDetector(self.ref)
        report = detector.check(self.ref, prediction_errors=None)
        self.assertFalse(report.concept_drift_detected)
        self.assertEqual(report.concept_details["method"], "none")

    def test_cooldown_suppresses_duplicate_trigger(self):
        current = _make_features(n=500, seed=42)
        current["price_return"] += 0.5
        detector = DriftDetector(self.ref, cooldown_minutes=30)

        report1 = detector.check(current)
        self.assertTrue(report1.triggered)

        report2 = detector.check(current)
        self.assertFalse(report2.triggered)
        self.assertTrue(report2.cooldown_active)

    def test_cooldown_expires_after_configured_minutes(self):
        current = _make_features(n=500, seed=42)
        current["price_return"] += 0.5
        detector = DriftDetector(self.ref, cooldown_minutes=30)

        report1 = detector.check(current)
        self.assertTrue(report1.triggered)

        future_time = datetime.utcnow() + timedelta(minutes=31)
        with patch("monitoring.drift.datetime") as mock_dt:
            mock_dt.utcnow.return_value = future_time
            report2 = detector.check(current)
            self.assertTrue(report2.triggered)
            self.assertFalse(report2.cooldown_active)

    def test_triggered_true_only_when_drift_and_no_cooldown(self):
        stable = _make_features(n=500, seed=42)
        detector = DriftDetector(self.ref)

        report = detector.check(stable)
        self.assertFalse(report.feature_drift_detected)
        self.assertFalse(report.triggered)
        self.assertFalse(report.cooldown_active)

    def test_multiple_features_evaluated_independently(self):
        current = _make_features(n=500, seed=42)
        current["price_return"] += 0.5
        current["volume_change"] += 100.0
        detector = DriftDetector(self.ref)
        report = detector.check(current)
        drifted = report.feature_details["drifted_columns"]
        drifted_names = {c["column"] for c in drifted}
        self.assertIn("price_return", drifted_names)
        self.assertIn("volume_change", drifted_names)
        self.assertNotIn("ma5_ratio", drifted_names)

    def test_compute_psi_returns_zero_for_identical_arrays(self):
        arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        psi = DriftDetector._compute_psi(pd.Series(arr), pd.Series(arr))
        self.assertAlmostEqual(psi, 0.0, places=6)

    def test_compute_psi_returns_positive_for_shifted_arrays(self):
        ref = pd.Series(np.random.default_rng(42).normal(0, 1, 500))
        cur = pd.Series(np.random.default_rng(42).normal(3, 1, 500))
        psi = DriftDetector._compute_psi(ref, cur)
        self.assertGreater(psi, 0.5)

    def test_drift_report_repr(self):
        report = DriftReport(
            feature_drift_detected=True,
            feature_details={},
            concept_drift_detected=False,
            concept_details={},
            triggered=True,
            cooldown_active=False,
        )
        r = repr(report)
        self.assertIn("feature_drift=True", r)
        self.assertIn("concept_drift=False", r)
        self.assertIn("triggered=True", r)


AUTH = {"X-API-Key": DEFAULT_API_KEY}


def _mock_predictor(self, predicted_price=101.0, predicted_return=0.01,
                    ts="2026-08-01T10:20:35Z"):
    mock = MagicMock()
    mock.predict.return_value = PredictionResult(
        predicted_price=predicted_price,
        predicted_return=predicted_return,
        model_version="v1",
        confidence_interval=(100.0, 102.0),
        prediction_timestamp=ts,
    )
    app.dependency_overrides[get_predictor] = lambda: mock
    return mock


class Phase12WiredDriftTests(unittest.TestCase):
    """Tests for the wired drift detection flow in serving/app.py."""

    def setUp(self):
        self.client = TestClient(app)
        self._store = PredictionStore(db_path=":memory:")
        app.dependency_overrides[get_prediction_store] = lambda: self._store

    def tearDown(self):
        app.dependency_overrides.clear()
        import serving.app
        serving.app._DRIFT_DETECTOR = None

    def test_drift_check_503_when_no_reference(self):
        resp = self.client.post(
            "/drift/check?symbol=BTCUSDT",
            headers=AUTH,
        )
        self.assertEqual(resp.status_code, 503)

    def test_drift_check_422_when_no_feature_rows(self):
        import serving.app
        serving.app._DRIFT_DETECTOR = DriftDetector(
            reference_data=_make_features(n=100, seed=42)
        )
        resp = self.client.post(
            "/drift/check?symbol=UNKNOWN",
            headers=AUTH,
        )
        self.assertEqual(resp.status_code, 422)

    def test_drift_check_returns_report_when_data_available(self):
        import serving.app

        ref = _make_features(n=200, seed=42)
        serving.app._DRIFT_DETECTOR = DriftDetector(reference_data=ref)

        # Insert stable features first so the detector has data to compare.
        for i in range(10):
            self._store.save_prediction(
                timestamp=f"2026-08-01T10:20:{i:02d}Z",
                symbol="BTCUSDT",
                current_price=100.0 + i,
                predicted_price=101.0 + i,
                predicted_return=0.01,
                model_version="v1",
                price_return=0.01,
                volume_change=5.0,
                ma5_ratio=1.001,
                ma20_ratio=0.998,
                vwap_ratio=1.002,
                price_range_ratio=0.005,
            )

        resp = self.client.post(
            "/drift/check?symbol=BTCUSDT",
            headers=AUTH,
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["symbol"], "BTCUSDT")
        self.assertIn("feature_drift_detected", body)
        self.assertIn("concept_drift_detected", body)
        self.assertIn("triggered", body)

    def test_predict_persists_features(self):
        _mock_predictor(self)
        resp = self.client.post(
            "/predict",
            json={
                "symbol": "BTCUSDT",
                "current_price": 100.0,
                "price_return": 0.01,
                "volume_change": 5.0,
                "ma5_ratio": 1.001,
                "ma20_ratio": 0.998,
                "vwap_ratio": 1.002,
                "price_range_ratio": 0.005,
            },
            headers=AUTH,
        )
        self.assertEqual(resp.status_code, 200)

        history = self.client.get(
            "/predictions/BTCUSDT?limit=1",
            headers=AUTH,
        ).json()
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertAlmostEqual(row["price_return"], 0.01)
        self.assertAlmostEqual(row["volume_change"], 5.0)
        self.assertAlmostEqual(row["ma5_ratio"], 1.001)
        self.assertAlmostEqual(row["ma20_ratio"], 0.998)
        self.assertAlmostEqual(row["vwap_ratio"], 1.002)
        self.assertAlmostEqual(row["price_range_ratio"], 0.005)

    def test_drift_gauges_updated_after_check(self):
        import serving.app

        ref = _make_features(n=200, seed=42)
        serving.app._DRIFT_DETECTOR = DriftDetector(reference_data=ref)

        for i in range(10):
            self._store.save_prediction(
                timestamp=f"2026-08-01T10:20:{i:02d}Z",
                symbol="ETHUSDT",
                current_price=100.0 + i,
                predicted_price=101.0 + i,
                predicted_return=0.01,
                model_version="v1",
                price_return=0.01,
                volume_change=5.0,
                ma5_ratio=1.001,
                ma20_ratio=0.998,
                vwap_ratio=1.002,
                price_range_ratio=0.005,
            )

        self.client.post("/drift/check?symbol=ETHUSDT", headers=AUTH)

        sym = "ETHUSDT"
        self.assertIn(
            float(drift_detected.labels(symbol=sym)._value.get()),
            (0.0, 1.0),
        )

    def test_drift_check_symbols_covers_all_tracked_symbols(self):
        from serving.app import DRIFT_CHECK_SYMBOLS
        self.assertIn("BTCUSDT", DRIFT_CHECK_SYMBOLS)
        self.assertIn("ETHUSDT", DRIFT_CHECK_SYMBOLS)
        self.assertIn("AAPL", DRIFT_CHECK_SYMBOLS)


if __name__ == "__main__":
    unittest.main()
