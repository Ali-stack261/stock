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
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import numpy as np
import pandas as pd

from monitoring.drift import DriftDetector, DriftReport


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


if __name__ == "__main__":
    unittest.main()
