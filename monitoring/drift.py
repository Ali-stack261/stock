"""monitoring/drift.py – Phase 12 Drift Detection

Compares live feature data and prediction errors against the training
(reference) dataset using statistical tests:

- **Population Stability Index (PSI)** — measures distribution shift.
- **Kolmogorov-Smirnov (KS) test** — tests whether two samples come from
  the same distribution.

If significant drift is detected on features (data drift) or on prediction
errors (concept drift), the detector returns ``triggered=True`` so that an
upstream retraining pipeline can react.  A cooldown timer prevents
retrain storms by suppressing duplicate triggers within a configurable
window.

Evidently AI is used when available; otherwise the module falls back to
lightweight scipy / numpy implementations so it remains testable in
minimal environments.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

logger = logging.getLogger(__name__)


class DriftReport:
    """Container for the result of a drift check."""

    def __init__(
        self,
        feature_drift_detected: bool,
        feature_details: dict,
        concept_drift_detected: bool,
        concept_details: dict,
        triggered: bool,
        cooldown_active: bool,
    ):
        self.feature_drift_detected = feature_drift_detected
        self.feature_details = feature_details
        self.concept_drift_detected = concept_drift_detected
        self.concept_details = concept_details
        self.triggered = triggered
        self.cooldown_active = cooldown_active

    def __repr__(self) -> str:
        return (
            f"DriftReport(feature_drift={self.feature_drift_detected}, "
            f"concept_drift={self.concept_drift_detected}, "
            f"triggered={self.triggered}, cooldown={self.cooldown_active})"
        )


class DriftDetector:
    """Detect feature and concept drift between reference and current data.

    Parameters
    ----------
    reference_data:
        Training dataset used as the baseline distribution.
    cooldown_minutes:
        Minimum minutes between automatic retrain triggers.
    psi_threshold:
        PSI score above which a feature is considered drifted.
    ks_alpha:
        Significance level for the KS test (p-value below this = drift).
    """

    def __init__(
        self,
        reference_data: pd.DataFrame,
        cooldown_minutes: int = 30,
        psi_threshold: float = 0.2,
        ks_alpha: float = 0.05,
        initial_last_trigger_time: datetime | None = None,
    ):
        self.reference_data = reference_data
        self.cooldown_minutes = cooldown_minutes
        self.psi_threshold = psi_threshold
        self.ks_alpha = ks_alpha
        self._last_trigger_time = initial_last_trigger_time

    @property
    def last_trigger_time(self) -> datetime | None:
        """Return the last time a retrain trigger fired."""
        return self._last_trigger_time

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def _is_cooled_down(self) -> bool:
        if self._last_trigger_time is None:
            return True
        return datetime.now(timezone.utc) - self._last_trigger_time >= timedelta(
            minutes=self.cooldown_minutes
        )

    def check(
        self,
        current_data: pd.DataFrame,
        prediction_errors: pd.Series | None = None,
    ) -> DriftReport:
        """Run drift detection on the supplied current data.

        Parameters
        ----------
        current_data:
            Live feature observations to compare against the reference.
        prediction_errors:
            Optional series of prediction errors (actual - predicted) used
            for concept drift detection.

        Returns
        -------
        DriftReport
            Structured result including whether drift was detected and
            whether the cooldown-gated retrain trigger fired.
        """
        feature_drift = self._check_feature_drift(current_data)
        concept_drift = self._check_concept_drift(prediction_errors)

        any_drift = feature_drift["detected"] or concept_drift["detected"]
        cooldown_active = not self._is_cooled_down()

        if any_drift and not cooldown_active:
            self._last_trigger_time = datetime.now(timezone.utc)
            logger.warning(
                "Drift detected — retrain trigger fired (cooldown=%d min)",
                self.cooldown_minutes,
            )
        elif any_drift and cooldown_active:
            logger.info(
                "Drift detected but retrain suppressed by cooldown (last trigger=%s)",
                self._last_trigger_time,
            )

        triggered = any_drift and not cooldown_active

        return DriftReport(
            feature_drift_detected=feature_drift["detected"],
            feature_details=feature_drift,
            concept_drift_detected=concept_drift["detected"],
            concept_details=concept_drift,
            triggered=triggered,
            cooldown_active=cooldown_active,
        )

    # ------------------------------------------------------------------
    # Feature drift
    # ------------------------------------------------------------------
    def _check_feature_drift(self, current_data: pd.DataFrame) -> dict:
        try:
            return self._check_feature_drift_evidently(current_data)
        except ImportError:
            return self._check_feature_drift_fallback(current_data)

    def _check_feature_drift_evidently(self, current_data: pd.DataFrame) -> dict:
        from evidently.metric_preset import DataDriftPreset
        from evidently.report import Report

        report = Report(metrics=[DataDriftPreset()])
        report.run(reference_data=self.reference_data, current_data=current_data)
        result = report.as_dict()

        drifted_columns = []
        for metric in result.get("metrics", []):
            if metric["metric"] == "DataDriftTable":
                for col in metric["result"]["drift_by_columns"]:
                    if col["drift_detected"]:
                        drifted_columns.append(
                            {
                                "column": col["column_name"],
                                "psi_score": col.get("psi", {}).get("score"),
                                "ks_test_p_value": col.get("ks_test", {}).get("p_value"),
                            }
                        )

        return {
            "detected": len(drifted_columns) > 0,
            "drifted_columns": drifted_columns,
            "method": "evidently",
        }

    def _check_feature_drift_fallback(self, current_data: pd.DataFrame) -> dict:
        common_cols = [
            c for c in self.reference_data.columns if c in current_data.columns
        ]
        drifted = []

        for col in common_cols:
            ref_vals = self.reference_data[col].dropna()
            cur_vals = current_data[col].dropna()
            if len(ref_vals) < 2 or len(cur_vals) < 2:
                continue
            ks_stat, p_value = ks_2samp(ref_vals, cur_vals)
            psi = self._compute_psi(ref_vals, cur_vals)
            if p_value < self.ks_alpha or psi > self.psi_threshold:
                drifted.append(
                    {
                        "column": col,
                        "psi_score": psi,
                        "ks_test_p_value": p_value,
                        "ks_statistic": ks_stat,
                    }
                )

        return {
            "detected": len(drifted) > 0,
            "drifted_columns": drifted,
            "method": "scipy",
        }

    # ------------------------------------------------------------------
    # Concept drift
    # ------------------------------------------------------------------
    def _check_concept_drift(self, prediction_errors: pd.Series | None = None) -> dict:
        if prediction_errors is None or len(prediction_errors) < 4:
            return {
                "detected": False,
                "details": "no prediction errors provided",
                "method": "none",
            }

        try:
            return self._check_concept_drift_evidently(prediction_errors)
        except ImportError:
            return self._check_concept_drift_fallback(prediction_errors)

    def _check_concept_drift_evidently(self, prediction_errors: pd.Series) -> dict:
        from evidently.metrics import RegressionErrorDistribution
        from evidently.report import Report

        mid = len(prediction_errors) // 2
        ref_errors = prediction_errors.iloc[:mid] if mid > 0 else prediction_errors
        cur_errors = prediction_errors.iloc[mid:]

        ref_df = pd.DataFrame(
            {"prediction": ref_errors.values, "target": ref_errors.values}
        )
        cur_df = pd.DataFrame(
            {"prediction": cur_errors.values, "target": cur_errors.values}
        )

        report = Report(metrics=[RegressionErrorDistribution()])
        report.run(reference_data=ref_df, current_data=cur_df)
        result = report.as_dict()

        for metric in result.get("metrics", []):
            if metric["metric"] == "RegressionErrorDistribution":
                drift_detected = metric["result"].get("drift_detected", False)
                return {
                    "detected": drift_detected,
                    "details": metric["result"],
                    "method": "evidently",
                }

        return {"detected": False, "details": {}, "method": "evidently"}

    def _check_concept_drift_fallback(self, prediction_errors: pd.Series) -> dict:
        mid = len(prediction_errors) // 2
        ref_errors = prediction_errors.iloc[:mid]
        cur_errors = prediction_errors.iloc[mid:]

        if len(ref_errors) < 2 or len(cur_errors) < 2:
            return {
                "detected": False,
                "details": "insufficient data for concept drift test",
                "method": "scipy",
            }

        ks_stat, p_value = ks_2samp(ref_errors, cur_errors)
        psi = self._compute_psi(ref_errors, cur_errors)

        return {
            "detected": p_value < self.ks_alpha or psi > self.psi_threshold,
            "details": {
                "ks_statistic": ks_stat,
                "ks_test_p_value": p_value,
                "psi_score": psi,
            },
            "method": "scipy",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_psi(reference: pd.Series, current: pd.Series, bins: int = 10) -> float:
        """Compute Population Stability Index between two distributions."""
        ref_counts, bin_edges = np.histogram(reference, bins=bins)
        cur_counts, _ = np.histogram(current, bins=bin_edges)

        ref_pct = ref_counts / len(reference)
        cur_pct = cur_counts / len(current)

        ref_pct = np.where(ref_pct == 0, 1e-10, ref_pct)
        cur_pct = np.where(cur_pct == 0, 1e-10, cur_pct)

        psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
        return float(psi)
