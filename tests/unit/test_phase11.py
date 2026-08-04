"""tests/unit/test_phase11.py – Phase 11 Prometheus monitoring tests.

Verifies that:
1. The /metrics scrape endpoint exists and returns 200 with text/plain content.
2. Successful /predict calls increment predict_requests_total{status="ok"}.
3. Failed /predict calls (model error) increment predict_errors_total and
   predict_requests_total{status="error"}.
4. The rolling_rmse and rolling_mae gauges are updated after a prediction
   is realized (second /predict call for the same symbol).
5. unrealized_predictions_total gauge reflects the backlog after each save.
"""

import unittest
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from serving.app import app, get_prediction_store, get_predictor
from serving.auth import DEFAULT_API_KEY
from serving.prediction_store import PredictionStore
from serving.predictor import PredictionResult

AUTH = {"X-API-Key": DEFAULT_API_KEY}


def _get_metric_value(metric_name: str, labels: dict) -> float:
    """Read a current sample value from the default Prometheus registry.

    For counters (e.g. ``predict_requests_total``), pass the metric name
    as defined — this version of prometheus-client stores the sample under
    the same name (no extra ``_total`` suffix on the sample).
    For histogram _count/_sum samples, pass the full sample name.
    Strip the ``le`` bucket label automatically so callers don't need to
    include it.
    """
    label_key = tuple(sorted(labels.items()))
    for metric in REGISTRY.collect():
        for sample in metric.samples:
            if sample.name == metric_name:
                sample_labels = tuple(
                    sorted((k, v) for k, v in sample.labels.items() if k != "le")
                )
                if sample_labels == label_key:
                    return sample.value
    return 0.0


class Phase11MetricsEndpointTests(unittest.TestCase):
    """Tests for the /metrics Prometheus scrape endpoint."""

    def setUp(self):
        self.client = TestClient(app)

    def test_metrics_endpoint_returns_200(self):
        resp = self.client.get("/prometheus")
        self.assertEqual(resp.status_code, 200)

    def test_metrics_endpoint_content_type_is_text(self):
        resp = self.client.get("/prometheus")
        self.assertIn("text/plain", resp.headers["content-type"])

    def test_metrics_endpoint_contains_predict_requests_total(self):
        resp = self.client.get("/prometheus")
        self.assertIn("predict_requests_total", resp.text)

    def test_metrics_endpoint_contains_predict_latency_seconds(self):
        resp = self.client.get("/prometheus")
        self.assertIn("predict_latency_seconds", resp.text)

    def test_metrics_endpoint_contains_rolling_rmse(self):
        resp = self.client.get("/prometheus")
        self.assertIn("rolling_rmse", resp.text)

    def test_metrics_endpoint_contains_rolling_mae(self):
        resp = self.client.get("/prometheus")
        self.assertIn("rolling_mae", resp.text)

    def test_metrics_endpoint_contains_unrealized_predictions_total(self):
        resp = self.client.get("/prometheus")
        self.assertIn("unrealized_predictions_total", resp.text)

    def test_metrics_endpoint_no_auth_required(self):
        """The /prometheus endpoint must be accessible without an API key."""
        resp = self.client.get("/prometheus")
        # Must not be 401 or 403.
        self.assertNotIn(resp.status_code, (401, 403))


class Phase11MetricsIncrementTests(unittest.TestCase):
    """Tests that verify Prometheus counters/gauges change correctly."""

    def setUp(self):
        self.client = TestClient(app)
        self._store = PredictionStore(db_path=":memory:")
        app.dependency_overrides[get_prediction_store] = lambda: self._store

    def tearDown(self):
        app.dependency_overrides.clear()

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

    # ------------------------------------------------------------------
    # /predict → ok path
    # ------------------------------------------------------------------
    def test_successful_predict_increments_ok_counter(self):
        self._mock_predictor()
        before = _get_metric_value(
            "predict_requests_total", {"symbol": "BTCUSDT", "status": "ok"}
        )
        self.client.post(
            "/predict",
            json={"symbol": "BTCUSDT", "current_price": 100.0},
            headers=AUTH,
        )
        after = _get_metric_value(
            "predict_requests_total", {"symbol": "BTCUSDT", "status": "ok"}
        )
        self.assertEqual(after - before, 1.0)

    def test_successful_predict_records_latency(self):
        self._mock_predictor()
        # Confirm the histogram count increases (latency was observed).
        before = _get_metric_value(
            "predict_latency_seconds_count", {"symbol": "BTCUSDT"}
        )  # histogram _count sample
        self.client.post(
            "/predict",
            json={"symbol": "BTCUSDT", "current_price": 100.0},
            headers=AUTH,
        )
        after = _get_metric_value(
            "predict_latency_seconds_count", {"symbol": "BTCUSDT"}
        )  # histogram _count sample
        self.assertGreater(after, before)

    # ------------------------------------------------------------------
    # /predict → error path (model raises RuntimeError)
    # ------------------------------------------------------------------
    def test_model_error_increments_error_counters(self):
        mock = MagicMock()
        mock.predict.side_effect = RuntimeError("no model loaded")
        app.dependency_overrides[get_predictor] = lambda: mock

        before_err = _get_metric_value(
            "predict_errors_total",
            {"symbol": "ETHUSDT", "error_type": "model_error"},
        )
        before_req = _get_metric_value(
            "predict_requests_total", {"symbol": "ETHUSDT", "status": "error"}
        )

        resp = self.client.post(
            "/predict",
            json={"symbol": "ETHUSDT", "current_price": 3000.0},
            headers=AUTH,
        )
        self.assertEqual(resp.status_code, 503)

        after_err = _get_metric_value(
            "predict_errors_total",
            {"symbol": "ETHUSDT", "error_type": "model_error"},
        )
        after_req = _get_metric_value(
            "predict_requests_total", {"symbol": "ETHUSDT", "status": "error"}
        )
        self.assertEqual(after_err - before_err, 1.0)
        self.assertEqual(after_req - before_req, 1.0)

    # ------------------------------------------------------------------
    # Gauge updates after realization
    # ------------------------------------------------------------------
    def test_rolling_rmse_gauge_set_after_second_predict(self):
        """After the second /predict, rolling_rmse gauge should be non-zero."""
        # First call
        self._mock_predictor(predicted_price=101.0, ts="2026-08-01T10:20:35Z")
        self.client.post(
            "/predict",
            json={"symbol": "SOLUSDT", "current_price": 100.0},
            headers=AUTH,
        )
        # Second call — realizes first prediction
        self._mock_predictor(predicted_price=102.0, ts="2026-08-01T10:20:36Z")
        self.client.post(
            "/predict",
            json={"symbol": "SOLUSDT", "current_price": 100.5},
            headers=AUTH,
        )
        rmse_val = _get_metric_value("rolling_rmse", {"symbol": "SOLUSDT"})
        # realized_error = 100.5 - 101.0 = -0.5  →  RMSE = 0.5
        self.assertAlmostEqual(rmse_val, 0.5, places=6)

    def test_unrealized_gauge_reflects_backlog(self):
        """unrealized_predictions_total should equal 1 after the first /predict."""
        self._mock_predictor()
        self.client.post(
            "/predict",
            json={"symbol": "BNBUSDT", "current_price": 500.0},
            headers=AUTH,
        )
        val = _get_metric_value(
            "unrealized_predictions_total", {"symbol": "BNBUSDT"}
        )
        self.assertEqual(val, 1.0)


if __name__ == "__main__":
    unittest.main()
