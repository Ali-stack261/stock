"""tests/unit/test_phase9.py – Phase 9 Model Serving tests.

Tests the FastAPI prediction service: health check, API key auth, rate
limiting, request/response logging, and the prediction endpoint (with a
mocked predictor so no Spark/MLflow is needed).
"""

import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from serving.app import RateLimiter, app, get_predictor
from serving.auth import DEFAULT_API_KEY, get_valid_api_keys
from serving.predictor import PredictionResult


class Phase9ServingTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def test_health_check_no_auth(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"status": "ok"})

    # ------------------------------------------------------------------
    # API key auth
    # ------------------------------------------------------------------
    def test_predict_without_api_key_returns_401(self):
        resp = self.client.post(
            "/predict",
            json={
                "symbol": "BTCUSDT",
                "current_price": 100.0,
            },
        )
        self.assertEqual(resp.status_code, 401)
        self.assertIn("API key", resp.json()["detail"])

    def test_predict_with_invalid_api_key_returns_401(self):
        resp = self.client.post(
            "/predict",
            json={"symbol": "BTCUSDT", "current_price": 100.0},
            headers={"X-API-Key": "wrong-key"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_get_valid_api_keys_from_env(self):
        import os

        old = os.environ.get("STOCK_API_KEYS")
        try:
            os.environ["STOCK_API_KEYS"] = "key-a,key-b,key-c"
            keys = get_valid_api_keys()
            self.assertEqual(keys, {"key-a", "key-b", "key-c"})
        finally:
            if old is not None:
                os.environ["STOCK_API_KEYS"] = old
            else:
                os.environ.pop("STOCK_API_KEYS", None)

    def test_get_valid_api_keys_default_when_env_unset(self):
        import os

        old = os.environ.pop("STOCK_API_KEYS", None)
        try:
            keys = get_valid_api_keys()
            self.assertEqual(keys, {DEFAULT_API_KEY})
        finally:
            if old is not None:
                os.environ["STOCK_API_KEYS"] = old

    # ------------------------------------------------------------------
    # Prediction endpoint (mocked predictor)
    # ------------------------------------------------------------------
    def test_predict_with_valid_api_key_returns_prediction(self):
        mock_predictor = MagicMock()
        mock_predictor.predict.return_value = PredictionResult(
            predicted_price=101.0,
            predicted_return=0.01,
            model_version="v1",
            confidence_interval=(100.0, 102.0),
            prediction_timestamp="2026-08-01T10:20:35Z",
        )
        app.dependency_overrides[get_predictor] = lambda: mock_predictor

        try:
            resp = self.client.post(
                "/predict",
                json={
                    "symbol": "BTCUSDT",
                    "current_price": 100.0,
                    "price_return": 0.001,
                    "volume_change": 5.0,
                    "ma5_ratio": 1.001,
                    "ma20_ratio": 0.998,
                    "vwap_ratio": 1.002,
                    "price_range_ratio": 0.005,
                },
                headers={"X-API-Key": DEFAULT_API_KEY},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertAlmostEqual(body["predicted_price"], 101.0)
            self.assertAlmostEqual(body["predicted_return"], 0.01)
            self.assertEqual(body["model_version"], "v1")
            self.assertEqual(body["confidence_interval"], [100.0, 102.0])
            self.assertEqual(body["prediction_timestamp"], "2026-08-01T10:20:35Z")

            # Verify the predictor was called with the right features.
            mock_predictor.predict.assert_called_once()
            call_args = mock_predictor.predict.call_args
            features = call_args[0][0]
            self.assertAlmostEqual(features["price_return"], 0.001)
            self.assertAlmostEqual(features["ma5_ratio"], 1.001)
            self.assertAlmostEqual(call_args[0][1], 100.0)  # current_price
        finally:
            app.dependency_overrides.clear()

    def test_predict_returns_503_when_no_production_model(self):
        mock_predictor = MagicMock()
        mock_predictor.predict.side_effect = RuntimeError("No production model found")
        app.dependency_overrides[get_predictor] = lambda: mock_predictor

        try:
            resp = self.client.post(
                "/predict",
                json={"symbol": "BTCUSDT", "current_price": 100.0},
                headers={"X-API-Key": DEFAULT_API_KEY},
            )
            self.assertEqual(resp.status_code, 503)
            self.assertIn("No production model", resp.json()["detail"])
        finally:
            app.dependency_overrides.clear()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------
    def test_rate_limiter_allows_under_limit(self):
        limiter = RateLimiter(window=60, max_reqs=3)
        for _ in range(3):
            limiter.check("client-1")  # should not raise

    def test_rate_limiter_blocks_over_limit(self):
        limiter = RateLimiter(window=60, max_reqs=2)
        limiter.check("client-1")
        limiter.check("client-1")
        with self.assertRaises(Exception) as ctx:
            limiter.check("client-1")
        self.assertIn("Rate limit exceeded", str(ctx.exception.detail))

    def test_rate_limiter_independent_per_client(self):
        limiter = RateLimiter(window=60, max_reqs=2)
        limiter.check("client-a")
        limiter.check("client-a")
        # client-b has its own budget
        limiter.check("client-b")  # should not raise


if __name__ == "__main__":
    unittest.main()