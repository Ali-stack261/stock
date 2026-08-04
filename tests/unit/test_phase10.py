"""tests/unit/test_phase10.py – Phase 10 Prediction Storage tests.

Tests the SQLite-backed PredictionStore and the new API endpoints that
expose stored predictions and rolling accuracy metrics.
"""

import unittest
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from serving.app import app, get_prediction_store, get_predictor
from serving.auth import DEFAULT_API_KEY
from serving.prediction_store import PredictionStore
from serving.predictor import PredictionResult


class PredictionStoreTests(unittest.TestCase):
    """Unit tests for the PredictionStore (no FastAPI, no Spark)."""

    def setUp(self):
        self.store = PredictionStore(db_path=":memory:")

    def test_save_and_get_prediction(self):
        pid = self.store.save_prediction(
            timestamp="2026-08-01T10:20:35Z",
            symbol="BTCUSDT",
            current_price=118420.0,
            predicted_price=118510.0,
            predicted_return=0.0008,
            model_version="v1",
            price_return=0.001,
            volume_change=5.0,
            ma5_ratio=1.001,
            ma20_ratio=0.998,
            vwap_ratio=1.002,
            price_range_ratio=0.005,
        )
        self.assertGreater(pid, 0)

        record = self.store.get_prediction(pid)
        self.assertIsNotNone(record)
        self.assertEqual(record.symbol, "BTCUSDT")
        self.assertAlmostEqual(record.current_price, 118420.0)
        self.assertAlmostEqual(record.predicted_price, 118510.0)
        self.assertEqual(record.model_version, "v1")
        self.assertIsNone(record.realized_error)
        self.assertAlmostEqual(record.price_return, 0.001)
        self.assertAlmostEqual(record.volume_change, 5.0)
        self.assertAlmostEqual(record.ma5_ratio, 1.001)
        self.assertAlmostEqual(record.ma20_ratio, 0.998)
        self.assertAlmostEqual(record.vwap_ratio, 1.002)
        self.assertAlmostEqual(record.price_range_ratio, 0.005)

    def test_save_prediction_without_features_backward_compat(self):
        pid = self.store.save_prediction(
            timestamp="2026-08-01T10:20:35Z",
            symbol="BTCUSDT",
            current_price=100.0,
            predicted_price=101.0,
            predicted_return=0.01,
            model_version="v1",
        )
        record = self.store.get_prediction(pid)
        self.assertIsNotNone(record)
        self.assertIsNone(record.price_return)
        self.assertIsNone(record.volume_change)

    def test_get_recent_feature_rows(self):
        for i in range(5):
            self.store.save_prediction(
                timestamp=f"2026-08-01T10:20:{i:02d}Z",
                symbol="BTCUSDT",
                current_price=100.0 + i,
                predicted_price=101.0 + i,
                predicted_return=0.01,
                model_version="v1",
                price_return=0.01 + i * 0.001,
                volume_change=5.0 + i,
                ma5_ratio=1.001,
                ma20_ratio=0.998,
                vwap_ratio=1.002,
                price_range_ratio=0.005,
            )
        df = self.store.get_recent_feature_rows("BTCUSDT", limit=3)
        self.assertEqual(len(df), 3)
        self.assertIn("price_return", df.columns)
        self.assertIn("ma5_ratio", df.columns)

    def test_get_recent_return_errors(self):
        self.store.save_prediction(
            timestamp="t1", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        self.store.save_prediction(
            timestamp="t2", symbol="BTCUSDT", current_price=100.0,
            predicted_price=102.0, predicted_return=0.02, model_version="v1",
        )
        self.store.backfill_realized_errors({"t1": 101.5, "t2": 100.0})

        s = self.store.get_recent_return_errors("BTCUSDT", limit=2)
        self.assertEqual(len(s), 2)
        # DESC order: t2 (newest) first, then t1.
        # t2: actual_return = (100.0 - 100) / 100 = 0.0, return_error = 0.0 - 0.02 = -0.02
        self.assertAlmostEqual(s.iloc[0], -0.02)
        # t1: actual_return = (101.5 - 100) / 100 = 0.015, return_error = 0.015 - 0.01 = 0.005
        self.assertAlmostEqual(s.iloc[1], 0.005)

    def test_get_recent_predictions(self):
        for i in range(5):
            self.store.save_prediction(
                timestamp=f"2026-08-01T10:20:{i:02d}Z",
                symbol="BTCUSDT",
                current_price=100.0 + i,
                predicted_price=101.0 + i,
                predicted_return=0.01,
                model_version="v1",
            )
        records = self.store.get_recent_predictions("BTCUSDT", limit=3)
        self.assertEqual(len(records), 3)
        # Most recent first (descending by timestamp).
        self.assertEqual(records[0].timestamp, "2026-08-01T10:20:04Z")

    def test_get_unrealized_predictions(self):
        self.store.save_prediction(
            timestamp="t1", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        self.store.save_prediction(
            timestamp="t2", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        unrealized = self.store.get_unrealized_predictions()
        self.assertEqual(len(unrealized), 2)

        # Backfill one realized price.
        updated = self.store.backfill_realized_errors({"t1": 101.5})
        self.assertEqual(updated, 1)

        unrealized = self.store.get_unrealized_predictions()
        self.assertEqual(len(unrealized), 1)
        self.assertEqual(unrealized[0].timestamp, "t2")

    def test_backfill_realized_errors(self):
        self.store.save_prediction(
            timestamp="t1", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        self.store.save_prediction(
            timestamp="t2", symbol="BTCUSDT", current_price=100.0,
            predicted_price=102.0, predicted_return=0.02, model_version="v1",
        )

        updated = self.store.backfill_realized_errors({"t1": 101.5, "t2": 101.0})
        self.assertEqual(updated, 2)

        rec1 = self.store.get_prediction(1)
        self.assertAlmostEqual(rec1.realized_error, 0.5)   # 101.5 - 101.0
        rec2 = self.store.get_prediction(2)
        self.assertAlmostEqual(rec2.realized_error, -1.0)  # 101.0 - 102.0

    def test_compute_rolling_rmse(self):
        # No realized predictions yet -> None.
        self.assertIsNone(self.store.compute_rolling_rmse())

        self.store.save_prediction(
            timestamp="t1", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        self.store.save_prediction(
            timestamp="t2", symbol="BTCUSDT", current_price=100.0,
            predicted_price=103.0, predicted_return=0.03, model_version="v1",
        )
        self.store.backfill_realized_errors({"t1": 101.0, "t2": 100.0})

        # Errors: 0.0 and -3.0 -> RMSE = sqrt((0 + 9) / 2) = sqrt(4.5)
        rmse = self.store.compute_rolling_rmse()
        self.assertAlmostEqual(rmse, (4.5) ** 0.5)

    def test_compute_rolling_mae(self):
        self.store.save_prediction(
            timestamp="t1", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        self.store.save_prediction(
            timestamp="t2", symbol="BTCUSDT", current_price=100.0,
            predicted_price=103.0, predicted_return=0.03, model_version="v1",
        )
        self.store.backfill_realized_errors({"t1": 101.0, "t2": 100.0})

        # |errors|: 0.0 and 3.0 -> MAE = 1.5
        mae = self.store.compute_rolling_mae()
        self.assertAlmostEqual(mae, 1.5)

    def test_compute_rolling_rmse_per_symbol(self):
        self.store.save_prediction(
            timestamp="t1", symbol="BTCUSDT", current_price=100.0,
            predicted_price=101.0, predicted_return=0.01, model_version="v1",
        )
        self.store.save_prediction(
            timestamp="t2", symbol="ETHUSDT", current_price=50.0,
            predicted_price=51.0, predicted_return=0.02, model_version="v1",
        )
        self.store.backfill_realized_errors({"t1": 101.0, "t2": 50.0})

        # BTCUSDT: error 0.0 -> RMSE 0.0
        self.assertAlmostEqual(self.store.compute_rolling_rmse("BTCUSDT"), 0.0)
        # ETHUSDT: error -1.0 -> RMSE 1.0
        self.assertAlmostEqual(self.store.compute_rolling_rmse("ETHUSDT"), 1.0)


class Phase10APITests(unittest.TestCase):
    """Tests for the new API endpoints (mocked predictor, in-memory store)."""

    def setUp(self):
        self.client = TestClient(app)
        # Use a single in-memory store instance so data persists across
        # requests within a test (each PredictionStore(":memory:") creates
        # its own isolated DB, so we must reuse the same instance).
        self._store = PredictionStore(db_path=":memory:")
        app.dependency_overrides[get_prediction_store] = lambda: self._store

    def tearDown(self):
        app.dependency_overrides.clear()

    def _mock_predictor(self, predicted_price=101.0, predicted_return=0.01):
        mock = MagicMock()
        mock.predict.return_value = PredictionResult(
            predicted_price=predicted_price,
            predicted_return=predicted_return,
            model_version="v1",
            confidence_interval=(100.0, 102.0),
            prediction_timestamp="2026-08-01T10:20:35Z",
        )
        app.dependency_overrides[get_predictor] = lambda: mock
        return mock

    def test_predict_stores_prediction(self):
        self._mock_predictor()
        resp = self.client.post(
            "/predict",
            json={"symbol": "BTCUSDT", "current_price": 100.0},
            headers={"X-API-Key": DEFAULT_API_KEY},
        )
        self.assertEqual(resp.status_code, 200)

        # The prediction should be retrievable via the history endpoint.
        hist = self.client.get(
            "/predictions/BTCUSDT",
            headers={"X-API-Key": DEFAULT_API_KEY},
        )
        self.assertEqual(hist.status_code, 200)
        body = hist.json()
        self.assertEqual(len(body), 1)
        self.assertEqual(body[0]["symbol"], "BTCUSDT")
        self.assertAlmostEqual(body[0]["predicted_price"], 101.0)
        self.assertEqual(body[0]["model_version"], "v1")
        self.assertIsNone(body[0]["realized_error"])

    def test_prediction_history_requires_auth(self):
        resp = self.client.get("/predictions/BTCUSDT")
        self.assertEqual(resp.status_code, 401)

    def test_prediction_history_empty(self):
        resp = self.client.get(
            "/predictions/ETHUSDT",
            headers={"X-API-Key": DEFAULT_API_KEY},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), [])

    def test_accuracy_metrics_no_realized(self):
        resp = self.client.get(
            "/metrics/accuracy",
            headers={"X-API-Key": DEFAULT_API_KEY},
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIsNone(body["rolling_rmse"])
        self.assertIsNone(body["rolling_mae"])

    def test_accuracy_metrics_with_realized(self):
        self._mock_predictor()
        # Make a prediction.
        self.client.post(
            "/predict",
            json={"symbol": "BTCUSDT", "current_price": 100.0},
            headers={"X-API-Key": DEFAULT_API_KEY},
        )
        # Backfill the realized price directly via the store.
        self._store.backfill_realized_errors({"2026-08-01T10:20:35Z": 101.5})

        resp = self.client.get(
            "/metrics/accuracy?symbol=BTCUSDT",
            headers={"X-API-Key": DEFAULT_API_KEY},
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIsNotNone(body["rolling_rmse"])
        self.assertIsNotNone(body["rolling_mae"])


    def test_predict_backfills_previous_prediction_for_same_symbol(self):
        """Second /predict call for a symbol realizes the first prediction's error."""
        AUTH = {"X-API-Key": DEFAULT_API_KEY}

        # ---- first call: no prior prediction to realize ----
        mock1 = MagicMock()
        mock1.predict.return_value = PredictionResult(
            predicted_price=101.0,
            predicted_return=0.01,
            model_version="v1",
            confidence_interval=(100.0, 102.0),
            prediction_timestamp="2026-08-01T10:20:35Z",
        )
        app.dependency_overrides[get_predictor] = lambda: mock1
        resp1 = self.client.post("/predict", json={"symbol": "BTCUSDT", "current_price": 100.0}, headers=AUTH)
        self.assertEqual(resp1.status_code, 200)

        # ---- second call: current_price = 100.8 realizes the first prediction ----
        mock2 = MagicMock()
        mock2.predict.return_value = PredictionResult(
            predicted_price=101.5,
            predicted_return=0.015,
            model_version="v1",
            confidence_interval=(100.0, 102.0),
            prediction_timestamp="2026-08-01T10:20:36Z",
        )
        app.dependency_overrides[get_predictor] = lambda: mock2
        resp2 = self.client.post("/predict", json={"symbol": "BTCUSDT", "current_price": 100.8}, headers=AUTH)
        self.assertEqual(resp2.status_code, 200)

        # The history endpoint returns most-recent-first; find the first prediction by ID.
        history = self.client.get("/predictions/BTCUSDT?limit=10", headers=AUTH).json()
        first_prediction = next(r for r in history if r["id"] == 1)
        self.assertIsNotNone(first_prediction["realized_error"])
        self.assertAlmostEqual(
            first_prediction["realized_error"], 100.8 - first_prediction["predicted_price"]
        )

        # The second (most-recent) prediction should still be unrealized.
        second_prediction = next(r for r in history if r["id"] == 2)
        self.assertIsNone(second_prediction["realized_error"])


if __name__ == "__main__":
    unittest.main()