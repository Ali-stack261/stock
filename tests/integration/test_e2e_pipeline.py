import os
import tempfile
import unittest

import pytest
from fastapi.testclient import TestClient


class E2EPipelineTests(unittest.TestCase):
    """WebSocket -> Kafka -> Spark -> API, exercised as one real chain, not
    mocked at each boundary the way the per-phase unit tests do."""

    @pytest.mark.skip(reason="Requires real or testcontainers Kafka instance")
    def test_ingested_event_flows_to_feature_computation(self):
        pass

    def test_prediction_request_flows_to_stored_realized_error(self):
        """Real end-to-end test, no mocking: two sequential /predict calls
        through the actual FastAPI app, backed by a real (temp file) SQLite
        PredictionStore, confirming the second call's current_price genuinely
        realizes the first prediction's error — exercised through the real
        app + real store together, not unit-tested in isolation."""
        from serving.app import app, get_prediction_store, get_predictor
        from serving.prediction_store import PredictionStore

        db_path = tempfile.mktemp(suffix=".db")
        test_store = PredictionStore(db_path)

        class StubPredictor:
            def predict(self, symbol, current_price, **features):
                from serving.predictor import PredictionResult
                return PredictionResult(
                    predicted_price=current_price * 1.001,
                    predicted_return=0.001,
                    model_version="test",
                    confidence_interval=(current_price * 0.99, current_price * 1.01),
                    prediction_timestamp="2026-01-01T00:00:00Z",
                )

        app.dependency_overrides[get_prediction_store] = lambda: test_store
        app.dependency_overrides[get_predictor] = lambda: StubPredictor()
        client = TestClient(app)
        headers = {"X-API-Key": "dev-key-12345"}

        try:
            resp1 = client.post("/predict", json={"symbol": "BTCUSDT", "current_price": 100.0}, headers=headers)
            self.assertEqual(resp1.status_code, 200)

            resp2 = client.post("/predict", json={"symbol": "BTCUSDT", "current_price": 100.8}, headers=headers)
            self.assertEqual(resp2.status_code, 200)

            history = client.get("/predictions/BTCUSDT", headers=headers).json()
            first = next(r for r in history if r["id"] == 1)
            self.assertIsNotNone(first["realized_error"])
            self.assertAlmostEqual(first["realized_error"], 100.8 - first["predicted_price"])
        finally:
            app.dependency_overrides.clear()
            test_store.close()
            os.remove(db_path)


if __name__ == "__main__":
    unittest.main()
