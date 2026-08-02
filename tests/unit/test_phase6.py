import os
import shutil
import tempfile
import uuid
from pathlib import Path
import unittest
import mlflow
from mlflow.tracking import MlflowClient

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streaming.spark_stream import build_spark_session
from training.train import (
    MODEL_FEATURE_COLS,
    chronological_split,
    evaluate_naive_baseline,
    evaluate_naive_return_baseline,
    predicted_return_to_price,
    prepare_training_data,
    should_promote_challenger,
    train_and_evaluate,
    train_gbt_model,
)


class Phase6Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"
        cls.spark = build_spark_session(app_name="test_phase6")
        cls._mlflow_tmp_dir = tempfile.mkdtemp()
        mlflow.set_tracking_uri(Path(cls._mlflow_tmp_dir).as_uri())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls._mlflow_tmp_dir, ignore_errors=True)

    def _sample_raw_data(self):
        schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("timestamp", StringType(), False),
            StructField("source", StringType(), False),
            StructField("idempotency_key", StringType(), False),
            StructField("received_at", StringType(), False),
        ])
        
        # A synthetic sequence of rising prices
        rows = []
        for i in range(20):
            rows.append((
                "BTCUSDT",
                100.0 + i,          # price steadily increases
                10.0,
                f"2026-08-01T00:00:{i:02d}",
                "binance",
                str(i),
                f"2026-08-01T00:00:{i:02d}"
            ))
        return self.spark.createDataFrame(rows, schema=schema)

    def test_prepare_training_data_creates_target_label(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        
        # Original length is 20. The last row has no "next" row, so it gets dropped.
        self.assertEqual(prepared_df.count(), 19)
        
        # Because prices went 100, 101, 102...
        # The first row's price should be 100 and target_price should be 101.
        first_row = prepared_df.orderBy("event_ts").first()
        self.assertEqual(first_row["price"], 100.0)
        self.assertEqual(first_row["target_price"], 101.0)
        # The return target is scale-invariant: (101 - 100) / 100 = 0.01
        self.assertAlmostEqual(first_row["target_return"], 0.01)

    def test_prepare_training_data_includes_ratio_features(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)

        cols = set(prepared_df.columns)
        # Raw price levels must never be fed to the tree model directly.
        # The scale-invariant ratio features must be present.
        self.assertIn("ma5_ratio", cols)
        self.assertIn("ma20_ratio", cols)
        self.assertIn("vwap_ratio", cols)
        self.assertIn("price_range_ratio", cols)
        for f in MODEL_FEATURE_COLS:
            self.assertIn(f, cols)
        # The model feature list itself must not contain raw price levels.
        self.assertNotIn("price", MODEL_FEATURE_COLS)
        self.assertNotIn("ma5", MODEL_FEATURE_COLS)
        self.assertNotIn("ma20", MODEL_FEATURE_COLS)

    def test_chronological_split_preserves_time_ordering(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        
        train_df, val_df, test_df = chronological_split(prepared_df, train_ratio=0.5, val_ratio=0.25)
        
        train_max_ts = train_df.agg({"event_ts": "max"}).collect()[0][0]
        val_min_ts = val_df.agg({"event_ts": "min"}).collect()[0][0]
        val_max_ts = val_df.agg({"event_ts": "max"}).collect()[0][0]
        test_min_ts = test_df.agg({"event_ts": "min"}).collect()[0][0]
        
        # Verify strict ordering in time
        self.assertLess(train_max_ts, val_min_ts)
        self.assertLess(val_max_ts, test_min_ts)

    def test_evaluate_naive_baseline(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        
        # Prices always go up by exactly 1.0. 
        # Naive prediction: next_price = current_price.
        # So naive error is exactly 1.0 for every row.
        # RMSE of 1.0 everywhere is 1.0.
        rmse = evaluate_naive_baseline(prepared_df, "rmse")
        self.assertAlmostEqual(rmse, 1.0)

    def test_evaluate_naive_return_baseline(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)

        # Each target_return is (price + 1 - price) / price = 1 / price.
        # The zero-return baseline predicts 0.0, so RMSE is the RMSE of the
        # actual returns themselves: sqrt(mean(return^2)).
        rmse = evaluate_naive_return_baseline(prepared_df, "rmse")

        returns = [r["target_return"] for r in prepared_df.select("target_return").collect()]
        expected_rmse = (sum(r * r for r in returns) / len(returns)) ** 0.5
        self.assertAlmostEqual(rmse, expected_rmse, places=6)

        # Independent sanity bounds: first return is 0.01; the rest are
        # slightly smaller (1/101, 1/102, ...), all > 0.0084.
        self.assertGreater(rmse, 0.0084)
        self.assertLess(rmse, 0.01)

    def test_predicted_return_to_price(self):
        self.assertAlmostEqual(predicted_return_to_price(100.0, 0.01), 101.0)
        self.assertAlmostEqual(predicted_return_to_price(100.0, -0.005), 99.5)
        self.assertAlmostEqual(predicted_return_to_price(118420.52, 0.0), 118420.52)

    def test_train_gbt_model_completes_without_error(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        train_df, val_df, _ = chronological_split(prepared_df, 0.6, 0.2)

        mlflow.set_experiment("test_completes_without_error")
        model, train_rmse, val_rmse, run_id = train_gbt_model(train_df, val_df)

        # It should run and return valid metrics
        self.assertIsNotNone(model)
        self.assertGreaterEqual(train_rmse, 0.0)
        self.assertGreaterEqual(val_rmse, 0.0)
        # The model now predicts returns; with a perfectly linear +1 trend the
        # constant return 0.01 is trivially learnable, so the model should beat
        # the zero-return baseline on validation.
        zero_return_val_rmse = evaluate_naive_return_baseline(val_df)
        self.assertLess(val_rmse, zero_return_val_rmse)

    def test_train_gbt_model_logs_to_mlflow(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        train_df, val_df, _ = chronological_split(prepared_df, 0.6, 0.2)

        experiment_id = mlflow.create_experiment(f"test_{uuid.uuid4().hex}")
        mlflow.set_experiment(experiment_id=experiment_id)

        model, train_rmse, val_rmse, run_id = train_gbt_model(train_df, val_df)

        client = MlflowClient()
        runs = client.search_runs(experiment_ids=[experiment_id])
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertIn("val_rmse", run.data.metrics)
        self.assertAlmostEqual(run.data.metrics["val_rmse"], val_rmse, places=4)
        self.assertEqual(run.data.params["maxDepth"], "5")
        # The model is trained on the scale-invariant return target.
        self.assertEqual(run.data.params["labelCol"], "target_return")
        self.assertNotIn("price", run.data.params["features"].split(","))
        # Confirm the model artifact was actually logged, not just metrics
        artifacts = client.list_artifacts(run.info.run_id)
        self.assertTrue(any(a.path == "gbt_model" for a in artifacts))

    def test_should_promote_challenger(self):
        # Better challenger -> promote
        self.assertTrue(should_promote_challenger(0.95, 1.0))
        # Worse challenger -> reject
        self.assertFalse(should_promote_challenger(1.05, 1.0))
        # No production model yet -> always promote (first deploy)
        self.assertTrue(should_promote_challenger(0.99, None))
        # Not enough improvement vs configured threshold
        self.assertFalse(
            should_promote_challenger(0.99, 1.0, min_improvement_pct=2.0)
        )

    def test_train_and_evaluate_enforces_gate(self):
        raw_df = self._sample_raw_data()

        report = train_and_evaluate(raw_df, train_ratio=0.6, val_ratio=0.2)

        # The full pipeline runs end-to-end and reports holdout metrics.
        self.assertIn("model", report)
        self.assertIn("run_id", report)
        self.assertIn("test_rmse", report)
        self.assertIn("zero_return_baseline_test_rmse", report)
        self.assertIn("price_persistence_baseline_test_rmse", report)
        self.assertIn("beats_baseline", report)
        self.assertIn("should_promote_challenger", report)
        self.assertIn("promotable", report)

        for key in ("train_rmse", "val_rmse", "test_rmse"):
            self.assertGreaterEqual(report[key], 0.0)

        # The linear +1 trend is perfectly learnable in return space, so on the
        # test holdout the model must beat the zero-return baseline.
        self.assertTrue(report["beats_baseline"])
        zero_return_rmse = evaluate_naive_return_baseline(
            chronological_split(prepare_training_data(raw_df), 0.6, 0.2)[2]
        )
        self.assertAlmostEqual(
            report["zero_return_baseline_test_rmse"], zero_return_rmse
        )
        # With no production model yet this is a first deploy -> promotable.
        self.assertTrue(report["promotable"])

if __name__ == "__main__":
    unittest.main()
