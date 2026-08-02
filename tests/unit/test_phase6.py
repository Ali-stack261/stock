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
    chronological_split,
    evaluate_naive_baseline,
    prepare_training_data,
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

    def test_train_gbt_model_completes_without_error(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        train_df, val_df, _ = chronological_split(prepared_df, 0.6, 0.2)
        
        feature_cols = ["price", "volume", "ma5"]
        
        mlflow.set_experiment("test_completes_without_error")
        model, train_rmse, val_rmse = train_gbt_model(train_df, val_df, feature_cols)
        
        # It should run and return valid metrics
        self.assertIsNotNone(model)
        self.assertGreaterEqual(train_rmse, 0.0)
        self.assertGreaterEqual(val_rmse, 0.0)

    def test_train_gbt_model_logs_to_mlflow(self):
        raw_df = self._sample_raw_data()
        prepared_df = prepare_training_data(raw_df)
        train_df, val_df, _ = chronological_split(prepared_df, 0.6, 0.2)
        feature_cols = ["price", "volume", "ma5"]

        experiment_id = mlflow.create_experiment(f"test_{uuid.uuid4().hex}")
        mlflow.set_experiment(experiment_id=experiment_id)

        model, train_rmse, val_rmse = train_gbt_model(train_df, val_df, feature_cols)

        client = MlflowClient()
        runs = client.search_runs(experiment_ids=[experiment_id])
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertIn("val_rmse", run.data.metrics)
        self.assertAlmostEqual(run.data.metrics["val_rmse"], val_rmse, places=4)
        self.assertEqual(run.data.params["maxDepth"], "5")
        # Confirm the model artifact was actually logged, not just metrics
        artifacts = client.list_artifacts(run.info.run_id)
        self.assertTrue(any(a.path == "gbt_model" for a in artifacts))

if __name__ == "__main__":
    unittest.main()
