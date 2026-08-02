import os
import shutil
import tempfile
import uuid
from pathlib import Path
import unittest

import mlflow
from mlflow.tracking import MlflowClient

from training.register_model import (
    ARCHIVED,
    MODEL_NAME,
    PRODUCTION,
    STAGING,
    TAG_BEATS_BASELINE,
    TAG_CANARY,
    TAG_PROMOTABLE,
    TAG_TEST_RMSE,
    get_production_model_rmse,
    promote_to_production,
    register_model_staging,
    run_registry_gate,
)


def _log_dummy_model(tracking_uri: str, experiment_name: str) -> str:
    """Log a tiny sklearn model and return its run_id."""
    from sklearn.linear_model import LinearRegression

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run() as run:
        mlflow.sklearn.log_model(LinearRegression(), "gbt_model")
        return run.info.run_id


class Phase8RegistryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"
        cls._mlflow_tmp_dir = tempfile.mkdtemp()
        cls.tracking_uri = Path(cls._mlflow_tmp_dir).as_uri()
        mlflow.set_tracking_uri(cls.tracking_uri)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._mlflow_tmp_dir, ignore_errors=True)

    def _unique_model_name(self) -> str:
        return f"{MODEL_NAME}_{uuid.uuid4().hex[:8]}"

    def test_register_model_staging_creates_version_with_tags(self):
        model_name = self._unique_model_name()
        run_id = _log_dummy_model(self.tracking_uri, f"exp_{uuid.uuid4().hex}")

        version = register_model_staging(
            run_id=run_id,
            model_name=model_name,
            test_rmse=0.001,
            beats_baseline=True,
            promotable=True,
        )

        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        self.assertEqual(len(versions), 1)
        self.assertEqual(versions[0].version, version)
        self.assertEqual(versions[0].current_stage, STAGING)
        self.assertEqual(versions[0].tags[TAG_TEST_RMSE], "0.001")
        self.assertEqual(versions[0].tags[TAG_BEATS_BASELINE], "True")
        self.assertEqual(versions[0].tags[TAG_PROMOTABLE], "True")

    def test_get_production_model_rmse_none_when_no_production(self):
        model_name = self._unique_model_name()
        run_id = _log_dummy_model(self.tracking_uri, f"exp_{uuid.uuid4().hex}")
        register_model_staging(run_id=run_id, model_name=model_name, test_rmse=0.001)

        # No production version yet -> None
        self.assertIsNone(get_production_model_rmse(model_name))

    def test_promote_to_production_archives_previous(self):
        model_name = self._unique_model_name()
        run_id = _log_dummy_model(self.tracking_uri, f"exp_{uuid.uuid4().hex}")

        v1 = register_model_staging(run_id=run_id, model_name=model_name, test_rmse=0.002)
        v2 = register_model_staging(run_id=run_id, model_name=model_name, test_rmse=0.001)

        promote_to_production(model_name, v1)
        promote_to_production(model_name, v2)

        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        stages = {v.version: v.current_stage for v in versions}
        self.assertEqual(stages[v2], PRODUCTION)
        self.assertEqual(stages[v1], ARCHIVED)

        # Production RMSE now reflects the promoted version's tag.
        self.assertAlmostEqual(get_production_model_rmse(model_name), 0.001)

    def test_promote_to_production_canary_tag(self):
        model_name = self._unique_model_name()
        run_id = _log_dummy_model(self.tracking_uri, f"exp_{uuid.uuid4().hex}")

        version = register_model_staging(run_id=run_id, model_name=model_name)
        promote_to_production(model_name, version, canary=True)

        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        self.assertEqual(versions[0].current_stage, PRODUCTION)
        # Canary tag is cleared after full promotion.
        self.assertEqual(versions[0].tags.get(TAG_CANARY), "false")

    def test_run_registry_gate_promotes_when_promotable(self):
        model_name = self._unique_model_name()
        run_id = _log_dummy_model(self.tracking_uri, f"exp_{uuid.uuid4().hex}")

        report = {
            "run_id": run_id,
            "test_rmse": 0.0009,
            "beats_baseline": True,
            "promotable": True,
        }

        result = run_registry_gate(report, model_name=model_name)

        self.assertEqual(result["status"], "promoted")
        self.assertTrue(result["promotable"])

        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        self.assertEqual(versions[0].current_stage, PRODUCTION)

    def test_run_registry_gate_stays_staging_when_not_promotable(self):
        model_name = self._unique_model_name()
        run_id = _log_dummy_model(self.tracking_uri, f"exp_{uuid.uuid4().hex}")

        report = {
            "run_id": run_id,
            "test_rmse": 0.5,
            "beats_baseline": False,
            "promotable": False,
        }

        result = run_registry_gate(report, model_name=model_name)

        self.assertEqual(result["status"], "staging_only")
        self.assertFalse(result["promotable"])

        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        self.assertEqual(versions[0].current_stage, STAGING)


if __name__ == "__main__":
    unittest.main()