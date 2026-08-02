"""serving/predictor.py – Phase 9 prediction logic.

Loads the latest production model from the MLflow Model Registry, runs a
prediction in return space (the model predicts ``target_return``), converts the
predicted return back to an absolute price, and computes a simple confidence
interval from the validation RMSE logged during training.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient

from training.register_model import MODEL_NAME, PRODUCTION, TAG_TEST_RMSE
from training.train import MODEL_FEATURE_COLS, predicted_return_to_price

# Default confidence interval half-width in standard deviations (≈95% CI).
DEFAULT_CI_Z = 1.96


@dataclass
class PredictionResult:
    """The output of a single prediction request."""

    predicted_price: float
    predicted_return: float
    model_version: str
    confidence_interval: tuple[float, float]
    prediction_timestamp: str


class Predictor:
    """Load the production model and serve predictions.

    The model is loaded lazily on the first ``predict()`` call and cached so
    subsequent requests are fast.  ``reload()`` fetches the latest production
    version — used for hot-reload after a model promotion.
    """

    def __init__(
        self,
        model_name: str = MODEL_NAME,
        tracking_uri: Optional[str] = None,
        ci_z: float = DEFAULT_CI_Z,
    ):
        self.model_name = model_name
        self.ci_z = ci_z
        if tracking_uri is not None:
            mlflow.set_tracking_uri(tracking_uri)

        self._model = None
        self._version: Optional[str] = None
        self._val_rmse: Optional[float] = None

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------
    def _load_model(self) -> None:
        """Load the latest Production model version from the registry."""
        client = MlflowClient()
        versions = client.search_model_versions(f"name='{self.model_name}'")
        prod = [v for v in versions if v.current_stage == PRODUCTION]
        if not prod:
            raise RuntimeError(
                f"No production model found for '{self.model_name}'. "
                "Train and promote a model first."
            )
        prod.sort(key=lambda v: v.last_updated_timestamp, reverse=True)
        latest = prod[0]

        model_uri = f"models:/{self.model_name}/{latest.version}"
        self._model = mlflow.spark.load_model(model_uri)
        self._version = latest.version

        tag = latest.tags.get(TAG_TEST_RMSE)
        self._val_rmse = float(tag) if tag is not None else None

    def reload(self) -> None:
        """Force-reload the latest production model (hot-reload after promotion)."""
        self._model = None
        self._version = None
        self._val_rmse = None
        self._load_model()

    @property
    def version(self) -> str:
        if self._version is None:
            self._load_model()
        assert self._version is not None
        return self._version

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------
    def predict(
        self,
        features: dict[str, float],
        current_price: float,
    ) -> PredictionResult:
        """Run a single prediction.

        Parameters
        ----------
        features:
            A dict of feature name → value for the model's feature columns
            (``MODEL_FEATURE_COLS``).
        current_price:
            The current observed price for the symbol, used to convert the
            predicted return back to an absolute price.

        Returns
        -------
        PredictionResult
            The predicted price, return, model version, confidence interval,
            and timestamp.
        """
        if self._model is None:
            self._load_model()

        # Build a single-row Spark DataFrame from the feature dict.
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        row = {col: float(features.get(col, 0.0)) for col in MODEL_FEATURE_COLS}
        input_df = spark.createDataFrame([row])

        preds = self._model.transform(input_df).collect()
        predicted_return = float(preds[0]["prediction"])
        predicted_price = predicted_return_to_price(current_price, predicted_return)

        # Confidence interval: predicted_price ± z * rmse * current_price.
        # The model's RMSE is in return space, so the price-space half-width
        # scales with the current price level.
        if self._val_rmse is not None:
            half_width = self.ci_z * self._val_rmse * current_price
        else:
            half_width = 0.0
        ci = (predicted_price - half_width, predicted_price + half_width)

        from datetime import datetime, timezone

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        return PredictionResult(
            predicted_price=predicted_price,
            predicted_return=predicted_return,
            model_version=f"v{self.version}",
            confidence_interval=ci,
            prediction_timestamp=ts,
        )