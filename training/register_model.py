"""training/register_model.py – Phase 8 Model Registry

Responsibilities
----------------
- Register a trained model to the MLflow Model Registry in the Staging stage.
- Read the current production model's holdout RMSE from the registry.
- Run the automated promotion gate (beats baseline + beats current prod model).
- Promote a Staging model to Production, archiving the previous production
  version for instant rollback.

Flow (matches the Phase 8 spec):

    MLflow
        │
        ▼
    Model Registry (Staging)
        │
        ▼
    Automated gate: beats baseline + beats current prod model on holdout
        │
        ▼
    Canary deployment (5% traffic)  ── optional tag, then full rollout
        │
        ▼
    Production Model (100% traffic)
"""

from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient

MODEL_NAME = "stock_gbt_return_model"
STAGING = "Staging"
PRODUCTION = "Production"
ARCHIVED = "Archived"

# Tag keys stored on each model version
TAG_TEST_RMSE = "test_rmse"
TAG_BEATS_BASELINE = "beats_baseline"
TAG_PROMOTABLE = "promotable"
TAG_CANARY = "canary"


def register_model_staging(
    run_id: str,
    model_name: str = MODEL_NAME,
    model_uri: Optional[str] = None,
    test_rmse: Optional[float] = None,
    beats_baseline: Optional[bool] = None,
    promotable: Optional[bool] = None,
) -> int:
    """Register a trained model to the MLflow Model Registry in Staging.

    Parameters
    ----------
    run_id:
        The MLflow run that logged the model.
    model_name:
        Registered model name.
    model_uri:
        URI of the logged model.  Defaults to ``runs:/<run_id>/gbt_model``.
    test_rmse:
        Holdout test RMSE, stored as a version tag for the promotion gate.
    beats_baseline:
        Whether the model beat the zero-return baseline, stored as a tag.
    promotable:
        Whether the model passed the full gate, stored as a tag.

    Returns
    -------
    int
        The registered model version number.
    """
    if model_uri is None:
        model_uri = f"runs:/{run_id}/gbt_model"

    result = mlflow.register_model(model_uri=model_uri, name=model_name)

    client = MlflowClient()
    if test_rmse is not None:
        client.set_model_version_tag(
            model_name, result.version, TAG_TEST_RMSE, str(test_rmse)
        )
    if beats_baseline is not None:
        client.set_model_version_tag(
            model_name, result.version, TAG_BEATS_BASELINE, str(beats_baseline)
        )
    if promotable is not None:
        client.set_model_version_tag(
            model_name, result.version, TAG_PROMOTABLE, str(promotable)
        )

    # A newly registered version defaults to stage None.  The Phase 8 flow
    # requires the model to land in Staging first, before the promotion gate
    # decides whether it moves to Production.
    client.transition_model_version_stage(model_name, result.version, STAGING)

    return result.version


def get_production_model_rmse(model_name: str = MODEL_NAME) -> Optional[float]:
    """Return the holdout test RMSE of the current production model version.

    Returns ``None`` if no model is in the Production stage yet.

    Parameters
    ----------
    model_name:
        Registered model name.

    Returns
    -------
    Optional[float]
        The production model's test RMSE, or ``None``.
    """
    client = MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")
    prod = [v for v in versions if v.current_stage == PRODUCTION]
    if not prod:
        return None
    prod.sort(key=lambda v: v.last_updated_timestamp, reverse=True)
    tag = prod[0].tags.get(TAG_TEST_RMSE)
    return float(tag) if tag is not None else None


def promote_to_production(
    model_name: str,
    version: int,
    canary: bool = False,
) -> int:
    """Promote a Staging model version to Production.

    The previous Production version (if any) is moved to Archived so it stays
    cached for instant rollback.

    Parameters
    ----------
    model_name:
        Registered model name.
    version:
        The model version to promote.
    canary:
        If ``True``, mark the version as a canary (5% traffic) via a tag before
        full promotion.

    Returns
    -------
    int
        The promoted version number.
    """
    client = MlflowClient()

    # Archive the current production version (kept for rollback).
    versions = client.search_model_versions(f"name='{model_name}'")
    for v in versions:
        if v.current_stage == PRODUCTION and v.version != version:
            client.transition_model_version_stage(model_name, v.version, ARCHIVED)

    if canary:
        client.set_model_version_tag(model_name, version, TAG_CANARY, "true")

    client.transition_model_version_stage(model_name, version, PRODUCTION)
    if canary:
        client.set_model_version_tag(model_name, version, TAG_CANARY, "false")

    return version


def run_registry_gate(
    report: dict,
    model_name: str = MODEL_NAME,
    canary: bool = False,
) -> dict:
    """Register a trained model and promote it if the gate passes.

    The model is always registered to the Staging stage.  It is promoted to
    Production only if ``report["promotable"]`` is ``True`` (i.e. it beat the
    zero-return baseline on the holdout and beat the current production model).

    Parameters
    ----------
    report:
        The report dict returned by ``train_and_evaluate``.  Must contain
        ``run_id``, ``test_rmse``, ``beats_baseline``, and ``promotable``.
    model_name:
        Registered model name.
    canary:
        If ``True``, route through a canary tag before full promotion.

    Returns
    -------
    dict
        Registry status: ``model_name``, ``version``, ``status``
        (``"promoted"`` or ``"staging_only"``), and ``promotable``.
    """
    run_id = report["run_id"]
    test_rmse = report["test_rmse"]
    beats_baseline = report["beats_baseline"]
    promotable = report["promotable"]

    version = register_model_staging(
        run_id=run_id,
        model_name=model_name,
        test_rmse=test_rmse,
        beats_baseline=beats_baseline,
        promotable=promotable,
    )

    if promotable:
        promote_to_production(model_name, version, canary=canary)
        status = "promoted"
    else:
        status = "staging_only"

    return {
        "model_name": model_name,
        "version": version,
        "status": status,
        "promotable": promotable,
    }