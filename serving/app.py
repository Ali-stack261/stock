"""serving/app.py – Phase 11 FastAPI prediction service with Prometheus monitoring.

Exposes a REST API for next-tick price predictions.  The model predicts the
next *return* (scale-invariant), and the service converts it back to an
absolute price for the response.

Endpoints
----------
- ``GET  /health``            — liveness check (no auth).
- ``POST /predict``           — returns a predicted price, model version, and
                                confidence interval.  Requires API key auth.
- ``GET  /predictions/{sym}`` — recent stored predictions for a symbol.
- ``GET  /metrics/accuracy``  — rolling RMSE / MAE (Phase 10).
- ``GET  /prometheus``        — Prometheus scrape endpoint (Phase 11, no auth).
- ``POST /drift/check``       — run drift detection for a symbol (Phase 12, auth required).

Phase 9 additions:
- API key auth on the prediction endpoint via ``X-API-Key`` header.
- Rate limiting — in-memory per-client sliding-window counter.
- Request/response logging for reproducibility and audit.

Phase 10 additions:
- Prediction persistence and realized-error backfill on each /predict call.

Phase 11 additions:
- Prometheus instruments (counters, histograms, gauges) for request rate,
  latency, error rate, rolling RMSE/MAE, and prediction staleness.
- ``/prometheus`` scrape endpoint (no auth) via prometheus_client ASGI app.
  Mounted at ``/prometheus`` (not ``/metrics``) to avoid shadowing the
  existing ``/metrics/accuracy`` JSON endpoint.

Phase 12 additions:
- Drift detection endpoint that compares recent live features and prediction
  errors against the training reference distribution.
- Periodic background drift checks every 15 minutes for configured symbols.
- Prometheus gauges for feature-drift and concept-drift status.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field

from monitoring.drift import DriftDetector
from serving.auth import verify_api_key
from serving.metrics import (
    drift_concept_drift_detected,
    drift_detected,
    drift_feature_drift_detected,
    predict_errors_total,
    predict_latency_seconds,
    predict_requests_total,
    realized_predictions_total,
    rolling_mae,
    rolling_rmse,
    rolling_rmse_return,
    unrealized_predictions_total,
    directional_accuracy,
)
from serving.prediction_store import PredictionStore
from serving.predictor import PredictionResult, Predictor

logger = logging.getLogger("serving")
logging.basicConfig(level=logging.INFO)


def _mask_key(key: str) -> str:
    """Show only the last 4 characters, e.g. 'dev-key-12345' -> '****2345'."""
    if len(key) <= 4:
        return "*" * len(key)
    return "*" * (len(key) - 4) + key[-4:]

# ---------------------------------------------------------------------------
# Rate limiter — simple in-memory sliding-window counter per API key.
# ---------------------------------------------------------------------------
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_MAX_REQUESTS = 100


class RateLimiter:
    """In-memory per-client rate limiter using a sliding window."""

    def __init__(self, window: int = RATE_LIMIT_WINDOW_SECONDS, max_reqs: int = RATE_LIMIT_MAX_REQUESTS):
        self.window = window
        self.max_reqs = max_reqs
        self._hits: dict[str, list[float]] = defaultdict(list)

    def check(self, client_id: str) -> None:
        """Raise 429 if ``client_id`` has exceeded the rate limit."""
        now = time.time()
        hits = self._hits[client_id]
        # Drop entries outside the window.
        self._hits[client_id] = [t for t in hits if now - t < self.window]
        if len(self._hits[client_id]) >= self.max_reqs:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded: {self.max_reqs} requests per {self.window}s.",
            )
        self._hits[client_id].append(now)


rate_limiter = RateLimiter()

# ---------------------------------------------------------------------------
# Prediction store — persists every prediction for later realized-error
# backfill and rolling online accuracy (Phase 10).
# ---------------------------------------------------------------------------
_prediction_store: PredictionStore | None = None


def get_prediction_store() -> PredictionStore:
    """Return the singleton PredictionStore instance."""
    global _prediction_store
    if _prediction_store is None:
        _prediction_store = PredictionStore()
    return _prediction_store


# ---------------------------------------------------------------------------
# Drift detection wiring (Phase 12)
# ---------------------------------------------------------------------------
DRIFT_CHECK_INTERVAL_SECONDS = 900  # 15 minutes
DRIFT_CHECK_SYMBOLS = ["BTCUSDT", "ETHUSDT", "AAPL"]
_DRIFT_TASK: asyncio.Task | None = None
_DRIFT_DETECTOR: DriftDetector | None = None


def _load_reference_data() -> pd.DataFrame | None:
    """Load the training reference feature sample saved by the training pipeline.

    Tries ``reference_features.parquet`` in the working directory (saved by
    ``train_and_evaluate``).  Falls back to ``None`` if the file is absent,
    in which case drift checks return ``triggered=False`` with a warning.
    """
    import os

    ref_path = "reference_features.parquet"
    if os.path.exists(ref_path):
        return pd.read_parquet(ref_path)
    return None


def _run_drift_check_for_symbol(symbol: str, store: PredictionStore) -> None:
    """Execute one drift check cycle for ``symbol`` and update Prometheus gauges."""
    if _DRIFT_DETECTOR is None:
        return

    # Load persisted cooldown state so it survives process restarts.
    last_trigger = store.get_last_drift_trigger_time(symbol)
    if last_trigger is not None:
        _DRIFT_DETECTOR._last_trigger_time = last_trigger

    recent_features = store.get_recent_feature_rows(symbol, limit=500)
    recent_errors = store.get_recent_return_errors(symbol, limit=500)

    if recent_features.empty:
        logger.info("drift check skipped for %s: no feature rows yet", symbol)
        return

    report = _DRIFT_DETECTOR.check(recent_features, prediction_errors=recent_errors)

    drift_detected.labels(symbol=symbol).set(int(report.triggered))
    drift_feature_drift_detected.labels(symbol=symbol).set(int(report.feature_drift_detected))
    drift_concept_drift_detected.labels(symbol=symbol).set(int(report.concept_drift_detected))

    if report.triggered:
        assert _DRIFT_DETECTOR.last_trigger_time is not None
        store.set_last_drift_trigger_time(symbol, _DRIFT_DETECTOR.last_trigger_time)
        logger.warning(
            "Drift trigger fired for %s: feature=%s concept=%s cooldown_active=%s",
            symbol,
            report.feature_drift_detected,
            report.concept_drift_detected,
            report.cooldown_active,
        )


async def _periodic_drift_task(store: PredictionStore) -> None:
    """Background loop that runs drift checks on a fixed interval."""
    global _DRIFT_DETECTOR
    reference = _load_reference_data()
    if reference is not None:
        _DRIFT_DETECTOR = DriftDetector(reference_data=reference, cooldown_minutes=30)
        logger.info("DriftDetector initialized with reference data (%d rows)", len(reference))
    else:
        logger.warning("No reference_features.parquet found — drift detection disabled until training saves one")

    while True:
        await asyncio.sleep(DRIFT_CHECK_INTERVAL_SECONDS)
        for symbol in DRIFT_CHECK_SYMBOLS:
            try:
                _run_drift_check_for_symbol(symbol, store)
            except Exception:
                logger.exception("drift check failed for %s", symbol)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
class PredictionRequest(BaseModel):
    """Input features for a prediction request.

    The model uses scale-invariant features (no raw price levels).  The
    ``current_price`` field is used to convert the predicted return back to
    an absolute price.
    """

    symbol: str = Field(..., examples=["BTCUSDT"])
    current_price: float = Field(..., gt=0, examples=[118420.52])
    price_return: float = Field(0.0, examples=[0.001])
    volume_change: float = Field(0.0, examples=[5.0])
    ma5_ratio: float = Field(1.0, examples=[1.001])
    ma20_ratio: float = Field(1.0, examples=[0.998])
    vwap_ratio: float = Field(1.0, examples=[1.002])
    price_range_ratio: float = Field(0.0, examples=[0.005])


class PredictionResponse(BaseModel):
    predicted_price: float
    predicted_return: float
    model_version: str
    confidence_interval: list[float]
    prediction_timestamp: str


class PredictionHistoryItem(BaseModel):
    """A single stored prediction row (Phase 10)."""

    id: int
    timestamp: str
    symbol: str
    current_price: float
    predicted_price: float
    predicted_return: float
    model_version: str
    realized_error: float | None = None
    price_return: float | None = None
    volume_change: float | None = None
    ma5_ratio: float | None = None
    ma20_ratio: float | None = None
    vwap_ratio: float | None = None
    price_range_ratio: float | None = None


class DriftCheckResponse(BaseModel):
    """Response from the drift check endpoint (Phase 12)."""

    symbol: str
    feature_drift_detected: bool
    concept_drift_detected: bool
    triggered: bool
    cooldown_active: bool
    feature_details: dict
    concept_details: dict


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _DRIFT_TASK
    store = get_prediction_store()
    _DRIFT_TASK = asyncio.create_task(_periodic_drift_task(store))
    yield
    if _DRIFT_TASK is not None:
        _DRIFT_TASK.cancel()


app = FastAPI(
    title="Real-Time Stock Prediction API",
    description="Next-tick price prediction service (Phase 9–12).",
    version="1.0.0",
    lifespan=_lifespan,
)


# Mount Prometheus scrape endpoint at /prometheus (no auth — standard pattern).
# NOTE: mounted at /prometheus`, NOT `/metrics`, to avoid shadowing the existing
# `/metrics/accuracy` JSON endpoint (Starlette mounts capture the entire subtree).
app.mount("/prometheus", make_asgi_app())

# The predictor is created lazily so the app can start without a Spark session.
_predictor: Predictor | None = None


def get_predictor() -> Predictor:
    """Return the singleton Predictor instance."""
    global _predictor
    if _predictor is None:
        _predictor = Predictor()
    return _predictor


@app.get("/health")
async def health():
    """Liveness check — no auth required."""
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionResponse)
async def predict(
    request: PredictionRequest,
    api_key: str = Depends(verify_api_key),
    predictor: Predictor = Depends(get_predictor),  # noqa: B008
    store: PredictionStore = Depends(get_prediction_store),  # noqa: B008
) -> PredictionResponse:
    """Return a next-tick price prediction.

    Requires a valid ``X-API-Key`` header.  Rate-limited per API key.
    Prometheus counters and latency histograms are updated on every call.
    """
    try:
        rate_limiter.check(api_key)
    except HTTPException:
        predict_errors_total.labels(symbol=request.symbol, error_type="rate_limited").inc()
        predict_requests_total.labels(symbol=request.symbol, status="error").inc()
        raise

    logger.info(
        "prediction request: symbol=%s current_price=%s api_key=%s",
        request.symbol,
        request.current_price,
        _mask_key(api_key),
    )

    features = {
        "price_return": request.price_return,
        "volume_change": request.volume_change,
        "ma5_ratio": request.ma5_ratio,
        "ma20_ratio": request.ma20_ratio,
        "vwap_ratio": request.vwap_ratio,
        "price_range_ratio": request.price_range_ratio,
    }

    _t0 = time.perf_counter()
    try:
        result: PredictionResult = predictor.predict(features, request.current_price)
    except RuntimeError as exc:
        logger.error("prediction failed: %s", exc)
        predict_errors_total.labels(symbol=request.symbol, error_type="model_error").inc()
        predict_requests_total.labels(symbol=request.symbol, status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        )
    finally:
        predict_latency_seconds.labels(symbol=request.symbol).observe(
            time.perf_counter() - _t0
        )

    logger.info(
        "prediction result: symbol=%s predicted_price=%s model_version=%s",
        request.symbol,
        result.predicted_price,
        result.model_version,
    )

    # Realize the previous pending prediction for this symbol using the price
    # observed *now* — this call's current_price is the actual outcome of
    # whatever the last prediction for this symbol was trying to guess.
    pending = store.get_oldest_unrealized_prediction(request.symbol)
    if pending is not None:
        store.realize_prediction(pending.id, request.current_price)
        realized_predictions_total.labels(symbol=request.symbol).inc()
        # Refresh accuracy gauges after each new realization.
        rmse = store.compute_rolling_rmse(symbol=request.symbol)
        rmse_return = store.compute_rolling_rmse_return(symbol=request.symbol)
        mae = store.compute_rolling_mae(symbol=request.symbol)
        dir_acc = store.compute_directional_accuracy(symbol=request.symbol)
        if rmse is not None:
            rolling_rmse.labels(symbol=request.symbol).set(rmse)
        if rmse_return is not None:
            rolling_rmse_return.labels(symbol=request.symbol).set(rmse_return)
        if mae is not None:
            rolling_mae.labels(symbol=request.symbol).set(mae)
        if dir_acc is not None:
            directional_accuracy.labels(symbol=request.symbol).set(dir_acc)

    # Persist the new prediction for later realized-error backfill (Phase 10).
    store.save_prediction(
        timestamp=result.prediction_timestamp,
        symbol=request.symbol,
        current_price=request.current_price,
        predicted_price=result.predicted_price,
        predicted_return=result.predicted_return,
        model_version=result.model_version,
        price_return=request.price_return,
        volume_change=request.volume_change,
        ma5_ratio=request.ma5_ratio,
        ma20_ratio=request.ma20_ratio,
        vwap_ratio=request.vwap_ratio,
        price_range_ratio=request.price_range_ratio,
    )

    # Update staleness gauge — count of unrealized predictions after save.
    unrealized = store.get_unrealized_predictions(symbol=request.symbol)
    unrealized_predictions_total.labels(symbol=request.symbol).set(len(unrealized))

    predict_requests_total.labels(symbol=request.symbol, status="ok").inc()

    return PredictionResponse(
        predicted_price=result.predicted_price,
        predicted_return=result.predicted_return,
        model_version=result.model_version,
        confidence_interval=list(result.confidence_interval),
        prediction_timestamp=result.prediction_timestamp,
    )


@app.get("/predictions/{symbol}", response_model=list[PredictionHistoryItem])
async def get_prediction_history(
    symbol: str,
    limit: int = 10,
    api_key: str = Depends(verify_api_key),
    store: PredictionStore = Depends(get_prediction_store),  # noqa: B008
) -> list[PredictionHistoryItem]:
    """Return recent stored predictions for a symbol (Phase 10)."""
    records = store.get_recent_predictions(symbol, limit=limit)
    return [
        PredictionHistoryItem(
            id=r.id,
            timestamp=r.timestamp,
            symbol=r.symbol,
            current_price=r.current_price,
            predicted_price=r.predicted_price,
            predicted_return=r.predicted_return,
            model_version=r.model_version,
            realized_error=r.realized_error,
            price_return=r.price_return,
            volume_change=r.volume_change,
            ma5_ratio=r.ma5_ratio,
            ma20_ratio=r.ma20_ratio,
            vwap_ratio=r.vwap_ratio,
            price_range_ratio=r.price_range_ratio,
        )
        for r in records
    ]


@app.get("/metrics/accuracy")
async def get_accuracy_metrics(
    symbol: str | None = None,
    api_key: str = Depends(verify_api_key),
    store: PredictionStore = Depends(get_prediction_store),  # noqa: B008
) -> dict:
    """Return rolling online accuracy metrics (RMSE, MAE) from realized predictions."""
    rmse = store.compute_rolling_rmse(symbol=symbol)
    mae = store.compute_rolling_mae(symbol=symbol)
    return {
        "symbol": symbol,
        "rolling_rmse": rmse,
        "rolling_mae": mae,
    }


@app.post("/drift/check", response_model=DriftCheckResponse)
async def run_drift_check(
    symbol: str,
    api_key: str = Depends(verify_api_key),
    store: PredictionStore = Depends(get_prediction_store),  # noqa: B008
) -> DriftCheckResponse:
    """Run drift detection for a symbol against the training reference.

    Requires a valid ``X-API-Key`` header.  Compares recent live features and
    prediction errors against the reference distribution saved during training.
    """
    if _DRIFT_DETECTOR is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DriftDetector not initialized — reference_features.parquet not found.",
        )

    recent_features = store.get_recent_feature_rows(symbol, limit=500)
    recent_errors = store.get_recent_return_errors(symbol, limit=500)

    if recent_features.empty:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"No feature data available for symbol={symbol} yet.",
        )

    report = _DRIFT_DETECTOR.check(recent_features, prediction_errors=recent_errors)

    drift_detected.labels(symbol=symbol).set(int(report.triggered))
    drift_feature_drift_detected.labels(symbol=symbol).set(int(report.feature_drift_detected))
    drift_concept_drift_detected.labels(symbol=symbol).set(int(report.concept_drift_detected))

    if report.triggered:
        logger.warning(
            "Drift trigger fired for %s via /drift/check: feature=%s concept=%s",
            symbol,
            report.feature_drift_detected,
            report.concept_drift_detected,
        )

    return DriftCheckResponse(
        symbol=symbol,
        feature_drift_detected=report.feature_drift_detected,
        concept_drift_detected=report.concept_drift_detected,
        triggered=report.triggered,
        cooldown_active=report.cooldown_active,
        feature_details=report.feature_details,
        concept_details=report.concept_details,
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Log all HTTP errors for audit."""
    logger.warning("HTTP %s: %s path=%s", exc.status_code, exc.detail, request.url.path)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})