"""serving/app.py – Phase 9 FastAPI prediction service.

Exposes a REST API for next-tick price predictions.  The model predicts the
next *return* (scale-invariant), and the service converts it back to an
absolute price for the response.

Endpoints
---------
- ``GET /health`` — liveness check (no auth).
- ``POST /predict`` — returns a predicted price, model version, and confidence
  interval.  Requires API key auth.

Security / observability additions (per Phase 9 spec):
- **API key auth** on the prediction endpoint via ``X-API-Key`` header.
- **Rate limiting** — a simple in-memory per-client request counter with a
  configurable window (sufficient for a single-instance deployment; swap for
  Redis-backed limiting in a multi-replica setup).
- **Request/response logging** — every prediction request and its result are
  logged for reproducibility and audit.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from serving.auth import verify_api_key
from serving.predictor import PredictionResult, Predictor
from serving.prediction_store import PredictionStore

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
_prediction_store: Optional[PredictionStore] = None


def get_prediction_store() -> PredictionStore:
    """Return the singleton PredictionStore instance."""
    global _prediction_store
    if _prediction_store is None:
        _prediction_store = PredictionStore()
    return _prediction_store


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
    realized_error: Optional[float] = None


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Real-Time Stock Prediction API",
    description="Next-tick price prediction service (Phase 9).",
    version="1.0.0",
)

# The predictor is created lazily so the app can start without a Spark session.
_predictor: Optional[Predictor] = None


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
    predictor: Predictor = Depends(get_predictor),
    store: PredictionStore = Depends(get_prediction_store),
) -> PredictionResponse:
    """Return a next-tick price prediction.

    Requires a valid ``X-API-Key`` header.  Rate-limited per API key.
    """
    rate_limiter.check(api_key)

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

    try:
        result: PredictionResult = predictor.predict(features, request.current_price)
    except RuntimeError as exc:
        logger.error("prediction failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
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

    # Persist the new prediction for later realized-error backfill (Phase 10).
    store.save_prediction(
        timestamp=result.prediction_timestamp,
        symbol=request.symbol,
        current_price=request.current_price,
        predicted_price=result.predicted_price,
        predicted_return=result.predicted_return,
        model_version=result.model_version,
    )

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
    store: PredictionStore = Depends(get_prediction_store),
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
        )
        for r in records
    ]


@app.get("/metrics/accuracy")
async def get_accuracy_metrics(
    symbol: Optional[str] = None,
    api_key: str = Depends(verify_api_key),
    store: PredictionStore = Depends(get_prediction_store),
) -> dict:
    """Return rolling online accuracy metrics (RMSE, MAE) from realized predictions."""
    rmse = store.compute_rolling_rmse(symbol=symbol)
    mae = store.compute_rolling_mae(symbol=symbol)
    return {
        "symbol": symbol,
        "rolling_rmse": rmse,
        "rolling_mae": mae,
    }


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Log all HTTP errors for audit."""
    logger.warning("HTTP %s: %s path=%s", exc.status_code, exc.detail, request.url.path)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})