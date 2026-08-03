"""serving/metrics.py – Phase 11 Prometheus instrumentation.

All Prometheus instruments for the real-time stock MLOps service are
registered here.  Import this module once (via ``serving.app``) — the
``prometheus_client`` default registry is process-global, so a single
import is sufficient.

Metric catalogue
----------------
Infrastructure / API
    predict_requests_total      Counter   Requests by symbol + outcome.
    predict_latency_seconds     Histogram Latency per /predict call.
    predict_errors_total        Counter   Error requests by symbol + type.

ML / accuracy
    rolling_rmse                Gauge     Live RMSE from realized predictions.
    rolling_mae                 Gauge     Live MAE from realized predictions.
    unrealized_predictions_total Gauge    Backlog of pending predictions (staleness proxy).
    realized_predictions_total  Counter   Cumulative count of realized predictions.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Infrastructure / API metrics
# ---------------------------------------------------------------------------

predict_requests_total = Counter(
    "predict_requests_total",
    "Total number of /predict requests.",
    labelnames=["symbol", "status"],  # status: 'ok' | 'error'
)

predict_latency_seconds = Histogram(
    "predict_latency_seconds",
    "End-to-end latency of /predict requests in seconds.",
    labelnames=["symbol"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

predict_errors_total = Counter(
    "predict_errors_total",
    "Total number of errored /predict requests.",
    labelnames=["symbol", "error_type"],  # error_type: 'model_error' | 'rate_limited'
)

# ---------------------------------------------------------------------------
# ML accuracy / staleness metrics
# ---------------------------------------------------------------------------

rolling_rmse = Gauge(
    "rolling_rmse",
    "Rolling RMSE over all realized predictions for a symbol.",
    labelnames=["symbol"],
)

rolling_rmse_return = Gauge(
    "rolling_rmse_return",
    "Rolling RMSE of realized return prediction error (scale-invariant, comparable across symbols).",
    labelnames=["symbol"],
)

rolling_mae = Gauge(
    "rolling_mae",
    "Rolling MAE over all realized predictions for a symbol.",
    labelnames=["symbol"],
)

unrealized_predictions_total = Gauge(
    "unrealized_predictions_total",
    "Number of predictions still awaiting realization (staleness proxy).",
    labelnames=["symbol"],
)

realized_predictions_total = Counter(
    "realized_predictions_total",
    "Cumulative number of predictions that have been realized.",
    labelnames=["symbol"],
)
