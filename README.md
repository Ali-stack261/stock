# Real-Time Stock Prediction Platform

This repository contains a real-time stock price prediction MLOps platform that predicts the next-tick return for assets based on real-time market data.

## Architecture

The pipeline consists of:
- **WebSocket Ingestion**: Real-time event ingestion from exchanges.
- **Kafka**: Message broker for ordered, resilient stream processing.
- **Spark Streaming**: Stateful feature engineering (e.g. tick-based MA5/MA20) via `applyInPandasWithState`.
- **MLflow**: Model registry with champion/challenger gating based on holdout performance.
- **FastAPI**: Real-time serving API predicting return-based scale-invariant targets.
- **Prometheus/Grafana**: Monitoring of prediction latency, request volume, and rolling RMSE/MAE accuracy.
- **Drift Detection**: Checking feature and concept drift over time.
- **Airflow**: Orchestration for retraining.
- **CI/CD**: GitHub Actions for testing, GHCR deployment, and Trivy security scanning.

## Current Capabilities & Known Limitations

- **Prediction Targets**: The model predicts the *return* rather than absolute price to remain scale-invariant.
- **Baseline Performance**: On realistic data, the model currently sits near the naive baseline (predicting zero return / persistence). This is an expected finding of the initial GBT model, and the architecture is built to detect and enforce this via strict gating.
- **Backtesting**: (Under active development) Backtesting evaluates whether model predictions generate profitable signals vs buy-and-hold.
- **Deployment**: We use K3s for self-hosted cluster deployment.

## Setup Instructions

### Prerequisites
- **JDK 17**: Must be installed (not JDK 21) for PySpark compatibility.
- **Python 3.10+**

### Installation

```bash
# Core requirements
pip install -r requirements.txt

# Serving requirements (if running FastAPI/Prometheus locally)
pip install -r requirements-serving.txt
```

### Environment Configuration

Create a `.env` file based on the provided `.env.example` (or similar templates in the repo) for dashboard and API configurations.

## Documentation

For full project history, investigations, CI/CD fixes, and prior phase notes, see the [docs/](./docs/) directory.
