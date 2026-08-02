# 📈 Real-Time Stock Price Prediction Platform (MLOps)

## Overview

This project is an end-to-end MLOps platform that predicts stock or cryptocurrency prices using a real-time streaming pipeline. The system continuously ingests live market data, processes it with Apache Spark, generates predictions using a machine learning model, monitors performance and data drift, and automatically retrains the model when necessary.

**Target SLAs**

| Metric | Target |
|---|---|
| End-to-end latency (event → prediction) | < 2s p95 |
| API response time | < 150ms p99 |
| System availability | 99.9% |
| Kafka consumer lag | < 500ms |
| Drift-triggered retrain cycle | < 30 min |

---

# 🏗️ System Architecture

```text
                    ┌────────────────────┐
                    │ Stock Market API   │
                    │ Finnhub WebSocket  │
                    │ Binance WebSocket  │
                    └─────────┬──────────┘
                              │
                    Real-Time Price Stream
                              │
                              ▼
                   Schema Registry (Avro)
                              │
                              ▼
                     Apache Kafka Topic
                          (partitioned by symbol)
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
          Spark Structured        Dead Letter Queue
             Streaming              (malformed events)
                    │
      ┌─────────────┴─────────────┐
      │                           │
      ▼                           ▼
Feature Engineering        Store Raw Events
      │                           │
      ▼                           ▼
Feature Store (Feast)      Data Lake (Parquet, partitioned
      │                     by symbol/date, versioned)
      ▼
Prediction Service (FastAPI, autoscaled)
      │
      ▼
PostgreSQL / TimescaleDB (predictions + audit trail)
      │
      ▼
React Dashboard  ◄──────  WebSocket push (live updates)
      │
      ▼
Grafana + Prometheus Monitoring ──► Alertmanager ──► Slack/PagerDuty
```

**Design notes added:**
- A **schema registry** enforces contract compatibility between producers and consumers, preventing silent breakage when upstream APIs change their payload shape.
- A **dead-letter queue (DLQ)** captures malformed/out-of-order events instead of crashing the stream or silently dropping data.
- A **feature store** (Feast) keeps online (low-latency) and offline (training) features consistent, avoiding train/serve skew — one of the most common failure modes in real-time ML systems.
- The dashboard receives **push updates over WebSocket** rather than polling, so the UI reflects predictions within the same latency budget as the pipeline.

---

# 🔄 Project Workflow

## Phase 1 – Real-Time Data Collection

This phase is responsible for connecting to live market feeds, ingesting real-time price events, and normalizing them into a consistent format before they are published downstream.

### Objective

Build a reliable ingestion layer that can:
- connect to one or more live market data providers,
- handle connection drops and stale feeds,
- validate incoming payloads,
- attach metadata for traceability,
- and forward well-formed events to the Kafka pipeline.

### Data Sources

- Binance WebSocket
- Finnhub WebSocket
- Alpha Vantage API (for fallback or batch comparison)

### Ingestion Flow

```text
Market Feed (WebSocket/API)
      │
      ▼
Connect + Authenticate
      │
      ▼
Receive Event
      │
      ▼
Normalize + Validate
      │
      ▼
Attach Metadata (source, idempotency key, received_at)
      │
      ▼
Publish to Kafka Producer
```

### Example Incoming Event

```json
{
  "symbol": "BTCUSDT",
  "price": 118420.52,
  "volume": 0.42,
  "timestamp": "2026-08-01T10:20:34Z"
}
```

### Normalized Event Shape

```json
{
  "symbol": "BTCUSDT",
  "price": 118420.52,
  "volume": 0.42,
  "timestamp": "2026-08-01T10:20:34Z",
  "source": "binance",
  "idempotency_key": "BTCUSDT-2026-08-01T10:20:34Z-binance",
  "received_at": "2026-08-01T10:20:35Z"
}
```

### Reliability Additions

- **Reconnect with exponential backoff + jitter** on socket drop; never busy-loop reconnect attempts.
- **Heartbeat/ping-pong monitoring** to detect silent connection death (sockets that appear open but stop sending data).
- **Multi-source failover**: if the primary feed becomes stale for more than a configured threshold, fail over to a secondary provider and tag the event with a `source` field.
- **Idempotency key** (`symbol` + `timestamp` + `source`) so replayed or retried messages do not double-count events.
- **Schema validation** before forwarding events so malformed payloads are rejected early and routed to a dead-letter queue if needed.

### Implementation Notes

A practical implementation usually includes:
- a dedicated WebSocket client module for live market feed ingestion,
- a validation layer for required fields and basic ranges,
- a lightweight event normalizer that adds traceability metadata,
- and a producer wrapper that hands off the event to Kafka.

### Suggested Files

- `producer/websocket_client.py`
- `producer/normalize_event.py`
- `producer/schema/market_event.avsc`

---

## Phase 2 – Kafka Producer

A Python application receives every market event, validates it against the Avro schema, and publishes it to an Apache Kafka topic.

```text
WebSocket
      │
      ▼
Validate against schema
      │
      ▼
Kafka Producer (acks=all, idempotent=true)
      │
      ▼
Topic: stock_prices (partitioned by symbol key)
```

**Reliability settings to configure explicitly:**
- `enable.idempotence=true` and `acks=all` to avoid duplicate/lost messages.
- Partition key = `symbol`, so all events for one ticker land in the same partition and preserve order.
- `retries` with bounded backoff, plus a producer-side circuit breaker that routes to the DLQ topic after repeated failures.

---

## Phase 3 – Stream Processing

Apache Spark Structured Streaming continuously consumes messages from Kafka.

```text
Kafka
   │
   ▼
Spark Structured Streaming (micro-batch or continuous mode)
   │
   ▼
Checkpointing to durable storage (S3/HDFS)
   │
   ▼
Watermarking for late data (handles out-of-order ticks)
```

**Fault tolerance additions:**
- **Checkpointing** to durable storage so Spark can resume exactly-once processing after a restart.
- **Watermarking** to bound how long the engine waits for late-arriving ticks before finalizing a windowed aggregate (important for MA/RSI windows).
- **Backpressure handling** via `maxOffsetsPerTrigger` to prevent a burst of volume from overwhelming the cluster.

---

## Phase 4 – Feature Engineering

A shared feature-engineering module now supports both streaming-safe and batch training-safe paths.

### Implementation

- `streaming.feature_engineering.compute_features(mode="streaming")` uses time-window aggregation and watermarking so Spark Structured Streaming avoids unsupported row-ordering window functions.
- `streaming.feature_engineering.compute_features(mode="batch")` uses row-ordering window functions for batch training and validation, preserving train/serve parity.
- Input validation is applied before feature computation to ensure `price > 0`, `volume >= 0`, and valid timestamps.

### Implemented features

- Moving Average (MA5)
- Moving Average (MA20)
- VWAP
- Price change
- Price return
- Volume change
- Price range

### Phase 4 parity guarantees
- streaming + batch pipelines share the same Python feature-engineering package
- feature naming is consistent across stream and batch outputs
- the same validated event schema is the source of truth for online and offline computation

### Data quality additions
- **Schema + range validation** before the feature pipeline runs
- **Feature parity tests** verify shared code across batch and stream modes
- Features can be exported to **offline Parquet** and later ingested into an online store such as Feast or Redis

---

## Phase 5 – Data Storage

Every incoming event is stored in a data lake for future training.

```text
data/
└── 2026/
    └── 08/
        └── 01/
            └── symbol=BTCUSDT/
                └── part-0001.parquet
```

**Additions:**
- Partition by `symbol` in addition to date, so training jobs for a single ticker don't scan the whole lake.
- Apply **compaction** on a schedule (small-file problem is common with streaming writes).
- Add a **retention/lifecycle policy** (e.g. move data older than 90 days to cold storage) to control cost.

Benefits:

- Historical analysis
- Model retraining
- Backtesting
- Auditing

---

## Phase 6 – Model Training

A scheduled workflow (Apache Airflow) periodically retrains the model.

```text
Historical Data
        │
        ▼
Data Validation (schema + drift sanity check)
        │
        ▼
Feature Engineering (shared code with streaming path)
        │
        ▼
Train Model (with time-based train/val/test split — never random split for time series)
        │
        ▼
Evaluate (backtest against holdout period)
        │
        ▼
Champion/Challenger comparison
        │
        ▼
MLflow
```

**Important correction for time-series ML:** splits must be **chronological**, not random, to avoid look-ahead leakage (training on data that "sees the future" relative to validation).

### Possible Models

- XGBoost
- LightGBM
- CatBoost
- LSTM
- Temporal Fusion Transformer (Advanced)
- Baseline: naive persistence (`predicted[t+1] = price[t]`) — always keep this as a sanity-check baseline; a model that can't beat it isn't adding value.

---

## Phase 7 – Experiment Tracking

Every training run is logged using MLflow.

Tracked information includes:

- Hyperparameters
- Features (with feature-store version hash)
- Training metrics
- Validation metrics (RMSE, MAE, MAPE, directional accuracy)
- Artifacts
- Model version
- Data version (link to the exact Parquet snapshot used)

---

## Phase 8 – Model Registry

The best-performing model is automatically registered, but only promoted to production after passing gating checks.

```text
MLflow
    │
    ▼
Model Registry (Staging)
    │
    ▼
Automated gate: beats baseline + beats current prod model on holdout
    │
    ▼
Canary deployment (5% traffic)
    │
    ▼
Production Model (100% traffic)
```

The prediction service always loads the latest production model, and keeps the **previous version cached** for instant rollback.

---

## Phase 9 – Model Serving

FastAPI exposes a REST API for predictions.

### Request

```json
{
  "rsi": 48,
  "ema20": 112,
  "macd": 0.8
}
```

### Response

```json
{
  "predicted_price": 118510.23,
  "model_version": "v9",
  "confidence_interval": [118320.10, 118700.55],
  "prediction_timestamp": "2026-08-01T10:20:35Z"
}
```

**Additions:**
- Return a **confidence interval** or prediction uncertainty, not just a point estimate — critical for any downstream trading decision.
- Include `model_version` in every response for traceability.
- **Rate limiting** and **API key auth** on the public endpoint.
- **Request/response logging** for reproducibility and later audit.

---

## Phase 10 – Prediction Storage

Predictions are stored for future evaluation.

| Timestamp | Symbol | Current Price | Predicted Price | Model Version | Realized Error (t+1) |
|-----------|--------|---------------|------------------|----------------|------------------------|
| 10:15 | BTCUSDT | 118420 | 118520 | v9 | (filled in after actual arrives) |
| 10:16 | BTCUSDT | 118450 | 118560 | v9 | |

A separate scheduled job joins predictions with realized prices once the actual future price is known, to compute rolling online accuracy.

---

## Phase 11 – Monitoring

### Infrastructure Monitoring

- CPU / Memory usage
- Kafka consumer lag
- Spark batch duration / processing rate
- API response time and error rate (4xx/5xx)
- Feature store read latency

### Machine Learning Monitoring

- Prediction latency
- Rolling RMSE / MAE (online, using realized-vs-predicted)
- Feature distribution shift
- Prediction distribution shift
- Model/concept drift score
- Prediction staleness (time since last successful inference per symbol)

Tools:

- Prometheus
- Grafana
- Alertmanager for paging on SLA breaches (e.g. lag > threshold, RMSE spike)

---

## Phase 12 – Drift Detection

Evidently AI continuously compares live data with the training dataset.

```text
Training Data
      │
      ▼
 Drift Detection (statistical tests: PSI, KS-test, KL-divergence)
      ▲
      │
Live Data
```

If significant drift is detected on either **features** (data drift) or **prediction error** (concept drift), retraining is triggered automatically — with a cooldown period to avoid retrain storms.

---

## Phase 13 – Automated Retraining

```text
Drift Detected (or scheduled interval)
       │
       ▼
Apache Airflow DAG
       │
       ▼
Retrain Model
       │
       ▼
Evaluate vs baseline + current production model
       │
       ▼
MLflow
       │
       ▼
Register New Model (Staging)
       │
       ▼
Canary Deploy → Full Rollout (or auto-rollback if canary underperforms)
```

---

## Phase 14 – CI/CD Pipeline

Every GitHub push triggers an automated deployment pipeline.

```text
GitHub Push
      │
      ▼
Run Unit + Integration Tests
      │
      ▼
Lint + Type Check (ruff/mypy)
      │
      ▼
Data/Model Validation Tests
      │
      ▼
Build Docker Image (multi-stage, minimal base)
      │
      ▼
Scan Image for Vulnerabilities
      │
      ▼
Push Image to Registry
      │
      ▼
Deploy to Staging → Smoke Tests
      │
      ▼
Deploy to Kubernetes (Production, rolling update)
```

---

# 🧪 Testing Strategy (new)

| Type | What it covers |
|---|---|
| Unit tests | Feature calculations (MA, RSI, MACD), producer/consumer logic |
| Integration tests | Kafka → Spark → Feature Store round-trip on a test topic |
| Contract tests | Schema registry compatibility checks on every producer change |
| Load tests | Kafka/Spark throughput at N× expected peak volume (e.g. market open) |
| Model tests | Baseline-beat check, backtest on holdout, prediction latency budget |
| Chaos tests | Kill a broker/Spark executor mid-stream, verify recovery from checkpoint |

---

# 🔐 Security & Compliance (new)

- Secrets (API keys, DB credentials) managed via a secrets manager (Vault/AWS Secrets Manager), never committed or baked into images.
- TLS between all internal services (Kafka, Postgres, FastAPI).
- API authentication (API keys or OAuth2) plus per-key rate limiting on the public prediction endpoint.
- Network policies restricting which pods can talk to Kafka/Postgres in Kubernetes.
- Audit log of every prediction request and every model promotion, for reproducibility and compliance if predictions ever inform real trades.
- Clear disclaimer in the dashboard/API docs that predictions are informational only and not financial advice.

---

# 💰 Cost & Scalability Considerations (new)

- Kafka topic partition count sized for target throughput (partitions ≈ target MB/s ÷ per-partition throughput).
- Spark cluster autoscaling tied to Kafka lag, not just CPU, since streaming bottlenecks usually show up as lag first.
- Feature store online layer (Redis) sized by (symbols × features × bytes) with TTL eviction for stale symbols.
- Data lake lifecycle rules (hot → warm → cold storage) to bound storage cost as history grows.
- Consider **spot/preemptible nodes** for the Spark batch training workload (not the always-on streaming job) to cut compute cost.

---

# 📁 Project Structure

```text
real-time-stock-mlops/
│
├── producer/
│   ├── websocket_client.py
│   ├── kafka_producer.py
│   └── schema/                # Avro schema definitions
│
├── streaming/
│   ├── spark_stream.py
│   ├── feature_engineering.py
│   └── schemas.py
│
├── feature_store/              # Feast repo (new)
│   ├── feature_definitions.py
│   └── feature_store.yaml
│
├── training/
│   ├── train.py
│   ├── evaluate.py
│   ├── backtest.py            # new
│   └── register_model.py
│
├── serving/
│   ├── app.py
│   ├── predictor.py
│   └── auth.py                # new
│
├── monitoring/
│   ├── drift.py
│   └── metrics.py
│
├── airflow/
│   └── dags/
│       ├── retrain_dag.py
│       └── data_quality_dag.py   # new
│
├── dashboard/
│
├── docker/
│
├── kubernetes/
│   ├── base/
│   └── overlays/               # new: staging vs prod
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── load/                   # new
│
├── data/
├── models/
├── mlruns/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .github/
    └── workflows/
```

---

# 🛠️ Technology Stack

| Layer | Technology |
|--------|------------|
| Programming Language | Python |
| Live Data Source | Binance WebSocket / Finnhub |
| Schema Management | Confluent Schema Registry (Avro) |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Feature Store | Feast |
| Data Storage | Parquet, PostgreSQL / TimescaleDB |
| Machine Learning | XGBoost / LightGBM |
| Data Validation | Great Expectations / Pandera |
| Experiment Tracking | MLflow |
| Model Registry | MLflow Registry |
| Model Serving | FastAPI |
| Workflow Orchestration | Apache Airflow |
| Containerization | Docker |
| Container Orchestration | Kubernetes |
| Monitoring | Prometheus + Grafana + Alertmanager |
| Drift Detection | Evidently AI |
| CI/CD | GitHub Actions |
| Secrets Management | HashiCorp Vault / AWS Secrets Manager |
| Dashboard | React |

---

# 🚀 Future Enhancements

- Multi-stock prediction
- Portfolio optimization
- Reinforcement learning for trading strategies
- Online learning models
- Real-time alerting system
- Explainable AI with SHAP (surface top feature contributions per prediction)
- Canary model deployments
- A/B testing for model versions
- Multi-cloud deployment (AWS/GCP/Azure)
- Ensemble of models per symbol (different tickers may favor different model types)

---

# 🎯 Learning Outcomes

By completing this project, you will gain hands-on experience with:

- Real-time data streaming
- Event-driven architectures
- Apache Kafka
- Spark Structured Streaming
- Time-series feature engineering
- Feature stores and train/serve consistency
- Machine learning model training
- Experiment tracking with MLflow
- Model versioning, registry, and canary rollout
- REST API deployment with FastAPI
- Containerization using Docker
- Kubernetes deployment
- Infrastructure and ML monitoring
- Data and concept drift detection
- Automated model retraining
- CI/CD for MLOps
- Security and secrets management for production ML systems
- Production-grade machine learning system design
