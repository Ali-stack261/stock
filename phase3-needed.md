# Phase 3 — Stream Processing / Feature Engineering: What's Needed

Repo: `Ali-stack261/stock`
Commit checked: `49e7b58` — "Add Phase 3 streaming components"

## What works

- `streaming/spark_stream.py` — Kafka source → parsed streaming DataFrame → Parquet sink
  with checkpointing. Structurally correct.
- Both unit tests pass:
```
test_build_spark_session ... ok
test_market_event_schema_has_expected_fields ... ok
Ran 2 tests in 12.487s
OK
```

## The bug

`compute_features()` in `streaming/feature_engineering.py` uses a row-ordering window
function to compute `price_change`:

```python
.withColumn("price_change", expr("price - lag(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)"))
```

Reproduced directly against a synthetic streaming DataFrame with the pipeline's own schema:

```
AnalysisException: [NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING]
Window function is not supported in LAG(PRICE, -1, NULL)...
Structured Streaming only supports time-window aggregation using the WINDOW function.
```

**Why:** Spark Structured Streaming does not support `LAG`/`LEAD`/`ROW_NUMBER`-style
row-ordering window functions on unbounded streaming data — full stop. This isn't a
config or version issue. Only **time-bucketed** `window()` aggregations (with a
watermark) are supported.

**Why the tests didn't catch it:** neither test actually runs `compute_features()`
against a streaming DataFrame — they only check the schema shape and that a
`SparkSession` builds. The bug is invisible until the pipeline is wired end to end:
`run_streaming_query(compute_features(create_streaming_dataframe(...)))`.

## Why this matters beyond `price_change`

The same `LAG`-based pattern will hit **every** feature in the original spec that needs
a previous value or a rolling window, not just `price_change`:

- Moving Average (MA5, MA20)
- Exponential Moving Average (EMA)
- RSI
- MACD
- VWAP
- Rolling Standard Deviation
- Momentum
- Percentage Return
- Volume Change

All of these need per-symbol running state across ticks — none of them can be computed
with plain SQL window functions on a raw streaming DataFrame.

## What to do

Pick one of these approaches (don't patch `price_change` alone — fix the underlying
pattern once, since every feature above needs the same kind of state):

1. **`flatMapGroupsWithState` / `applyInPandasWithState`** — maintain explicit per-symbol
   state (last N prices, running sums for MA/EMA, etc.) updated on each new event. Most
   flexible, handles arbitrary lookback windows and indicators like RSI/MACD that need
   several prior values.

2. **Time-based `window()` aggregation with a watermark** — bucket events into fixed
   time windows (e.g. 1-minute) and aggregate within each window. Works well for
   VWAP/rolling stats over a time period, less natural for tick-count-based indicators
   (MA5 = last 5 *ticks*, not last 5 *minutes*).

3. **Move stateful feature computation downstream of Spark** — have Spark do only
   validation/enrichment/raw storage, and compute the actual rolling
   features in the online feature store (e.g. Redis-backed) or in the serving layer,
   where maintaining small per-symbol rolling buffers is simpler than in a distributed
   streaming job. Reasonable for an MVP; revisit if throughput/latency demands push
   the computation back into Spark later.

## Net effect

Phase 3's ingestion/sink plumbing is solid. The feature engineering step as committed
will fail on first real run, and the same fix needs to generalize to every windowed
feature in the spec, not just the one currently implemented.
