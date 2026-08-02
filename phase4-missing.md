# Phase 4 — Feature Engineering: What's Missing

Repo: `Ali-stack261/stock`
Commit checked: `6fc27d3` — "Push Phase 4 feature engineering updates"

## What's done

**Batch path (`compute_batch_features`)** — uses real row-based windows (`LAG`, `rowsBetween`),
which is valid since this runs against static training data, not a live stream:
- `price_change`
- `price_return`
- `volume_change`
- `ma5`
- `ma20`
- `vwap`
- `price_range`

All tested and passing (`test_compute_batch_features_pipeline`).

**Streaming path (`compute_stream_features`)** — uses time-window aggregation
(`groupBy(window(...))` + watermark), the streaming-safe pattern:
- `first_price`, `last_price`, `avg_price`, `max_price`, `min_price`, `volume_sum`
- `vwap`
- `price_change`, `price_return`
- `price_range`

Tested and passing (`test_compute_features_streaming_pipeline`).

## What's missing

### 1. `ma5` / `ma20` missing from the streaming path
The batch path computes them; the streaming path does not. Confirmed directly —
the streaming test's own `expected_columns` list excludes `ma5`/`ma20`.

**Why this matters:** the streaming path is what live inference will actually use.
If a model is trained on batch features that include MA5/MA20 but the serving
pipeline only has access to streaming features that don't, that's a train/serve
skew — the model will get a different (incomplete) feature vector at inference
time than it saw during training.

### 2. `FEATURE_COLUMNS` constant is inconsistent with streaming output
```python
FEATURE_COLUMNS = ["symbol", "price_change", "price_return", "volume_change",
                    "ma5", "ma20", "vwap", "price_range"]
```
Lists `ma5`/`ma20` as if universally available, but only the batch function
produces them. Not an active bug yet — nothing in the repo references
`FEATURE_COLUMNS` currently — but will cause a `KeyError`/missing-column error
the moment someone wires this into training or serving code against the
streaming output.

### 3. EMA not implemented (either path)
Exponential Moving Average requires genuine per-symbol running state
(previous EMA value decayed by a smoothing factor) — not expressible as a
simple time-window aggregate.

### 4. RSI not implemented (either path)
Relative Strength Index requires tracking smoothed average gains/losses over
a rolling period per symbol — stateful, not a plain aggregation.

### 5. MACD not implemented (either path)
MACD is the difference between two EMAs (typically 12-period and 26-period),
plus a signal line (EMA of the MACD line itself). Depends on EMA existing first.

### 6. Rolling standard deviation not implemented (either path)

### 7. Momentum not implemented (either path)

## Net effect

Against the original spec's 11-feature list (MA5, MA20, EMA, RSI, MACD, VWAP,
rolling std, momentum, % return, volume change, price difference), only
5 are implemented, and only partially (MA5/MA20 batch-only):

| Feature | Batch | Streaming |
|---|---|---|
| price_change / price difference | ✅ | ✅ |
| price_return / % return | ✅ | ✅ |
| volume_change | ✅ | ❌ |
| ma5 | ✅ | ❌ |
| ma20 | ✅ | ❌ |
| vwap | ✅ | ✅ |
| price_range | ✅ | ✅ |
| ema | ❌ | ❌ |
| rsi | ❌ | ❌ |
| macd | ❌ | ❌ |
| rolling std | ❌ | ❌ |
| momentum | ❌ | ❌ |

The stateful indicators (EMA, RSI, MACD, rolling std, momentum) will most likely
need `flatMapGroupsWithState`/`applyInPandasWithState` for the streaming path,
since they require maintaining running per-symbol state across ticks that
neither row-based windows (batch-only) nor time-window aggregation (streaming)
naturally support.
