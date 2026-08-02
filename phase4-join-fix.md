# Phase 4 — Streaming Feature Join: How to Fix

Repo: `Ali-stack261/stock`
Commit checked: `222f056` — "Finalize Phase 4 streaming feature parity and test coverage"

## The suite fails as committed

```
FAILED (errors=1)
File "streaming/feature_engineering.py", line 115, in compute_stream_features
    .join(
pyspark.errors.exceptions.captured.AnalysisException:
[AMBIGUOUS_REFERENCE] Reference `symbol` is ambiguous, could be: [`symbol`, `symbol`]
```

This is not a deep streaming-semantics issue — it's a basic DataFrame join mistake.
`base_window`, `ma5_window`, and `ma20_window` all have a column named `symbol`.
Joining them with explicit column-object equality (`base_window["symbol"] ==
ma5_window["symbol"]`) keeps both `symbol` columns in the joined result, and Spark
can't disambiguate which one `base_window["symbol"]` refers to once the second join
is chained on.

## Fix 1 — resolve the join ambiguity

Two straightforward options:

**Option A — join on the column name string instead of an equality expression,
and drop duplicate columns immediately:**
```python
joined = (
    base_window
    .join(ma5_window, on="symbol", how="left")  # only works if window-end also matches — see note below
    .join(ma20_window, on="symbol", how="left")
)
```
This alone isn't quite right because you also need to match on the window boundary,
not just `symbol` — see Option B.

**Option B (recommended) — alias each DataFrame before joining, and join on both
`symbol` and the matching window-end explicitly:**
```python
base = base_window.alias("base")
ma5 = ma5_window.alias("ma5")
ma20 = ma20_window.alias("ma20")

joined = (
    base
    .join(
        ma5,
        (col("base.symbol") == col("ma5.symbol")) &
        (col("base.window").getField("end") == col("ma5.ma5_window").getField("end")),
        how="left",
    )
    .join(
        ma20,
        (col("base.symbol") == col("ma20.symbol")) &
        (col("base.window").getField("end") == col("ma20.ma20_window").getField("end")),
        how="left",
    )
    .select(
        col("base.window"),
        col("base.symbol"),
        col("base.first_price"), col("base.last_price"), col("base.avg_price"),
        col("base.max_price"), col("base.min_price"),
        col("base.first_volume"), col("base.last_volume"), col("base.volume_sum"),
        col("base.vwap"),
        col("ma5.ma5"),
        col("ma20.ma20"),
    )
)
```
Aliasing (`.alias("base")`, `.alias("ma5")`, `.alias("ma20")`) makes every column
reference unambiguous by prefix, including after multiple chained joins.

**After fixing, verify by actually starting a streaming query — not just checking
`.schema`** (see "Test coverage gap" below):
```python
query = featured.writeStream.format("memory").queryName("check").outputMode("append").start()
# let it run briefly, then query.stop()
```
`.schema` alone did not catch this bug or the earlier `LAG` bug in this repo's history —
only `.start()` forces the full analysis/planning path that surfaces these errors.

## Fix 2 — the deeper issue: MA5/MA20 mean different things in batch vs. streaming

Even once the join works, there's a second, more subtle mismatch:

- **Batch** `ma5`/`ma20` = average of the **last 5 / last 20 raw trades** (tick-based,
  via `rowsBetween(-4, 0)` / `rowsBetween(-19, 0)`)
- **Streaming** `ma5`/`ma20` = average over the **last 5 minutes / last 20 minutes**
  of wall-clock time (time-based, via `window(col, "5 minutes", ...)`)

These are genuinely different quantities. In a high-volume minute, "last 5 minutes"
might contain hundreds of trades; in a quiet stretch, almost none. A model trained on
tick-based MA5/MA20 will see systematically different values at inference time from a
time-based streaming MA5/MA20 — this is a train/serve skew, just moved one level deeper
than the crash.

**Two ways to resolve this, pick one:**

1. **Make both tick-based.** Streaming per-symbol tick-count windows aren't expressible
   with `groupBy(window(...))` — they need `flatMapGroupsWithState` (or
   `applyInPandasWithState`) maintaining a small per-symbol deque of the last 20 prices,
   updated on every incoming event. More correct, more implementation effort.

2. **Make both time-based.** Redefine the batch MA5/MA20 to also average over 5-minute /
   20-minute wall-clock windows instead of tick counts (`groupBy(window(...))` on the
   historical data, same as the streaming path). Simpler, and guarantees exact parity
   since both paths run the literal same aggregation logic — just point both
   `compute_batch_features` and `compute_stream_features` at one shared windowing
   function instead of maintaining two definitions.

Option 2 is the lower-effort, lower-risk fix: have one `compute_time_windowed_features()`
function used by both `mode="batch"` and `mode="streaming"`, so there's structurally no
way for the two to drift apart again.

## Test coverage gap to close either way

`test_compute_features_streaming_pipeline` only checks `features_df.schema.fields` —
it never calls `.start()`. This repo has now hit two separate `AnalysisException`s
(the original `LAG` bug, and this join ambiguity) that `.schema` access alone did not
catch. Add a `.writeStream.format("memory")...start()` step to this test (as shown
above) so future changes to `compute_stream_features` are actually exercised through
Spark's full query-planning path, not just checked for column names.

## Net effect

1. Fix the join (alias + explicit column references) — mechanical, low-risk.
2. Decide tick-based vs. time-based MA5/MA20 and make batch and streaming share one
   definition — this is the real design decision, not just a bug fix.
3. Strengthen the streaming test to call `.start()`, not just `.schema`.
