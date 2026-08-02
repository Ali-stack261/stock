# Phase 4 — Streaming Feature Join: Still Not Working (New Root Cause)

Repo: `Ali-stack261/stock`
Commit checked: `48492c2` — "Push Phase 4 join fix and shared time-window feature implementation"

## Progress made

- The `[AMBIGUOUS_REFERENCE]` bug from the previous commit is fixed — DataFrames are
  now properly aliased (`.alias("base")`, `.alias("ma5")`, `.alias("ma20")`) before
  joining.
- Batch and streaming now share one function, `compute_time_window_features()`,
  eliminating the tick-based-vs-time-based definition mismatch flagged earlier.
- The test suite now actually starts a streaming query
  (`.writeStream...start()` + `processAllAvailable()`) instead of only checking
  `.schema` — real regression coverage, as recommended.
- **Batch path: fully passing**, verified independently:
  ```
  test_compute_batch_features_pipeline ... ok
  Ran 1 test in 31.773s
  OK
  ```

## New failure — a different, deeper problem

The streaming test now fails with a new error (confirms the join logic reaches Spark's
planner, but Spark rejects the join itself):

```
AnalysisException: Stream-stream LeftOuter join between two streaming DataFrame/Datasets
is not supported without a watermark in the join keys, or a watermark on the nullable
side and an appropriate range condition
```

**Why:** `base_window`, `ma5_window`, and `ma20_window` are all streaming, watermarked,
*aggregated* DataFrames. Joining two streaming DataFrames with `how="left"` requires
more than equality on watermarked/time-derived columns — Spark requires an actual
**range condition** (e.g. `left_time BETWEEN right_time - interval X AND right_time +
interval X`) in the join predicate for left outer stream-stream joins, so it can bound
how long to retain state on both sides. Pure equality (`window.end == ma5_window.end`)
doesn't satisfy this, even though the values are logically equal by construction.

## Why this is more than a one-line fix

Patching this with a literal range condition (e.g. `BETWEEN window.end AND window.end`)
would technically satisfy Spark's syntax check, but it's fighting the framework: joining
three independently-aggregated streaming DataFrames is an unusual and fragile pattern in
Structured Streaming generally — each side maintains its own state store, keyed and
evicted independently, and getting them to align exactly on window boundaries in a
long-running streaming job is more brittle than it looks in a short test run.

## Recommended direction: stop joining, use one pass with `flatMapGroupsWithState`

Rather than computing three separate windowed aggregates and joining them, maintain a
small per-symbol rolling state (e.g. a bounded deque of recent prices/volumes) via
`flatMapGroupsWithState` (or `applyInPandasWithState`), and emit `first_price`,
`last_price`, `avg_price`, `ma5`, `ma20`, `vwap`, etc. directly from that single state
update — no joins at all.

Benefits:
- Avoids stream-stream join restrictions entirely
- One state store instead of three, simpler to reason about and checkpoint
- Also the natural place to eventually add EMA/RSI/MACD, which need the same kind of
  per-symbol running state and were already going to require `flatMapGroupsWithState`
  (per the earlier Phase 4 gap analysis)
- Since batch and streaming currently share `compute_time_window_features()`, batch
  would need its own separate implementation again if streaming moves to
  `flatMapGroupsWithState` — that's an acceptable tradeoff (batch already handles
  row-based windows fine with plain window functions, no streaming restrictions apply
  there)

## Net effect

The join-based architecture, even with the alias fix, does not work end-to-end in
streaming mode — confirmed empirically, not by inspection alone (batch path passes,
streaming path throws `AnalysisException` when actually started). The next fix should
replace the three-way join with a single `flatMapGroupsWithState` pass for the
streaming path, rather than attempting to patch the join condition further.
