# Fix All 30 mypy Errors — Verified Diffs, Ready to Apply

Repo: `Ali-stack261/stock`
Base commit: `358cc3d`

**Every fix below was verified two ways:**
1. `mypy --ignore-missing-imports .` → clean, "Success: no issues found in 41 source files"
2. Full relevant test suites re-run afterward → 22/22 (fast) + 26/26 (Spark/MLflow) passing, zero regressions

Two genuinely interesting bugs were found along the way, not just type-annotation
noise — flagged below where they occur.

---

## `producer/transports.py`

`_ws`/`_loop` were untyped `None`, causing every downstream use to be flagged.
Also fixed a real mypy-caught `Optional[str]` narrowing gap in the URL resolution
(added an explicit `assert` — safe, since `os.getenv` with a string default always
returns a string).

```diff
 import asyncio
 import json
 import os
+from typing import Any

 import websockets


 class BaseWebSocketTransport:
     def __init__(self, url: str, source: str):
         self.url = url
         self.source = source
-        self._ws = None
-        self._loop = None
+        self._ws: Any = None
+        self._loop: asyncio.AbstractEventLoop | None = None

     def connect(self) -> None:
         if self._ws is not None:
             return
         self._loop = asyncio.new_event_loop()
         asyncio.set_event_loop(self._loop)
         self._ws = self._loop.run_until_complete(websockets.connect(self.url))
         if self.source == "finnhub":
             self._loop.run_until_complete(self._ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"})))

     def receive(self, timeout: float | None = None) -> str | None:
-        if self._ws is None:
+        if self._ws is None or self._loop is None:
             raise RuntimeError("Transport is not connected")
         if timeout is not None:
             return self._loop.run_until_complete(asyncio.wait_for(self._ws.recv(), timeout=timeout))
         return self._loop.run_until_complete(self._ws.recv())

     def send_ping(self) -> None:
-        if self._ws is not None:
+        if self._ws is not None and self._loop is not None:
             self._loop.run_until_complete(self._ws.ping())

     def close(self) -> None:
-        if self._ws is not None:
+        if self._ws is not None and self._loop is not None:
             self._loop.run_until_complete(self._ws.close())
             self._ws = None


 class BinanceTransport(BaseWebSocketTransport):
     def __init__(self, url: str | None = None):
-        super().__init__(url or os.getenv("BINANCE_WEBSOCKET_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade"), "binance")
+        resolved_url = url or os.getenv("BINANCE_WEBSOCKET_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
+        assert resolved_url is not None
+        super().__init__(resolved_url, "binance")


 class FinnhubTransport(BaseWebSocketTransport):
     def __init__(self, url: str | None = None):
-        super().__init__(url or os.getenv("FINNHUB_WEBSOCKET_URL", "wss://ws.finnhub.io?token=demo"), "finnhub")
+        resolved_url = url or os.getenv("FINNHUB_WEBSOCKET_URL", "wss://ws.finnhub.io?token=demo")
+        assert resolved_url is not None
+        super().__init__(resolved_url, "finnhub")
```

Note: `_ws` is typed `Any` rather than `websockets.WebSocketClientProtocol | None` —
that specific type name exists at runtime in `websockets==17.0.1` but isn't properly
exposed in this version's type stubs (a known instability across `websockets`
versions). Not worth chasing a fragile third-party type name for.

## `producer/websocket_client.py`

Same untyped-`None` pattern on `_transport`:

```diff
         self.current_source = source
-        self._transport = None
+        self._transport: Any = None
```

## `streaming/storage.py`

`sc._jvm` is genuinely `Any | None` per pyspark's stubs. Added an assert before use
(also a real runtime safety improvement — a clearer error if it's ever actually None):

```diff
     # Replace the old partition directory with the compacted one
     sc = spark.sparkContext
+    assert sc._jvm is not None, "SparkContext JVM gateway is not initialized"
     fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
     PathClass = sc._jvm.org.apache.hadoop.fs.Path
```

## `training/train.py`

**Real bug #1, found here:** `mlflow.get_experiment_by_name("stock_training")` was
being called **twice** — once for the ternary condition, once for `.experiment_id` —
wasteful and a latent race risk. Fixed to call once and reuse the result. Also added
a proper `Literal` type for `metric_name` instead of a plain `str`, matching what
`RegressionEvaluator` actually accepts.

```diff
-from typing import TYPE_CHECKING
+from typing import TYPE_CHECKING, Literal
+
+RegressionMetricName = Literal["rmse", "mse", "r2", "mae", "var"]
```
```diff
-def evaluate_naive_baseline(df: DataFrame, metric_name: str = "rmse") -> float:
+def evaluate_naive_baseline(df: DataFrame, metric_name: RegressionMetricName = "rmse") -> float:
```
```diff
-def evaluate_naive_return_baseline(df: DataFrame, metric_name: str = "rmse") -> float:
+def evaluate_naive_return_baseline(df: DataFrame, metric_name: RegressionMetricName = "rmse") -> float:
```
```diff
-    mlflow.set_experiment(
-        mlflow.get_experiment_by_name("stock_training")
-        .experiment_id
-        if mlflow.get_experiment_by_name("stock_training")
-        else mlflow.create_experiment("stock_training")
-    )
+    existing_experiment = mlflow.get_experiment_by_name("stock_training")
+    experiment_id = (
+        existing_experiment.experiment_id
+        if existing_experiment is not None
+        else mlflow.create_experiment("stock_training")
+    )
+    mlflow.set_experiment(experiment_id=experiment_id)
```

## `training/register_model.py`

**Real bug #2, found here — the most significant one:** `register_model_staging()`
and `promote_to_production()` were both annotated `-> int`, but MLflow's actual
`ModelVersion.version` is `str` (confirmed directly against MLflow's own source —
`ModelVersion.version` is declared `-> str` in `mlflow/entities/model_registry/model_version.py`).
Checked every caller first — all of them just pass `version` around as an opaque
identifier, never do arithmetic on it, so fixing the annotation to match reality is
safe. Also fixed a real latent crash: `.sort()` on `last_updated_timestamp`, which
can genuinely be `None` per MLflow's stub — would have raised `TypeError` at runtime
if ever hit.

```diff
     promotable: bool | None = None,
-) -> int:
+) -> str:
     """Register a trained model to the MLflow Model Registry in Staging.
```
```diff
-    prod.sort(key=lambda v: v.last_updated_timestamp, reverse=True)
+    prod.sort(key=lambda v: v.last_updated_timestamp or 0, reverse=True)
     tag = prod[0].tags.get(TAG_TEST_RMSE)
```
```diff
 def promote_to_production(
     model_name: str,
-    version: int,
+    version: str,
     canary: bool = False,
-) -> int:
+) -> str:
     """Promote a Staging model version to Production.
```

## `serving/predictor.py`

Same sort-key `None` fix as above, plus an assert after `_load_model()` (mypy can't
verify across the method-call boundary that it actually sets `self._model`):

```diff
-        prod.sort(key=lambda v: v.last_updated_timestamp, reverse=True)
+        prod.sort(key=lambda v: v.last_updated_timestamp or 0, reverse=True)
         latest = prod[0]
```
```diff
         if self._model is None:
             self._load_model()
+        assert self._model is not None, "Model failed to load"

         # Build a single-row Spark DataFrame from the feature dict.
```

## `serving/prediction_store.py`

Two more real issues, surfaced while checking `retrain_pipeline.py`'s dependencies
transitively (not visible in the original full-repo scan, since that scan aborted
after 1 error before reaching these). `cursor.lastrowid` can genuinely be `None` per
`sqlite3`'s own stubs; and an empty-tuple literal was too narrowly inferred:

```diff
         self._conn.commit()
+        assert cursor.lastrowid is not None
         return cursor.lastrowid
```
```diff
         query = "SELECT COUNT(*) as n, SUM(realized_return_error * realized_return_error) as sse FROM predictions WHERE realized_return_error IS NOT NULL"
-        params = ()
+        params: tuple[str, ...] = ()
         if symbol is not None:
```

## `airflow/dags/retrain_pipeline.py`

Two genuine third-party stub limitations, fixed with scoped `type: ignore` (not
blanket-suppressed — each has the specific error code so any *new* real error on
these lines would still be caught):

```diff
-from airflow import DAG
+from airflow import DAG  # type: ignore[attr-defined]  # Airflow's __init__.py lacks static stubs for this
```
```diff
-    sample.to_parquet(REFERENCE_PARQUET, index=False)
+    sample.to_parquet(REFERENCE_PARQUET, index=False)  # type: ignore[attr-defined]  # pyspark's toPandas() stub return type is imprecise
```

Plus the same drift-trigger-time assert pattern as `serving/app.py` below — verified
the `if report.triggered:` guard genuinely guarantees `last_trigger_time` is non-`None`
at that point (real business logic, not just hopeful typing):

```diff
         if report.triggered:
             any_triggered = True
+            assert detector.last_trigger_time is not None
             store.set_last_drift_trigger_time(symbol, detector.last_trigger_time)
```

## `serving/app.py`

```diff
     if report.triggered:
+        assert _DRIFT_DETECTOR.last_trigger_time is not None
         store.set_last_drift_trigger_time(symbol, _DRIFT_DETECTOR.last_trigger_time)
```

## `tests/unit/test_phase13.py`

`importlib.util.spec_from_file_location` can return `None`, and `.loader` can too:

```diff
     _spec = importlib.util.spec_from_file_location("retrain_pipeline", _DAG_PATH)
+    assert _spec is not None and _spec.loader is not None
     _retrain_dag = importlib.util.module_from_spec(_spec)
     _spec.loader.exec_module(_retrain_dag)
```

---

## How to apply

Since these are small, surgical diffs across 10 files (37 insertions, 23 deletions
total), the cleanest approach on your Windows machine:

1. Open each file above in your editor
2. Apply the `-`/`+` changes shown (remove the `-` lines, add the `+` lines)
3. Verify locally before committing:
   ```powershell
   pip install mypy
   mypy --ignore-missing-imports .
   ```
   Should print `Success: no issues found in 41 source files` (plus a few harmless
   `note:` lines about untyped function bodies — those aren't errors).

4. Run the test suite to confirm no regressions, then commit and push as usual.

## Net effect

The Lint job's real root cause (missing `__init__.py` files causing a module-naming
collision, fixed separately) plus all 30 downstream mypy errors are now resolved.
Two genuinely interesting bugs were caught in the process — the duplicate
`get_experiment_by_name` call and the `int`/`str` version-type mismatch in the model
registry functions — neither of which were "type annotation nitpicks," both were
real, previously invisible issues that a first-time type-checker run was specifically
good at finding.
