# Project Improvement Plan — All 7 Priorities

Repo: `Ali-stack261/stock`

Recommended order (adjusted from the original numbering — see reasoning inline):
**1 → 3+5 (coupled) → 2 → 6 → 7 → 4**

---

## Priority 1 — Fix README

**Impact: Very High. Cost: Low. Do this first.**

The README has been touched incrementally across 14 phases and is very likely stale
relative to what's actually built now (real WebSocket ingestion, Kafka, Spark
streaming with tick-based features, MLflow registry with champion/challenger gating,
FastAPI serving, Prometheus/Grafana, drift detection, Airflow retraining, full CI/CD
with GHCR + Trivy scanning).

### What to include
- **Architecture diagram** reflecting the real, current pipeline (not the original
  14-phase plan as originally scoped — several things evolved: return-based
  prediction instead of raw price, tick-based MA5/MA20 via `applyInPandasWithState`,
  GHCR instead of a generic registry)
- **Current capabilities, stated honestly** — including the known limitation that the
  model sits at the naive baseline on realistic data (this is a *finding*, not a
  flaw to hide — stating it explicitly is more credible than silence)
- **Setup instructions** that actually work: JDK 17 (not 21 — this repo's history is
  full of why that distinction matters), `requirements.txt` vs `requirements-serving.txt`,
  the `.env`/`.env.example` pattern for the dashboard
- **A "known limitations" section** — no backtest yet (until Priority 3 lands), no
  Argo CD (until Priority 4), self-hosted deployment tradeoffs
- Link to `docs/` once Priority 7's cleanup lands, rather than dumping every
  investigation doc inline

---

## Priority 3 (moved up) — Backtesting + Priority 5's directional-accuracy gap

**Impact: Very High — arguably the single most valuable addition to this project.**

Nothing like this exists yet. Every metric so far (RMSE, MAE) measures *prediction
error*, not *whether the predictions would make money as a strategy*. Given this
project's own earlier finding — the model sits at the naive baseline on realistic
data — a real backtest makes that concrete and quantified rather than a qualitative
aside.

### New module: `training/backtest.py`

```python
def generate_signals(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Predicted return > 0 → BUY, < 0 → SELL. Adds a 'signal' column."""
    predictions_df["signal"] = predictions_df["predicted_return"].apply(
        lambda r: 1 if r > 0 else -1
    )
    return predictions_df

def compute_directional_accuracy(predictions_df: pd.DataFrame) -> float:
    """% of predictions where sign(predicted_return) == sign(realized_return).
    This is Priority 5's missing metric — and it's the actual input the
    backtest's BUY/SELL logic depends on, not a separate concern."""
    correct = (
        (predictions_df["predicted_return"] > 0) == (predictions_df["realized_return"] > 0)
    )
    return correct.mean()

def run_backtest(predictions_df: pd.DataFrame, initial_capital: float = 10000.0) -> dict:
    """Simulate: signal=1 → long the realized return, signal=-1 → short it.
    Returns total return, Sharpe, max drawdown, win rate, profit factor."""
    df = generate_signals(predictions_df)
    df["strategy_return"] = df["signal"] * df["realized_return"]
    df["equity"] = initial_capital * (1 + df["strategy_return"]).cumprod()

    total_return = (df["equity"].iloc[-1] / initial_capital) - 1
    sharpe = (
        df["strategy_return"].mean() / df["strategy_return"].std() * (252 ** 0.5)
        if df["strategy_return"].std() > 0 else 0.0
    )
    running_max = df["equity"].cummax()
    drawdown = (df["equity"] - running_max) / running_max
    max_drawdown = drawdown.min()

    wins = df[df["strategy_return"] > 0]
    losses = df[df["strategy_return"] < 0]
    win_rate = len(wins) / len(df) if len(df) > 0 else 0.0
    profit_factor = (
        wins["strategy_return"].sum() / abs(losses["strategy_return"].sum())
        if len(losses) > 0 and losses["strategy_return"].sum() != 0 else float("inf")
    )

    return {
        "total_return": total_return,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "directional_accuracy": compute_directional_accuracy(df),
        "n_trades": len(df),
    }
```

### Critical: also backtest a "buy and hold" baseline for comparison

A backtest without a comparison baseline is easy to misread. Add:
```python
def buy_and_hold_return(predictions_df: pd.DataFrame) -> float:
    """What you'd get just holding the asset the whole period, no signals at all."""
    return (1 + predictions_df["realized_return"]).prod() - 1
```
Report both side by side. **Do not claim profitability unless
`total_return` genuinely and consistently beats `buy_and_hold_return` across
multiple time windows** — a single lucky backtest window proves very little.

### Wire into training pipeline
Add backtest results to `train_and_evaluate()`'s report dict and log them to MLflow
alongside the existing RMSE/MAE metrics, so every training run has a strategy-level
evaluation, not just an error-level one.

### Directional accuracy also needs a Prometheus gauge (closing Priority 5's real gap)
```python
directional_accuracy = Gauge(
    "directional_accuracy", "Rolling % of predictions with correct sign.",
    labelnames=["symbol"],
)
```
Set it in `serving/app.py` alongside the existing `rolling_rmse`/`rolling_mae` gauges,
computed from `PredictionStore` the same way those already are.

---

## Priority 2 — Proper walk-forward validation

**Impact: Very High.**

What exists today (`chronological_split`) is a **single** train/val/test split. Walk-forward
validation is different and more rigorous: multiple rolling windows, retraining and
re-evaluating repeatedly across time, which tests whether the model holds up across
*different* market regimes, not just one arbitrary split point.

### New function in `training/train.py`
```python
def walk_forward_validate(
    df: DataFrame, feature_cols: list[str], n_windows: int = 5,
    train_ratio: float = 0.7,
) -> list[dict]:
    """Split the full timeline into n_windows sequential chunks. For each chunk,
    train on train_ratio of it, evaluate on the remainder, before sliding to the
    next chunk. Returns one result dict per window — do not average them into a
    single number without also reporting the spread; a model that's great in one
    window and terrible in another is a different finding than "consistently OK."
    """
    results = []
    window_size = df.count() // n_windows
    for i in range(n_windows):
        window_df = df.filter(
            (col("rank") >= i * window_size) & (col("rank") < (i + 1) * window_size)
        )
        train_df, val_df, _ = chronological_split(window_df, train_ratio, 1 - train_ratio)
        model, train_rmse, val_rmse = train_gbt_model(train_df, val_df, feature_cols)
        baseline_rmse = evaluate_naive_return_baseline(val_df)
        results.append({
            "window": i, "train_rmse": train_rmse, "val_rmse": val_rmse,
            "baseline_rmse": baseline_rmse, "beats_baseline": val_rmse < baseline_rmse,
        })
    return results
```

### Also add the moving-average baseline (the specific gap in Priority 2's list)
`evaluate_naive_baseline`/`evaluate_naive_return_baseline` (persistence: predict no
change) already exist. A moving-average baseline is a genuinely different, slightly
harder bar:
```python
def evaluate_moving_average_baseline(df: DataFrame, window: int = 5) -> float:
    """Predict next return as the average of the last `window` realized returns,
    rather than naive zero-change. A model that can't beat a 5-period moving
    average isn't adding value over the simplest possible smoothing."""
    ...
```

### Report all three side by side
Naive persistence, moving average, and the actual ML model — on every window, not
just once. This is what makes "the model beats baseline" a credible claim rather than
a single lucky comparison.

---

## Priority 6 — Integration / E2E testing

**Impact: High.** Worth noting explicitly: most of the *real* bugs actually found
across this entire build (`state.get()` being a property not a method, the `int`/`str`
mismatch in MLflow's `ModelVersion.version`, exchange payload-shape mismatches) were
integration-shaped problems — invisible to per-component unit tests, only found by
actually running things end-to-end. This priority isn't just "more tests," it's
closing the exact category of gap that's bitten this project the most.

### New test file: `tests/integration/test_e2e_pipeline.py`
```python
class E2EPipelineTests(unittest.TestCase):
    """WebSocket → Kafka → Spark → API, exercised as one real chain, not
    mocked at each boundary the way the per-phase unit tests do."""

    def test_ingested_event_flows_to_feature_computation(self):
        # 1. Feed a real (or realistic fixture) exchange payload through the
        #    actual adapter (producer/adapters.py), not a synthetic dict.
        # 2. Publish it via a real (test-topic) Kafka producer.
        # 3. Consume it via the real Spark streaming pipeline.
        # 4. Assert the computed features match hand-calculated expected values.
        ...

    def test_prediction_request_flows_to_stored_realized_error(self):
        # Two sequential real /predict calls against a running TestClient,
        # confirming the second call's current_price genuinely realizes
        # the first prediction's error in PredictionStore — the same
        # scenario already unit-tested in isolation, now exercised through
        # the actual FastAPI app + real SQLite store together.
        ...
```

Needs a real (or `testcontainers`-managed) Kafka instance for the first test — worth
adding `testcontainers` as a dev dependency specifically for this, rather than mocking
Kafka, since the whole point is testing the real integration.

### CI wiring
Add as its own job in `.github/workflows/ci.yml`, separate from the existing phase-based
test jobs — likely slower (spins up real Kafka), so shouldn't block the fast feedback
loop the existing job split already provides.

---

## Priority 7 — Clean repository

**Impact: Low cost, real value for anyone (including future-you) reading this repo.**

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
mkdir docs
git mv ci-failure-diagnosis.md docs/
git mv mypy-all-30-fixes.md docs/          # if it still exists
git mv trivy-*.md docs/
git mv ci-*.md docs/
git mv phase*.md docs/
```
Also remove the stray `alerts.json` (2.4MB, flagged earlier as an accidental commit)
and the duplicate `dashboard/dashboard.Dockerfile` while cleaning house:
```powershell
git rm alerts.json
git rm dashboard/dashboard.Dockerfile
echo "alerts.json" >> .gitignore
```
Update the new README (Priority 1) to link to `docs/` for anyone who wants the full
investigation history, rather than it cluttering the repo root.

---

## Priority 4 — Argo CD

**Impact: High, but infrastructure polish rather than project substance — sequence
this last.**

```
GitHub Actions → Container Registry (GHCR) → GitOps manifests repo/dir → Argo CD → Kubernetes
```

Pairs naturally with the self-hosted `k3s` cluster already being set up — Argo CD can
run directly in that same cluster.

### Rough shape
1. Install Argo CD into the k3s cluster:
   ```bash
   kubectl create namespace argocd
   kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
   ```
2. Restructure `kubernetes/staging/` and `kubernetes/production/` as an Argo CD
   `Application` resource pointing at this repo's manifests directory
3. **Change what CI does**: instead of `kubectl apply` + `kubectl set image` directly
   (the current `deploy-staging`/`deploy-production` jobs), CI's job becomes just
   *updating the image tag in the manifest files and committing that change* — Argo CD
   itself watches the repo and handles the actual `kubectl apply` reconciliation. This
   is the core GitOps shift: CI stops being the thing that touches the cluster
   directly.
4. Removes the need for the self-hosted runner to have `kubectl` deploy permissions
   at all for the *push* side — Argo CD, running inside the cluster, does the pulling
   instead.

## Net effect

Priorities 1, 3, 5's gap, and 2 make the project's actual claims more honest and
better-evidenced (what does it really do, does it really work, would it really make
money, does it hold up across time) — that's the substantive work. 6 hardens exactly
the category of bug that's caused the most real problems in this build so far. 7 is
cheap housekeeping. 4 is a genuine architectural improvement but doesn't change what
the project *proves* — appropriately last.
