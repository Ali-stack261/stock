# CI Failure Diagnosis: Lint (Confirmed + Fixed) vs. the Other Three (Still Unknown)

Repo: `Ali-stack261/stock`
Commit checked: `5cd91a1`

GitHub Actions run showed 4 red X's: Airflow Tests, Fast Tests, Lint, Spark+MLflow
Tests. Deploy jobs correctly show "Skipped" since their dependencies failed. This doc
covers what I could and couldn't confirm locally.

## Lint — root cause confirmed, one real bug found underneath

Ran `ruff check .` directly against the repo: **185 violations, first time lint has
ever run against this codebase** across all 14 phases. That alone explains the
10-second failure.

### Breakdown by rule

```
 68 UP045   use `X | None` instead of `Optional[X]`
 22 F401    unused imports
 18 I001    import block not sorted
 15 UP006   use `list`/`dict` instead of `List`/`Dict`
  9 F821    undefined name
  8 UP035   deprecated typing import
  7 PIE804  unnecessary dict kwargs
  5 BLE001  blind except
  5 B008    function call in default argument
  4 UP037   unnecessary quoted annotation
  4 RUF059  unused variable
  4 DTZ003  naive datetime.utcnow() usage
  3 TRY004  wrong exception type
  3 DTZ001  naive datetime construction
  2 SIM117  nested `with` should be combined
  2 F841    unused local variable
  2 F811    redefinition
  1 S112    silent except-continue
  1 RUF100  unused noqa
  1 FLY002  static join could be f-string
  1 DTZ011  naive date.today()
```

**176 of these are harmless backlog** — style/modernization preferences that
accumulated because nothing was ever checking for them. Bulk-fixable:
```bash
ruff check . --fix
```
This alone resolves everything except the 9 `F821`s and a handful of others needing
manual judgment (`BLE001`/`TRY004` blind-except patterns, `DTZ*` naive-datetime usage
— the same `datetime.utcnow()` deprecation flagged earlier in this project, now
showing up formally as a lint violation too).

### The 9 `F821`s split into two real categories — not all equally important

**6 are false-positive-adjacent** — string-quoted forward-reference type hints where
the referenced module isn't imported at module scope (`-> "pyspark.ml.PipelineModel"`
in `training/train.py`, `-> "pyspark.sql.streaming.StreamingQuery"` in
`streaming/storage.py`, `-> "pd.DataFrame"` / `-> "pd.Series"` in
`serving/prediction_store.py`). These were written this way deliberately to avoid an
unconditional heavy `pyspark`/`pandas` import in files that don't otherwise need it.
Not a runtime bug (Python never evaluates these strings unless something calls
`typing.get_type_hints()`), but should be fixed properly rather than suppressed:
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyspark
    import pandas as pd
```
Add this guard at the top of each affected file. Zero runtime cost, and `ruff`/`mypy`
can now resolve the names.

**3 are a genuine, previously-undiscovered runtime bug** — confirmed by reading the
actual code, not just the lint output. In `monitoring/drift.py`:
```python
def _check_feature_drift(self, current_data):
    try:
        from evidently.report import Report            # local import, scoped to THIS method
        from evidently.metric_preset import DataDriftPreset
        return self._check_feature_drift_evidently(current_data)
    except ImportError:
        return self._check_feature_drift_fallback(current_data)

def _check_feature_drift_evidently(self, current_data: pd.DataFrame) -> dict:
    report = Report(metrics=[DataDriftPreset()])   # <-- NameError waiting to happen
```
`Report`/`DataDriftPreset` are local variables scoped to `_check_feature_drift`'s
function frame — they don't exist in `_check_feature_drift_evidently`'s separate
scope. **This has been completely dormant and untested this entire project**, since
`evidently` was deliberately never added to `requirements.txt` (the scipy/numpy
fallback was always what actually ran). If `evidently` is ever installed and this
path executes, it crashes immediately with `NameError: name 'Report' is not defined`.

**Fix** — move the imports into the method that actually uses them:
```python
def _check_feature_drift_evidently(self, current_data: pd.DataFrame) -> dict:
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset
    report = Report(metrics=[DataDriftPreset()])
    ...
```
And the same pattern for the `RegressionErrorDistribution` case at line 251
(concept-drift check) — same bug, same fix.

## Fast Tests, Airflow Tests, Spark+MLflow Tests — NOT reproducible locally

This is the honest, important part: I ran the exact equivalent of the "Fast Tests"
job in a genuinely clean virtualenv (fresh `pip install -r requirements.txt`, same
test files: `test_phase1.py`, `test_phase2.py`, `test_transports.py`,
`test_adapters.py`, `test_phase9.py`) and **it passed cleanly — 22/22 in 7.3s.**

This rules out a code or dependency bug as the cause of that specific job's failure —
if it were a real code/dependency problem, my clean-venv reproduction should have hit
it too, the same way it caught the earlier `distutils`/`setuptools` issue. Since it
didn't, the failure is most likely something specific to the actual GitHub Actions
workflow configuration or runner environment — possibilities, roughly in order of
likelihood:
- A YAML syntax/configuration issue in the `fast-tests` job definition itself
  (wrong working directory, wrong file path, a step ordering issue)
- A caching problem with `actions/setup-python`'s `cache: "pip"` serving a stale or
  corrupted cache
- Something about how GitHub's runner differs from this sandbox that isn't visible
  from the code alone

**I can't diagnose these three further without the actual log text** — I don't have
GitHub API access right now (rate-limited on this sandbox's shared IP) and reasoning
from the code alone hasn't turned up anything, since a faithful local reproduction of
the simplest of the three jobs passed.

## What to do next

1. Apply the Lint fixes above (`ruff check . --fix` + the 3 manual `F821` fixes) —
   this one is fully diagnosed and actionable right now.
2. For the other three: open the **Fast Tests** job's "Details" page specifically
   (simplest job, most likely to have a short, clear error) and paste the actual log
   output. Given a faithful local reproduction passed, whatever's failing is probably
   visible immediately in the log rather than requiring deep investigation.
