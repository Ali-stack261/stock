# Two Fixes: Real Perl CVE Suppression + Serving-Only Requirements Split

Repo: `Ali-stack261/stock`
Commit checked: `442eea1` (`.trivyignore` already has more zookeeper CVE IDs added
since the version last reviewed here — good, independent progress already made)

---

## Part 1 — Add the four real, verified perl-base CVEs to `.trivyignore`

All four confirmed via the actual Trivy report (not fabricated): blank "Fixed
Version" on every one, two marked `fix_deferred` by Debian's own security team, two
`affected` with no fix yet. `perl-base` is Debian's protected/essential package
(`dpkg` depends on it) — further purge attempts won't work and shouldn't be tried
again.

### Append to the existing `.trivyignore` (don't replace — it already has valid
jackson-mapper-asl and zookeeper entries)

```diff
 # jackson-mapper-asl 1.9.13 (org.codehaus.jackson) — abandoned upstream since ~2013,
 # forked into com.fasterxml.jackson. No fixed version exists or will exist.
 CVE-2019-10202

 # zookeeper 3.6.3 — PySpark's pom.xml specifies 3.9.2, but an open Spark packaging
 # bug (SPARK-49844, https://issues.apache.org/jira/browse/SPARK-49844) causes the
 # old 3.6.3 jar to be bundled anyway across the entire pyspark<4 line. Not fixed by
 # bumping to a later 3.5.x patch version — confirmed via Spark's own issue tracker.
 CVE-2023-44981
 CVE-2026-8376
 CVE-2026-57433
 CVE-2026-42496
 CVE-2026-13221
+
+# perl-base 5.40.1-6 — Debian's protected/essential package (dpkg depends on it),
+# cannot be removed from the base image. All four CVEs below have no fixed version
+# available at all; two are explicitly fix_deferred by Debian's own security team
+# (a deliberate decision, not neglect). Confirmed via actual Trivy scan output.
+CVE-2026-42497
+CVE-2026-48962
+CVE-2026-57432
+CVE-2026-9538
```

---

## Part 2 — Serving-only requirements file

### Confirmed via the real import graph — what `serving/app.py` and everything it
transitively imports actually needs

Checked directly (grepped every `import`/`from` across `serving/*.py`,
`monitoring/drift.py`, `streaming/*.py`, `training/train.py`,
`training/register_model.py` — the full chain the serving container executes):

```
fastapi, uvicorn, pydantic       — the API itself
pyspark, pyarrow                 — model inference, feature computation
mlflow                           — model loading/registry lookups
pandas, numpy, scipy             — drift detection (ks_2samp), data handling
prometheus-client                — metrics endpoint
cryptography, protobuf           — transitive pins, keep for consistency
setuptools<81                    — build-time fix (distutils compat)
```

**Never imported anywhere in the serving chain:** `apache-airflow` (only used by
`airflow/dags/retrain_pipeline.py`, never executed by the serving container),
`pytest`/`ruff`/`mypy` (dev/test tools), `kafka-python`/`websockets`/
`websocket-client` (only used by `producer/`, the ingestion side — never imported by
`serving/`).

### New file: `requirements-serving.txt`

```
fastapi>=0.100.0
uvicorn>=0.23.0
pyspark==3.5.3
pyarrow==17.0.0
pandas>=2.2
numpy>=1.26,<2.0
scipy>=1.11
mlflow>=2.10.0
prometheus-client>=0.20.0
cryptography>=43.0.0,<50.0.0
protobuf>=4.25.8,<7.0
setuptools<81
```

Keep the existing `requirements.txt` unchanged for local dev/training/Airflow use —
this new file is specifically for what actually gets built into the serving image.

### Dockerfile change

```diff
 WORKDIR /build
-COPY requirements.txt .
-RUN pip install --no-cache-dir -r requirements.txt
+COPY requirements-serving.txt .
+RUN pip install --no-cache-dir -r requirements-serving.txt
```

## Verification before pushing

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
docker build -f docker/serving-api.Dockerfile -t serving-api-test .

# Confirm airflow is genuinely gone
docker run --rm serving-api-test python -c "import airflow" 2>&1
# Should now FAIL with ModuleNotFoundError — that's correct, expected

# Confirm the actual app still starts and imports cleanly
docker run --rm serving-api-test python -c "
from serving.app import app
print('serving.app imports successfully without airflow installed')
"
```
If the second check fails, something in the serving chain has a hidden dependency on
Airflow not caught by the grep above — worth investigating before merging rather than
assuming.

## How to apply

```powershell
git add .trivyignore requirements-serving.txt docker/serving-api.Dockerfile
git commit -m "fix: suppress verified-unfixable perl-base CVEs; split serving-only requirements to drop Airflow from the image"
git push origin master
```

## Net effect

Perl findings should finally disappear from future scans (correctly suppressed, not
chased further). The Airflow/apache-airflow-providers-http findings should disappear
entirely too — not patched, genuinely absent from the image, which is the more
correct fix since that package never belonged in a prediction-serving container in
the first place. `cryptography`/`pyarrow` findings remain (real fixes exist for those
but weren't in scope for this pass) — worth a follow-up once this lands and the count
drops again.
