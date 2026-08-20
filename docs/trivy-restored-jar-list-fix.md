# Fix: Two Jar Patterns Dropped During Previous Edit — Restored

Repo: `Ali-stack261/stock`
Commit checked: `e2cd59d`

## Root cause, confirmed by diffing the actual repo content

Compared the current `docker/serving-api.Dockerfile` against the previous version
directly:

**Before (`bacdadd`):**
```dockerfile
RUN find / -path "*/pyspark/jars/hadoop-client-runtime*.jar" -delete \
    && find / -path "*/pyspark/jars/zookeeper*.jar" -delete \
    && find / -path "*/pyspark/jars/derby*.jar" -delete \
    && find / -path "*/pyspark/jars/avro*.jar" -delete \
    && find / -path "*/pyspark/jars/jackson-mapper-asl*.jar" -delete
```

**Current (`e2cd59d`):**
```dockerfile
RUN find / -path "*/pyspark/jars/zookeeper*.jar" -delete \
    && find / -path "*/pyspark/jars/derby*.jar" -delete \
    && find / -path "*/pyspark/jars/jackson-mapper-asl*.jar" -delete \
    && find / -path "*/pyspark/jars/curator-*.jar" -delete \
    && find / -path "*/pyspark/jars/hadoop-yarn-server-web-proxy*.jar" -delete \
    && find / -path "*/pyspark/jars/dropwizard-metrics-hadoop-metrics2-reporter*.jar" -delete
```

**`hadoop-client-runtime*.jar` and `avro*.jar` are missing from the current version.**
When the three new patterns were added, these two existing lines were dropped instead
of the new ones being appended — this fully explains why `avro-1.11.2.jar` and the
`hadoop-client-runtime` finding are still showing up in the scan: they're no longer
targeted for deletion at all. Not a caching issue, not a Docker/CI infrastructure
problem — a straightforward edit that lost two lines.

## Confirmed working correctly, not the problem here

`.trivyignore` exists and is correctly wired into both Trivy steps
(`trivyignores: ".trivyignore"` present at both the SARIF and table-format steps) —
that part of the setup is genuinely fine.

## The fix — full replacement block, not a diff

To avoid a repeat of the same line-dropping mistake, replace the **entire** `RUN
find ...` block with this complete version rather than trying to manually merge a
diff:

```dockerfile
# Remove Spark/Hadoop-bundled jars for subsystems this project never uses
# (HDFS/YARN, ZooKeeper cluster coordination, embedded Hive/Derby, Avro).
# Eliminates the large majority of Trivy findings, which live in these
# vendored jars rather than this project's actual dependencies.
RUN find / -path "*/pyspark/jars/hadoop-client-runtime*.jar" -delete \
    && find / -path "*/pyspark/jars/zookeeper*.jar" -delete \
    && find / -path "*/pyspark/jars/derby*.jar" -delete \
    && find / -path "*/pyspark/jars/avro*.jar" -delete \
    && find / -path "*/pyspark/jars/jackson-mapper-asl*.jar" -delete \
    && find / -path "*/pyspark/jars/curator-*.jar" -delete \
    && find / -path "*/pyspark/jars/hadoop-yarn-server-web-proxy*.jar" -delete \
    && find / -path "*/pyspark/jars/dropwizard-metrics-hadoop-metrics2-reporter*.jar" -delete
```

All 8 patterns together — the original 5 plus the 3 added in the last round. Reminder
from that same fix: `hadoop-client-api*.jar`, `hadoop-shaded-guava*.jar`, and
`parquet-hadoop*.jar` are deliberately **not** in this list — Spark's Parquet I/O
(which this project depends on) likely needs them even in local mode.

## Verification — mandatory, this is the third round without it catching this

This exact class of problem (a line silently dropped during editing) is precisely
what the verification step is for, and it hasn't been run before pushing the last two
rounds. Please actually run this before pushing this time:

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
docker build -f docker/serving-api.Dockerfile -t serving-api-test .

# Confirm ALL 8 targeted jars are actually gone
docker run --rm serving-api-test find / -name "*.jar" -path "*pyspark*"
```

Check the output against this expected list — should **NOT** appear:
`hadoop-client-runtime*`, `zookeeper*` (except none), `derby*`, `avro*`,
`jackson-mapper-asl*`, `curator-*`, `hadoop-yarn-server-web-proxy*`,
`dropwizard-metrics-hadoop-metrics2-reporter*`

Should **still** appear: `hadoop-client-api*`, `hadoop-shaded-guava*`,
`parquet-hadoop*`, `netty-*`

Then confirm Spark and Parquet still actually work:
```powershell
docker run --rm serving-api-test python -c "
from streaming.spark_stream import build_spark_session
spark = build_spark_session('check')
df = spark.range(10).toDF('n')
df.write.mode('overwrite').parquet('/tmp/test_parquet')
result = spark.read.parquet('/tmp/test_parquet')
print('Parquet round-trip row count:', result.count())
spark.stop()
"
```

**Only push after seeing both of these confirm correctly** — pasting the actual
output here first, if anything looks off, is faster than another round-trip through
a full CI run.

## How to apply

```powershell
git add docker/serving-api.Dockerfile
git commit -m "fix: restore hadoop-client-runtime and avro jar deletion, dropped in previous edit"
git push origin master
```

## Net effect

This should be the fix that actually moves the count significantly — all 8 patterns
verified against the real jar list, none accidentally dropped this time, assuming the
local verification above is actually run and confirms clean before pushing.
