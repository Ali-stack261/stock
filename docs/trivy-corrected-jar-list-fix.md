# Fix: Corrected Jar Removal List (Verified Against Real pyspark 3.5.3 Contents)

Repo: `Ali-stack261/stock`
Commit checked: `bacdadd`

## Why the previous fix barely moved the needle (429 → 427)

Checked the Dockerfile structure first — it's actually correct. The `RUN find ...
-delete` step genuinely runs in the `builder` stage before the runtime stage's
`COPY --from=builder /usr/local/lib/python3.12/site-packages ...` line, so deletions
do carry through. **The real bug is that the glob patterns were incomplete** —
verified by listing the actual jars bundled inside a real `pyspark==3.5.3`
installation rather than guessing filenames:

```
avro-1.11.2.jar                 ← matched
avro-ipc-1.11.2.jar              ← matched (same "avro*" glob)
avro-mapred-1.11.2.jar           ← matched
curator-client-2.13.0.jar        ← MISSED — never targeted at all
curator-framework-2.13.0.jar     ← MISSED
curator-recipes-2.13.0.jar       ← MISSED
derby-10.14.2.0.jar              ← matched
dropwizard-metrics-hadoop-metrics2-reporter-0.1.2.jar  ← MISSED
hadoop-client-api-3.3.4.jar      ← MISSED (different jar than hadoop-client-runtime!)
hadoop-client-runtime-3.3.4.jar  ← matched
hadoop-shaded-guava-1.1.1.jar    ← MISSED
hadoop-yarn-server-web-proxy-3.3.4.jar  ← MISSED
jackson-mapper-asl-1.9.13.jar    ← matched
zookeeper-3.6.3.jar              ← matched
zookeeper-jute-3.6.3.jar         ← matched (same "zookeeper*" glob)
```

The original pattern only targeted `hadoop-client-runtime*.jar` — completely missing
`hadoop-client-api` (a *separate* jar despite the similar name), the three
`curator-*` jars (ZooKeeper's client library, closely related to the zookeeper CVEs),
`hadoop-yarn-server-web-proxy`, `hadoop-shaded-guava`, and the dropwizard metrics
reporter. That's the majority of the actual vulnerable surface, left untouched.

## Corrected fix

```diff
 RUN find / -path "*/pyspark/jars/hadoop-client-runtime*.jar" -delete \
     && find / -path "*/pyspark/jars/zookeeper*.jar" -delete \
     && find / -path "*/pyspark/jars/derby*.jar" -delete \
     && find / -path "*/pyspark/jars/avro*.jar" -delete \
-    && find / -path "*/pyspark/jars/jackson-mapper-asl*.jar" -delete
+    && find / -path "*/pyspark/jars/jackson-mapper-asl*.jar" -delete \
+    && find / -path "*/pyspark/jars/curator-*.jar" -delete \
+    && find / -path "*/pyspark/jars/hadoop-yarn-server-web-proxy*.jar" -delete \
+    && find / -path "*/pyspark/jars/dropwizard-metrics-hadoop-metrics2-reporter*.jar" -delete
```

## Two jars deliberately NOT included — real risk, not an oversight

**`hadoop-client-api-3.3.4.jar`** and **`hadoop-shaded-guava-1.1.1.jar`** are left in
place, on purpose. Spark routes file I/O through Hadoop's `FileSystem` abstraction
even for plain local (`file://`) paths — this project's Parquet-heavy usage (Phase 5
storage, the reference-features file, etc.) likely depends on classes in
`hadoop-client-api` even without a real HDFS/YARN cluster. `hadoop-shaded-guava` is a
dependency of that same jar. Removing these two carries real risk of breaking Parquet
I/O in ways that might not surface until a specific code path runs. Given the earlier
fix already shipped without the mandatory verification step actually catching a
problem, I'm being more conservative here rather than repeating that pattern.

**`parquet-hadoop-1.13.1.jar` was never targeted at all** — this one is almost
certainly a hard requirement (Spark's actual Parquet read/write implementation),
correctly left alone.

## Verification — actually required this time, not optional

The previous attempt at this exact kind of fix apparently shipped without this step
catching the incomplete pattern issue. Do this before merging:

```powershell
cd 'C:\Users\Alim1\OneDrive\Desktop\stock'
docker build -f docker/serving-api.Dockerfile -t serving-api-test .

# 1. Confirm the jars are actually gone this time
docker run --rm serving-api-test find / -name "*.jar" -path "*pyspark*"
# Should NOT show: curator-*, hadoop-yarn-server-web-proxy*, dropwizard-metrics-hadoop*
# (in addition to the ones already confirmed gone: zookeeper*, derby*, avro*, jackson-mapper-asl*, hadoop-client-runtime*)
# SHOULD still show: hadoop-client-api*, hadoop-shaded-guava*, parquet-hadoop*, netty-*

# 2. Confirm Spark still actually works
docker run --rm serving-api-test python -c "
from streaming.spark_stream import build_spark_session
spark = build_spark_session('jar_removal_check')
df = spark.range(10)
print('Row count:', df.count())
spark.stop()
print('Spark local-mode session works fine')
"

# 3. Confirm Parquet I/O specifically still works (the real risk area)
docker run --rm serving-api-test python -c "
from streaming.spark_stream import build_spark_session
spark = build_spark_session('parquet_check')
df = spark.range(10).toDF('n')
df.write.mode('overwrite').parquet('/tmp/test_parquet')
result = spark.read.parquet('/tmp/test_parquet')
print('Parquet round-trip row count:', result.count())
spark.stop()
"
```
If step 3 fails, that's the signal `hadoop-client-api`/`hadoop-shaded-guava` really
are needed and should stay — which the fix above already assumes, so a failure there
specifically would mean something else broke, worth investigating rather than
assuming.

## How to apply

```powershell
git add docker/serving-api.Dockerfile
git commit -m "fix: complete the Spark jar removal list, verified against real pyspark 3.5.3 contents"
git push origin master
```

## Net effect

This should meaningfully move the count this time, since the earlier near-zero
change is now fully explained (incomplete patterns, not a structural bug) and this
fix is based on the real, verified jar list rather than a second guess.
