# spark-env.ps1 – source this before running PySpark on JDK 17 / JDK 21.
#
# Usage:
#   . .\spark-env.ps1          # dot-source to apply in current shell
#   . .\spark-env.ps1; python your_script.py
#
# Why this is needed:
#   Spark 3.5.x uses internal JVM APIs that are encapsulated by default in
#   Java 17+. Without these --add-opens flags, Arrow's DirectByteBuffer
#   initialisation fails with InaccessibleObjectException or sun.misc.Unsafe
#   errors.

$env:SPARK_SUBMIT_OPTS = "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

Write-Host "SPARK_SUBMIT_OPTS set:" $env:SPARK_SUBMIT_OPTS
