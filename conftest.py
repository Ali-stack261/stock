"""Root conftest.py – environment setup for PySpark on Windows.

HADOOP_HOME
-----------
Spark's Hadoop shell integration requires HADOOP_HOME to point to a directory
containing ``winutils.exe`` for full streaming support on Windows. For tests
that don't need actual file-system permissions (batch queries, schema checks),
pointing HADOOP_HOME at any existing directory suppresses the Shell class-init
crash that otherwise kills the JVM before the test logic runs.

Tests that do need winutils.exe (streaming checkpoint writes via
FileContextBasedCheckpointFileManager) are individually decorated with
``@pytest.mark.skipif(sys.platform == "win32", ...)`` so they are skipped
cleanly on Windows rather than failing with an opaque JVM error.

JVM module opens (JDK 17 / 21)
--------------------------------
When running PySpark via spark-submit, set SPARK_SUBMIT_OPTS (see
spark-env.ps1 in the repo root). That env var is not read when PySpark is
launched directly from Python (e.g. pytest); in that case the JVM args can be
passed via SparkConf("spark.driver.extraJavaOptions") inside each SparkSession,
or via ``--driver-java-options`` inside PYSPARK_SUBMIT_ARGS. The tests in this
repo do not currently need those options because they run in local mode with
JDK features that are still accessible by default.
"""

import os
import sys
import tempfile

# ---------------------------------------------------------------------------
# HADOOP_HOME — suppress the Hadoop Shell class-init crash on Windows
# ---------------------------------------------------------------------------
if sys.platform == "win32":
    # Use the downloaded winutils binary in the .hadoop folder
    hadoop_home = os.path.abspath(os.path.join(os.path.dirname(__file__), ".hadoop"))
    hadoop_bin = os.path.join(hadoop_home, "bin")
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    os.environ["HADOOP_HOME"] = hadoop_home
    # hadoop.home.dir is the Java system property fallback; set it as an env
    # var so PySpark picks it up when building the JVM command line.
    os.environ["hadoop.home.dir"] = hadoop_home
