"""
SparkSession factory for the local-computer Phase 8.8.E deployment.

Creates a SparkSession with sensible defaults for our use case:

* ``master = 'local[*]'`` — Spark uses every available core.
* Driver memory set from ``SPARK_DRIVER_MEMORY`` (default ``4g``).
  Increase for larger transforms; on a 16GB box we typically run with
  ``8g``.
* Executor memory irrelevant in ``local[*]`` mode (driver = executor).
* Adaptive Query Execution + dynamic partition pruning enabled — these
  are the features that make Spark beat well-tuned parallel Postgres
  for join-heavy transforms.
* Arrow enabled for pandas interop (faster Python⟷JVM transfer).
* Java -XX:+UseG1GC to keep GC pauses bounded.
* Shuffle in local filesystem (``./spark-shuffle``); cleaned up on exit.

The PostgreSQL JDBC driver must be on the Spark classpath. Set
``SPARK_JDBC_JAR`` env var to its path; default looks for
``./jars/postgresql-42.7.3.jar``. If neither is present, the session
factory raises a clear error pointing at the fix.

Usage::

    from ingestion.common.spark import spark_session

    with spark_session(app_name="vald_silver_assessment_metric") as spark:
        df = spark.read.format("jdbc").option(...).load()
        df.write.format("jdbc").option(...).mode("append").save()

The returned session is stopped on exit. If you need cross-stage reuse
(e.g. silver + gold in one Spark JVM), pass an outer SparkSession via
``existing_session=...``.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

from ingestion.common.logging import get_logger

logger = get_logger(__name__)


_DEFAULT_DRIVER_MEMORY = "4g"
_DEFAULT_JDBC_JAR = "jars/postgresql-42.7.3.jar"


def _resolve_jdbc_jar() -> str:
    """Locate the PostgreSQL JDBC driver JAR.

    Search order:
      1. ``SPARK_JDBC_JAR`` env var (absolute or relative path)
      2. ``./jars/postgresql-42.7.3.jar`` (workspace default)
      3. Raise with a clear remediation message.
    """
    env_path = os.environ.get("SPARK_JDBC_JAR")
    if env_path:
        if Path(env_path).is_file():
            return env_path
        raise FileNotFoundError(
            f"SPARK_JDBC_JAR env var points at {env_path} but the file does not exist."
        )

    default = Path.cwd() / _DEFAULT_JDBC_JAR
    if default.is_file():
        return str(default)

    raise FileNotFoundError(
        "PostgreSQL JDBC driver JAR not found. "
        "Either set SPARK_JDBC_JAR=/path/to/postgresql.jar OR place the JAR at "
        f"{default}. Get it from https://jdbc.postgresql.org/download/."
    )


@contextmanager
def spark_session(
    *,
    app_name: str = "performance_etl",
    driver_memory: Optional[str] = None,
    extra_conf: Optional[dict[str, str]] = None,
    existing_session: Any = None,
) -> Iterator[Any]:
    """Yield a configured SparkSession; stop it on exit.

    Parameters
    ----------
    app_name : str
        Visible in Spark UI / logs. Use stage-specific names to make
        Spark UI reading easier.
    driver_memory : str, optional
        Override for ``spark.driver.memory``. Default reads
        ``SPARK_DRIVER_MEMORY`` env var or falls back to ``4g``.
    extra_conf : dict, optional
        Extra ``spark.X`` config keys to set on the builder.
    existing_session : SparkSession, optional
        If provided, reuse it instead of building a new one. Useful
        when running multiple Spark transforms back-to-back in the
        same JVM. Caller is responsible for stopping it.

    Yields
    ------
    SparkSession
    """
    if existing_session is not None:
        # Caller manages lifecycle.
        yield existing_session
        return

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ImportError(
            "pyspark is not installed in this environment. "
            "Phase 8.8.E targets are guarded behind is_spark_available() checks; "
            "if you reached this code, the caller should have skipped the Spark "
            "path. Install pyspark>=3.5 to use Spark transforms."
        ) from exc

    jdbc_jar = _resolve_jdbc_jar()
    memory = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", _DEFAULT_DRIVER_MEMORY)

    # Use a per-session local dir so concurrent jobs don't collide.
    local_dir = tempfile.mkdtemp(prefix="spark-", suffix=f"-{app_name[:20]}")

    builder = (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", memory)
            .config("spark.jars", jdbc_jar)
            .config("spark.local.dir", local_dir)
            # Adaptive Query Execution: Spark's own runtime optimizer.
            # On for everything modern (>= 3.0).
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            # Arrow: 10-100× faster Python<->JVM data transfer.
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # G1GC for predictable pause times under heavy memory pressure.
            .config("spark.driver.extraJavaOptions",
                    "-XX:+UseG1GC -XX:+ExplicitGCInvokesConcurrent")
            # Shuffle / spill compression — almost always net win.
            .config("spark.shuffle.compress", "true")
            .config("spark.shuffle.spill.compress", "true")
            # SQL session — UTC for predictable joins on timestamptz.
            .config("spark.sql.session.timeZone", "UTC")
    )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "spark_session | app=%s master=local[*] driver_memory=%s jdbc_jar=%s local_dir=%s",
        app_name, memory, jdbc_jar, local_dir,
    )

    try:
        yield spark
    finally:
        try:
            spark.stop()
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
