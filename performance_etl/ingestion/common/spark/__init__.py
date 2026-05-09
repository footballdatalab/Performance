"""
PySpark integration (Phase 8.8.E, Option 1).

Spark is used as a **compute engine** over PostgreSQL — it reads bronze
data via JDBC, runs the transform in distributed memory, and writes
silver back via JDBC (or COPY for bulk paths). The lakehouse stays
Postgres-only; Spark is **not** a storage layer here.

Local-computer deploy: ``SparkSession.builder.master('local[*]')`` —
Spark uses every available core. No cluster, no Hadoop, no metastore.

Public API:

* :func:`ingestion.common.spark.session.spark_session` — context
  manager that creates a properly-configured local SparkSession.
* :func:`ingestion.common.spark.jdbc.read_jdbc` — read a query into a
  Spark DataFrame with sensible partitioning hints.
* :func:`ingestion.common.spark.jdbc.write_jdbc` — write a DataFrame
  back to Postgres. ``mode='append'`` recommended; the caller drives
  any UPSERT semantics via stage tables + atomic_publish (8.7.A).

When PySpark is not installed in the runtime environment, importing
these helpers fails fast with a clear ``ImportError``. The rest of the
ETL never imports them transitively, so the absence of pyspark is not
a runtime concern for non-Spark code paths.
"""

from __future__ import annotations

__all__ = [
    "spark_session",
    "read_jdbc",
    "write_jdbc",
    "is_spark_available",
]


def is_spark_available() -> bool:
    """Cheap check: can we import pyspark in this runtime?"""
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


# Lazy re-exports — importing this package should not eagerly import
# pyspark (the library is heavy, ~250MB on disk, slow to import).
def __getattr__(name: str):
    if name == "spark_session":
        from ingestion.common.spark.session import spark_session
        return spark_session
    if name == "read_jdbc":
        from ingestion.common.spark.jdbc import read_jdbc
        return read_jdbc
    if name == "write_jdbc":
        from ingestion.common.spark.jdbc import write_jdbc
        return write_jdbc
    raise AttributeError(f"module 'ingestion.common.spark' has no attribute {name!r}")
