"""Unit tests for ingestion.common.spark (Phase 8.8.E).

We don't actually start a SparkSession in unit tests — pyspark startup
takes 5-15 seconds and we want fast unit tests. Instead we test:

  1. The ``is_spark_available`` import-check.
  2. The JDBC option-building helpers (pure functions).
  3. The ``read_jdbc`` / ``write_jdbc`` argument validation against a
     mocked DataFrameReader/Writer chain.
  4. The ``spark_session`` config string construction (without
     actually calling getOrCreate).
"""

from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.extensions = types.ModuleType("psycopg2.extensions")
    psycopg2_stub.extensions.connection = object
    psycopg2_stub.extensions.cursor = object
    psycopg2_stub.extras = types.ModuleType("psycopg2.extras")
    psycopg2_stub.extras.execute_values = lambda *args, **kwargs: None
    psycopg2_stub.extras.RealDictCursor = object
    psycopg2_pool_stub = types.ModuleType("psycopg2.pool")
    psycopg2_pool_stub.ThreadedConnectionPool = object
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extensions"] = psycopg2_stub.extensions
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras
    sys.modules["psycopg2.pool"] = psycopg2_pool_stub


# ---------------------------------------------------------------------------
# is_spark_available
# ---------------------------------------------------------------------------

def test_is_spark_available_returns_a_bool() -> None:
    """Defensive: should always return True or False, never raise."""
    from ingestion.common.spark import is_spark_available
    result = is_spark_available()
    assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# jdbc.read_jdbc / write_jdbc — argument validation + option-building
# ---------------------------------------------------------------------------

def test_read_jdbc_requires_bounds_when_partition_column_is_set() -> None:
    from ingestion.common.spark.jdbc import read_jdbc
    spark = MagicMock()
    with pytest.raises(ValueError, match="lower_bound"):
        read_jdbc(
            spark,
            query="SELECT * FROM bronze.test",
            partition_column="id",
            num_partitions=4,
            config={"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"},
        )


def test_read_jdbc_builds_correct_jdbc_options() -> None:
    from ingestion.common.spark.jdbc import read_jdbc
    spark = MagicMock()
    config = {"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"}

    read_jdbc(
        spark,
        query="SELECT id, foo FROM bronze.test",
        partition_column="id",
        lower_bound=1,
        upper_bound=1_000_000,
        num_partitions=8,
        fetchsize=20_000,
        config=config,
    )

    # Verify .options(...) was called with our expected keys.
    options_call = spark.read.format.return_value.options
    options_call.assert_called_once()
    kwargs = options_call.call_args.kwargs
    assert kwargs["url"] == "jdbc:postgresql://h:5432/d"
    assert kwargs["driver"] == "org.postgresql.Driver"
    assert kwargs["partitionColumn"] == "id"
    assert kwargs["lowerBound"] == "1"
    assert kwargs["upperBound"] == "1000000"
    assert kwargs["numPartitions"] == "8"
    assert kwargs["fetchsize"] == "20000"
    # The query is wrapped in parens for JDBC's dbtable option.
    assert kwargs["dbtable"] == "(SELECT id, foo FROM bronze.test) AS subq"


def test_read_jdbc_without_partition_column_does_not_set_partition_keys() -> None:
    from ingestion.common.spark.jdbc import read_jdbc
    spark = MagicMock()
    read_jdbc(
        spark,
        query="SELECT * FROM bronze.tiny",
        config={"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"},
    )
    options_call = spark.read.format.return_value.options
    kwargs = options_call.call_args.kwargs
    assert "partitionColumn" not in kwargs
    assert "lowerBound" not in kwargs
    assert "upperBound" not in kwargs


def test_write_jdbc_passes_mode_and_batchsize() -> None:
    from ingestion.common.spark.jdbc import write_jdbc
    df = MagicMock()
    config = {"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"}

    write_jdbc(
        df,
        table="silver.vald_assessment_metric",
        mode="append",
        batchsize=5000,
        truncate=False,
        config=config,
    )

    options_call = df.write.format.return_value.options
    options_call.assert_called_once()
    kwargs = options_call.call_args.kwargs
    assert kwargs["dbtable"] == "silver.vald_assessment_metric"
    assert kwargs["batchsize"] == "5000"
    assert kwargs["truncate"] == "false"
    assert kwargs["reWriteBatchedInserts"] == "true"

    df.write.format.return_value.options.return_value.mode.assert_called_once_with("append")


def test_get_partition_bounds_returns_min_max_count() -> None:
    from ingestion.common.spark.jdbc import get_partition_bounds

    class _Db:
        def fetch_all_dict(self, sql: str, params=None):
            return [{"lower": 1, "upper": 1000, "total": 999}]

    lower, upper, total = get_partition_bounds(_Db(), "bronze.test", "id")
    assert lower == 1
    assert upper == 1000
    assert total == 999


def test_get_partition_bounds_handles_empty_table() -> None:
    from ingestion.common.spark.jdbc import get_partition_bounds

    class _Db:
        def fetch_all_dict(self, sql: str, params=None):
            return []

    lower, upper, total = get_partition_bounds(_Db(), "bronze.empty", "id")
    assert lower is None
    assert upper is None
    assert total == 0


# ---------------------------------------------------------------------------
# session.spark_session
# ---------------------------------------------------------------------------

def test_spark_session_with_existing_session_yields_passthrough() -> None:
    """When the caller passes existing_session, we return it as-is."""
    from ingestion.common.spark.session import spark_session
    sentinel = MagicMock(name="spark_session_sentinel")
    with spark_session(existing_session=sentinel) as got:
        assert got is sentinel
    # The caller's responsibility — we did NOT call .stop().
    sentinel.stop.assert_not_called()


def test_spark_session_raises_if_jdbc_jar_env_points_to_missing_file(tmp_path) -> None:
    """SPARK_JDBC_JAR pointing at a nonexistent path raises a clear error."""
    from ingestion.common.spark.session import spark_session
    import os

    bogus = str(tmp_path / "does_not_exist.jar")
    old = os.environ.get("SPARK_JDBC_JAR")
    os.environ["SPARK_JDBC_JAR"] = bogus
    try:
        with pytest.raises(FileNotFoundError, match="does not exist"):
            with spark_session(app_name="test"):
                pass
    finally:
        if old is None:
            os.environ.pop("SPARK_JDBC_JAR", None)
        else:
            os.environ["SPARK_JDBC_JAR"] = old


def test_spark_session_raises_with_remediation_when_jar_missing(tmp_path, monkeypatch) -> None:
    """Without SPARK_JDBC_JAR and without ./jars/postgresql.jar, raise with a fix hint."""
    from ingestion.common.spark.session import spark_session

    monkeypatch.delenv("SPARK_JDBC_JAR", raising=False)
    monkeypatch.chdir(tmp_path)  # no jars/ here

    with pytest.raises(FileNotFoundError) as exc:
        with spark_session(app_name="test"):
            pass
    msg = str(exc.value)
    assert "JDBC driver" in msg
    assert "SPARK_JDBC_JAR" in msg
    assert "jdbc.postgresql.org" in msg
