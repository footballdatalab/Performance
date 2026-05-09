"""
Spark <-> Postgres JDBC helpers (Phase 8.8.E, Option 1).

Reading
-------
:func:`read_jdbc` returns a Spark DataFrame backed by a Postgres
query. Two key knobs:

* ``partition_column`` + ``num_partitions`` + ``lower_bound`` /
  ``upper_bound`` — Spark splits the SELECT into N independent JDBC
  cursors, each pulling a disjoint range. This is the **only** way to
  parallelize JDBC reads. Without it, the entire result set comes
  through a single cursor and Spark can't beat parallel Postgres.
* ``fetchsize`` — server-side cursor size. Default 10k is good; raise
  to 50k+ for narrow rows, leave at 10k for wide JSONB rows.

Writing
-------
:func:`write_jdbc` writes a DataFrame back to Postgres via JDBC. For
bulk silver-layer writes the better path is usually:
  1. Write the DataFrame to a Spark-managed *stage* table with
     ``mode='overwrite'``.
  2. Use the existing 8.7.A ``atomic_publish_table`` helper (from the
     non-Spark code path) to swap stage→live.

This avoids JDBC's row-by-row INSERT semantics and benefits from the
existing UPSERT plumbing.

For OLTP-style appends (e.g. quality flags, timing rows), JDBC append
is fine.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from ingestion.common.config import get_db_config
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


def _build_jdbc_url(config: dict[str, Any]) -> str:
    return (
        f"jdbc:postgresql://{config['host']}:{config['port']}/{config['dbname']}"
    )


def _common_jdbc_options(config: dict[str, Any]) -> dict[str, str]:
    return {
        "url": _build_jdbc_url(config),
        "driver": "org.postgresql.Driver",
        "user": config["user"],
        "password": config["password"],
        # ApplicationName is visible in pg_stat_activity for debugging.
        "ApplicationName": "spark_etl",
        # Server-side cursors so Postgres doesn't materialize the whole
        # result set in memory before streaming.
        "fetchsize": "10000",
    }


def read_jdbc(
    spark: Any,
    *,
    query: str,
    partition_column: Optional[str] = None,
    lower_bound: Optional[Any] = None,
    upper_bound: Optional[Any] = None,
    num_partitions: int = 4,
    fetchsize: int = 10_000,
    config: Optional[dict[str, Any]] = None,
) -> Any:
    """Read a Postgres query into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        From :func:`ingestion.common.spark.session.spark_session`.
    query : str
        A SELECT (no trailing semicolon). Wrap in parens internally
        before passing to JDBC's ``dbtable`` option.
    partition_column : str, optional
        Numeric / timestamp column for parallel range partitioning.
        Strongly recommended for >1M-row reads. Without it Spark uses
        a single cursor and parallelism is wasted.
    lower_bound, upper_bound : any, optional
        Range bounds for partitioning. Required when
        ``partition_column`` is set.
    num_partitions : int
        Spark partition count. Default 4. Tune to ``num_cores`` /
        ``num_jdbc_clients_postgres_can_handle``.
    fetchsize : int
        Server-side cursor batch size.
    config : dict, optional
        Override the DB config. Default uses :func:`get_db_config`.
    """
    db_config = config or get_db_config()
    options = _common_jdbc_options(db_config)
    options["dbtable"] = f"({query}) AS subq"
    options["fetchsize"] = str(fetchsize)

    if partition_column is not None:
        if lower_bound is None or upper_bound is None:
            raise ValueError(
                "partition_column requires lower_bound + upper_bound for "
                "Spark to split the JDBC read."
            )
        options["partitionColumn"] = partition_column
        options["lowerBound"] = str(lower_bound)
        options["upperBound"] = str(upper_bound)
        options["numPartitions"] = str(num_partitions)

    logger.info(
        "spark.read_jdbc | query=%r partition_column=%s num_partitions=%d fetchsize=%d",
        query[:80] + "..." if len(query) > 80 else query,
        partition_column,
        num_partitions,
        fetchsize,
    )

    return (
        spark.read.format("jdbc")
            .options(**options)
            .load()
    )


def write_jdbc(
    df: Any,
    *,
    table: str,
    mode: str = "append",
    batchsize: int = 10_000,
    truncate: bool = False,
    config: Optional[dict[str, Any]] = None,
) -> None:
    """Write a Spark DataFrame to Postgres via JDBC.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to write.
    table : str
        Fully-qualified destination (``schema.table``).
    mode : {'append', 'overwrite', 'ignore', 'errorifexists'}
        Standard Spark write mode. Use ``'append'`` for stage tables
        you'll later atomic-swap into the live table; use
        ``'overwrite'`` only with extreme care.
    batchsize : int
        JDBC INSERT batch size. Default 10k is good for narrow rows.
    truncate : bool
        When ``mode='overwrite'``, ``truncate=True`` does a TRUNCATE
        instead of DROP+CREATE. Default False.
    config : dict, optional
        Override the DB config.
    """
    db_config = config or get_db_config()
    options = _common_jdbc_options(db_config)
    options["dbtable"] = table
    options["batchsize"] = str(batchsize)
    options["truncate"] = "true" if truncate else "false"
    # Allow Postgres-side rewriting of batch INSERTs into multi-VALUES
    # form, which is much faster than per-row INSERT.
    options["reWriteBatchedInserts"] = "true"

    logger.info(
        "spark.write_jdbc | table=%s mode=%s batchsize=%d truncate=%s",
        table, mode, batchsize, truncate,
    )

    (
        df.write.format("jdbc")
          .options(**options)
          .mode(mode)
          .save()
    )


def get_partition_bounds(
    db: Any,
    table: str,
    partition_column: str,
) -> tuple[Optional[int], Optional[int], int]:
    """Return ``(min, max, total_rows)`` for the partition column.

    Used to populate ``lower_bound`` / ``upper_bound`` for
    :func:`read_jdbc`. We can't parallelize without these.

    Returns ``(None, None, 0)`` for empty tables — caller should fall
    back to a non-partitioned read in that case.
    """
    rows = db.fetch_all_dict(
        f"""
        SELECT
            MIN({partition_column})::bigint  AS lower,
            MAX({partition_column})::bigint  AS upper,
            COUNT(*)                          AS total
          FROM {table}
        """
    )
    if not rows:
        return None, None, 0
    row = rows[0]
    return row["lower"], row["upper"], row["total"]
