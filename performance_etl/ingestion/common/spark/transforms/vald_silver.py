"""
Spark spike: silver.vald_assessment_metric build (Phase 8.8.E, Option 1).

Why this transform first?
-------------------------
* It's the heaviest single silver stage (~4h09m bronze->silver in the
  pre-cleanup baseline). Anything that helps here has the largest
  wall-clock impact.
* It's a pure functional transformation: bronze tests + bronze metrics
  + profile dim → long-form silver fact rows. No mutation, no per-row
  external API. Ideal Spark fit.
* The cross-join + jsonb expansion + per-family rule application are
  exactly the workload Spark's Catalyst optimizer + AQE was designed
  for.

Architecture (Option 1)
-----------------------
1. Read bronze tables via JDBC (parallel cursors per range partition).
2. Transform in distributed Spark memory (Catalyst applies join/filter
   pushdown, AQE coalesces shuffle partitions at runtime).
3. Write the result to a stage table via JDBC (``mode='append'``
   into a freshly-created etl_staging schema table).
4. The non-Spark code path then atomic-swaps stage→live via the
   existing 8.7.A ``atomic_publish_table`` helper. **No** UPSERT in
   Spark — we keep all UPSERT logic in pure Python+Postgres for
   simpler reasoning.

This is a SPIKE — it exists to measure whether Spark genuinely beats
parallel Postgres for our workload on a local-computer deploy. The
measurement question is:

    JDBC read throughput (Spark <-> Postgres on the same machine,
    using the loopback interface) — is it the bottleneck?

If yes, Option 1 doesn't pay off and we'd need Option 2 (Parquet
intermediate). If no, this transform productionizes.

Run with::

    python -m ingestion.common.spark.transforms.vald_silver \\
        --module nordics --measure-only

The ``--measure-only`` flag runs the transform but writes to a
discardable temp table so we can compare wall-clock against the
pure-Python baseline without polluting production silver.
"""

from __future__ import annotations

import argparse
from typing import Any, Optional

from ingestion.common.config import get_db_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.common.spark import is_spark_available
from ingestion.common.spark.session import spark_session
from ingestion.common.spark.jdbc import (
    get_partition_bounds,
    read_jdbc,
    write_jdbc,
)
from ingestion.common.timing import pipeline_run, track_stage

logger = get_logger(__name__)


def build_nordbord_metrics_via_spark(
    *,
    measure_only: bool = False,
    output_table: Optional[str] = None,
    profile_table: str = "silver.vald_athlete_profile",
    bronze_tests_table: str = "bronze.vald_nordbord_tests",
    bronze_metrics_table: str = "bronze.vald_nordbord_test_metrics",
) -> dict[str, Any]:
    """Build the NordBord per-test silver fact table in Spark.

    Returns a summary dict with row counts and elapsed timings — same
    shape as the non-Spark `_load_nordbord_metrics` so the two can be
    compared directly.

    measure_only=True writes to ``etl_staging.spark_spike_nordics_<run_id>``
    instead of a production silver table; caller is responsible for
    dropping the temp table after measurement.
    """
    if not is_spark_available():
        raise RuntimeError(
            "PySpark is not installed. "
            "build_nordbord_metrics_via_spark requires pyspark>=3.5."
        )

    db = DatabaseManager(get_db_config())
    try:
        with pipeline_run("vald", db=db) as run_id:
            destination = output_table or (
                f"etl_staging.spark_spike_nordics_{str(run_id).replace('-', '_')}"
                if measure_only
                else "silver.vald_assessment_metric_spark_stage"
            )

            # Get bounds for parallel read partitioning. NordBord has
            # `test_id` (UUID-like text) — not numeric. So we partition
            # on a derived bigint via the row hash. This works for any
            # natural-key column.
            with track_stage(
                "vald", "spark.nordics.bounds", db=db,
            ) as _m:
                lower, upper, total_rows = get_partition_bounds(
                    db, bronze_tests_table, "id"
                )
                _m["rows_read"] = total_rows

            with spark_session(app_name=f"vald_silver_nordics_{run_id}") as spark:
                with track_stage(
                    "vald", "spark.nordics.read_bronze", db=db,
                ) as _m:
                    # Range-partition the read so multiple JDBC cursors
                    # pull disjoint id ranges in parallel.
                    df_tests = read_jdbc(
                        spark,
                        query=f"SELECT * FROM {bronze_tests_table}",
                        partition_column="id" if total_rows > 100_000 else None,
                        lower_bound=lower,
                        upper_bound=upper,
                        num_partitions=8,
                    )
                    df_metrics = read_jdbc(
                        spark,
                        query=f"SELECT * FROM {bronze_metrics_table}",
                    )
                    df_profiles = read_jdbc(
                        spark,
                        query=(
                            f"SELECT provider_profile_id, target_group_id, "
                            f"target_category_id, target_category_name, "
                            f"target_group_name, provider_full_name "
                            f"FROM {profile_table} WHERE is_active = TRUE"
                        ),
                    )
                    _m["rows_read"] = total_rows

                with track_stage(
                    "vald", "spark.nordics.transform", db=db,
                ) as _m:
                    # The actual transform — same join logic as
                    # silver_etl._load_nordbord_metrics, expressed in
                    # DataFrame API so Spark can optimize / parallelize.
                    df_joined = (
                        df_tests.alias("t")
                        .join(
                            df_metrics.alias("m"),
                            df_tests["test_id"] == df_metrics["test_id"],
                            how="left",
                        )
                        .join(
                            df_profiles.alias("p"),
                            df_tests["profile_id"] == df_profiles["provider_profile_id"],
                            how="inner",
                        )
                    )
                    # The full per-metric expansion would be the next
                    # step (jsonb_array_elements equivalent in Spark
                    # via explode + struct-walking). For the SPIKE we
                    # stop at the join to measure Spark's join-only
                    # performance vs pure Postgres.
                    transformed_count = df_joined.count()
                    _m["rows_written"] = transformed_count

                with track_stage(
                    "vald", "spark.nordics.write", db=db,
                    extra={"destination": destination, "measure_only": measure_only},
                ) as _m:
                    write_jdbc(
                        df_joined,
                        table=destination,
                        mode="overwrite",
                        truncate=False,
                    )
                    _m["rows_written"] = transformed_count

            summary = {
                "transform": "build_nordbord_metrics_via_spark",
                "destination": destination,
                "measure_only": measure_only,
                "bronze_tests_rows": total_rows,
                "rows_written": transformed_count,
                "run_id": str(run_id),
            }
            logger.info("Spark spike summary: %s", summary)
            return summary
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--module",
        choices=["nordics"],
        default="nordics",
        help=(
            "Which silver family to build. Currently only 'nordics' is "
            "implemented in Spark; others to follow once 'nordics' "
            "proves Spark beats parallel Postgres on this hardware."
        ),
    )
    parser.add_argument(
        "--measure-only",
        action="store_true",
        help=(
            "Write to a discardable temp table instead of production "
            "silver. Use for wall-clock comparison runs."
        ),
    )
    args = parser.parse_args()

    if args.module == "nordics":
        summary = build_nordbord_metrics_via_spark(measure_only=args.measure_only)
        print(summary)


if __name__ == "__main__":
    main()
