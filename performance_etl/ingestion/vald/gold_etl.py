"""
VALD gold ETL.

Publishes the scoped silver assessment fact table into family-specific gold
tables for downstream analytics and maintains the reference-metric coverage
audit used to validate gold promotion coverage by test name.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal
from itertools import groupby
from typing import Any

from ingestion.common.config import get_env
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.vald.cutoff import VALD_CUTOFF_UTC

logger = get_logger(__name__)

_UPPER_THRESHOLD_MULTIPLIER = 1.2
_DEFAULT_GOLD_FAMILY_WORKERS = 5
_SPEED_REFERENCE_PATTERN = re.compile(r"^split_(\d+)_cumulative_time$")
_FORCEDECKS_REFERENCE_PRIORITY = {
    "takeoff_jump_height_imp_mom": 1,
    "takeoff_jump_height_flight_time": 2,
    "takeoff_push_up_height_flight_time": 3,
    "rebound_rebound_jump_height_imp_mom": 4,
    "takeoff_peak_drop_landing_force": 5,
    "performance_peak_vertical_force": 6,
    "performance_concentric_peak_force": 7,
    "performance_eccentric_peak_force": 8,
    "balance_cop_range_medial_lateral": 9,
    "balance_cop_range_anterior_posterior": 10,
}

GOLD_TABLES = {
    "nordics": "gold.vald_nordics",
    "forceframe": "gold.vald_forceframe",
    "forcedecks": "gold.vald_forcedecks",
    "dynamo": "gold.vald_dynamo",
    "speed": "gold.vald_speed",
}

# Phase 8.7.B (2026-05-09): natural-key columns per gold mart, used for
# INSERT … ON CONFLICT DO UPDATE. UNIQUE indexes were added in Phase 8.7.B.1
# (sql/ddl/gold/58) on these column lists with NULLS NOT DISTINCT.
GOLD_CONFLICT_COLUMNS_BY_FAMILY = {
    "nordics":    ("test_id", "metric_name", "side"),
    "forceframe": ("test_id", "metric_name", "side"),
    "forcedecks": ("test_id", "metric_name", "side", "rep_number"),
    "dynamo":     ("test_id", "metric_name", "side", "rep_number"),
    "speed":      ("test_id", "metric_name", "rep_number"),
}

GOLD_COMMON_COLUMNS = [
    "provider_profile_id",
    "athlete_name",
    "team_name",
    "team_group_name",
    "team_group_id",
    "category_id",
    "test_date",
    "source_module",
    "assessment_family",
    "test_id",
    "test_name",
    "test_type",
    "metric_name",
    "metric_value",
    "metric_unit",
    "created_at",
    "updated_at",
]
GOLD_COLUMNS_BY_FAMILY = {
    "nordics": [*GOLD_COMMON_COLUMNS[:-2], "side", *GOLD_COMMON_COLUMNS[-2:]],
    "forceframe": [*GOLD_COMMON_COLUMNS[:-2], "side", *GOLD_COMMON_COLUMNS[-2:]],
    "forcedecks": [
        *GOLD_COMMON_COLUMNS[:-2],
        "side",
        "rep_number",
        *GOLD_COMMON_COLUMNS[-2:],
    ],
    "dynamo": [
        *GOLD_COMMON_COLUMNS[:-2],
        "side",
        "rep_number",
        *GOLD_COMMON_COLUMNS[-2:],
    ],
    "speed": [*GOLD_COMMON_COLUMNS[:-2], "rep_number", *GOLD_COMMON_COLUMNS[-2:]],
}

DEFAULT_GOLD_ASSESSMENT_SOURCE_TABLE = "silver.vald_assessment_metric"
DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE = "silver.vald_reference_metric_coverage"
REFERENCE_METRIC_COVERAGE_COLUMNS = [
    "source_table",
    "source_module",
    "assessment_family",
    "test_name",
    "reference_metric_name",
    "coverage_status",
    "source_test_count",
    "latest_test_date",
    "created_at",
    "updated_at",
]


def run_gold_etl(
    db: DatabaseManager,
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    family_workers: int | None = None,
    *,
    assessment_source_table: str = DEFAULT_GOLD_ASSESSMENT_SOURCE_TABLE,
    target_tables: dict[str, str] | None = None,
    coverage_table: str = DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
    scoped_test_ids_by_family: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Publish family-specific gold marts from the silver long fact table."""
    day_window = _validate_day_window(day_start_utc, day_end_utc)
    resolved_target_tables = dict(GOLD_TABLES if target_tables is None else target_tables)
    coverage_summary = _refresh_reference_metric_coverage(
        db,
        assessment_source_table=assessment_source_table,
        coverage_table=coverage_table,
    )
    has_incremental_scope = scoped_test_ids_by_family is not None
    summary: dict[str, Any] = {
        "assessment_scope": (
            "incremental_test_ids"
            if has_incremental_scope
            else "day_window" if day_window else "full"
        ),
        "tables": {},
        "coverage": coverage_summary,
        "total_rows": 0,
        "total_source_rows": 0,
        "total_excluded_above_threshold_rows": 0,
        "total_excluded_below_threshold_rows": 0,
        "total_excluded_outside_threshold_rows": 0,
    }

    ordered_families = list(resolved_target_tables.items())
    publish_window_start = day_window[0] if day_window else None
    publish_window_end = day_window[1] if day_window else None
    max_workers = min(
        len(ordered_families),
        _resolve_gold_family_workers(family_workers),
    )

    family_results: dict[str, dict[str, int]] = {}
    if max_workers == 1:
        for family, table_name in ordered_families:
            family_results[table_name] = _publish_gold_family(
                db,
                family=family,
                table_name=table_name,
                assessment_source_table=assessment_source_table,
                coverage_table=coverage_table,
                day_start_utc=publish_window_start,
                day_end_utc=publish_window_end,
                scoped_test_ids=None if not has_incremental_scope else scoped_test_ids_by_family.get(family, []),
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(
                    _publish_gold_family,
                    db,
                    family=family,
                    table_name=table_name,
                    assessment_source_table=assessment_source_table,
                    coverage_table=coverage_table,
                    day_start_utc=publish_window_start,
                    day_end_utc=publish_window_end,
                    scoped_test_ids=None if not has_incremental_scope else scoped_test_ids_by_family.get(family, []),
                ): table_name
                for family, table_name in ordered_families
            }
            for future in as_completed(future_to_table):
                family_results[future_to_table[future]] = future.result()

    for _, table_name in ordered_families:
        stats = family_results[table_name]
        summary["tables"][table_name] = stats
        summary["total_rows"] += stats["inserted_rows"]
        summary["total_source_rows"] += stats["source_rows"]
        summary["total_excluded_above_threshold_rows"] += stats["excluded_above_threshold_rows"]
        summary["total_excluded_below_threshold_rows"] += stats["excluded_below_threshold_rows"]
        summary["total_excluded_outside_threshold_rows"] += stats["excluded_outside_threshold_rows"]

    logger.info("VALD gold ETL summary: %s", summary)
    return summary


def is_above_gold_threshold(
    metric_value: float | None,
    threshold_value: float | None,
) -> bool:
    """Return True when a metric value is above the upper gold threshold."""
    return (
        metric_value is not None
        and threshold_value is not None
        and metric_value > threshold_value
    )


def is_below_gold_threshold(
    metric_value: float | None,
    threshold_value: float | None,
) -> bool:
    """Return True when a metric value is below the lower gold threshold."""
    return (
        metric_value is not None
        and threshold_value is not None
        and metric_value < threshold_value
    )


def _refresh_reference_metric_coverage(
    db: DatabaseManager,
    *,
    assessment_source_table: str,
    coverage_table: str,
) -> dict[str, Any]:
    """Rebuild the persisted reference-metric coverage audit."""
    source_rows = _fetch_reference_metric_source_rows(db)
    reference_candidates = _fetch_reference_metric_candidate_rows(
        db,
        assessment_source_table=assessment_source_table,
    )
    reference_catalog = _build_reference_metric_catalog(reference_candidates)
    coverage_rows = _build_reference_metric_coverage_rows(
        source_rows,
        reference_catalog=reference_catalog,
    )
    _replace_reference_metric_coverage_rows(
        db,
        coverage_table=coverage_table,
        coverage_rows=coverage_rows,
    )

    unmapped_rows = [
        {
            "source_table": row["source_table"],
            "source_module": row["source_module"],
            "assessment_family": row["assessment_family"],
            "test_name": row["test_name"],
            "source_test_count": row["source_test_count"],
        }
        for row in coverage_rows
        if row["coverage_status"] == "unmapped"
    ]
    covered_count = sum(1 for row in coverage_rows if row["coverage_status"] == "covered")
    summary = {
        "rows_written": len(coverage_rows),
        "covered_count": covered_count,
        "unmapped_count": len(unmapped_rows),
        "unmapped_test_names": unmapped_rows,
    }
    if unmapped_rows:
        logger.warning("VALD gold coverage has %d unmapped test names", len(unmapped_rows))
    return summary


def _fetch_reference_metric_source_rows(
    db: DatabaseManager,
) -> list[dict[str, Any]]:
    """Return one coverage source row per module/family/test_name."""
    return db.fetch_all_dict(
        f"""
        SELECT
            source_table,
            source_module,
            assessment_family,
            test_name,
            source_test_count,
            latest_test_date
        FROM (
            SELECT
                'bronze.vald_forcedecks_tests' AS source_table,
                'forcedecks' AS source_module,
                'forcedecks' AS assessment_family,
                NULLIF(BTRIM(t.test_type), '') AS test_name,
                COUNT(DISTINCT t.test_id)::BIGINT AS source_test_count,
                MAX(COALESCE(t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc)) AS latest_test_date
            FROM bronze.vald_forcedecks_tests t
            WHERE COALESCE(t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc)
                  >= '{VALD_CUTOFF_UTC}'::timestamptz
            GROUP BY NULLIF(BTRIM(t.test_type), '')

            UNION ALL

            SELECT
                'bronze.vald_forceframe_tests' AS source_table,
                'forceframe' AS source_module,
                'forceframe' AS assessment_family,
                CASE
                    WHEN NULLIF(BTRIM(t.test_position_name), '') IS NOT NULL THEN
                        CONCAT(
                            COALESCE(NULLIF(BTRIM(t.test_type_name), ''), 'ForceFrame'),
                            ' - ',
                            NULLIF(BTRIM(t.test_position_name), '')
                        )
                    ELSE COALESCE(NULLIF(BTRIM(t.test_type_name), ''), 'ForceFrame')
                END AS test_name,
                COUNT(DISTINCT t.test_id)::BIGINT AS source_test_count,
                MAX(COALESCE(t.test_date_utc, t.modified_date_utc)) AS latest_test_date
            FROM bronze.vald_forceframe_tests t
            WHERE COALESCE(t.test_date_utc, t.modified_date_utc) >= '{VALD_CUTOFF_UTC}'::timestamptz
            GROUP BY
                CASE
                    WHEN NULLIF(BTRIM(t.test_position_name), '') IS NOT NULL THEN
                        CONCAT(
                            COALESCE(NULLIF(BTRIM(t.test_type_name), ''), 'ForceFrame'),
                            ' - ',
                            NULLIF(BTRIM(t.test_position_name), '')
                        )
                    ELSE COALESCE(NULLIF(BTRIM(t.test_type_name), ''), 'ForceFrame')
                END

            UNION ALL

            SELECT
                'bronze.vald_nordbord_tests' AS source_table,
                'nordbord' AS source_module,
                'nordics' AS assessment_family,
                COALESCE(NULLIF(BTRIM(t.test_type_name), ''), 'NordBord') AS test_name,
                COUNT(DISTINCT t.test_id)::BIGINT AS source_test_count,
                MAX(COALESCE(t.test_date_utc, t.modified_date_utc)) AS latest_test_date
            FROM bronze.vald_nordbord_tests t
            WHERE COALESCE(t.test_date_utc, t.modified_date_utc) >= '{VALD_CUTOFF_UTC}'::timestamptz
            GROUP BY COALESCE(NULLIF(BTRIM(t.test_type_name), ''), 'NordBord')

            UNION ALL

            SELECT
                'bronze.vald_smartspeed_test_summaries' AS source_table,
                'smartspeed' AS source_module,
                'speed' AS assessment_family,
                COALESCE(NULLIF(BTRIM(s.test_name), ''), NULLIF(BTRIM(s.test_type_name), '')) AS test_name,
                COUNT(DISTINCT s.test_id)::BIGINT AS source_test_count,
                MAX(s.test_date_utc) AS latest_test_date
            FROM bronze.vald_smartspeed_test_summaries s
            WHERE s.test_date_utc >= '{VALD_CUTOFF_UTC}'::timestamptz
            GROUP BY COALESCE(NULLIF(BTRIM(s.test_name), ''), NULLIF(BTRIM(s.test_type_name), ''))

            UNION ALL

            SELECT
                'bronze.vald_dynamo_tests' AS source_table,
                'dynamo' AS source_module,
                'dynamo' AS assessment_family,
                COALESCE(
                    NULLIF(
                        BTRIM(
                            CONCAT_WS(
                                ' ',
                                NULLIF(BTRIM(t.body_region), ''),
                                NULLIF(BTRIM(t.movement), ''),
                                NULLIF(BTRIM(t.position), '')
                            )
                        ),
                        ''
                    ),
                    'DynaMo'
                ) AS test_name,
                COUNT(DISTINCT t.test_id)::BIGINT AS source_test_count,
                MAX(COALESCE(t.start_time_utc, t.analysed_date_utc)) AS latest_test_date
            FROM bronze.vald_dynamo_tests t
            WHERE COALESCE(t.start_time_utc, t.analysed_date_utc) >= '{VALD_CUTOFF_UTC}'::timestamptz
            GROUP BY
                COALESCE(
                    NULLIF(
                        BTRIM(
                            CONCAT_WS(
                                ' ',
                                NULLIF(BTRIM(t.body_region), ''),
                                NULLIF(BTRIM(t.movement), ''),
                                NULLIF(BTRIM(t.position), '')
                            )
                        ),
                        ''
                    ),
                    'DynaMo'
                )
        ) source_catalog
        ORDER BY source_module, assessment_family, test_name NULLS FIRST
        """
    )


def _fetch_reference_metric_candidate_rows(
    db: DatabaseManager,
    *,
    assessment_source_table: str,
) -> list[dict[str, Any]]:
    """Return aggregated metric candidates used to resolve reference metrics."""
    return db.fetch_all_dict(
        f"""
        SELECT
            source_module,
            assessment_family,
            test_name,
            metric_name,
            MAX(metric_value) AS max_metric_value
        FROM {assessment_source_table}
        WHERE test_date >= '{VALD_CUTOFF_UTC}'::timestamptz
          AND metric_value IS NOT NULL
        GROUP BY
            source_module,
            assessment_family,
            test_name,
            metric_name
        ORDER BY
            source_module,
            assessment_family,
            test_name NULLS FIRST,
            metric_name
        """
    )


def _build_reference_metric_catalog(
    metric_rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str | None], str]:
    """Resolve one reference metric per source-module/family/test-name combination."""
    grouped_rows = sorted(
        (
            {
                "source_module": str(row["source_module"]),
                "assessment_family": str(row["assessment_family"]),
                "test_name": _normalise_optional_text(row.get("test_name")),
                "metric_name": str(row["metric_name"]),
                "max_metric_value": row.get("max_metric_value"),
            }
            for row in metric_rows
        ),
        key=lambda row: (
            row["source_module"],
            row["assessment_family"],
            "" if row["test_name"] is None else row["test_name"],
            row["metric_name"],
        ),
    )

    catalog: dict[tuple[str, str, str | None], str] = {}
    for key, rows_iter in groupby(
        grouped_rows,
        key=lambda row: (
            row["source_module"],
            row["assessment_family"],
            row["test_name"],
        ),
    ):
        rows = list(rows_iter)
        metric_name = _select_reference_metric_name(
            source_module=key[0],
            assessment_family=key[1],
            metric_rows=rows,
        )
        if metric_name is not None:
            catalog[key] = metric_name
    return catalog


def _select_reference_metric_name(
    *,
    source_module: str,
    assessment_family: str,
    metric_rows: list[dict[str, Any]],
) -> str | None:
    """Return the resolved reference metric name for one test-name group."""
    if not metric_rows:
        return None

    if source_module == "smartspeed" and assessment_family == "speed":
        ranked_splits = [
            (_extract_speed_split_number(row["metric_name"]), row["metric_name"])
            for row in metric_rows
        ]
        ranked_splits = [
            (split_number, metric_name)
            for split_number, metric_name in ranked_splits
            if split_number is not None
        ]
        if not ranked_splits:
            return None
        ranked_splits.sort(key=lambda item: (-item[0], item[1]))
        return ranked_splits[0][1]

    if source_module == "forcedecks" and assessment_family == "forcedecks":
        ranked_rows = [
            row
            for row in metric_rows
            if row["metric_name"] in _FORCEDECKS_REFERENCE_PRIORITY
        ]
        if not ranked_rows:
            return None
        ranked_rows.sort(
            key=lambda row: (
                _FORCEDECKS_REFERENCE_PRIORITY[row["metric_name"]],
                -_coerce_metric_sort_value(row.get("max_metric_value")),
                row["metric_name"],
            )
        )
        return ranked_rows[0]["metric_name"]

    if source_module == "forceframe" and assessment_family == "forceframe":
        return "max_force" if any(row["metric_name"] == "max_force" for row in metric_rows) else None

    if source_module == "nordbord" and assessment_family == "nordics":
        return "max_force" if any(row["metric_name"] == "max_force" for row in metric_rows) else None

    if source_module == "dynamo" and assessment_family == "dynamo":
        return (
            "max_force_newtons"
            if any(row["metric_name"] == "max_force_newtons" for row in metric_rows)
            else None
        )

    return None


def _build_reference_metric_coverage_rows(
    source_rows: list[dict[str, Any]],
    *,
    reference_catalog: dict[tuple[str, str, str | None], str],
) -> list[dict[str, Any]]:
    """Build the persisted coverage rows from bronze-source test names."""
    rows: list[dict[str, Any]] = []
    for row in source_rows:
        source_module = str(row["source_module"])
        assessment_family = str(row["assessment_family"])
        test_name = _normalise_optional_text(row.get("test_name"))
        reference_metric_name = reference_catalog.get(
            (source_module, assessment_family, test_name)
        )
        rows.append(
            {
                "source_table": str(row["source_table"]),
                "source_module": source_module,
                "assessment_family": assessment_family,
                "test_name": test_name,
                "reference_metric_name": reference_metric_name,
                "coverage_status": "covered" if reference_metric_name else "unmapped",
                "source_test_count": int(row.get("source_test_count") or 0),
                "latest_test_date": row.get("latest_test_date"),
            }
        )
    rows.sort(
        key=lambda row: (
            row["source_module"],
            row["assessment_family"],
            "" if row["test_name"] is None else row["test_name"],
        )
    )
    return rows


def _replace_reference_metric_coverage_rows(
    db: DatabaseManager,
    *,
    coverage_table: str,
    coverage_rows: list[dict[str, Any]],
) -> None:
    """Atomically replace the persisted coverage table contents.

    Phase 8.7.A (2026-05-09): the previous code TRUNCATE+INSERTed in two
    statements, leaving `coverage_table` momentarily empty if the second
    statement failed. We now build a stage table, populate it, and atomically
    swap stage→live via :func:`atomic_publish_table` so the live table is
    never empty. Locked decision #7 satisfied.
    """
    from ingestion.common.atomic_publish import (
        atomic_publish_table,
        build_stage_table_like,
    )

    stage_table = build_stage_table_like(db, live_table=coverage_table)
    if coverage_rows:
        db.insert_batch_raw(stage_table, coverage_rows)
    atomic_publish_table(db, live_table=coverage_table, stage_table=stage_table)


def _build_gold_insert_sql(
    table_name: str,
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    *,
    family: str | None = None,
    assessment_source_table: str = DEFAULT_GOLD_ASSESSMENT_SOURCE_TABLE,
    coverage_table: str = DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
    scoped_test_ids: list[str] | None = None,
) -> str:
    """Return the threshold-based gold insert SQL for one assessment family."""
    resolved_family = _resolve_gold_family(table_name, family=family)
    gold_columns = GOLD_COLUMNS_BY_FAMILY[resolved_family]
    selection_ctes, insert_source_sql = _build_gold_selection_sql(
        family=resolved_family,
        coverage_table=coverage_table,
    )
    return f"""
        {_build_threshold_cte(
            assessment_source_table=assessment_source_table,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
            scoped_test_ids=scoped_test_ids,
        )}
        ,
        {selection_ctes}
        INSERT INTO {table_name} ({", ".join(gold_columns)})
        SELECT {", ".join(f"s.{column}" for column in gold_columns)}
        {insert_source_sql}
    """


def _build_gold_publish_sql(
    table_name: str,
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    *,
    family: str | None = None,
    assessment_source_table: str = DEFAULT_GOLD_ASSESSMENT_SOURCE_TABLE,
    coverage_table: str = DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
    scoped_test_ids: list[str] | None = None,
) -> str:
    """Return a single-pass gold publish SQL for one assessment family.

    Phase 8.7.B (2026-05-09): added ``ON CONFLICT (natural_key) DO UPDATE SET …``
    to the embedded INSERT. The natural-key UNIQUE indexes were added in
    Phase 8.7.B.1 on the production gold marts. UPSERT is correct for both:

      * Full-rebuild path (Phase 8.7.A): writes go into a fresh stage table
        whose schema mirrors the live mart (including the UNIQUE index, via
        ``LIKE … INCLUDING ALL``). Stage starts empty so ON CONFLICT never
        fires; harmless overhead.
      * Scoped path (day_window or scoped_test_ids): writes go directly to
        live; any pre-existing row matching the natural key is updated in
        place instead of needing a DELETE first.
    """
    resolved_family = _resolve_gold_family(table_name, family=family)
    gold_columns = GOLD_COLUMNS_BY_FAMILY[resolved_family]
    selection_ctes, insert_source_sql = _build_gold_selection_sql(
        family=resolved_family,
        coverage_table=coverage_table,
    )
    conflict_columns = GOLD_CONFLICT_COLUMNS_BY_FAMILY[resolved_family]
    update_columns = [c for c in gold_columns if c not in conflict_columns]
    on_conflict_clause = (
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
    )
    return f"""
        {_build_threshold_cte(
            assessment_source_table=assessment_source_table,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
            scoped_test_ids=scoped_test_ids,
        )}
        ,
        {selection_ctes}
        ,
        family_stats AS (
            SELECT
                COUNT(*)::BIGINT AS source_rows,
                COUNT(*) FILTER (WHERE is_above_threshold)::BIGINT AS excluded_above_threshold_rows,
                COUNT(*) FILTER (WHERE is_below_threshold)::BIGINT AS excluded_below_threshold_rows,
                COUNT(*) FILTER (WHERE is_above_threshold OR is_below_threshold)::BIGINT AS excluded_outside_threshold_rows
            FROM labeled_source
        ),
        inserted_rows AS (
            INSERT INTO {table_name} ({", ".join(gold_columns)})
            SELECT {", ".join(f"s.{column}" for column in gold_columns)}
            {insert_source_sql}
            {on_conflict_clause}
            RETURNING 1
        )
        SELECT
            source_rows,
            excluded_above_threshold_rows,
            excluded_below_threshold_rows,
            excluded_outside_threshold_rows,
            COALESCE((SELECT COUNT(*)::BIGINT FROM inserted_rows), 0::BIGINT) AS inserted_rows
        FROM family_stats
    """


def _build_gold_stats_sql(
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    *,
    assessment_source_table: str = DEFAULT_GOLD_ASSESSMENT_SOURCE_TABLE,
    scoped_test_ids: list[str] | None = None,
) -> str:
    """Return the stats SQL for one assessment family."""
    return f"""
        {_build_threshold_cte(
            assessment_source_table=assessment_source_table,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
            scoped_test_ids=scoped_test_ids,
        )}
        SELECT
            COUNT(*)::BIGINT AS source_rows,
            COUNT(*) FILTER (WHERE is_above_threshold)::BIGINT AS excluded_above_threshold_rows,
            COUNT(*) FILTER (WHERE is_below_threshold)::BIGINT AS excluded_below_threshold_rows,
            COUNT(*) FILTER (WHERE is_above_threshold OR is_below_threshold)::BIGINT AS excluded_outside_threshold_rows
        FROM labeled_source
    """


def _build_gold_selection_sql(
    *,
    family: str,
    coverage_table: str,
) -> tuple[str, str]:
    """Return family-specific selection CTEs plus the insert source query."""
    coverage_cte = f"""
        covered_reference_catalog AS MATERIALIZED (
            SELECT
                source_module,
                assessment_family,
                test_name,
                reference_metric_name
            FROM {coverage_table}
            WHERE coverage_status = 'covered'
        ),
    """
    if family == "speed":
        metric_order = "candidate.metric_value ASC"
        selection_ctes = f"""
        {coverage_cte}
        winner_candidates AS MATERIALIZED (
            SELECT s.*
            FROM labeled_source s
            JOIN covered_reference_catalog c
                ON c.source_module = s.source_module
               AND c.assessment_family = s.assessment_family
               AND c.test_name IS NOT DISTINCT FROM s.test_name
               AND c.reference_metric_name = s.metric_name
            WHERE NOT s.is_above_threshold
              AND NOT s.is_below_threshold
              AND s.rep_number IS NOT NULL
        ),
        selected_partitions AS MATERIALIZED (
            SELECT
                test_id,
                rep_number
            FROM (
                SELECT
                    candidate.test_id,
                    candidate.rep_number,
                    ROW_NUMBER() OVER (
                        PARTITION BY candidate.test_id
                        ORDER BY
                            {metric_order} NULLS LAST,
                            candidate.rep_number ASC,
                            candidate.metric_row_key ASC
                    ) AS rn
                FROM winner_candidates candidate
            ) ranked_candidates
            WHERE rn = 1
        )
        """
        insert_source_sql = f"""
        FROM labeled_source s
        JOIN selected_partitions p
            ON p.test_id = s.test_id
           AND p.rep_number IS NOT DISTINCT FROM s.rep_number
        WHERE NOT s.is_above_threshold
          AND NOT s.is_below_threshold
        """
        return selection_ctes, insert_source_sql

    if family == "forcedecks":
        selection_ctes = f"""
        {coverage_cte}
        winner_candidates AS MATERIALIZED (
            SELECT s.*
            FROM labeled_source s
            JOIN covered_reference_catalog c
                ON c.source_module = s.source_module
               AND c.assessment_family = s.assessment_family
               AND c.test_name IS NOT DISTINCT FROM s.test_name
               AND c.reference_metric_name = s.metric_name
            WHERE NOT s.is_above_threshold
              AND NOT s.is_below_threshold
              AND s.rep_number IS NOT NULL
        ),
        test_side_modes AS MATERIALIZED (
            SELECT
                candidate.test_id,
                BOOL_OR(candidate.side IN ('left', 'right')) AS has_lateralized_side
            FROM winner_candidates candidate
            GROUP BY candidate.test_id
        ),
        selection_candidates AS MATERIALIZED (
            SELECT
                candidate.*,
                CASE
                    WHEN mode.has_lateralized_side AND candidate.side IN ('left', 'right')
                        THEN candidate.side
                    WHEN NOT mode.has_lateralized_side
                        THEN candidate.side
                    ELSE NULL
                END AS selection_side
            FROM winner_candidates candidate
            JOIN test_side_modes mode
                ON mode.test_id = candidate.test_id
            WHERE (
                    mode.has_lateralized_side
                    AND candidate.side IN ('left', 'right')
                )
               OR NOT mode.has_lateralized_side
        ),
        selected_partitions AS MATERIALIZED (
            SELECT
                test_id,
                side,
                rep_number
            FROM (
                SELECT
                    candidate.test_id,
                    candidate.selection_side AS side,
                    candidate.rep_number,
                    ROW_NUMBER() OVER (
                        PARTITION BY candidate.test_id, candidate.selection_side
                        ORDER BY
                            candidate.metric_value DESC NULLS LAST,
                            candidate.rep_number ASC,
                            candidate.metric_row_key ASC
                    ) AS rn
                FROM selection_candidates candidate
            ) ranked_candidates
            WHERE rn = 1
        )
        """
        insert_source_sql = """
        FROM labeled_source s
        JOIN selected_partitions p
            ON p.test_id = s.test_id
           AND p.side IS NOT DISTINCT FROM s.side
           AND p.rep_number IS NOT DISTINCT FROM s.rep_number
        WHERE NOT s.is_above_threshold
          AND NOT s.is_below_threshold
        """
        return selection_ctes, insert_source_sql

    side_select = "s.side AS side" if family == "dynamo" else "NULL::VARCHAR AS side"
    side_join = "AND p.side IS NOT DISTINCT FROM s.side" if family == "dynamo" else ""
    selection_ctes = f"""
        {coverage_cte}
        selected_partitions AS MATERIALIZED (
            SELECT DISTINCT
                s.test_id,
                {side_select}
            FROM labeled_source s
            JOIN covered_reference_catalog c
                ON c.source_module = s.source_module
               AND c.assessment_family = s.assessment_family
               AND c.test_name IS NOT DISTINCT FROM s.test_name
               AND c.reference_metric_name = s.metric_name
            WHERE NOT s.is_above_threshold
              AND NOT s.is_below_threshold
        )
    """
    insert_source_sql = f"""
        FROM labeled_source s
        JOIN selected_partitions p
            ON p.test_id = s.test_id
           {side_join}
        WHERE NOT s.is_above_threshold
          AND NOT s.is_below_threshold
    """
    return selection_ctes, insert_source_sql


def _build_threshold_cte(
    *,
    assessment_source_table: str = DEFAULT_GOLD_ASSESSMENT_SOURCE_TABLE,
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: list[str] | None = None,
) -> str:
    """Return the shared cohort-threshold CTE used by gold stats and inserts."""
    _validate_day_window(day_start_utc, day_end_utc)
    labeled_source_day_filter = ""
    if day_start_utc is not None and day_end_utc is not None:
        labeled_source_day_filter = """
              AND s.test_date >= %s
              AND s.test_date < %s
        """
    labeled_source_scope_filter = ""
    if scoped_test_ids:
        labeled_source_scope_filter = """
              AND s.test_id = ANY(%s::uuid[])
        """
    return f"""
        WITH cohort_percentiles AS MATERIALIZED (
            SELECT
                assessment_family,
                team_group_id,
                test_type,
                metric_name,
                side,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS percentile_99,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY metric_value) AS percentile_25
            FROM {assessment_source_table}
            WHERE assessment_family = %s
              AND test_date >= '{VALD_CUTOFF_UTC}'::timestamptz
              AND metric_value IS NOT NULL
            GROUP BY
                assessment_family,
                team_group_id,
                test_type,
                metric_name,
                side
        ),
        cohort_thresholds AS MATERIALIZED (
            SELECT
                assessment_family,
                team_group_id,
                test_type,
                metric_name,
                side,
                percentile_99,
                percentile_25,
                percentile_99 * {_UPPER_THRESHOLD_MULTIPLIER} AS upper_threshold_value,
                percentile_25 AS lower_threshold_value
            FROM cohort_percentiles
        ),
        labeled_source AS MATERIALIZED (
            SELECT
                s.*,
                (
                    s.metric_value IS NOT NULL
                    AND t.upper_threshold_value IS NOT NULL
                    AND s.metric_value > t.upper_threshold_value
                ) AS is_above_threshold,
                (
                    s.metric_value IS NOT NULL
                    AND t.lower_threshold_value IS NOT NULL
                    AND s.metric_value < t.lower_threshold_value
                ) AS is_below_threshold
            FROM {assessment_source_table} s
            LEFT JOIN cohort_thresholds t
                ON t.assessment_family = s.assessment_family
               AND t.team_group_id = s.team_group_id
               AND t.metric_name = s.metric_name
               AND t.test_type IS NOT DISTINCT FROM s.test_type
               AND t.side IS NOT DISTINCT FROM s.side
            WHERE s.assessment_family = %s
              AND s.test_date >= '{VALD_CUTOFF_UTC}'::timestamptz
              {labeled_source_scope_filter}
              {labeled_source_day_filter}
        )
    """


def _publish_gold_family(
    db: DatabaseManager,
    *,
    family: str,
    table_name: str,
    assessment_source_table: str,
    coverage_table: str,
    day_start_utc: datetime | None,
    day_end_utc: datetime | None,
    scoped_test_ids: list[str] | None,
) -> dict[str, int]:
    """Clear and rebuild one gold family slice, returning publish stats.

    Phase 8.7.A (2026-05-09): in **full-rebuild** mode (no day window, no
    scoped test ids), the publish now writes into a stage table and atomic-
    swaps it onto the live name on success. The live gold table is never
    empty during the rebuild; locked decision #7 is satisfied.

    Scoped paths (day_window / scoped_test_ids) still use the historical
    clear-then-insert pattern against the live table — the DELETEs there
    are refactored to UPSERTs in Phase 8.7.B.
    """
    if scoped_test_ids is not None and not scoped_test_ids:
        return {
            "source_rows": 0,
            "excluded_above_threshold_rows": 0,
            "excluded_below_threshold_rows": 0,
            "excluded_outside_threshold_rows": 0,
            "inserted_rows": 0,
        }
    params = _build_gold_query_params(
        family,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        scoped_test_ids=scoped_test_ids,
    )

    is_full_rebuild = (
        scoped_test_ids is None
        and day_start_utc is None
        and day_end_utc is None
    )

    if is_full_rebuild:
        # Phase 8.7.A: build into stage, then atomic-swap.
        from ingestion.common.atomic_publish import (
            atomic_publish_table,
            build_stage_table_like,
        )

        stage_table = build_stage_table_like(db, live_table=table_name)
        try:
            with db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        _build_gold_publish_sql(
                            stage_table,
                            family=family,
                            assessment_source_table=assessment_source_table,
                            coverage_table=coverage_table,
                            day_start_utc=day_start_utc,
                            day_end_utc=day_end_utc,
                            scoped_test_ids=scoped_test_ids,
                        ),
                        params,
                    )
                    row = cur.fetchone()
        except Exception:
            try:
                db.execute(f"DROP TABLE IF EXISTS {stage_table} CASCADE")
            except Exception:  # pragma: no cover
                pass
            raise
        atomic_publish_table(db, live_table=table_name, stage_table=stage_table)
    else:
        # Scoped (windowed or per-test_id) — write directly into live.
        # Phase 8.7.B (2026-05-09): the previous _clear_gold_table_rows_cursor
        # call before the INSERT is gone — _build_gold_publish_sql now uses
        # ON CONFLICT (natural_key) DO UPDATE so the same statement replaces
        # any pre-existing rows for the scope. Locked decision #7 satisfied:
        # no DELETE on the live gold table from this code path.
        with db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _build_gold_publish_sql(
                        table_name,
                        family=family,
                        assessment_source_table=assessment_source_table,
                        coverage_table=coverage_table,
                        day_start_utc=day_start_utc,
                        day_end_utc=day_end_utc,
                        scoped_test_ids=scoped_test_ids,
                    ),
                    params,
                )
                row = cur.fetchone()

    stats = {
        "source_rows": int(row[0]) if row and row[0] is not None else 0,
        "excluded_above_threshold_rows": int(row[1]) if row and row[1] is not None else 0,
        "excluded_below_threshold_rows": int(row[2]) if row and row[2] is not None else 0,
        "excluded_outside_threshold_rows": int(row[3]) if row and row[3] is not None else 0,
        "inserted_rows": int(row[4]) if row and row[4] is not None else 0,
    }
    logger.info(
        "Published %d/%d rows to %s (%d above and %d below threshold excluded)",
        stats["inserted_rows"],
        stats["source_rows"],
        table_name,
        stats["excluded_above_threshold_rows"],
        stats["excluded_below_threshold_rows"],
    )
    return stats


def _fetch_gold_threshold_stats(
    db: DatabaseManager,
    family: str,
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
) -> dict[str, int]:
    """Return source and exclusion counts for one gold family."""
    row = db.fetch_one(
        _build_gold_stats_sql(day_start_utc=day_start_utc, day_end_utc=day_end_utc),
        _build_gold_query_params(
            family,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
        ),
    )
    return {
        "source_rows": int(row[0]) if row and row[0] is not None else 0,
        "excluded_above_threshold_rows": int(row[1]) if row and row[1] is not None else 0,
        "excluded_below_threshold_rows": int(row[2]) if row and row[2] is not None else 0,
        "excluded_outside_threshold_rows": int(row[3]) if row and row[3] is not None else 0,
    }


def _resolve_gold_family(
    table_name: str,
    *,
    family: str | None = None,
) -> str:
    """Return the resolved gold family for a gold target table."""
    if family is not None:
        return family
    for candidate_family, candidate_table in GOLD_TABLES.items():
        if candidate_table == table_name:
            return candidate_family
    msg = f"Unable to resolve gold family for table {table_name}"
    raise ValueError(msg)


def _resolve_gold_columns(
    table_name: str,
    *,
    family: str | None = None,
) -> list[str]:
    """Return the destination columns for a gold family table."""
    return GOLD_COLUMNS_BY_FAMILY[_resolve_gold_family(table_name, family=family)]


def _build_gold_query_params(
    family: str,
    *,
    day_start_utc: datetime | None,
    day_end_utc: datetime | None,
    scoped_test_ids: list[str] | None = None,
) -> tuple[Any, ...]:
    """Return the parameter tuple used by gold stats and insert statements."""
    _validate_day_window(day_start_utc, day_end_utc)
    params: list[Any] = [family, family]
    if scoped_test_ids:
        params.append(scoped_test_ids)
    if day_start_utc is not None and day_end_utc is not None:
        params.extend([day_start_utc, day_end_utc])
    return tuple(params)


# Phase 8.7.B (2026-05-09): the legacy `_clear_gold_table_rows` and
# `_clear_gold_table_rows_cursor` helpers were removed. Their three branches
# (TRUNCATE / DELETE-by-test_id / DELETE-by-day-window) are all replaced:
#   * TRUNCATE branch → Phase 8.7.A's atomic stage→live swap
#   * DELETE-by-test_id branch → Phase 8.7.B UPSERT in `_build_gold_publish_sql`
#   * DELETE-by-day-window branch → same UPSERT
# Locked decision #7 satisfied: no DELETE/TRUNCATE on live gold marts.


def _resolve_gold_family_workers(requested_workers: int | None = None) -> int:
    """Return the bounded worker count used for parallel gold publishing."""
    if requested_workers is not None:
        return max(1, int(requested_workers))
    raw_value = get_env("VALD_GOLD_FAMILY_WORKERS")
    if raw_value in (None, ""):
        return _DEFAULT_GOLD_FAMILY_WORKERS
    return max(1, int(raw_value))


def _extract_speed_split_number(metric_name: str) -> int | None:
    """Return the parsed SmartSpeed cumulative split number."""
    match = _SPEED_REFERENCE_PATTERN.match(metric_name)
    if match is None:
        return None
    return int(match.group(1))


def _coerce_metric_sort_value(value: Any) -> float:
    """Return a deterministic numeric sort value for reference metric ranking."""
    if value is None:
        return float("-inf")
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _normalise_optional_text(value: Any) -> str | None:
    """Strip a text value and return ``None`` for blank inputs."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _validate_day_window(
    day_start_utc: datetime | None,
    day_end_utc: datetime | None,
) -> tuple[datetime, datetime] | None:
    """Validate the optional gold intraday window."""
    if day_start_utc is None and day_end_utc is None:
        return None
    if day_start_utc is None or day_end_utc is None:
        raise ValueError("Both day_start_utc and day_end_utc are required together.")
    if day_start_utc.tzinfo is None or day_end_utc.tzinfo is None:
        raise ValueError("Gold day windows must be timezone-aware.")
    if day_start_utc >= day_end_utc:
        raise ValueError("Gold day window start must be earlier than the end.")
    return day_start_utc, day_end_utc
