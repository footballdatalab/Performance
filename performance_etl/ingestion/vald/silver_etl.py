"""
VALD silver ETL.

Builds the target-group-scoped silver entities for VALD:

1. ``silver.vald_target_group_membership`` from exact group/category matches.
2. ``silver.vald_athlete_profile`` for profiles assigned to exactly one target
   Active group.
3. ``silver.vald_assessment_metric`` as a long-form fact table across the
   populated VALD assessment families.
"""

from __future__ import annotations

import math
import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator, Sequence

import psycopg2.extras

from ingestion.common.config import load_provider_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.common.perf import analyze_table
from ingestion.common.quality import QualityEngine, QualityFlag
from ingestion.common.timing import track_stage
from ingestion.vald.cutoff import (
    VALD_CUTOFF_UTC,
    VALD_NEW_ENTRY_MIN_TESTS_UTC,
    is_on_or_after_new_entry_min_tests_date,
    is_on_or_after_vald_cutoff,
)
from ingestion.vald.metric_utils import build_metric_row_key

logger = get_logger(__name__)

NORDBORD_BASE_METRICS = [
    "left_avg_force",
    "left_impulse",
    "left_max_force",
    "left_torque",
    "left_calibration",
    "left_repetitions",
    "right_avg_force",
    "right_impulse",
    "right_max_force",
    "right_torque",
    "right_calibration",
    "right_repetitions",
]

FORCEFRAME_BASE_METRICS = [
    "inner_left_avg_force",
    "inner_left_impulse",
    "inner_left_max_force",
    "inner_left_repetitions",
    "inner_right_avg_force",
    "inner_right_impulse",
    "inner_right_max_force",
    "inner_right_repetitions",
    "outer_left_avg_force",
    "outer_left_impulse",
    "outer_left_max_force",
    "outer_left_repetitions",
    "outer_right_avg_force",
    "outer_right_impulse",
    "outer_right_max_force",
    "outer_right_repetitions",
]

DYNAMO_SUMMARY_METRICS = [
    "max_force_newtons",
    "avg_force_newtons",
    "max_impulse_ns",
    "avg_impulse_ns",
    "max_rfd_nps",
    "avg_rfd_nps",
    "avg_time_to_peak_s",
    "min_time_to_peak_s",
    "max_rom_degrees",
    "avg_rom_degrees",
]

DYNAMO_REPETITION_METRICS = [
    "impulse_ns",
    "rfd_nps",
    "time_to_peak_s",
    "rom_degrees",
]

DYNAMO_REPETITION_PAYLOAD_EXCLUDED_METRICS = {
    "force",
    "force_newtons",
}

ASSESSMENT_COLUMNS = [
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
    "side",
    "rep_number",
    "metric_row_key",
]

PROFILE_COLUMNS = [
    "provider_profile_id",
    "tenant_id",
    "provider_full_name",
    "provider_given_name",
    "provider_family_name",
    "provider_status",
    "first_seen_at",
    "last_seen_at",
    "target_group_id",
    "target_group_name",
    "target_category_id",
    "target_category_name",
]

MEMBERSHIP_COLUMNS = [
    "provider_profile_id",
    "tenant_id",
    "target_group_id",
    "target_group_name",
    "target_category_id",
    "target_category_name",
    "is_ambiguous",
    "include_in_gold",
    "raw_id",
]

_ID_LIKE_SUFFIXES = ("_id", "_index", "_utc")
SILVER_TABLES = {
    "membership": "silver.vald_target_group_membership",
    "profile": "silver.vald_athlete_profile",
    "assessment": "silver.vald_assessment_metric",
}


def run_silver_etl(
    db: DatabaseManager,
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    *,
    table_overrides: dict[str, str] | None = None,
    sync_quality_flags: bool = True,
    scoped_test_ids_by_family: dict[str, list[str]] | None = None,
    refresh_reference_entities: bool = True,
) -> dict[str, Any]:
    """Run the scoped VALD silver ETL."""
    day_window = _validate_day_window(day_start_utc, day_end_utc)
    tables = _resolve_silver_tables(table_overrides)
    target_groups = _load_target_groups()
    logger.info(
        "VALD silver ETL: rebuilding scoped entities for %d target groups",
        len(target_groups),
    )

    bronze_summary = _backfill_forcedecks_trial_results(db)

    profile_lookup = (
        {}
        if refresh_reference_entities
        else _load_profile_lookup_from_table(
            db,
            profile_table=tables["profile"],
        )
    )
    if not refresh_reference_entities and not profile_lookup:
        logger.info(
            "VALD silver ETL requested cached profile reuse but %s is empty; "
            "rebuilding reference entities instead.",
            tables["profile"],
        )
        refresh_reference_entities = True

    if refresh_reference_entities:
        membership_rows, membership_summary = _build_target_group_membership(
            db,
            target_groups,
        )
        # Phase 8.7.A: atomic stage→live swap (no TRUNCATE on the live table).
        with _atomic_replace_table_in_place(db, tables["membership"]) as stage_table:
            _insert_rows(
                db,
                stage_table,
                membership_rows,
                MEMBERSHIP_COLUMNS,
            )

        if sync_quality_flags:
            quality_summary = _sync_overlap_quality_flags(db, membership_rows)
        else:
            quality_summary = {
                "deferred": True,
                "open_flags_superseded": 0,
                "flags_written": 0,
                "ambiguous_profiles": membership_summary.get("ambiguous_profiles", 0),
            }

        profile_rows = _build_scoped_profile_rows(db, membership_table=tables["membership"])
        profiles_upserted = _upsert_scoped_profiles(
            db,
            profile_rows,
            profile_table=tables["profile"],
        )
        profiles_deactivated = _deactivate_excluded_profiles(
            db,
            profile_table=tables["profile"],
            membership_table=tables["membership"],
        )
        profile_lookup = _build_profile_lookup(profile_rows)
    else:
        membership_summary = {
            "skipped": True,
            "skip_reason": "reference_entities_unchanged",
            "membership_rows": 0,
            "distinct_target_profiles": len(profile_lookup),
            "included_profiles": len(profile_lookup),
            "ambiguous_profiles": 0,
            "matched_target_groups": 0,
            "unmatched_target_groups": [],
        }
        quality_summary = {
            "skipped": True,
            "skip_reason": "reference_entities_unchanged",
            "open_flags_superseded": 0,
            "flags_written": 0,
            "ambiguous_profiles": 0,
        }
        profiles_upserted = 0
        profiles_deactivated = 0

    has_incremental_scope = scoped_test_ids_by_family is not None
    is_full_rebuild = day_window is None and not has_incremental_scope

    if is_full_rebuild:
        # Phase 8.7.A: atomic stage→live swap. Load runs against the stage
        # table; on successful exit the stage is published into the live
        # name. The live assessment table is never empty during the rebuild.
        with _atomic_replace_table_in_place(db, tables["assessment"]) as stage_table:
            metric_summary = _load_assessment_metrics(
                db,
                profile_lookup,
                profile_table=tables["profile"],
                assessment_table=stage_table,
                day_start_utc=None,
                day_end_utc=None,
                scoped_test_ids_by_family=None,
            )
    else:
        # Scoped (windowed or per-test_id) refresh — write directly into the
        # live table. Phase 8.7.B (2026-05-09): the previous
        # _delete_assessment_metric_rows / _delete_assessment_metric_rows_by_scope
        # calls are gone — _load_assessment_metrics' INSERT now uses
        # ON CONFLICT (metric_row_key) DO UPDATE so any pre-existing row in the
        # scope is updated in place. Locked decision #7 satisfied.
        metric_summary = _load_assessment_metrics(
            db,
            profile_lookup,
            profile_table=tables["profile"],
            assessment_table=tables["assessment"],
            day_start_utc=day_window[0] if day_window else None,
            day_end_utc=day_window[1] if day_window else None,
            scoped_test_ids_by_family=scoped_test_ids_by_family,
        )

    # Phase 8.8.B: refresh planner stats so downstream gold reads pick
    # good plans. Without this, the next silver_to_gold run uses stale
    # statistics and chooses bad join orders / index strategies on
    # silver.vald_assessment_metric. Cheap (seconds, not minutes) and
    # safe inside the ETL pipeline (skip_locked=True swallows any
    # transient lock failure).
    with track_stage("vald", "silver.analyze", db=db):
        analyze_table(db, tables["assessment"], skip_locked=True)
        if refresh_reference_entities:
            analyze_table(db, tables["profile"], skip_locked=True)
            analyze_table(db, tables["membership"], skip_locked=True)

    summary = {
        "assessment_scope": (
            "incremental_test_ids"
            if has_incremental_scope
            else "day_window" if day_window else "full"
        ),
        "reference_entities_refreshed": refresh_reference_entities,
        "target_groups": {
            "configured_groups": len(target_groups),
            "group_names": [group["group_name"] for group in target_groups],
        },
        "bronze_backfill": bronze_summary,
        "membership": membership_summary,
        "quality": quality_summary,
        "profiles": {
            "profiles_upserted": profiles_upserted,
            "profiles_deactivated": profiles_deactivated,
        },
        "assessment_metrics": metric_summary,
        "tables": dict(tables),
    }
    logger.info("VALD silver ETL complete: %s", summary)
    return summary


def rebuild_overlap_quality_flags(
    db: DatabaseManager,
    *,
    membership_table: str = SILVER_TABLES["membership"],
) -> dict[str, Any]:
    """Rebuild overlap quality flags from a membership table snapshot."""
    membership_rows = db.fetch_all_dict(
        f"""
        SELECT
            provider_profile_id,
            tenant_id,
            target_group_id,
            target_group_name,
            target_category_id,
            target_category_name,
            is_ambiguous,
            include_in_gold,
            raw_id
        FROM {membership_table}
        WHERE is_ambiguous = TRUE
        """
    )
    return _sync_overlap_quality_flags(db, membership_rows)


def _resolve_silver_tables(
    table_overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Return the target silver tables for the current ETL run."""
    tables = dict(SILVER_TABLES)
    if table_overrides:
        tables.update(table_overrides)
    return tables


def _load_target_groups() -> list[dict[str, str]]:
    """Load and validate the configured target groups."""
    provider_config = load_provider_config("vald")
    groups = provider_config.get("target_groups") or []
    if not groups:
        raise ValueError("VALD config does not define any target_groups")

    seen_pairs: set[tuple[str, str]] = set()
    normalised_groups: list[dict[str, str]] = []
    for group in groups:
        group_id = str(group.get("group_id") or "").strip()
        group_name = str(group.get("group_name") or "").strip()
        category_id = str(group.get("category_id") or "").strip()
        if not group_id or not group_name or not category_id:
            raise ValueError(f"Invalid target group config: {group}")

        key = (group_id, category_id)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        normalised_groups.append(
            {
                "group_id": group_id,
                "group_name": group_name,
                "category_id": category_id,
            }
        )

    return normalised_groups


def _resolve_silver_fd_buckets() -> int:
    """Hash bucket count for the parallel ForceDecks trial-results backfill."""
    raw_value = os.environ.get("VALD_SILVER_FD_BUCKETS")
    if raw_value in (None, ""):
        return 8
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return 8
    return max(1, parsed)


def _backfill_forcedecks_trial_results(db: DatabaseManager) -> dict[str, Any]:
    """Backfill parsed ForceDecks trial results when the bronze table is empty.

    The expansion is the most expensive Silver step: jsonb_array_elements on
    millions of trial rows is single-core in Postgres. We shard by
    ``abs(hashtext(profile_id::text)) % N`` so N independent transactions can
    run in parallel — Postgres ends up using one backend per shard, each
    scanning a disjoint slice and writing to disjoint primary keys.
    """
    trial_count = _fetch_scalar(
        db,
        "SELECT COUNT(*) FROM bronze.vald_forcedecks_trials",
    )
    existing_results = _fetch_scalar(
        db,
        "SELECT COUNT(*) FROM bronze.vald_forcedecks_trial_results",
    )

    if not trial_count:
        return {
            "trial_count": 0,
            "trial_results_count": existing_results or 0,
            "trial_results_inserted": 0,
            "backfill_performed": False,
        }

    if existing_results:
        logger.info(
            "ForceDecks trial-result backfill skipped: bronze table already has %d rows",
            existing_results,
        )
        return {
            "trial_count": trial_count,
            "trial_results_count": existing_results,
            "trial_results_inserted": 0,
            "backfill_performed": False,
        }

    bucket_count = _resolve_silver_fd_buckets()
    logger.info(
        "Backfilling bronze.vald_forcedecks_trial_results from %d trials across %d bucket(s)",
        trial_count,
        bucket_count,
    )

    # Phase 8.7.A (2026-05-09): the previous code TRUNCATE'd the table here as
    # a defensive guard before parallel workers started inserting. It only ever
    # fired when `existing_results == 0` (the early-return above guarantees an
    # empty table at this point), and workers shard on disjoint hash buckets so
    # cannot collide on inserts. The TRUNCATE was therefore redundant — and it
    # violates locked decision #7 (no DELETE/TRUNCATE in ETL). Removed.

    bucket_sql = """
        INSERT INTO bronze.vald_forcedecks_trial_results (
            trial_id,
            test_id,
            profile_id,
            result_id,
            value,
            time,
            limb,
            repeat,
            raw_id,
            batch_id
        )
        SELECT
            t.trial_id,
            t.test_id,
            t.profile_id,
            COALESCE(
                NULLIF(result_item.elem->>'resultId', '')::INTEGER,
                NULLIF(result_item.elem->'definition'->>'id', '')::INTEGER
            ) AS result_id,
            NULLIF(result_item.elem->>'value', '')::NUMERIC AS value,
            NULLIF(result_item.elem->>'time', '')::NUMERIC AS time,
            COALESCE(NULLIF(result_item.elem->>'limb', ''), t.limb) AS limb,
            NULLIF(result_item.elem->>'repeat', '')::INTEGER AS repeat,
            t.raw_id,
            t.batch_id
        FROM bronze.vald_forcedecks_trials t
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(t.results, '[]'::jsonb)) AS result_item(elem)
        WHERE (abs(hashtext(t.profile_id::text)) %% %s) = %s
          AND COALESCE(
              NULLIF(result_item.elem->>'resultId', '')::INTEGER,
              NULLIF(result_item.elem->'definition'->>'id', '')::INTEGER
          ) IS NOT NULL
    """

    def _insert_bucket(bucket_id: int) -> int:
        with db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL work_mem = '512MB'")
                cur.execute("SET LOCAL max_parallel_workers_per_gather = 2")
                cur.execute(bucket_sql, (bucket_count, bucket_id))
                return cur.rowcount

    inserted = 0
    if bucket_count == 1:
        inserted = _insert_bucket(0)
    else:
        with ThreadPoolExecutor(
            max_workers=bucket_count,
            thread_name_prefix="vald-fd-backfill",
        ) as pool:
            futures = {
                pool.submit(_insert_bucket, bucket): bucket
                for bucket in range(bucket_count)
            }
            for future in as_completed(futures):
                inserted += future.result()

    logger.info(
        "ForceDecks trial-result backfill complete: inserted %d rows",
        inserted,
    )
    return {
        "trial_count": trial_count,
        "trial_results_count": inserted,
        "trial_results_inserted": inserted,
        "backfill_performed": True,
    }


def _build_target_group_membership(
    db: DatabaseManager,
    target_groups: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build the scoped membership table rows from bronze profile categories."""
    target_by_pair = {
        (group["group_id"], group["category_id"]): group for group in target_groups
    }
    target_priority_by_pair = {
        (group["group_id"], group["category_id"]): index
        for index, group in enumerate(target_groups)
    }
    matched_groups: set[str] = set()
    matched_profiles: dict[str, list[dict[str, Any]]] = defaultdict(list)

    rows = db.fetch_all_dict(
        """
        SELECT
            pc.vald_profile_id AS provider_profile_id,
            COALESCE(p.tenant_id, pc.tenant_id) AS tenant_id,
            pc.category_id,
            pc.category_name,
            pc.group_id,
            pc.group_name,
            pc.raw_id
        FROM bronze.vald_profile_categories pc
        LEFT JOIN bronze.vald_profiles p
            ON p.vald_profile_id = pc.vald_profile_id
        WHERE pc.group_id IS NOT NULL
          AND pc.category_id IS NOT NULL
        """
    )

    for row in rows:
        profile_id = str(row["provider_profile_id"])
        group_id = str(row["group_id"])
        category_id = str(row["category_id"])
        pair = (group_id, category_id)
        target_group = target_by_pair.get(pair)
        if not target_group:
            continue

        matched_groups.add(group_id)
        matched_profiles[profile_id].append(
            {
                "provider_profile_id": profile_id,
                "tenant_id": str(row["tenant_id"]),
                "target_group_id": group_id,
                "target_group_name": row.get("group_name") or target_group["group_name"],
                "target_category_id": category_id,
                "target_category_name": row.get("category_name")
                or target_group["group_name"],
                "raw_id": row.get("raw_id"),
            }
        )

    membership_rows: list[dict[str, Any]] = []
    ambiguous_profiles = 0
    included_profiles = 0
    for membership_list in matched_profiles.values():
        is_ambiguous = len(membership_list) > 1
        if is_ambiguous:
            ambiguous_profiles += 1
        primary_membership = _select_primary_membership(
            membership_list,
            target_priority_by_pair=target_priority_by_pair,
        )
        if primary_membership is not None:
            included_profiles += 1

        for membership in sorted(
            membership_list,
                key=lambda item: (item["target_group_name"], item["target_category_name"]),
        ):
            membership_rows.append(
                {
                    **membership,
                    "is_ambiguous": is_ambiguous,
                    "include_in_gold": membership == primary_membership,
                }
            )

    summary = {
        "membership_rows": len(membership_rows),
        "distinct_target_profiles": len(matched_profiles),
        "included_profiles": included_profiles,
        "ambiguous_profiles": ambiguous_profiles,
        "matched_target_groups": len(matched_groups),
        "unmatched_target_groups": [
            group["group_name"]
            for group in target_groups
            if group["group_id"] not in matched_groups
        ],
    }
    logger.info("VALD target-group membership summary: %s", summary)
    return membership_rows, summary


def _sync_overlap_quality_flags(
    db: DatabaseManager,
    membership_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reconcile the open overlap flags for ambiguous target-group matches.

    Phase 8.7.D (2026-05-09): the previous code DELETEd open flags and
    re-asserted them, which destroyed the audit trail (flag_id reset on
    every silver run, reviewer notes lost) and violated locked decision
    #7 (no DELETE in ETL). The new pattern:

      1. Mark every currently-``'open'`` ``'duplicate_suspect'`` flag
         from this source as ``'superseded'``. Audit trail is preserved
         (the row stays in the table) and the partial index
         ``idx_quality_flag_status WHERE resolution_status='open'`` no
         longer surfaces it as live.
      2. Insert the freshly-derived flags. The ON CONFLICT clause
         reactivates any (record, metric, flag_type) tuple that's still
         ambiguous back to ``'open'`` with refreshed values. Manually
         reviewed rows (``resolution_status NOT IN ('open','superseded')``)
         are NOT touched — coaches' reviewer notes survive.

    Net effect:
      * still-ambiguous flags stay ``'open'`` (audit retained, ID
        preserved across runs);
      * no-longer-ambiguous flags stay ``'superseded'`` (visible for
        audit, hidden from "open" readers);
      * manually reviewed flags are preserved untouched.

    Locked decision #7 satisfied: no DELETE on the live flag table.
    """
    # Step 1 — supersede currently-open flags from this source. This
    # handles the "no longer ambiguous" case: any open flag whose tuple
    # the new run does NOT re-assert keeps its ``'superseded'`` marker.
    supersede_sql = """
        UPDATE silver.data_quality_flag
           SET resolution_status = 'superseded'
         WHERE source_table = 'silver.vald_target_group_membership'
           AND flag_type = 'duplicate_suspect'
           AND resolution_status = 'open'
    """
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(supersede_sql)
            superseded = cur.rowcount

    # Step 2 — collect the new ambiguous-membership flags to assert.
    memberships_by_profile: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in membership_rows:
        if row["is_ambiguous"]:
            memberships_by_profile[str(row["provider_profile_id"])].append(row)

    flags: list[QualityFlag] = []
    for profile_id, rows in memberships_by_profile.items():
        first_row = rows[0]
        selected_row = next((row for row in rows if row["include_in_gold"]), None)
        flags.append(
            QualityFlag(
                source_table="silver.vald_target_group_membership",
                record_id=profile_id,
                metric_name="target_group_overlap",
                metric_value=float(len(rows)),
                flag_type="duplicate_suspect",
                severity="warning",
                details={
                    "matched_group_ids": [row["target_group_id"] for row in rows],
                    "matched_group_names": [row["target_group_name"] for row in rows],
                    "matched_category_ids": [
                        row["target_category_id"] for row in rows
                    ],
                    "selected_group_id": selected_row["target_group_id"] if selected_row else None,
                    "selected_group_name": selected_row["target_group_name"] if selected_row else None,
                    "selected_category_id": selected_row["target_category_id"] if selected_row else None,
                    "include_in_gold": selected_row is not None,
                    "gold_selection_strategy": "target_group_config_order",
                },
                profile_id=profile_id,
                tenant_id=str(first_row["tenant_id"]),
            )
        )

    # Step 3 — UPSERT the new flags. The ON CONFLICT clause reactivates
    # ``'superseded'`` rows back to ``'open'`` (so still-ambiguous flags
    # keep their stable flag_id) but leaves manually reviewed rows alone
    # (status NOT IN ('open','superseded')). This is the bespoke
    # alternative to ``QualityEngine.persist_flags`` because that helper
    # only reactivates ``'open'`` rows; here we explicitly want the
    # "supersede + restore" semantic.
    flags_written = _upsert_overlap_flags(db, flags)

    summary = {
        "open_flags_superseded": superseded,
        "flags_written": flags_written,
        "ambiguous_profiles": len(memberships_by_profile),
    }
    logger.info("VALD overlap quality summary: %s", summary)
    return summary


def _upsert_overlap_flags(
    db: DatabaseManager,
    flags: list[QualityFlag],
) -> int:
    """Phase 8.7.D: UPSERT for the supersede-and-reactivate flow.

    Reactivates ``'superseded'`` rows back to ``'open'`` so still-ambiguous
    flags keep their stable ``flag_id`` across silver runs. Manually
    reviewed rows (resolution_status NOT IN ('open','superseded')) are
    left untouched.

    This is intentionally separate from
    ``ingestion.common.quality.QualityEngine.persist_flags`` (which only
    updates ``'open'`` rows) so we don't change shared semantics.
    """
    if not flags:
        return 0

    import json

    sql = """
        INSERT INTO silver.data_quality_flag
            (source_table, record_id, profile_id, tenant_id, test_date,
             metric_name, metric_value, flag_type, severity, details, batch_id)
        VALUES %s
        ON CONFLICT (source_table, record_id, metric_name, flag_type)
        DO UPDATE SET
            metric_value      = EXCLUDED.metric_value,
            severity          = EXCLUDED.severity,
            details           = EXCLUDED.details,
            batch_id          = EXCLUDED.batch_id,
            profile_id        = EXCLUDED.profile_id,
            tenant_id         = EXCLUDED.tenant_id,
            test_date         = EXCLUDED.test_date,
            resolution_status = 'open'
        WHERE silver.data_quality_flag.resolution_status IN ('open', 'superseded')
    """
    template = (
        "(%(source_table)s, %(record_id)s, %(profile_id)s, %(tenant_id)s,"
        " %(test_date)s, %(metric_name)s, %(metric_value)s, %(flag_type)s,"
        " %(severity)s, %(details)s, %(batch_id)s)"
    )
    records = [
        {
            "source_table": f.source_table,
            "record_id": f.record_id,
            "profile_id": f.profile_id,
            "tenant_id": f.tenant_id,
            "test_date": f.test_date,
            "metric_name": f.metric_name,
            "metric_value": f.metric_value,
            "flag_type": f.flag_type,
            "severity": f.severity,
            "details": json.dumps(f.details),
            "batch_id": f.batch_id,
        }
        for f in flags
    ]
    with db.connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, records, template=template)
    logger.info("Upserted %d overlap quality flags (supersede-reactivate)", len(records))
    return len(records)


def _select_primary_membership(
    membership_list: list[dict[str, Any]],
    *,
    target_priority_by_pair: dict[tuple[str, str], int],
) -> dict[str, Any] | None:
    """Select the single membership row that should flow into gold."""
    if not membership_list:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[int, str, str, str]:
        pair = (str(row["target_group_id"]), str(row["target_category_id"]))
        return (
            target_priority_by_pair.get(pair, len(target_priority_by_pair)),
            str(row["target_group_name"]),
            str(row["target_category_name"]),
            str(row.get("raw_id") or ""),
        )

    return min(membership_list, key=sort_key)


def _build_scoped_profile_rows(
    db: DatabaseManager,
    *,
    membership_table: str = SILVER_TABLES["membership"],
) -> list[dict[str, Any]]:
    """Build the included target-group athlete profile records."""
    rows = db.fetch_all_dict(
        f"""
        SELECT
            p.vald_profile_id AS provider_profile_id,
            p.tenant_id,
            p.given_name,
            p.family_name,
            p.created_at AS source_created_at,
            p.updated_at AS source_updated_at,
            m.target_group_id,
            m.target_group_name,
            m.target_category_id,
            m.target_category_name
        FROM bronze.vald_profiles p
        JOIN {membership_table} m
            ON m.provider_profile_id = p.vald_profile_id
           AND m.tenant_id = p.tenant_id
           AND m.include_in_gold = TRUE
        ORDER BY p.vald_profile_id
        """
    )

    profile_rows: list[dict[str, Any]] = []
    for row in rows:
        given_name = _clean_name_part(row.get("given_name"))
        family_name = _clean_name_part(row.get("family_name"))
        full_name = " ".join(part for part in (given_name, family_name) if part) or None

        profile_rows.append(
            {
                "provider_profile_id": str(row["provider_profile_id"]),
                "tenant_id": str(row["tenant_id"]),
                "provider_full_name": full_name,
                "provider_given_name": given_name,
                "provider_family_name": family_name,
                "provider_status": "active",
                "first_seen_at": row.get("source_created_at")
                or row.get("source_updated_at"),
                "last_seen_at": row.get("source_updated_at")
                or row.get("source_created_at"),
                "target_group_id": str(row["target_group_id"]),
                "target_group_name": row.get("target_group_name"),
                "target_category_id": str(row["target_category_id"]),
                "target_category_name": row.get("target_category_name"),
            }
        )

    return profile_rows


def _upsert_scoped_profiles(
    db: DatabaseManager,
    profile_rows: list[dict[str, Any]],
    *,
    profile_table: str = SILVER_TABLES["profile"],
) -> int:
    """Upsert the scoped rows into ``silver.vald_athlete_profile``."""
    if not profile_rows:
        return 0

    # Phase 8.7.C site #13 (2026-05-09): the upsert now reactivates rows that
    # were previously soft-deleted. If a profile leaves a target group then
    # later returns, _deactivate_excluded_profiles marks is_active=FALSE; the
    # next run that includes the profile will re-set is_active=TRUE here and
    # clear deactivated_at. The default for fresh INSERTs (is_active=TRUE) is
    # preserved by the column DEFAULT, so we don't need to list it in INSERT
    # columns.
    sql = f"""
        INSERT INTO {profile_table} (
            provider_profile_id,
            tenant_id,
            provider_full_name,
            provider_given_name,
            provider_family_name,
            provider_status,
            first_seen_at,
            last_seen_at,
            target_group_id,
            target_group_name,
            target_category_id,
            target_category_name
        )
        VALUES %s
        ON CONFLICT (provider_profile_id, tenant_id)
        DO UPDATE SET
            provider_full_name = EXCLUDED.provider_full_name,
            provider_given_name = EXCLUDED.provider_given_name,
            provider_family_name = EXCLUDED.provider_family_name,
            provider_status = EXCLUDED.provider_status,
            first_seen_at = COALESCE(
                {profile_table}.first_seen_at,
                EXCLUDED.first_seen_at
            ),
            last_seen_at = EXCLUDED.last_seen_at,
            target_group_id = EXCLUDED.target_group_id,
            target_group_name = EXCLUDED.target_group_name,
            target_category_id = EXCLUDED.target_category_id,
            target_category_name = EXCLUDED.target_category_name,
            is_active = TRUE,
            deactivated_at = NULL,
            updated_at = now()
    """
    template = "(" + ", ".join(f"%({column})s" for column in PROFILE_COLUMNS) + ")"
    with db.connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                sql,
                profile_rows,
                template=template,
            )
    logger.info("Upserted %d scoped VALD athlete profiles", len(profile_rows))
    return len(profile_rows)


def _deactivate_excluded_profiles(
    db: DatabaseManager,
    *,
    profile_table: str = SILVER_TABLES["profile"],
    membership_table: str = SILVER_TABLES["membership"],
) -> int:
    """Soft-delete silver VALD profiles outside the included target groups.

    Phase 8.7.C site #13 (2026-05-09): the previous code DELETEd these rows,
    destroying the audit trail and orphaning historical assessment metrics
    that JOIN to the profile dimension. We now UPDATE ``is_active=FALSE`` and
    stamp ``deactivated_at`` so:

      * Historical reports can still resolve a former athlete's name + DOB.
      * The Phase 8.7.C reader audit added ``WHERE p.is_active = TRUE`` to
        every consumer that wants only the current squad — those readers
        see the same data as before.
      * If the athlete returns to a target group later, ``_upsert_scoped_profiles``
        re-sets ``is_active=TRUE`` and clears ``deactivated_at`` in the same
        run (clean reactivation).

    Locked decision #7 satisfied: no DELETE on the live profile table.
    """
    sql = f"""
        UPDATE {profile_table} AS p
           SET is_active = FALSE,
               deactivated_at = COALESCE(p.deactivated_at, now()),
               updated_at = now()
         WHERE p.is_active = TRUE
           AND NOT EXISTS (
               SELECT 1
                 FROM {membership_table} m
                WHERE m.provider_profile_id = p.provider_profile_id
                  AND m.tenant_id = p.tenant_id
                  AND m.include_in_gold = TRUE
           )
    """
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            deactivated = cur.rowcount
    logger.info("Deactivated %d non-target VALD athlete profiles", deactivated)
    return deactivated


# Backwards-compat alias for any caller still using the old name. The function
# itself no longer DELETEs — the new name reflects the soft-delete semantic.
_delete_excluded_profiles = _deactivate_excluded_profiles


def _build_profile_lookup(
    profile_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build a fast lookup for included profile metadata."""
    return {
        str(row["provider_profile_id"]): {
            "provider_profile_id": str(row["provider_profile_id"]),
            "athlete_name": row.get("provider_full_name"),
            "team_name": row.get("target_category_name"),
            "team_group_name": row.get("target_group_name"),
            "team_group_id": row.get("target_group_id"),
            "category_id": row.get("target_category_id"),
        }
        for row in profile_rows
    }


def _load_profile_lookup_from_table(
    db: DatabaseManager,
    *,
    profile_table: str = SILVER_TABLES["profile"],
) -> dict[str, dict[str, Any]]:
    """Load included profile metadata from the persisted silver profile table."""
    rows = db.fetch_all_dict(
        f"""
        SELECT
            provider_profile_id,
            provider_full_name,
            target_category_name,
            target_group_name,
            target_group_id,
            target_category_id
        FROM {profile_table}
        WHERE is_active = TRUE
        """
    )
    return _build_profile_lookup(rows)


def _load_min_tests_qualifying_profiles(
    db: DatabaseManager,
    source_table: str,
    date_expr: str,
    profile_column: str = "profile_id",
    min_tests: int = 2,
) -> frozenset[str]:
    """Return profile IDs with >= min_tests distinct tests from the new-entry gate date onwards."""
    rows = db.fetch_all_dict(
        f"""
        SELECT {profile_column}::text AS profile_id
        FROM {source_table}
        WHERE {date_expr} >= %s::timestamptz
        GROUP BY {profile_column}
        HAVING COUNT(DISTINCT test_id) >= %s
        """,
        (VALD_NEW_ENTRY_MIN_TESTS_UTC, min_tests),
    )
    return frozenset(row["profile_id"] for row in rows)


def _load_assessment_metrics(
    db: DatabaseManager,
    profile_lookup: dict[str, dict[str, Any]],
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids_by_family: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Load the long-form assessment metrics into silver."""
    by_family: dict[str, int] = defaultdict(int)
    has_incremental_scope = scoped_test_ids_by_family is not None
    nordics_scope = None if not has_incremental_scope else scoped_test_ids_by_family.get("nordics", [])
    forceframe_scope = None if not has_incremental_scope else scoped_test_ids_by_family.get("forceframe", [])
    speed_scope = None if not has_incremental_scope else scoped_test_ids_by_family.get("speed", [])
    dynamo_scope = None if not has_incremental_scope else scoped_test_ids_by_family.get("dynamo", [])
    forcedecks_scope = None if not has_incremental_scope else scoped_test_ids_by_family.get("forcedecks", [])

    # Phase 8.8.A: each family is wrapped in track_stage so the
    # bronze_to_silver flame graph attributes time to specific families.
    # All 5 share the run_id set by the enclosing pipeline_run.
    nordics = 0
    if not (has_incremental_scope and not nordics_scope):
        with track_stage(
            "vald", "silver.assessment_metric", sub_stage="nordics", db=db,
        ) as _m:
            nordics = _load_nordbord_metrics(
                db,
                profile_lookup,
                profile_table=profile_table,
                assessment_table=assessment_table,
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                scoped_test_ids=nordics_scope,
            )
            _m["rows_written"] = nordics
    by_family["nordics"] += nordics

    forceframe = 0
    if not (has_incremental_scope and not forceframe_scope):
        with track_stage(
            "vald", "silver.assessment_metric", sub_stage="forceframe", db=db,
        ) as _m:
            forceframe = _load_forceframe_metrics(
                db,
                profile_lookup,
                profile_table=profile_table,
                assessment_table=assessment_table,
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                scoped_test_ids=forceframe_scope,
            )
            _m["rows_written"] = forceframe
    by_family["forceframe"] += forceframe

    smartspeed_counts: dict[str, int] = {}
    if not (has_incremental_scope and not speed_scope):
        with track_stage(
            "vald", "silver.assessment_metric", sub_stage="smartspeed", db=db,
        ) as _m:
            smartspeed_counts = _load_smartspeed_metrics(
                db,
                profile_lookup,
                profile_table=profile_table,
                assessment_table=assessment_table,
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                scoped_test_ids=speed_scope,
            )
            _m["rows_written"] = sum(smartspeed_counts.values())
    for family, count in smartspeed_counts.items():
        by_family[family] += count

    dynamo = 0
    if not (has_incremental_scope and not dynamo_scope):
        with track_stage(
            "vald", "silver.assessment_metric", sub_stage="dynamo", db=db,
        ) as _m:
            dynamo = _load_dynamo_metrics(
                db,
                profile_lookup,
                profile_table=profile_table,
                assessment_table=assessment_table,
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                scoped_test_ids=dynamo_scope,
            )
            _m["rows_written"] = dynamo
    by_family["dynamo"] += dynamo

    forcedecks = 0
    if not (has_incremental_scope and not forcedecks_scope):
        with track_stage(
            "vald", "silver.assessment_metric", sub_stage="forcedecks", db=db,
            extra={
                "sharded": (
                    not has_incremental_scope
                    and day_start_utc is None
                    and day_end_utc is None
                ),
            },
        ) as _m:
            forcedecks = _insert_forcedecks_family(
                db,
                family="forcedecks",
                profile_table=profile_table,
                assessment_table=assessment_table,
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                scoped_test_ids=forcedecks_scope,
                enable_full_refresh_sharding=(
                    not has_incremental_scope
                    and day_start_utc is None
                    and day_end_utc is None
                ),
            )
            _m["rows_written"] = forcedecks
    by_family["forcedecks"] += forcedecks

    summary = {
        "total_inserted": sum(by_family.values()),
        "by_family": dict(by_family),
    }
    logger.info("VALD assessment metric summary: %s", summary)
    return summary


def _load_nordbord_metrics(
    db: DatabaseManager,
    profile_lookup: dict[str, dict[str, Any]],
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: Sequence[str] | None = None,
) -> int:
    """Insert NordBord rows into the long fact table."""
    window_predicate, window_params = _build_day_window_predicate(
        "COALESCE(t.test_date_utc, t.modified_date_utc)",
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    )
    scope_predicate, scope_params = _build_test_id_scope_predicate(
        "t.test_id",
        scoped_test_ids=scoped_test_ids,
    )
    where_clauses = [predicate for predicate in (window_predicate, scope_predicate) if predicate]
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query_params = (*window_params, *scope_params)
    rows = db.fetch_all_dict(
        f"""
        SELECT
            t.test_id,
            t.profile_id,
            t.test_date_utc,
            t.modified_date_utc,
            t.test_type_name,
            t.left_avg_force,
            t.left_impulse,
            t.left_max_force,
            t.left_torque,
            t.left_calibration,
            t.left_repetitions,
            t.right_avg_force,
            t.right_impulse,
            t.right_max_force,
            t.right_torque,
            t.right_calibration,
            t.right_repetitions,
            m.metrics_payload
        FROM bronze.vald_nordbord_tests t
        LEFT JOIN bronze.vald_nordbord_test_metrics m
            ON m.test_id = t.test_id
        JOIN {profile_table} p
            ON p.provider_profile_id = t.profile_id
           AND p.is_active = TRUE
        {where_sql}
        ORDER BY t.test_date_utc, t.test_id
        """,
        query_params or None,
    )

    qualifying_profiles = _load_min_tests_qualifying_profiles(
        db,
        source_table="bronze.vald_nordbord_tests",
        date_expr="COALESCE(test_date_utc, modified_date_utc)",
    )

    metrics: list[dict[str, Any]] = []
    for row in rows:
        context = profile_lookup.get(str(row["profile_id"]))
        if not context:
            continue
        test_date = row.get("test_date_utc") or row.get("modified_date_utc")
        if not test_date:
            continue
        if (
            is_on_or_after_new_entry_min_tests_date(test_date)
            and str(row["profile_id"]) not in qualifying_profiles
        ):
            continue

        seen: set[tuple[str, str | None, int | None]] = set()
        for column in NORDBORD_BASE_METRICS:
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="nordbord",
                assessment_family="nordics",
                test_id=str(row["test_id"]),
                test_name=row.get("test_type_name") or "NordBord",
                test_type=row.get("test_type_name"),
                metric_name=column,
                metric_value=row.get(column),
                seen=seen,
            )

        for metric_name, metric_value, metric_unit in _iter_numeric_payload_metrics(
            row.get("metrics_payload"),
        ):
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="nordbord",
                assessment_family="nordics",
                test_id=str(row["test_id"]),
                test_name=row.get("test_type_name") or "NordBord",
                test_type=row.get("test_type_name"),
                metric_name=metric_name,
                metric_value=metric_value,
                metric_unit=metric_unit,
                seen=seen,
            )

    return _insert_rows(db, assessment_table, metrics, ASSESSMENT_COLUMNS)


def _load_forceframe_metrics(
    db: DatabaseManager,
    profile_lookup: dict[str, dict[str, Any]],
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: Sequence[str] | None = None,
) -> int:
    """Insert ForceFrame rows into the long fact table."""
    window_predicate, window_params = _build_day_window_predicate(
        "COALESCE(t.test_date_utc, t.modified_date_utc)",
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    )
    scope_predicate, scope_params = _build_test_id_scope_predicate(
        "t.test_id",
        scoped_test_ids=scoped_test_ids,
    )
    where_clauses = [predicate for predicate in (window_predicate, scope_predicate) if predicate]
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query_params = (*window_params, *scope_params)
    rows = db.fetch_all_dict(
        f"""
        SELECT
            t.test_id,
            t.profile_id,
            t.test_date_utc,
            t.modified_date_utc,
            t.test_type_name,
            t.test_position_name,
            t.inner_left_avg_force,
            t.inner_left_impulse,
            t.inner_left_max_force,
            t.inner_left_repetitions,
            t.inner_right_avg_force,
            t.inner_right_impulse,
            t.inner_right_max_force,
            t.inner_right_repetitions,
            t.outer_left_avg_force,
            t.outer_left_impulse,
            t.outer_left_max_force,
            t.outer_left_repetitions,
            t.outer_right_avg_force,
            t.outer_right_impulse,
            t.outer_right_max_force,
            t.outer_right_repetitions,
            m.metrics_payload
        FROM bronze.vald_forceframe_tests t
        LEFT JOIN bronze.vald_forceframe_test_metrics m
            ON m.test_id = t.test_id
        JOIN {profile_table} p
            ON p.provider_profile_id = t.profile_id
           AND p.is_active = TRUE
        {where_sql}
        ORDER BY t.test_date_utc, t.test_id
        """,
        query_params or None,
    )

    qualifying_profiles = _load_min_tests_qualifying_profiles(
        db,
        source_table="bronze.vald_forceframe_tests",
        date_expr="COALESCE(test_date_utc, modified_date_utc)",
    )

    metrics: list[dict[str, Any]] = []
    for row in rows:
        context = profile_lookup.get(str(row["profile_id"]))
        if not context:
            continue
        test_date = row.get("test_date_utc") or row.get("modified_date_utc")
        if not test_date:
            continue
        if (
            is_on_or_after_new_entry_min_tests_date(test_date)
            and str(row["profile_id"]) not in qualifying_profiles
        ):
            continue

        test_name = row.get("test_type_name") or "ForceFrame"
        if row.get("test_position_name"):
            test_name = f"{test_name} - {row['test_position_name']}"

        seen: set[tuple[str, str | None, int | None]] = set()
        for column in FORCEFRAME_BASE_METRICS:
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="forceframe",
                assessment_family="forceframe",
                test_id=str(row["test_id"]),
                test_name=test_name,
                test_type=row.get("test_type_name"),
                metric_name=column,
                metric_value=row.get(column),
                seen=seen,
            )

        for metric_name, metric_value, metric_unit in _iter_numeric_payload_metrics(
            row.get("metrics_payload"),
        ):
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="forceframe",
                assessment_family="forceframe",
                test_id=str(row["test_id"]),
                test_name=test_name,
                test_type=row.get("test_type_name"),
                metric_name=metric_name,
                metric_value=metric_value,
                metric_unit=metric_unit,
                seen=seen,
            )

    return _insert_rows(db, assessment_table, metrics, ASSESSMENT_COLUMNS)


def _load_smartspeed_metrics(
    db: DatabaseManager,
    profile_lookup: dict[str, dict[str, Any]],
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: Sequence[str] | None = None,
) -> dict[str, int]:
    """Insert SmartSpeed rows into the long fact table."""
    window_predicate, window_params = _build_day_window_predicate(
        "COALESCE(d.test_date_utc, s.test_date_utc)",
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    )
    scope_predicate, scope_params = _build_test_id_scope_predicate(
        "s.test_id",
        scoped_test_ids=scoped_test_ids,
    )
    where_clauses = [predicate for predicate in (window_predicate, scope_predicate) if predicate]
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query_params = (*window_params, *scope_params)
    rows = db.fetch_all_dict(
        f"""
        SELECT
            s.test_id,
            COALESCE(d.profile_id, s.profile_id) AS profile_id,
            s.test_name,
            s.test_type_name,
            s.test_date_utc AS summary_test_date_utc,
            d.test_date_utc AS detail_test_date_utc,
            d.additional_test_result,
            d.rep_results
        FROM bronze.vald_smartspeed_test_summaries s
        LEFT JOIN bronze.vald_smartspeed_test_details d
            ON d.test_id = s.test_id
        JOIN {profile_table} p
            ON p.provider_profile_id = COALESCE(d.profile_id, s.profile_id)
           AND p.is_active = TRUE
        {where_sql}
        ORDER BY COALESCE(d.test_date_utc, s.test_date_utc), s.test_id
        """,
        query_params or None,
    )

    qualifying_profiles = _load_min_tests_qualifying_profiles(
        db,
        source_table="bronze.vald_smartspeed_test_summaries",
        date_expr="test_date_utc",
    )

    metrics: list[dict[str, Any]] = []
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        context = profile_lookup.get(str(row["profile_id"]))
        if not context:
            continue

        test_date = row.get("detail_test_date_utc") or row.get("summary_test_date_utc")
        if not test_date:
            continue
        if (
            is_on_or_after_new_entry_min_tests_date(test_date)
            and str(row["profile_id"]) not in qualifying_profiles
        ):
            continue

        family = "speed"
        seen: set[tuple[str, str | None, int | None]] = set()

        for payload in (row.get("additional_test_result"),):
            for metric_name, metric_value, metric_unit in _iter_numeric_payload_metrics(
                payload,
            ):
                _append_metric_row(
                    metrics,
                    context=context,
                    test_date=test_date,
                    source_module="smartspeed",
                    assessment_family=family,
                    test_id=str(row["test_id"]),
                    test_name=row.get("test_name"),
                    test_type=row.get("test_type_name"),
                    metric_name=metric_name,
                    metric_value=metric_value,
                    metric_unit=metric_unit,
                    seen=seen,
                )

        rep_results = row.get("rep_results") or []
        if isinstance(rep_results, list):
            for rep_index, rep in enumerate(rep_results, 1):
                if not isinstance(rep, dict):
                    continue
                rep_number = _coerce_int(
                    rep.get("repIndex") or rep.get("repNumber") or rep_index
                )

                for key, value in rep.items():
                    if key in {"repIndex", "repNumber", "jumpResults", "splitResults"}:
                        continue
                    metric_value = _coerce_numeric(value)
                    if metric_value is None:
                        continue
                    metric_name = _camel_to_snake(key)
                    _append_metric_row(
                        metrics,
                        context=context,
                        test_date=test_date,
                        source_module="smartspeed",
                        assessment_family=family,
                        test_id=str(row["test_id"]),
                        test_name=row.get("test_name"),
                        test_type=row.get("test_type_name"),
                        metric_name=metric_name,
                        metric_value=metric_value,
                        metric_unit=_infer_smartspeed_unit(metric_name),
                        rep_number=rep_number,
                        seen=seen,
                    )

                jump_results = rep.get("jumpResults") or []
                for jump_pos, jump in enumerate(jump_results, 1):
                    if not isinstance(jump, dict):
                        continue
                    jump_number = _coerce_int(jump.get("jumpIndex") or jump_pos) or jump_pos
                    for key, value in jump.items():
                        if key in {"jumpIndex", "splitCompleteDate"}:
                            continue
                        metric_value = _coerce_numeric(value)
                        if metric_value is None:
                            continue
                        metric_name = f"jump_{jump_number}_{_camel_to_snake(key)}"
                        _append_metric_row(
                            metrics,
                            context=context,
                            test_date=test_date,
                            source_module="smartspeed",
                            assessment_family=family,
                            test_id=str(row["test_id"]),
                            test_name=row.get("test_name"),
                            test_type=row.get("test_type_name"),
                            metric_name=metric_name,
                            metric_value=metric_value,
                            metric_unit=_infer_smartspeed_unit(metric_name),
                            rep_number=rep_number,
                            seen=seen,
                        )

                split_results = rep.get("splitResults") or []
                for split_pos, split in enumerate(split_results, 1):
                    if not isinstance(split, dict):
                        continue
                    split_number = (
                        _coerce_int(split.get("splitIndex") or split_pos) or split_pos
                    )
                    for key, value in split.items():
                        if key in {"splitIndex", "gateIndex", "splitCompleteDate"}:
                            continue
                        if key == "additionalSplitData" and isinstance(value, dict):
                            for nested_key, nested_value in value.items():
                                metric_value = _coerce_numeric(nested_value)
                                if metric_value is None:
                                    continue
                                metric_name = (
                                    f"split_{split_number}_{_camel_to_snake(nested_key)}"
                                )
                                _append_metric_row(
                                    metrics,
                                    context=context,
                                    test_date=test_date,
                                    source_module="smartspeed",
                                    assessment_family=family,
                                    test_id=str(row["test_id"]),
                                    test_name=row.get("test_name"),
                                    test_type=row.get("test_type_name"),
                                    metric_name=metric_name,
                                    metric_value=metric_value,
                                    metric_unit=_infer_smartspeed_unit(metric_name),
                                    rep_number=rep_number,
                                    seen=seen,
                                )
                            continue

                        metric_value = _coerce_numeric(value)
                        if metric_value is None:
                            continue
                        metric_name = f"split_{split_number}_{_camel_to_snake(key)}"
                        _append_metric_row(
                            metrics,
                            context=context,
                            test_date=test_date,
                            source_module="smartspeed",
                            assessment_family=family,
                            test_id=str(row["test_id"]),
                            test_name=row.get("test_name"),
                            test_type=row.get("test_type_name"),
                            metric_name=metric_name,
                            metric_value=metric_value,
                            metric_unit=_infer_smartspeed_unit(metric_name),
                            rep_number=rep_number,
                            seen=seen,
                        )

    inserted = _insert_rows(db, assessment_table, metrics, ASSESSMENT_COLUMNS)
    for metric in metrics:
        counts[metric["assessment_family"]] += 1
    logger.info("Inserted %d SmartSpeed assessment rows", inserted)
    return dict(counts)


def _load_dynamo_metrics(
    db: DatabaseManager,
    profile_lookup: dict[str, dict[str, Any]],
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: Sequence[str] | None = None,
) -> int:
    """Insert DynaMo summary and repetition rows into the long fact table."""
    window_predicate, window_params = _build_day_window_predicate(
        "COALESCE(t.start_time_utc, t.analysed_date_utc)",
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    )
    scope_predicate, scope_params = _build_test_id_scope_predicate(
        "t.test_id",
        scoped_test_ids=scoped_test_ids,
    )
    where_clauses = [predicate for predicate in (window_predicate, scope_predicate) if predicate]
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query_params = (*window_params, *scope_params)
    summary_rows = db.fetch_all_dict(
        f"""
        SELECT
            t.test_id,
            t.profile_id,
            t.test_category,
            t.body_region,
            t.movement,
            t.position,
            t.start_time_utc,
            t.analysed_date_utc,
            s.movement_type,
            s.side,
            s.max_force_newtons,
            s.avg_force_newtons,
            s.max_impulse_ns,
            s.avg_impulse_ns,
            s.max_rfd_nps,
            s.avg_rfd_nps,
            s.avg_time_to_peak_s,
            s.min_time_to_peak_s,
            s.max_rom_degrees,
            s.avg_rom_degrees,
            s.summary_payload
        FROM bronze.vald_dynamo_tests t
        JOIN bronze.vald_dynamo_rep_summaries s
            ON s.test_id = t.test_id
        JOIN {profile_table} p
            ON p.provider_profile_id = t.profile_id
           AND p.is_active = TRUE
        {where_sql}
        ORDER BY t.start_time_utc, t.test_id
        """,
        query_params or None,
    )

    repetition_rows = db.fetch_all_dict(
        f"""
        SELECT
            t.test_id,
            t.profile_id,
            t.test_category,
            t.body_region,
            t.movement,
            t.position,
            t.start_time_utc,
            t.analysed_date_utc,
            r.repetition_number,
            r.side,
            r.impulse_ns,
            r.rfd_nps,
            r.time_to_peak_s,
            r.rom_degrees,
            r.rep_payload
        FROM bronze.vald_dynamo_tests t
        JOIN bronze.vald_dynamo_repetitions r
            ON r.test_id = t.test_id
        JOIN {profile_table} p
            ON p.provider_profile_id = t.profile_id
           AND p.is_active = TRUE
        {where_sql}
        ORDER BY t.start_time_utc, t.test_id, r.repetition_number
        """,
        query_params or None,
    )

    qualifying_profiles = _load_min_tests_qualifying_profiles(
        db,
        source_table="bronze.vald_dynamo_tests",
        date_expr="COALESCE(start_time_utc, analysed_date_utc)",
    )

    metrics: list[dict[str, Any]] = []
    for row in summary_rows:
        context = profile_lookup.get(str(row["profile_id"]))
        if not context:
            continue
        test_date = (
            row.get("start_time_utc")
            or row.get("analysed_date_utc")
        )
        if not test_date:
            continue
        if (
            is_on_or_after_new_entry_min_tests_date(test_date)
            and str(row["profile_id"]) not in qualifying_profiles
        ):
            continue

        seen: set[tuple[str, str | None, int | None]] = set()
        for column in DYNAMO_SUMMARY_METRICS:
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="dynamo",
                assessment_family="dynamo",
                test_id=str(row["test_id"]),
                test_name=_compose_dynamo_test_name(row),
                test_type=row.get("test_category"),
                metric_name=column,
                metric_value=row.get(column),
                side=row.get("side"),
                seen=seen,
            )

        for metric_name, metric_value, metric_unit in _iter_numeric_payload_metrics(
            row.get("summary_payload"),
        ):
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="dynamo",
                assessment_family="dynamo",
                test_id=str(row["test_id"]),
                test_name=_compose_dynamo_test_name(row),
                test_type=row.get("test_category"),
                metric_name=metric_name,
                metric_value=metric_value,
                metric_unit=metric_unit,
                side=row.get("side"),
                seen=seen,
            )

    for row in repetition_rows:
        context = profile_lookup.get(str(row["profile_id"]))
        if not context:
            continue
        test_date = (
            row.get("start_time_utc")
            or row.get("analysed_date_utc")
        )
        if not test_date:
            continue
        if (
            is_on_or_after_new_entry_min_tests_date(test_date)
            and str(row["profile_id"]) not in qualifying_profiles
        ):
            continue

        seen: set[tuple[str, str | None, int | None]] = set()
        rep_number = _coerce_int(row.get("repetition_number"))
        for column in DYNAMO_REPETITION_METRICS:
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="dynamo",
                assessment_family="dynamo",
                test_id=str(row["test_id"]),
                test_name=_compose_dynamo_test_name(row),
                test_type=row.get("test_category"),
                metric_name=column,
                metric_value=row.get(column),
                side=row.get("side"),
                rep_number=rep_number,
                seen=seen,
            )

        for metric_name, metric_value, metric_unit in _iter_numeric_payload_metrics(
            row.get("rep_payload"),
        ):
            if metric_name in DYNAMO_REPETITION_PAYLOAD_EXCLUDED_METRICS:
                continue
            _append_metric_row(
                metrics,
                context=context,
                test_date=test_date,
                source_module="dynamo",
                assessment_family="dynamo",
                test_id=str(row["test_id"]),
                test_name=_compose_dynamo_test_name(row),
                test_type=row.get("test_category"),
                metric_name=metric_name,
                metric_value=metric_value,
                metric_unit=metric_unit,
                side=row.get("side"),
                rep_number=rep_number,
                seen=seen,
            )

    return _insert_rows(db, assessment_table, metrics, ASSESSMENT_COLUMNS)


def _resolve_silver_fd_group_workers() -> int:
    """Worker count for parallel per-target-group ForceDecks Silver inserts."""
    raw_value = os.environ.get("VALD_SILVER_FD_GROUP_WORKERS")
    if raw_value in (None, ""):
        return 4
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return 4
    return max(1, parsed)


def _insert_forcedecks_family(
    db: DatabaseManager,
    family: str,
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: Sequence[str] | None = None,
    enable_full_refresh_sharding: bool = False,
) -> int:
    """Insert the ForceDecks family directly in SQL.

    When ``enable_full_refresh_sharding`` is True and the call has no
    day-window or test-id scope, the work is sharded across
    ``target_group_id`` slices and dispatched to a thread pool so Postgres
    runs each ``INSERT … SELECT`` on its own backend. For scoped/incremental
    runs (or when sharding is disabled) the single-statement path is used.
    """
    can_shard = (
        enable_full_refresh_sharding
        and day_start_utc is None
        and day_end_utc is None
        and scoped_test_ids is None
    )
    if can_shard:
        target_group_ids = _list_silver_target_group_ids(db, profile_table)
        if target_group_ids:
            max_workers = max(
                1, min(_resolve_silver_fd_group_workers(), len(target_group_ids))
            )
            logger.info(
                "ForceDecks Silver: sharded insert across %d target_group_id(s) with %d workers",
                len(target_group_ids),
                max_workers,
            )
            total_inserted = 0
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="vald-fd-silver",
            ) as pool:
                futures = {
                    pool.submit(
                        _insert_forcedecks_family_for_group,
                        db,
                        family,
                        profile_table=profile_table,
                        assessment_table=assessment_table,
                        day_start_utc=None,
                        day_end_utc=None,
                        scoped_test_ids=None,
                        target_group_id=group_id,
                    ): group_id
                    for group_id in target_group_ids
                }
                for future in as_completed(futures):
                    total_inserted += future.result()
            logger.info(
                "Inserted %d ForceDecks assessment rows for family=%s (sharded)",
                total_inserted,
                family,
            )
            return total_inserted

    return _insert_forcedecks_family_for_group(
        db,
        family,
        profile_table=profile_table,
        assessment_table=assessment_table,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        scoped_test_ids=scoped_test_ids,
        target_group_id=None,
    )


def _list_silver_target_group_ids(
    db: DatabaseManager,
    profile_table: str,
) -> list[str]:
    """Distinct ``target_group_id`` values currently present in the silver profile table."""
    rows = db.fetch_all_dict(
        f"""
        SELECT DISTINCT target_group_id
        FROM {profile_table}
        WHERE target_group_id IS NOT NULL
          AND is_active = TRUE
        ORDER BY target_group_id
        """
    )
    return [str(row["target_group_id"]) for row in rows]


def _insert_forcedecks_family_for_group(
    db: DatabaseManager,
    family: str,
    *,
    profile_table: str = SILVER_TABLES["profile"],
    assessment_table: str = SILVER_TABLES["assessment"],
    day_start_utc: datetime | None = None,
    day_end_utc: datetime | None = None,
    scoped_test_ids: Sequence[str] | None = None,
    target_group_id: str | None = None,
) -> int:
    """Insert the ForceDecks family directly in SQL, optionally scoped by group."""
    window_predicate, window_params = _build_day_window_predicate(
        "COALESCE(tr.recorded_utc, t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc)",
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
    )
    scope_predicate, scope_params = _build_test_id_scope_predicate(
        "t.test_id",
        scoped_test_ids=scoped_test_ids,
    )
    window_sql = f"\n              AND {window_predicate}" if window_predicate else ""
    scope_sql = f"\n              AND {scope_predicate}" if scope_predicate else ""
    group_sql = ""
    group_params: tuple[Any, ...] = ()
    if target_group_id is not None:
        group_sql = "\n              AND p.target_group_id = %s"
        group_params = (target_group_id,)
    sql = f"""
        WITH new_period_test_counts AS MATERIALIZED (
            SELECT
                profile_id,
                COUNT(DISTINCT test_id) AS test_count
            FROM bronze.vald_forcedecks_tests
            WHERE COALESCE(recorded_date_utc, analysed_date_utc, modified_date_utc)
                  >= '{VALD_NEW_ENTRY_MIN_TESTS_UTC}'::timestamptz
            GROUP BY profile_id
        ),
        source_rows AS (
            SELECT
                p.provider_profile_id,
                p.provider_full_name AS athlete_name,
                p.target_category_name AS team_name,
                p.target_group_name AS team_group_name,
                p.target_group_id AS team_group_id,
                p.target_category_id AS category_id,
                COALESCE(tr.recorded_utc, t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc) AS test_date,
                tr.trial_id,
                'forcedecks' AS source_module,
                %s AS assessment_family,
                t.test_id,
                t.test_type AS test_name,
                t.test_type,
                COALESCE(
                    NULLIF(
                        TRIM(BOTH '_' FROM LOWER(
                            REGEXP_REPLACE(
                                CONCAT_WS('_', NULLIF(d.result_group, ''), NULLIF(d.result_name, '')),
                                '[^A-Za-z0-9]+',
                                '_',
                                'g'
                            )
                        )),
                        ''
                    ),
                    CONCAT('result_', r.result_id::text)
                ) AS metric_name,
                r.value AS metric_value,
                COALESCE(NULLIF(d.result_unit_name, ''), NULLIF(d.result_unit, '')) AS metric_unit,
                CASE
                    WHEN LOWER(COALESCE(r.limb, tr.limb, '')) IN ('left', 'leftside') THEN 'left'
                    WHEN LOWER(COALESCE(r.limb, tr.limb, '')) IN ('right', 'rightside') THEN 'right'
                    WHEN LOWER(COALESCE(r.limb, tr.limb, '')) IN ('both', 'bilateral') THEN 'bilateral'
                    WHEN LOWER(COALESCE(r.limb, tr.limb, '')) = 'trial' THEN 'trial'
                    ELSE NULLIF(
                        TRIM(BOTH '_' FROM LOWER(
                            REGEXP_REPLACE(COALESCE(r.limb, tr.limb, ''), '[^A-Za-z0-9]+', '_', 'g')
                        )),
                        ''
                    )
                END AS side,
                CASE WHEN r.repeat IS NULL THEN NULL ELSE r.repeat + 1 END AS raw_repeat_number
            FROM bronze.vald_forcedecks_trial_results r
            JOIN bronze.vald_forcedecks_tests t
                ON t.test_id = r.test_id
               AND t.profile_id = r.profile_id
            LEFT JOIN bronze.vald_forcedecks_trials tr
                ON tr.trial_id = r.trial_id
            LEFT JOIN bronze.vald_forcedecks_result_definitions d
                ON d.result_id = r.result_id
            JOIN {profile_table} p
                ON p.provider_profile_id = t.profile_id
               AND p.is_active = TRUE
            LEFT JOIN new_period_test_counts ntc
                ON ntc.profile_id = t.profile_id
            WHERE COALESCE(
                tr.recorded_utc, t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc
            ) IS NOT NULL
              AND COALESCE(
                tr.recorded_utc, t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc
              ) >= %s::timestamptz
              {window_sql}
              {scope_sql}
              {group_sql}
              AND r.value IS NOT NULL
              AND (
                  COALESCE(
                      tr.recorded_utc, t.recorded_date_utc, t.analysed_date_utc, t.modified_date_utc
                  ) < '{VALD_NEW_ENTRY_MIN_TESTS_UTC}'::timestamptz
                  OR COALESCE(ntc.test_count, 0) >= 2
              )
        ),
        prepared AS (
            SELECT
                provider_profile_id,
                athlete_name,
                team_name,
                team_group_name,
                team_group_id,
                category_id,
                test_date,
                trial_id,
                source_module,
                assessment_family,
                test_id,
                test_name,
                test_type,
                metric_name,
                metric_value,
                metric_unit,
                side,
                COALESCE(
                    CASE
                        WHEN trial_id IS NOT NULL THEN DENSE_RANK() OVER (
                            PARTITION BY test_id, COALESCE(side, '__unsided__')
                            ORDER BY test_date ASC, trial_id ASC
                        )::INTEGER
                        ELSE NULL
                    END,
                    raw_repeat_number
                ) AS rep_number,
                COALESCE(
                    NULLIF(
                        TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM metric_value::text)),
                        ''
                    ),
                    '0'
                ) AS normalized_metric_value
            FROM source_rows
        ),
        deduplicated_prepared AS (
            SELECT
                provider_profile_id,
                athlete_name,
                team_name,
                team_group_name,
                team_group_id,
                category_id,
                test_date,
                trial_id,
                source_module,
                assessment_family,
                test_id,
                test_name,
                test_type,
                metric_name,
                metric_value,
                metric_unit,
                side,
                rep_number,
                normalized_metric_value,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        provider_profile_id,
                        team_group_id,
                        test_id,
                        assessment_family,
                        source_module,
                        metric_name,
                        COALESCE(side, ''),
                        COALESCE(rep_number::text, ''),
                        normalized_metric_value,
                        COALESCE(trial_id::text, '')
                    ORDER BY
                        test_date ASC,
                        COALESCE(metric_unit, '') ASC,
                        COALESCE(test_name, '') ASC,
                        COALESCE(test_type, '') ASC
                ) AS duplicate_rank
            FROM prepared
        )
        INSERT INTO {assessment_table} (
            provider_profile_id,
            athlete_name,
            team_name,
            team_group_name,
            team_group_id,
            category_id,
            test_date,
            source_module,
            assessment_family,
            test_id,
            test_name,
            test_type,
            metric_name,
            metric_value,
            metric_unit,
            side,
            rep_number,
            metric_row_key
        )
        SELECT
            provider_profile_id,
            athlete_name,
            team_name,
            team_group_name,
            team_group_id,
            category_id,
            test_date,
            source_module,
            assessment_family,
            test_id,
            test_name,
            test_type,
            metric_name,
            metric_value,
            metric_unit,
            side,
            rep_number,
            md5(
                CONCAT_WS(
                    '|',
                    provider_profile_id::text,
                    team_group_id::text,
                    test_id::text,
                    assessment_family,
                    source_module,
                    metric_name,
                    COALESCE(side, ''),
                    COALESCE(rep_number::text, ''),
                    normalized_metric_value,
                    COALESCE(trial_id::text, '')
                )
            ) AS metric_row_key
        FROM deduplicated_prepared
        WHERE duplicate_rank = 1
        ON CONFLICT (metric_row_key) WHERE metric_row_key IS NOT NULL
        DO UPDATE SET
            provider_profile_id = EXCLUDED.provider_profile_id,
            athlete_name        = EXCLUDED.athlete_name,
            team_name           = EXCLUDED.team_name,
            team_group_name     = EXCLUDED.team_group_name,
            team_group_id       = EXCLUDED.team_group_id,
            category_id         = EXCLUDED.category_id,
            test_date           = EXCLUDED.test_date,
            source_module       = EXCLUDED.source_module,
            assessment_family   = EXCLUDED.assessment_family,
            test_id             = EXCLUDED.test_id,
            test_name           = EXCLUDED.test_name,
            test_type           = EXCLUDED.test_type,
            metric_name         = EXCLUDED.metric_name,
            metric_value        = EXCLUDED.metric_value,
            metric_unit         = EXCLUDED.metric_unit,
            side                = EXCLUDED.side,
            rep_number          = EXCLUDED.rep_number,
            updated_at          = now()
    """
    # Phase 8.7.B (2026-05-09): the INSERT now uses ON CONFLICT against the
    # partial UNIQUE index uq_vald_assessment_metric_row_key (which was already
    # in the schema). UPSERT replaces the old DELETE-then-INSERT pattern in
    # _delete_assessment_metric_rows / _delete_assessment_metric_rows_by_scope
    # for scoped silver rebuilds. The full-rebuild path uses the Phase 8.7.A
    # atomic-swap helper and writes into a fresh stage table — ON CONFLICT
    # never fires there but is harmless.
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL work_mem = '512MB'")
            cur.execute("SET LOCAL max_parallel_workers_per_gather = 4")
            params: tuple[Any, ...] = (
                family,
                VALD_CUTOFF_UTC,
                *window_params,
                *scope_params,
                *group_params,
            )
            cur.execute(sql, params)
            inserted = cur.rowcount
    if target_group_id is None:
        logger.info(
            "Inserted %d ForceDecks assessment rows for family=%s",
            inserted,
            family,
        )
    else:
        logger.info(
            "Inserted %d ForceDecks rows for family=%s group=%s",
            inserted,
            family,
            target_group_id,
        )
    return inserted


def _append_metric_row(
    target_rows: list[dict[str, Any]],
    *,
    context: dict[str, Any],
    test_date: Any,
    source_module: str,
    assessment_family: str,
    test_id: str,
    test_name: str | None,
    test_type: str | None,
    metric_name: str,
    metric_value: Any,
    metric_unit: str | None = None,
    side: str | None = None,
    rep_number: int | None = None,
    seen: set[tuple[str, str | None, int | None]] | None = None,
) -> None:
    """Normalise a metric row and append it to the target list."""
    numeric_value = _coerce_numeric(metric_value)
    if numeric_value is None or not test_date or not is_on_or_after_vald_cutoff(test_date):
        return

    normalised_metric_name = _normalise_metric_name(metric_name)
    if not normalised_metric_name:
        return

    side_from_name, stripped_metric_name = _extract_side_from_metric_name(
        normalised_metric_name
    )
    final_side = _normalize_side(side) or side_from_name
    final_metric_name = stripped_metric_name or normalised_metric_name

    key = (final_metric_name, final_side, rep_number)
    if seen is not None and key in seen:
        return
    if seen is not None:
        seen.add(key)

    metric_row_key = build_metric_row_key(
        provider_profile_id=context["provider_profile_id"],
        team_group_id=context["team_group_id"],
        test_id=test_id,
        assessment_family=assessment_family,
        source_module=source_module,
        metric_name=final_metric_name,
        side=final_side,
        rep_number=rep_number,
        metric_value=numeric_value,
    )

    target_rows.append(
        {
            "provider_profile_id": context["provider_profile_id"],
            "athlete_name": context.get("athlete_name"),
            "team_name": context.get("team_name") or context.get("team_group_name"),
            "team_group_name": context.get("team_group_name"),
            "team_group_id": context.get("team_group_id"),
            "category_id": context.get("category_id"),
            "test_date": test_date,
            "source_module": source_module,
            "assessment_family": assessment_family,
            "test_id": test_id,
            "test_name": test_name or test_type,
            "test_type": test_type,
            "metric_name": final_metric_name,
            "metric_value": numeric_value,
            "metric_unit": metric_unit or _infer_metric_unit(final_metric_name),
            "side": final_side,
            "rep_number": rep_number,
            "metric_row_key": metric_row_key,
        }
    )


def _iter_numeric_payload_metrics(
    payload: Any,
    prefix: str | None = None,
) -> list[tuple[str, float, str | None]]:
    """Return numeric leaf metrics from a JSON-like payload."""
    if payload is None:
        return []

    metrics: list[tuple[str, float, str | None]] = []

    def walk(value: Any, path: list[str]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if _looks_like_identifier_key(key):
                    continue
                walk(item, [*path, _camel_to_snake(str(key))])
            return

        if isinstance(value, list):
            for index, item in enumerate(value, 1):
                walk(item, [*path, str(index)])
            return

        numeric_value = _coerce_numeric(value)
        if numeric_value is None:
            return

        metric_name = "_".join(part for part in ([prefix] if prefix else []) + path)
        metrics.append(
            (
                _normalise_metric_name(metric_name),
                numeric_value,
                _infer_metric_unit(metric_name),
            )
        )

    walk(payload, [])
    return metrics


def _build_test_name(parts: Sequence[str | None], default: str) -> str:
    """Compose a readable test name from optional parts."""
    cleaned = [part.strip() for part in parts if part and str(part).strip()]
    return " ".join(cleaned) if cleaned else default


def _compose_dynamo_test_name(row: dict[str, Any]) -> str:
    """Build a readable DynaMo test name."""
    return _build_test_name(
        [
            row.get("body_region"),
            row.get("movement"),
            row.get("position"),
        ],
        "DynaMo",
    )


def _infer_smartspeed_unit(metric_name: str) -> str | None:
    """SmartSpeed-specific unit hints."""
    metric = metric_name.lower()
    if "flight_time" in metric or "contact_time" in metric:
        return "ms"
    if metric.endswith("height_m"):
        return "m"
    if metric.endswith("weight_kg"):
        return "kg"
    if "split_time" in metric or "cumulative_time" in metric:
        return "s"
    if metric.startswith("total_") or metric == "reaction_time":
        return "s"
    return _infer_metric_unit(metric_name)


def _clean_name_part(value: Any) -> str | None:
    """Trim and normalise whitespace in a name part."""
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def _camel_to_snake(name: str) -> str:
    """Convert camelCase or PascalCase strings to snake_case."""
    if not name:
        return ""
    first_pass = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    second_pass = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first_pass)
    return second_pass.replace(" ", "_").replace("-", "_").lower()


def _normalise_metric_name(name: str) -> str:
    """Normalise a metric name for consistent storage."""
    if not name:
        return ""
    snake = _camel_to_snake(name)
    return re.sub(r"_+", "_", snake).strip("_")


def _extract_side_from_metric_name(metric_name: str) -> tuple[str | None, str]:
    """Pull a laterality prefix out of the metric name when present."""
    prefixes = (
        "inner_left_",
        "inner_right_",
        "outer_left_",
        "outer_right_",
        "left_",
        "right_",
    )
    for prefix in prefixes:
        if metric_name.startswith(prefix):
            return prefix.rstrip("_"), metric_name[len(prefix) :]
    return None, metric_name


def _normalize_side(value: Any) -> str | None:
    """Normalise laterality labels across VALD modules."""
    if value is None:
        return None
    normalised = _normalise_metric_name(str(value))
    side_map = {
        "left": "left",
        "left_side": "left",
        "right": "right",
        "right_side": "right",
        "bilateral": "bilateral",
        "both": "bilateral",
        "trial": "trial",
        "inner_left": "inner_left",
        "inner_right": "inner_right",
        "outer_left": "outer_left",
        "outer_right": "outer_right",
        "left_then_right": "left_then_right",
        "right_then_left": "right_then_left",
    }
    return side_map.get(normalised, normalised or None)


def _infer_metric_unit(metric_name: str) -> str | None:
    """Infer a human-readable unit label from a metric name."""
    metric = metric_name.lower()
    if "flight_time" in metric or "contact_time" in metric:
        return "ms"
    if metric.endswith("_kg") or "weight_kg" in metric:
        return "kg"
    if "per_kg" in metric:
        return "per_kg"
    if "newtons_per_second" in metric or metric.endswith("_nps") or "_rfd_" in metric:
        return "N/s"
    if "newton_seconds" in metric or metric.endswith("_ns") or "impulse" in metric:
        return "Ns"
    if metric.endswith("_seconds") or "_time_to_" in metric or metric.endswith("_time") or "duration" in metric:
        return "s"
    if "degrees" in metric or "range_of_motion" in metric or metric.endswith("_rom"):
        return "degrees"
    if "torque" in metric:
        return "Nm"
    if "force" in metric or "baseline" in metric or "threshold" in metric:
        return "N"
    if "height_m" in metric:
        return "m"
    if metric.endswith("_pct") or "percent" in metric or "asymmetry" in metric:
        return "%"
    if "repetition" in metric or metric.endswith("_count") or metric == "rep_count":
        return "count"
    return None


def _looks_like_identifier_key(key: str) -> bool:
    """Return True for payload keys that are identifiers rather than metrics."""
    normalised = _normalise_metric_name(key)
    if normalised in {
        "id",
        "test_id",
        "athlete_id",
        "profile_id",
        "tenant_id",
        "group_id",
        "category_id",
        "jump_index",
        "split_index",
        "gate_index",
    }:
        return True
    return normalised.endswith(_ID_LIKE_SUFFIXES)


def _coerce_numeric(value: Any) -> float | None:
    """Convert a value to a finite float when possible."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, (int, float)):
        if math.isfinite(float(value)):
            return float(value)
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _coerce_int(value: Any) -> int | None:
    """Convert a value to an int when possible."""
    numeric = _coerce_numeric(value)
    if numeric is None:
        return None
    return int(numeric)


def _validate_day_window(
    day_start_utc: datetime | None,
    day_end_utc: datetime | None,
) -> tuple[datetime, datetime] | None:
    """Validate the optional assessment rebuild window."""
    if day_start_utc is None and day_end_utc is None:
        return None
    if day_start_utc is None or day_end_utc is None:
        raise ValueError("Both day_start_utc and day_end_utc are required together.")
    if day_start_utc.tzinfo is None or day_end_utc.tzinfo is None:
        raise ValueError("Assessment day windows must be timezone-aware.")
    if day_start_utc >= day_end_utc:
        raise ValueError("Assessment day window start must be earlier than the end.")
    return day_start_utc, day_end_utc


def _build_day_window_predicate(
    expression: str,
    *,
    day_start_utc: datetime | None,
    day_end_utc: datetime | None,
) -> tuple[str, tuple[Any, ...]]:
    """Return an optional SQL predicate for the assessment day window."""
    if day_start_utc is None and day_end_utc is None:
        return "", ()
    _validate_day_window(day_start_utc, day_end_utc)
    return (
        f"{expression} >= %s AND {expression} < %s",
        (day_start_utc, day_end_utc),
    )


def _build_test_id_scope_predicate(
    expression: str,
    *,
    scoped_test_ids: Sequence[str] | None,
) -> tuple[str, tuple[Any, ...]]:
    """Return an optional SQL predicate for a scoped list of test ids."""
    if not scoped_test_ids:
        return "", ()
    return (
        f"{expression} = ANY(%s::uuid[])",
        (list(scoped_test_ids),),
    )


# Phase 8.7.B (2026-05-09): the legacy `_delete_assessment_metric_rows` and
# `_delete_assessment_metric_rows_by_scope` helpers were removed. Their
# DELETE-then-INSERT pattern is now subsumed by the ON CONFLICT clause inside
# `_load_assessment_metrics`. Scoped silver rebuilds replace pre-existing rows
# in place via UPSERT — no DELETE on the live silver.vald_assessment_metric.


@contextmanager
def _atomic_replace_table_in_place(
    db: DatabaseManager,
    table_name: str,
) -> "Iterator[str]":
    """Yield a stage table; atomic-publish it onto ``table_name`` on success.

    Phase 8.7.A: replaces the previous ``_truncate_table(); _insert_rows()``
    pattern. The live table is never empty during the operation. On any
    exception inside the context, the orphaned stage is dropped and the
    exception propagates — the live table remains unchanged.

    Usage::

        with _atomic_replace_table_in_place(db, tables["membership"]) as stage:
            _insert_rows(db, stage, membership_rows, MEMBERSHIP_COLUMNS)
    """
    from ingestion.common.atomic_publish import (
        atomic_publish_table,
        build_stage_table_like,
    )

    stage_table = build_stage_table_like(db, live_table=table_name)
    try:
        yield stage_table
    except Exception:
        # Best-effort cleanup so etl_staging doesn't accumulate orphans.
        try:
            db.execute(f"DROP TABLE IF EXISTS {stage_table} CASCADE")
        except Exception:  # pragma: no cover
            pass
        raise
    atomic_publish_table(db, live_table=table_name, stage_table=stage_table)


def _insert_rows(
    db: DatabaseManager,
    table_name: str,
    rows: list[dict[str, Any]],
    columns: Sequence[str],
    *,
    chunk_size: int = 5000,
) -> int:
    """Batch-insert rows with ``execute_values``."""
    if not rows:
        return 0

    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    template = "(" + ", ".join(f"%({column})s" for column in columns) + ")"
    inserted = 0
    with db.connection() as conn:
        with conn.cursor() as cur:
            for start in range(0, len(rows), chunk_size):
                chunk = rows[start : start + chunk_size]
                psycopg2.extras.execute_values(
                    cur,
                    sql,
                    chunk,
                    template=template,
                )
                inserted += len(chunk)
    return inserted


def _fetch_scalar(
    db: DatabaseManager,
    sql: str,
    params: Sequence[Any] | None = None,
) -> Any:
    """Fetch a single scalar value."""
    row = db.fetch_one(sql, tuple(params) if params else None)
    return row[0] if row else None
