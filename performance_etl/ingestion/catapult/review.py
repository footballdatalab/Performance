"""
Catapult bounded review and audit flow.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from ingestion.catapult.catalog import RAW_TO_BRONZE_TABLE_MAP, REFERENCE_RAW_TABLES
from ingestion.catapult.client import CatapultAccountConfig, CatapultClient, build_catapult_runtime_config
from ingestion.catapult.pipeline import run_extract_raw, run_raw_to_bronze_stage
from ingestion.catapult.replay_scope import batch_ids_for_account, build_batch_ids_by_source_table
from ingestion.common.config import get_db_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_REVIEW_ACCOUNTS = "U15,U16"
_DEFAULT_REVIEW_DAYS = 5
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_NUMERIC_RE = re.compile(r"^[0-9]+$")
_REFERENCE_RAW_TABLES = set(REFERENCE_RAW_TABLES)

_IMPORTANT_COLUMNS = {
    "bronze.catapult_teams": ["team_id", "team_name", "team_code"],
    "bronze.catapult_positions": ["position_id", "position_name"],
    "bronze.catapult_parameters": ["parameter_id", "parameter_name", "parameter_slug", "parameter_unit"],
    "bronze.catapult_venues": ["venue_id", "venue_name", "venue_city", "venue_country"],
    "bronze.catapult_tag_types": ["tag_type_id", "tag_type_name"],
    "bronze.catapult_tags": ["tag_id", "tag_type_id", "tag_name"],
    "bronze.catapult_athletes": [
        "athlete_id",
        "current_team_id",
        "position_id",
        "first_name",
        "last_name",
        "full_name",
        "gender",
        "nickname",
        "height",
        "weight",
        "date_of_birth",
        "jersey_number",
        "velocity_max",
        "heart_rate_max",
        "player_load_max",
    ],
    "bronze.catapult_activities": [
        "activity_id",
        "activity_name",
        "start_time",
        "end_time",
    ],
    "bronze.catapult_periods": ["period_id", "activity_id", "period_name", "start_time", "end_time"],
    "bronze.catapult_annotations": [
        "annotation_id",
        "annotation_scope",
        "activity_id",
        "period_id",
        "athlete_id",
        "annotation_text",
        "recorded_at",
    ],
    "bronze.catapult_entity_tags": ["record_hash", "entity_type", "entity_id", "tag_id", "tagged_at"],
    "bronze.catapult_stats": [
        "activity_id",
        "athlete_id",
        "period_id",
        "period_key",
        "start_time",
        "end_time",
        "total_distance",
        "player_load",
    ],
    "bronze.catapult_efforts": [
        "record_hash",
        "activity_id",
        "athlete_id",
        "effort_type",
        "start_time",
        "end_time",
    ],
    "bronze.catapult_events": [
        "record_hash",
        "activity_id",
        "athlete_id",
        "event_type",
        "occurred_at",
    ],
    "bronze.catapult_sensor_data": [
        "record_hash",
        "activity_id",
        "athlete_id",
        "recorded_at",
        "latitude",
        "longitude",
        "velocity",
    ],
}

_REQUIRED_COLUMNS = {
    "bronze.catapult_teams": ["team_id"],
    "bronze.catapult_positions": ["position_id"],
    "bronze.catapult_parameters": ["parameter_id"],
    "bronze.catapult_venues": ["venue_id"],
    "bronze.catapult_tag_types": ["tag_type_id"],
    "bronze.catapult_tags": ["tag_id"],
    "bronze.catapult_athletes": ["athlete_id"],
    "bronze.catapult_activities": ["activity_id", "start_time"],
    "bronze.catapult_periods": ["period_id", "activity_id"],
    "bronze.catapult_annotations": ["annotation_id", "annotation_scope"],
    "bronze.catapult_entity_tags": ["record_hash", "entity_type", "entity_id", "tag_id"],
    "bronze.catapult_stats": ["activity_id", "athlete_id", "start_time", "period_key"],
    "bronze.catapult_efforts": ["record_hash", "activity_id", "athlete_id", "start_time"],
    "bronze.catapult_events": ["record_hash", "activity_id", "athlete_id", "occurred_at"],
    "bronze.catapult_sensor_data": ["record_hash", "activity_id", "athlete_id", "recorded_at"],
}

_TIME_COLUMNS = {
    "bronze.catapult_activities": "start_time",
    "bronze.catapult_periods": "start_time",
    "bronze.catapult_annotations": "recorded_at",
    "bronze.catapult_entity_tags": "tagged_at",
    "bronze.catapult_stats": "start_time",
    "bronze.catapult_efforts": "start_time",
    "bronze.catapult_events": "occurred_at",
    "bronze.catapult_sensor_data": "recorded_at",
}

_ID_COLUMNS = {
    "bronze.catapult_teams": ["team_id"],
    "bronze.catapult_positions": ["position_id"],
    "bronze.catapult_parameters": ["parameter_id"],
    "bronze.catapult_venues": ["venue_id"],
    "bronze.catapult_tag_types": ["tag_type_id"],
    "bronze.catapult_tags": ["tag_id", "tag_type_id"],
    "bronze.catapult_athletes": ["athlete_id", "current_team_id", "position_id"],
    "bronze.catapult_activities": ["activity_id"],
    "bronze.catapult_periods": ["period_id", "activity_id"],
    "bronze.catapult_annotations": ["annotation_id", "activity_id", "period_id", "athlete_id"],
    "bronze.catapult_entity_tags": ["entity_id", "tag_id"],
    "bronze.catapult_stats": ["activity_id", "athlete_id", "period_id", "period_key"],
    "bronze.catapult_efforts": ["activity_id", "athlete_id"],
    "bronze.catapult_events": ["activity_id", "athlete_id"],
    "bronze.catapult_sensor_data": ["activity_id", "athlete_id"],
}

_PAIR_TABLES = {
    "stats": ("bronze.catapult_stats", "raw.catapult_stats"),
    "efforts": ("bronze.catapult_efforts", "raw.catapult_efforts"),
    "events": ("bronze.catapult_events", "raw.catapult_events"),
    "sensor_data": ("bronze.catapult_sensor_data", "raw.catapult_sensor_data"),
}
_STAGE_API_NAMES = {
    "activities": "activities_raw",
    "periods": "periods_raw",
    "annotations": "annotations_raw",
    "stats": "stats_raw",
    "efforts": "efforts_raw",
    "events": "events_raw",
    "sensor_data": "sensor_data_raw",
}
_REFERENCE_ENDPOINT_BY_RAW_TABLE = {
    "raw.catapult_teams": "teams",
    "raw.catapult_athletes": "athletes",
    "raw.catapult_positions": "positions",
    "raw.catapult_parameters": "parameters",
    "raw.catapult_venues": "venues",
    "raw.catapult_tag_types": "tag_types",
    "raw.catapult_tags": "tags",
    "raw.catapult_entity_tags": "entity_tags",
}
_REFERENCE_ENDPOINT_TABLES = {endpoint: raw_table for raw_table, endpoint in _REFERENCE_ENDPOINT_BY_RAW_TABLE.items()}

def run_review(
    *,
    accounts: str = _DEFAULT_REVIEW_ACCOUNTS,
    days: int = _DEFAULT_REVIEW_DAYS,
    include_reference: bool = True,
    include_sensor_data: bool = True,
    audit_only: bool = False,
) -> dict[str, Any]:
    """Run the bounded Catapult review flow for the requested accounts."""
    with DatabaseManager(get_db_config()) as db:
        if audit_only:
            raw_summary, replay_summary = _build_audit_only_context(
                db=db,
                accounts=accounts,
                days=days,
                include_reference=include_reference,
            )
        else:
            raw_summary = run_extract_raw(
                accounts=accounts,
                include_reference=include_reference,
                include_sensor_data=include_sensor_data,
                include_activity_athlete_enumeration=True,
                days=days,
                pair_source="stats",
            )
            batch_ids_by_source_table = build_batch_ids_by_source_table(raw_summary)
            replay_summary = run_raw_to_bronze_stage(batch_ids_by_source_table=batch_ids_by_source_table)

        audit_summary = audit_review(
            db=db,
            raw_summary=raw_summary,
            replay_summary=replay_summary,
        )

    _compact_activity_pair_output(raw_summary)

    failures = list(raw_summary.get("errors", [])) + list(audit_summary.get("failures", []))
    return {
        "raw": raw_summary,
        "raw_to_bronze": replay_summary,
        "audit": audit_summary,
        "mode": "audit_only" if audit_only else "extract_and_replay",
        "passed": not failures and bool(audit_summary.get("passed", True)),
        "failures": failures,
    }


def audit_review(
    *,
    db: DatabaseManager,
    raw_summary: dict[str, Any],
    replay_summary: dict[str, Any],
) -> dict[str, Any]:
    """Audit the Catapult review run using the exact batches produced by the review flow."""
    summary: dict[str, Any] = {
        "accounts": {},
        "passed": True,
        "failures": [],
    }
    if raw_summary.get("errors"):
        summary["failures"].extend(raw_summary["errors"])
        summary["passed"] = False

    for account_name, account_raw_summary in dict(raw_summary.get("accounts", {})).items():
        account_result: dict[str, Any] = {
            "tables": {},
            "relationships": {},
            "coverage": {},
            "coverage_inputs": {},
            "warnings": [],
            "failures": [],
        }
        expected_pairs = _expected_micro_pairs(account_raw_summary)
        activity_pairs = _summary_pairs(account_raw_summary, "activity_athlete_enumeration")
        device_pairs = _summary_pairs(account_raw_summary, "activity_devices")
        account_result["coverage_inputs"] = {
            "activity_pair_count": len(activity_pairs),
            "device_pair_count": len(device_pairs),
            "activity_pairs_without_device_mapping": len(activity_pairs - device_pairs),
            "device_pairs_without_activity_mapping": len(device_pairs - activity_pairs),
            "expected_pair_source": "activity_devices" if device_pairs else "activity_athlete_enumeration",
        }
        for raw_table, bronze_table in RAW_TO_BRONZE_TABLE_MAP.items():
            batch_ids = batch_ids_for_account(raw_table, account_raw_summary)
            raw_profile = _profile_raw_table(
                db=db,
                raw_table=raw_table,
                account_name=account_name,
                batch_ids=batch_ids,
            )
            bronze_profile = _profile_bronze_table(
                db=db,
                raw_table=raw_table,
                bronze_table=bronze_table,
                account_name=account_name,
                batch_ids=batch_ids,
            )
            replay_table_summary = dict(replay_summary.get("tables", {}).get(raw_table, {}))
            classification = _classify_zero_row_table(raw_table, raw_profile["row_count"], bronze_profile["row_count"])

            table_result = {
                "raw_table": raw_table,
                "bronze_table": bronze_table,
                "raw": raw_profile,
                "bronze": bronze_profile,
                "replay": replay_table_summary,
                "zero_row_classification": classification,
            }
            account_result["tables"][bronze_table] = table_result

            if classification is not None:
                account_result["warnings"].append(
                    f"{account_name} {bronze_table}: {classification['status']} - {classification['reason']}"
                )

            _collect_table_failures(
                summary=summary,
                account_result=account_result,
                account_name=account_name,
                bronze_table=bronze_table,
                bronze_profile=bronze_profile,
                replay_table_summary=replay_table_summary,
            )

        relationship_results = _audit_relationships(
            db=db,
            account_name=account_name,
            account_raw_summary=account_raw_summary,
        )
        account_result["relationships"] = relationship_results
        for relationship_name, relationship_result in relationship_results.items():
            if relationship_result["orphan_count"] > 0:
                message = (
                    f"{account_name} {relationship_name} has {relationship_result['orphan_count']} orphaned rows"
                )
                account_result["failures"].append(message)
                summary["failures"].append(message)
                summary["passed"] = False

        coverage_results = _audit_coverage(
            db=db,
            account_name=account_name,
            account_raw_summary=account_raw_summary,
            expected_pairs=expected_pairs,
        )
        account_result["coverage"] = coverage_results
        for coverage_name, coverage_result in coverage_results.items():
            if coverage_result["extra_pairs"] > 0:
                message = (
                    f"{account_name} {coverage_name} produced {coverage_result['extra_pairs']} pairs outside "
                    f"the expected device-mapped micro slice"
                )
                account_result["failures"].append(message)
                summary["failures"].append(message)
                summary["passed"] = False
            if coverage_result["missing_pairs"] > 0:
                account_result["warnings"].append(
                    f"{account_name} {coverage_name} is missing {coverage_result['missing_pairs']} expected pairs"
                )

        summary["accounts"][account_name] = account_result

    return summary


def main_run_review(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the Catapult review flow."""
    parser = argparse.ArgumentParser(description="Run the bounded Catapult review and audit flow.")
    parser.add_argument(
        "--accounts",
        type=str,
        default=_DEFAULT_REVIEW_ACCOUNTS,
        help="Comma-separated Catapult account names/team codes to review.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=_DEFAULT_REVIEW_DAYS,
        help="Latest rolling day window to review. Default: 5.",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip Catapult reference endpoint capture.",
    )
    parser.add_argument(
        "--skip-sensor-data",
        action="store_true",
        help="Skip Catapult sensor-data extraction during review.",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default=None,
        help="Optional file path for the full JSON review output.",
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Reuse the latest completed Catapult raw/replay batches and rerun only the audit/reporting layer.",
    )
    args = parser.parse_args(argv)

    result = run_review(
        accounts=args.accounts,
        days=args.days,
        include_reference=not args.skip_reference,
        include_sensor_data=not args.skip_sensor_data,
        audit_only=args.audit_only,
    )
    rendered = json.dumps(result, indent=2, default=str)
    print(rendered)
    if args.json_out:
        output_path = Path(args.json_out)
        output_path.write_text(rendered, encoding="utf-8")
    return 0 if result.get("passed") else 1


def _build_audit_only_context(
    *,
    db: DatabaseManager,
    accounts: str,
    days: int,
    include_reference: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    runtime_config = build_catapult_runtime_config()
    selected_accounts = _select_review_accounts(accounts, runtime_config.accounts)
    raw_summary: dict[str, Any] = {
        "accounts": {},
        "total_extracted": 0,
        "total_loaded": 0,
        "has_new_data": False,
        "errors": [],
        "days": days,
        "pair_source": "activity_devices",
        "audit_only": True,
    }
    replay_summary = _build_audit_only_replay_summary(db)

    for account in selected_accounts:
        client = CatapultClient(runtime_config, account)
        account_summary = _build_audit_only_account_summary(
            db=db,
            account=account,
            client=client,
            days=days,
            include_reference=include_reference,
        )
        raw_summary["accounts"][account.name] = account_summary
        raw_summary["total_extracted"] += int(account_summary.get("total_extracted", 0) or 0)
        raw_summary["total_loaded"] += int(account_summary.get("total_loaded", 0) or 0)

    return raw_summary, replay_summary


def _build_audit_only_replay_summary(db: DatabaseManager) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "tables": {},
        "processed_raw_rows": 0,
        "loaded_rows": 0,
        "skipped_rows": 0,
        "has_new_data": False,
        "audit_only": True,
    }
    for raw_table in RAW_TO_BRONZE_TABLE_MAP:
        batch = _latest_completed_batch(db, api_name=f"replay:{raw_table}")
        if not batch:
            summary["tables"][raw_table] = {
                "processed_raw_rows": 0,
                "loaded_rows": 0,
                "skipped_rows": 0,
                "skip_reasons": {},
                "batch_id": None,
            }
            continue
        processed_raw_rows = int(batch.get("records_extracted", 0) or 0)
        loaded_rows = int(batch.get("records_loaded", 0) or 0)
        summary["tables"][raw_table] = {
            "processed_raw_rows": processed_raw_rows,
            "loaded_rows": loaded_rows,
            "skipped_rows": 0,
            "skip_reasons": {},
            "batch_id": str(batch["batch_id"]),
            "started_at": batch.get("started_at"),
            "completed_at": batch.get("completed_at"),
        }
        summary["processed_raw_rows"] += processed_raw_rows
        summary["loaded_rows"] += loaded_rows
        summary["has_new_data"] = True
    return summary


def _build_audit_only_account_summary(
    *,
    db: DatabaseManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    days: int,
    include_reference: bool,
) -> dict[str, Any]:
    account_summary: dict[str, Any] = {
        "reference": {},
        "activities": {},
        "periods": {},
        "annotations": {},
        "activity_athlete_enumeration": {},
        "activity_devices": {},
        "stats": {},
        "efforts": {},
        "events": {},
        "sensor_data": {},
        "errors": [],
        "total_extracted": 0,
        "total_loaded": 0,
        "audit_only": True,
    }
    if include_reference:
        account_summary["reference"] = _build_reference_stage_summary(db, account.name)

    for stage_key, api_name in _STAGE_API_NAMES.items():
        batch = _latest_completed_batch(db, api_name=api_name, account_name=account.name)
        raw_table = f"raw.catapult_{stage_key}"
        account_summary[stage_key] = _build_stage_summary(raw_table, batch, account.name, db)

    activities = _fetch_slice_activities(
        db=db,
        account_name=account.name,
        batch_id=str(account_summary["activities"].get("batch_id") or ""),
        days=days,
    )
    account_summary["activity_athlete_enumeration"] = _enumerate_activity_pairs(
        client=client,
        account_name=account.name,
        activities=activities,
        endpoint_kind="athletes",
    )
    account_summary["activity_devices"] = _enumerate_activity_pairs(
        client=client,
        account_name=account.name,
        activities=activities,
        endpoint_kind="devices",
    )

    reference_summary = dict(account_summary.get("reference", {}))
    account_summary["total_extracted"] += int(reference_summary.get("records_extracted", 0) or 0)
    account_summary["total_loaded"] += int(reference_summary.get("raw_rows_written", 0) or 0)
    for stage_key in _STAGE_API_NAMES:
        stage_summary = dict(account_summary.get(stage_key, {}))
        account_summary["total_extracted"] += int(stage_summary.get("records_extracted", 0) or 0)
        account_summary["total_loaded"] += int(stage_summary.get("raw_rows_written", 0) or 0)

    return account_summary


def _build_reference_stage_summary(db: DatabaseManager, account_name: str) -> dict[str, Any]:
    batch = _latest_completed_batch(db, api_name="reference_raw", account_name=account_name)
    summary: dict[str, Any] = {
        "batch_id": str(batch["batch_id"]) if batch else None,
        "records_extracted": int(batch.get("records_extracted", 0) or 0) if batch else 0,
        "raw_rows_written": 0,
        "endpoints": {},
        "entity_tags": {"raw_rows_written": 0},
        "audit_only": True,
    }
    if not batch:
        return summary

    batch_id = str(batch["batch_id"])
    for endpoint_name, raw_table in _REFERENCE_ENDPOINT_TABLES.items():
        raw_rows_written = _count_batch_rows(db, raw_table, account_name, batch_id)
        if endpoint_name == "entity_tags":
            summary["entity_tags"] = {
                "raw_rows_written": raw_rows_written,
                "records_extracted": raw_rows_written,
            }
        else:
            summary["endpoints"][endpoint_name] = {
                "raw_rows_written": raw_rows_written,
                "records_extracted": raw_rows_written,
            }
        summary["raw_rows_written"] += raw_rows_written
    return summary


def _build_stage_summary(
    raw_table: str,
    batch: dict[str, Any] | None,
    account_name: str,
    db: DatabaseManager,
) -> dict[str, Any]:
    if not batch:
        return {
            "batch_id": None,
            "records_extracted": 0,
            "raw_rows_written": 0,
            "audit_only": True,
        }
    batch_id = str(batch["batch_id"])
    return {
        "batch_id": batch_id,
        "records_extracted": int(batch.get("records_extracted", 0) or 0),
        "raw_rows_written": _count_batch_rows(db, raw_table, account_name, batch_id),
        "started_at": batch.get("started_at"),
        "completed_at": batch.get("completed_at"),
        "audit_only": True,
    }


def _count_batch_rows(
    db: DatabaseManager,
    raw_table: str,
    account_name: str,
    batch_id: str,
) -> int:
    row = db.fetch_one(
        f"""
        SELECT COUNT(*)
        FROM {raw_table}
        WHERE source_account = %s
          AND batch_id::text = %s
        """,
        (account_name, batch_id),
    )
    return int(row[0]) if row and row[0] is not None else 0


def _fetch_slice_activities(
    *,
    db: DatabaseManager,
    account_name: str,
    batch_id: str,
    days: int,
) -> list[dict[str, str]]:
    if batch_id:
        rows = db.fetch_all_dict(
            """
            SELECT DISTINCT activity_id::text AS activity_id
            FROM bronze.catapult_activities
            WHERE source_account = %s
              AND raw_id IN (
                  SELECT raw_id
                  FROM raw.catapult_activities
                  WHERE source_account = %s
                    AND batch_id::text = %s
              )
            ORDER BY activity_id::text
            """,
            (account_name, account_name, batch_id),
        )
        activities = [{"id": str(row["activity_id"])} for row in rows if row.get("activity_id")]
        if activities:
            return activities

    rows = db.fetch_all_dict(
        """
        SELECT DISTINCT activity_id::text AS activity_id
        FROM bronze.catapult_activities
        WHERE source_account = %s
          AND start_time >= NOW() - (%s || ' days')::interval
        ORDER BY activity_id::text
        """,
        (account_name, days),
    )
    return [{"id": str(row["activity_id"])} for row in rows if row.get("activity_id")]


def _enumerate_activity_pairs(
    *,
    client: CatapultClient,
    account_name: str,
    activities: list[dict[str, str]],
    endpoint_kind: str,
) -> dict[str, Any]:
    athlete_activity_pairs: set[tuple[str, str]] = set()
    activities_covered = 0
    for activity in activities:
        activity_id = _normalize_identifier(activity.get("id"))
        if activity_id is None:
            continue
        response = client.get(f"/activities/{activity_id}/{endpoint_kind}")
        payload = response.json()
        activities_covered += 1
        for row in payload:
            if endpoint_kind == "athletes":
                athlete_id = _normalize_identifier(row.get("id") or row.get("athlete_id"))
            else:
                athlete_id = _normalize_identifier(row.get("athlete_id"))
            if athlete_id is None:
                continue
            athlete_activity_pairs.add((athlete_id, activity_id))
    return {
        "records_extracted": len(athlete_activity_pairs),
        "raw_rows_written": 0,
        "activities_covered": activities_covered,
        "account_name": account_name,
        "audit_only": True,
        "pairs": sorted(athlete_activity_pairs),
    }


def _latest_completed_batch(
    db: DatabaseManager,
    *,
    api_name: str,
    account_name: str | None = None,
) -> dict[str, Any] | None:
    sql = """
        SELECT batch_id::text AS batch_id,
               source_account,
               api_name,
               records_extracted,
               records_loaded,
               started_at,
               completed_at
        FROM raw.ingestion_batch_log
        WHERE provider = 'catapult'
          AND status = 'completed'
          AND api_name = %s
    """
    params: list[Any] = [api_name]
    if account_name is not None:
        sql += " AND source_account = %s"
        params.append(account_name)
    sql += " ORDER BY started_at DESC, batch_id DESC LIMIT 1"
    return db.fetch_one_dict(sql, tuple(params))


def _select_review_accounts(
    accounts: str,
    runtime_accounts: tuple[CatapultAccountConfig, ...],
) -> list[CatapultAccountConfig]:
    if accounts.strip().lower() == "all":
        return list(runtime_accounts)

    requested = {part.strip().lower() for part in accounts.split(",") if part.strip()}
    selected: list[CatapultAccountConfig] = []
    for account in runtime_accounts:
        aliases = {account.name.lower(), account.team_code.lower()}
        if requested & aliases:
            selected.append(account)

    if not selected:
        raise ValueError(f"No Catapult accounts matched '{accounts}'.")
    return selected


def _profile_raw_table(
    *,
    db: DatabaseManager,
    raw_table: str,
    account_name: str,
    batch_ids: list[str],
) -> dict[str, Any]:
    if not batch_ids:
        return {
            "row_count": 0,
            "min_ingested_at": None,
            "max_ingested_at": None,
        }

    row = db.fetch_one_dict(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MIN(ingested_at) AS min_ingested_at,
            MAX(ingested_at) AS max_ingested_at
        FROM {raw_table}
        WHERE source_account = %s
          AND batch_id::text = ANY(%s)
        """,
        (account_name, batch_ids),
    ) or {}
    return {
        "row_count": int(row.get("row_count", 0) or 0),
        "min_ingested_at": row.get("min_ingested_at"),
        "max_ingested_at": row.get("max_ingested_at"),
    }


def _profile_bronze_table(
    *,
    db: DatabaseManager,
    raw_table: str,
    bronze_table: str,
    account_name: str,
    batch_ids: list[str],
) -> dict[str, Any]:
    column_types = _get_column_types(db, bronze_table)
    if not batch_ids:
        if raw_table in _REFERENCE_RAW_TABLES:
            return _profile_bronze_table_current_account(
                db=db,
                bronze_table=bronze_table,
                account_name=account_name,
                column_types=column_types,
            )
        return {
            "row_count": 0,
            "time_column": _TIME_COLUMNS.get(bronze_table),
            "min_time": None,
            "max_time": None,
            "important_columns": {},
            "id_profiles": _empty_id_profiles(bronze_table, column_types),
            "column_types": column_types,
        }

    important_columns = list(_IMPORTANT_COLUMNS.get(bronze_table, []))
    time_column = _TIME_COLUMNS.get(bronze_table)
    select_parts = ["COUNT(*) AS row_count"]
    if time_column:
        select_parts.append(f"MIN({time_column}) AS min_time")
        select_parts.append(f"MAX({time_column}) AS max_time")
    for column in important_columns:
        alias = f"{column}__blank_count"
        select_parts.append(
            f"SUM(CASE WHEN {column} IS NULL OR NULLIF(BTRIM({column}::text), '') IS NULL THEN 1 ELSE 0 END) AS {alias}"
        )

    row = db.fetch_one_dict(
        f"""
        SELECT
            {", ".join(select_parts)}
        FROM {bronze_table}
        WHERE source_account = %s
          AND raw_id IN ({_scoped_raw_id_subquery(raw_table)})
        """,
        (account_name, account_name, batch_ids),
    ) or {}

    row_count = int(row.get("row_count", 0) or 0)
    important_column_stats = {}
    for column in important_columns:
        blank_count = int(row.get(f"{column}__blank_count", 0) or 0)
        important_column_stats[column] = {
            "blank_count": blank_count,
            "blank_rate": (blank_count / row_count) if row_count else 0.0,
        }

    result = {
        "row_count": row_count,
        "time_column": time_column,
        "min_time": row.get("min_time"),
        "max_time": row.get("max_time"),
        "important_columns": important_column_stats,
        "id_profiles": _profile_identifier_columns(
            db=db,
            raw_table=raw_table,
            bronze_table=bronze_table,
            account_name=account_name,
            batch_ids=batch_ids,
            column_types=column_types,
        ),
        "column_types": column_types,
    }
    if row_count == 0 and raw_table in _REFERENCE_RAW_TABLES:
        return _profile_bronze_table_current_account(
            db=db,
            bronze_table=bronze_table,
            account_name=account_name,
            column_types=column_types,
        )
    return result


def _profile_bronze_table_current_account(
    *,
    db: DatabaseManager,
    bronze_table: str,
    account_name: str,
    column_types: dict[str, str],
) -> dict[str, Any]:
    important_columns = list(_IMPORTANT_COLUMNS.get(bronze_table, []))
    time_column = _TIME_COLUMNS.get(bronze_table)
    select_parts = ["COUNT(*) AS row_count"]
    if time_column:
        select_parts.append(f"MIN({time_column}) AS min_time")
        select_parts.append(f"MAX({time_column}) AS max_time")
    for column in important_columns:
        alias = f"{column}__blank_count"
        select_parts.append(
            f"SUM(CASE WHEN {column} IS NULL OR NULLIF(BTRIM({column}::text), '') IS NULL THEN 1 ELSE 0 END) AS {alias}"
        )

    row = db.fetch_one_dict(
        f"""
        SELECT
            {", ".join(select_parts)}
        FROM {bronze_table}
        WHERE source_account = %s
        """,
        (account_name,),
    ) or {}
    row_count = int(row.get("row_count", 0) or 0)
    important_column_stats = {}
    for column in important_columns:
        blank_count = int(row.get(f"{column}__blank_count", 0) or 0)
        important_column_stats[column] = {
            "blank_count": blank_count,
            "blank_rate": (blank_count / row_count) if row_count else 0.0,
        }

    return {
        "row_count": row_count,
        "time_column": time_column,
        "min_time": row.get("min_time"),
        "max_time": row.get("max_time"),
        "important_columns": important_column_stats,
        "id_profiles": _profile_identifier_columns_current_account(
            db=db,
            bronze_table=bronze_table,
            account_name=account_name,
            column_types=column_types,
        ),
        "column_types": column_types,
    }


def _profile_identifier_columns(
    *,
    db: DatabaseManager,
    raw_table: str,
    bronze_table: str,
    account_name: str,
    batch_ids: list[str],
    column_types: dict[str, str],
) -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    for column in _ID_COLUMNS.get(bronze_table, []):
        values = [
            str(row[0])
            for row in db.fetch_all(
                f"""
                SELECT DISTINCT {column}::text
                FROM {bronze_table}
                WHERE source_account = %s
                  AND raw_id IN ({_scoped_raw_id_subquery(raw_table)})
                  AND {column} IS NOT NULL
                  AND NULLIF(BTRIM({column}::text), '') IS NOT NULL
                ORDER BY {column}::text
                """,
                (account_name, account_name, batch_ids),
            )
        ]
        shape_counts = {
            "uuid": 0,
            "numeric": 0,
            "text": 0,
        }
        for value in values:
            shape_counts[_classify_identifier_shape(value)] += 1
        profiles[column] = {
            "database_type": column_types.get(column),
            "distinct_count": len(values),
            "shape_counts": shape_counts,
            "sample_values": values[:5],
        }
    return profiles


def _profile_identifier_columns_current_account(
    *,
    db: DatabaseManager,
    bronze_table: str,
    account_name: str,
    column_types: dict[str, str],
) -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    for column in _ID_COLUMNS.get(bronze_table, []):
        values = [
            str(row[0])
            for row in db.fetch_all(
                f"""
                SELECT DISTINCT {column}::text
                FROM {bronze_table}
                WHERE source_account = %s
                  AND {column} IS NOT NULL
                  AND NULLIF(BTRIM({column}::text), '') IS NOT NULL
                ORDER BY {column}::text
                """,
                (account_name,),
            )
        ]
        shape_counts = {
            "uuid": 0,
            "numeric": 0,
            "text": 0,
        }
        for value in values:
            shape_counts[_classify_identifier_shape(value)] += 1
        profiles[column] = {
            "database_type": column_types.get(column),
            "distinct_count": len(values),
            "shape_counts": shape_counts,
            "sample_values": values[:5],
        }
    return profiles


def _get_column_types(db: DatabaseManager, bronze_table: str) -> dict[str, str]:
    schema_name, table_name = bronze_table.split(".", 1)
    rows = db.fetch_all_dict(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema_name, table_name),
    )
    return {str(row["column_name"]): str(row["data_type"]) for row in rows}


def _empty_id_profiles(bronze_table: str, column_types: dict[str, str]) -> dict[str, Any]:
    return {
        column: {
            "database_type": column_types.get(column),
            "distinct_count": 0,
            "shape_counts": {"uuid": 0, "numeric": 0, "text": 0},
            "sample_values": [],
        }
        for column in _ID_COLUMNS.get(bronze_table, [])
    }


def _audit_relationships(
    *,
    db: DatabaseManager,
    account_name: str,
    account_raw_summary: dict[str, Any],
) -> dict[str, Any]:
    relationships = {
        "athletes_to_positions": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_athletes",
            raw_table="raw.catapult_athletes",
            batch_ids=batch_ids_for_account("raw.catapult_athletes", account_raw_summary),
            child_key="position_id",
            parent_table="bronze.catapult_positions",
            parent_key="position_id",
            filter_sql="child.position_id IS NOT NULL",
        ),
        "periods_to_activities": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_periods",
            raw_table="raw.catapult_periods",
            batch_ids=batch_ids_for_account("raw.catapult_periods", account_raw_summary),
            child_key="activity_id",
            parent_table="bronze.catapult_activities",
            parent_key="activity_id",
            filter_sql="child.activity_id IS NOT NULL",
        ),
        "annotations_activity_scope": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_annotations",
            raw_table="raw.catapult_annotations",
            batch_ids=batch_ids_for_account("raw.catapult_annotations", account_raw_summary),
            child_key="activity_id",
            parent_table="bronze.catapult_activities",
            parent_key="activity_id",
            filter_sql="child.annotation_scope = 'activity' AND child.activity_id IS NOT NULL",
        ),
        "annotations_period_scope": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_annotations",
            raw_table="raw.catapult_annotations",
            batch_ids=batch_ids_for_account("raw.catapult_annotations", account_raw_summary),
            child_key="period_id",
            parent_table="bronze.catapult_periods",
            parent_key="period_id",
            filter_sql="child.annotation_scope = 'period' AND child.period_id IS NOT NULL",
        ),
        "annotations_athlete_scope": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_annotations",
            raw_table="raw.catapult_annotations",
            batch_ids=batch_ids_for_account("raw.catapult_annotations", account_raw_summary),
            child_key="athlete_id",
            parent_table="bronze.catapult_athletes",
            parent_key="athlete_id",
            filter_sql="child.annotation_scope = 'athlete' AND child.athlete_id IS NOT NULL",
        ),
        "stats_to_activities": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_stats",
            raw_table="raw.catapult_stats",
            batch_ids=batch_ids_for_account("raw.catapult_stats", account_raw_summary),
            child_key="activity_id",
            parent_table="bronze.catapult_activities",
            parent_key="activity_id",
            filter_sql="child.activity_id IS NOT NULL",
        ),
        "stats_to_athletes": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_stats",
            raw_table="raw.catapult_stats",
            batch_ids=batch_ids_for_account("raw.catapult_stats", account_raw_summary),
            child_key="athlete_id",
            parent_table="bronze.catapult_athletes",
            parent_key="athlete_id",
            filter_sql="child.athlete_id IS NOT NULL",
        ),
        "stats_to_periods": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_stats",
            raw_table="raw.catapult_stats",
            batch_ids=batch_ids_for_account("raw.catapult_stats", account_raw_summary),
            child_key="period_id",
            parent_table="bronze.catapult_periods",
            parent_key="period_id",
            filter_sql="child.period_id IS NOT NULL",
        ),
        "efforts_to_activities": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_efforts",
            raw_table="raw.catapult_efforts",
            batch_ids=batch_ids_for_account("raw.catapult_efforts", account_raw_summary),
            child_key="activity_id",
            parent_table="bronze.catapult_activities",
            parent_key="activity_id",
            filter_sql="child.activity_id IS NOT NULL",
        ),
        "efforts_to_athletes": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_efforts",
            raw_table="raw.catapult_efforts",
            batch_ids=batch_ids_for_account("raw.catapult_efforts", account_raw_summary),
            child_key="athlete_id",
            parent_table="bronze.catapult_athletes",
            parent_key="athlete_id",
            filter_sql="child.athlete_id IS NOT NULL",
        ),
        "events_to_activities": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_events",
            raw_table="raw.catapult_events",
            batch_ids=batch_ids_for_account("raw.catapult_events", account_raw_summary),
            child_key="activity_id",
            parent_table="bronze.catapult_activities",
            parent_key="activity_id",
            filter_sql="child.activity_id IS NOT NULL",
        ),
        "events_to_athletes": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_events",
            raw_table="raw.catapult_events",
            batch_ids=batch_ids_for_account("raw.catapult_events", account_raw_summary),
            child_key="athlete_id",
            parent_table="bronze.catapult_athletes",
            parent_key="athlete_id",
            filter_sql="child.athlete_id IS NOT NULL",
        ),
        "sensor_data_to_activities": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_sensor_data",
            raw_table="raw.catapult_sensor_data",
            batch_ids=batch_ids_for_account("raw.catapult_sensor_data", account_raw_summary),
            child_key="activity_id",
            parent_table="bronze.catapult_activities",
            parent_key="activity_id",
            filter_sql="child.activity_id IS NOT NULL",
        ),
        "sensor_data_to_athletes": _count_orphans(
            db=db,
            account_name=account_name,
            child_table="bronze.catapult_sensor_data",
            raw_table="raw.catapult_sensor_data",
            batch_ids=batch_ids_for_account("raw.catapult_sensor_data", account_raw_summary),
            child_key="athlete_id",
            parent_table="bronze.catapult_athletes",
            parent_key="athlete_id",
            filter_sql="child.athlete_id IS NOT NULL",
        ),
    }
    return relationships


def _count_orphans(
    *,
    db: DatabaseManager,
    account_name: str,
    child_table: str,
    raw_table: str,
    batch_ids: list[str],
    child_key: str,
    parent_table: str,
    parent_key: str,
    filter_sql: str,
) -> dict[str, Any]:
    if not batch_ids:
        return {"orphan_count": 0}

    row = db.fetch_one_dict(
        f"""
        SELECT COUNT(*) AS orphan_count
        FROM {child_table} child
        LEFT JOIN {parent_table} parent
          ON parent.source_account = child.source_account
         AND parent.{parent_key} = child.{child_key}
        WHERE child.source_account = %s
          AND child.raw_id IN ({_scoped_raw_id_subquery(raw_table)})
          AND {filter_sql}
          AND parent.{parent_key} IS NULL
        """,
        (account_name, account_name, batch_ids),
    ) or {}
    return {"orphan_count": int(row.get("orphan_count", 0) or 0)}


def _audit_coverage(
    *,
    db: DatabaseManager,
    account_name: str,
    account_raw_summary: dict[str, Any],
    expected_pairs: set[tuple[str, str]],
) -> dict[str, Any]:
    coverage_results: dict[str, Any] = {}
    for coverage_name, (bronze_table, raw_table) in _PAIR_TABLES.items():
        observed_pairs = _fetch_distinct_pairs(
            db=db,
            bronze_table=bronze_table,
            raw_table=raw_table,
            account_name=account_name,
            batch_ids=batch_ids_for_account(raw_table, account_raw_summary),
        )
        missing_pairs = sorted(expected_pairs - observed_pairs)
        extra_pairs = sorted(observed_pairs - expected_pairs)
        coverage_results[coverage_name] = {
            "expected_pair_count": len(expected_pairs),
            "observed_pair_count": len(observed_pairs),
            "missing_pairs": len(missing_pairs),
            "extra_pairs": len(extra_pairs),
            "missing_pair_samples": missing_pairs[:10],
            "extra_pair_samples": extra_pairs[:10],
        }
    return coverage_results


def _fetch_distinct_pairs(
    *,
    db: DatabaseManager,
    bronze_table: str,
    raw_table: str,
    account_name: str,
    batch_ids: list[str],
) -> set[tuple[str, str]]:
    if not batch_ids:
        return set()
    rows = db.fetch_all(
        f"""
        SELECT DISTINCT athlete_id::text, activity_id::text
        FROM {bronze_table}
        WHERE source_account = %s
          AND raw_id IN ({_scoped_raw_id_subquery(raw_table)})
          AND athlete_id IS NOT NULL
          AND activity_id IS NOT NULL
        """,
        (account_name, account_name, batch_ids),
    )
    return {(str(row[0]), str(row[1])) for row in rows}


def _collect_table_failures(
    *,
    summary: dict[str, Any],
    account_result: dict[str, Any],
    account_name: str,
    bronze_table: str,
    bronze_profile: dict[str, Any],
    replay_table_summary: dict[str, Any],
) -> None:
    skipped_rows = int(replay_table_summary.get("skipped_rows", 0) or 0)
    if skipped_rows > 0:
        message = f"{account_name} {bronze_table} replay skipped {skipped_rows} rows"
        account_result["failures"].append(message)
        summary["failures"].append(message)
        summary["passed"] = False

    for column, profile in bronze_profile.get("id_profiles", {}).items():
        database_type = str(profile.get("database_type") or "")
        if database_type and database_type not in {"text", "character varying"}:
            message = f"{account_name} {bronze_table}.{column} is stored as {database_type}, expected text-safe storage"
            account_result["failures"].append(message)
            summary["failures"].append(message)
            summary["passed"] = False

    row_count = int(bronze_profile.get("row_count", 0) or 0)
    if row_count == 0:
        return
    required_columns = _REQUIRED_COLUMNS.get(bronze_table, [])
    important_columns = dict(bronze_profile.get("important_columns", {}))
    for column in required_columns:
        blank_count = int(important_columns.get(column, {}).get("blank_count", 0) or 0)
        if blank_count > 0:
            message = f"{account_name} {bronze_table}.{column} has {blank_count} blank/null rows in the review slice"
            account_result["failures"].append(message)
            summary["failures"].append(message)
            summary["passed"] = False


def _classify_zero_row_table(raw_table: str, raw_count: int, bronze_count: int) -> dict[str, str] | None:
    if raw_count > 0 or bronze_count > 0:
        return None
    if raw_table == "raw.catapult_entity_tags":
        return {
            "status": "unresolved",
            "reason": "Provider references in this repo still do not confirm a readable entity_tags endpoint.",
        }
    return {
        "status": "expected_empty",
        "reason": "The 5-day review slice did not produce rows for this table.",
    }


def _classify_identifier_shape(value: str) -> str:
    if _UUID_RE.match(value):
        return "uuid"
    if _NUMERIC_RE.match(value):
        return "numeric"
    return "text"


def _normalize_identifier(value: Any) -> str | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return normalized or None


def _scoped_raw_id_subquery(raw_table: str) -> str:
    return f"SELECT raw_id FROM {raw_table} WHERE source_account = %s AND batch_id::text = ANY(%s)"


def _compact_activity_pair_output(raw_summary: dict[str, Any]) -> None:
    for account_summary in dict(raw_summary.get("accounts", {})).values():
        for summary_key in ("activity_athlete_enumeration", "activity_devices"):
            enumeration = dict(account_summary.get(summary_key, {}))
            pairs = enumeration.pop("pairs", None)
            if pairs is not None:
                enumeration["pair_count"] = len(pairs)
                account_summary[summary_key] = enumeration


def _summary_pairs(account_raw_summary: dict[str, Any], summary_key: str) -> set[tuple[str, str]]:
    return {
        (str(athlete_id), str(activity_id))
        for athlete_id, activity_id in account_raw_summary.get(summary_key, {}).get("pairs", [])
    }


def _expected_micro_pairs(account_raw_summary: dict[str, Any]) -> set[tuple[str, str]]:
    device_pairs = _summary_pairs(account_raw_summary, "activity_devices")
    if device_pairs:
        return device_pairs
    return _summary_pairs(account_raw_summary, "activity_athlete_enumeration")
