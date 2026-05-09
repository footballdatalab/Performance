"""
VALD stage-based pipeline orchestration.
"""

from __future__ import annotations

import argparse
import py_compile
import subprocess
import time
import traceback
import zlib
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Generator

from ingestion.bootstrap import (
    bootstrap_database as platform_bootstrap_database,
)
from ingestion.bootstrap import (
    main_bootstrap_database as platform_main_bootstrap_database,
)
from ingestion.catapult.catalog import (
    BRONZE_TABLES as CATAPULT_BRONZE_TABLES,
)
from ingestion.catapult.catalog import (
    PARTITIONED_BRONZE_TABLES as CATAPULT_PARTITIONED_BRONZE_TABLES,
)
from ingestion.catapult.catalog import (
    RAW_TABLES as CATAPULT_RAW_TABLES,
)
from ingestion.catapult.catalog import (
    UNSUPPORTED_CATAPULT_TABLES,
)
from ingestion.common.batch import BatchManager
from ingestion.common.config import get_db_config, get_env, load_provider_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.common.timing import pipeline_run, track_stage
from ingestion.common.watermark import WatermarkManager
from ingestion.vald.catalog import (
    ACTIVE_BRONZE_TABLES,
    ACTIVE_GOLD_TABLES,
    ACTIVE_SILVER_TABLES,
    ALL_MODULES,
    INTRADAY_DEFERRED_RAW_TABLES,
    MODULE_BRONZE_TABLES,
    MODULE_RAW_TABLES,
    OBSOLETE_VALD_TABLES,
    REFERENCE_BRONZE_TABLES,
    REFERENCE_RAW_TABLES,
)
from ingestion.vald.client import ValdClient
from ingestion.vald.cutoff import VALD_CUTOFF_UTC
from ingestion.vald.day_window import resolve_lisbon_day_window_utc
from ingestion.vald.endpoints.tenants import TenantsEndpoint
from ingestion.vald.extractors.dynamo_extractor import DynaMoExtractor
from ingestion.vald.extractors.forcedecks_extractor import ForceDecksExtractor
from ingestion.vald.extractors.forceframe_extractor import ForceFrameExtractor
from ingestion.vald.extractors.nordbord_extractor import NordBordExtractor
from ingestion.vald.extractors.reference_extractor import ValdReferenceExtractor
from ingestion.vald.extractors.smartspeed_extractor import SmartSpeedExtractor
from ingestion.vald.gold_etl import (
    DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
    GOLD_COLUMNS_BY_FAMILY,
    GOLD_TABLES,
    REFERENCE_METRIC_COVERAGE_COLUMNS,
    run_gold_etl,
)
from ingestion.vald.loaders.raw_loader import ValdRawLoader
from ingestion.vald.raw_replay import replay_raw_to_bronze
from ingestion.vald.silver_etl import (
    ASSESSMENT_COLUMNS,
    MEMBERSHIP_COLUMNS,
    PROFILE_COLUMNS,
    SILVER_TABLES,
    rebuild_overlap_quality_flags,
    run_silver_etl,
)

logger = get_logger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SQL_ADMIN_ROOT = _PROJECT_ROOT / "sql" / "admin"
_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_VALD_REPLAY_CURSOR_TABLE = "raw.vald_replay_cursor"
_DEFAULT_TENANT_WORKERS = 4


def _resolve_tenant_workers() -> int:
    """Return the bounded worker count used for parallel tenant extraction."""
    raw_value = os.environ.get("VALD_TENANT_WORKERS")
    if raw_value in (None, ""):
        return _DEFAULT_TENANT_WORKERS
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return _DEFAULT_TENANT_WORKERS
    return max(1, parsed)

_VALIDATION_PYTEST_TARGETS = [
    "tests/unit/test_bootstrap_database.py",
    "tests/unit/vald/test_cutoff.py",
    "tests/unit/catapult/test_bootstrap.py",
    "tests/unit/catapult/test_bronze_loader.py",
    "tests/unit/catapult/test_client.py",
    "tests/unit/catapult/test_pipeline.py",
    "tests/unit/catapult/test_raw_loader.py",
    "tests/unit/catapult/test_raw_replay.py",
    "tests/unit/catapult/test_schema.py",
    "tests/unit/vald/test_reference_extractor.py",
    "tests/unit/vald/test_raw_loader.py",
    "tests/unit/vald/test_bronze_loader.py",
    "tests/unit/vald/test_day_window.py",
    "tests/unit/vald/test_gold_etl.py",
    "tests/unit/vald/test_pipeline.py",
    "tests/unit/vald/test_raw_replay.py",
    "tests/unit/vald/test_silver_etl.py",
    "tests/unit/vald/test_vald_airflow_dag.py",
    "tests/unit/test_clean_catapult_tables.py",
    "tests/unit/test_clean_vald_tables.py",
]
_VALIDATION_COMPILE_TARGETS = [
    "script/bootstrap_database.py",
    "script/run_catapult_ingestion.py",
    "script/run_catapult_extract_raw.py",
    "script/run_catapult_raw_to_bronze.py",
    "script/run_vald_ingestion.py",
    "script/run_vald_reset_rebuild.py",
    "script/run_vald_extract_raw.py",
    "script/run_vald_raw_to_bronze.py",
    "script/run_vald_bronze_to_silver.py",
    "script/run_vald_silver_to_gold.py",
    "script/run_vald_resume_pipeline.py",
    "script/validate_vald_pipeline.py",
    "ingestion/bootstrap.py",
    "ingestion/catapult/catalog.py",
    "ingestion/catapult/pipeline.py",
    "ingestion/catapult/client.py",
    "ingestion/catapult/bootstrap.py",
    "ingestion/catapult/loaders/raw_loader.py",
    "ingestion/catapult/loaders/bronze_loader.py",
    "ingestion/catapult/raw_replay.py",
    "script/clean_database.py",
    "script/clean_vald_tables.py",
    "script/clean_catapult_tables.py",
    "airflow/dags/vald_pipeline.py",
]
_REQUIRED_VALD_COLUMNS = [
    ("raw.vald_dynamo_tests", "page_number"),
    ("raw.vald_smartspeed_test_summaries", "page_number"),
    ("bronze.vald_forcedecks_tests", "notes"),
    ("bronze.vald_forcedecks_tests", "parameter"),
    ("bronze.vald_profiles", "external_id"),
    ("silver.vald_reference_metric_coverage", "source_table"),
    ("silver.vald_reference_metric_coverage", "reference_metric_name"),
    ("silver.vald_reference_metric_coverage", "coverage_status"),
    ("gold.vald_forceframe", "side"),
    ("gold.vald_nordics", "side"),
    ("gold.vald_speed", "rep_number"),
]
_FORBIDDEN_VALD_COLUMNS = [
    ("raw.vald_profiles", "page_number"),
    ("raw.vald_forcedecks_tests", "page_number"),
    ("raw.vald_forcedecks_trials", "page_number"),
    ("raw.vald_forcedecks_result_definitions", "page_number"),
    ("raw.vald_forceframe_tests", "page_number"),
    ("raw.vald_forceframe_test_metrics", "page_number"),
    ("raw.vald_forceframe_force_traces", "page_number"),
    ("raw.vald_nordbord_tests", "page_number"),
    ("raw.vald_nordbord_test_metrics", "page_number"),
    ("raw.vald_nordbord_ecc_exercises", "page_number"),
    ("raw.vald_nordbord_ecc_repetitions", "page_number"),
    ("raw.vald_smartspeed_test_details", "page_number"),
    ("raw.vald_dynamo_test_details", "page_number"),
    ("raw.vald_dynamo_traces", "page_number"),
    ("bronze.vald_dynamo_repetitions", "force_newtons"),
    ("bronze.vald_dynamo_tests", "modified_date_utc"),
    ("bronze.vald_profiles", "being_merged_with"),
    ("bronze.vald_profiles", "date_of_birth"),
    ("bronze.vald_profiles", "email"),
    ("bronze.vald_profiles", "merge_expiry"),
    ("bronze.vald_profiles", "sex"),
    ("bronze.vald_profiles", "sync_id"),
    ("bronze.vald_smartspeed_test_summaries", "additional_options"),
    ("bronze.vald_smartspeed_test_summaries", "running_summary"),
    ("bronze.vald_smartspeed_test_summaries", "jumping_summary"),
    ("silver.vald_athlete_profile", "provider_birth_date"),
    ("silver.vald_athlete_profile", "provider_email"),
    ("silver.vald_athlete_profile", "provider_external_id"),
    ("silver.vald_athlete_profile", "provider_sex"),
    ("silver.vald_athlete_profile", "provider_sync_id"),
    ("silver.vald_athlete_profile", "raw_payload_hash"),
    ("gold.vald_forceframe", "rep_number"),
    ("gold.vald_nordics", "rep_number"),
    ("gold.vald_speed", "side"),
]
_SUPPORTED_NON_VALD_GOLD_TABLES = [
    "gold.focus_upload_sessions",
    "gold.focus_uploaded_data",
]
_PIPELINE_STAGE_ORDER = [
    "raw",
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
]
_PIPELINE_STAGE_ALIASES = {
    "bronze": "raw_to_bronze",
    "silver": "bronze_to_silver",
    "gold": "silver_to_gold",
}
_FULL_REBUILD_STAGE_SCHEMA = "etl_staging"
_VALD_LIVE_WRITE_LOCK_NAMESPACE = zlib.crc32(b"performance_etl") & 0x7FFFFFFF
_VALD_LIVE_WRITE_LOCK_RESOURCE = zlib.crc32(b"vald_live_write") & 0x7FFFFFFF
_NO_INTRADAY_CHANGES_SKIP_REASON = (
    "No impacted intraday VALD tests or reference changes detected."
)
_INTRADAY_INCREMENTAL_SCOPE_TABLES = {
    "raw.vald_forcedecks_tests": ("forcedecks", "bronze.vald_forcedecks_tests"),
    "raw.vald_forcedecks_trials": ("forcedecks", "bronze.vald_forcedecks_trials"),
    "raw.vald_forceframe_tests": ("forceframe", "bronze.vald_forceframe_tests"),
    "raw.vald_forceframe_test_metrics": ("forceframe", "bronze.vald_forceframe_test_metrics"),
    "raw.vald_nordbord_tests": ("nordics", "bronze.vald_nordbord_tests"),
    "raw.vald_nordbord_test_metrics": ("nordics", "bronze.vald_nordbord_test_metrics"),
    "raw.vald_smartspeed_test_summaries": ("speed", "bronze.vald_smartspeed_test_summaries"),
    "raw.vald_smartspeed_test_details": ("speed", "bronze.vald_smartspeed_test_details"),
    "raw.vald_dynamo_tests": ("dynamo", "bronze.vald_dynamo_tests"),
    "raw.vald_dynamo_test_details": ("dynamo", "bronze.vald_dynamo_repetitions"),
}


@dataclass(frozen=True)
class _StageTableSpec:
    live_table: str
    stage_table: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class _BronzePublishSpec:
    live_table: str
    stage_table: str
    mode: str
    key_columns: tuple[str, ...]


class ValdPipelineBusyError(RuntimeError):
    """Raised when another VALD live silver/gold writer already holds the lock."""


def resolve_modules(
    requested: str,
    provider_config: dict[str, Any],
) -> list[str]:
    """Return the list of enabled module names to run."""
    yaml_enabled = {
        module_cfg["name"]
        for module_cfg in provider_config.get("modules", [])
        if module_cfg.get("enabled", True)
    }

    if requested.strip().lower() == "all":
        return [module for module in ALL_MODULES if module in yaml_enabled]

    requested_list = [
        module.strip().lower()
        for module in requested.split(",")
        if module.strip()
    ]
    invalid = [module for module in requested_list if module not in ALL_MODULES]
    if invalid:
        logger.warning("Unknown module(s) ignored: %s", invalid)
    return [module for module in requested_list if module in yaml_enabled]


def discover_tenant_ids(vald_client: ValdClient) -> list[str]:
    """Fetch tenant ids from the VALD tenants API."""
    env_tenant = get_env("VALD_TENANT_ID", "")
    if env_tenant:
        logger.info("Using tenant id from VALD_TENANT_ID: %s", env_tenant)
        return [env_tenant]

    endpoint = TenantsEndpoint(vald_client.tenants_client)
    tenants = endpoint.get_tenants()
    tenant_ids = [
        tenant.get("id") or tenant.get("tenantId")
        for tenant in tenants
        if tenant.get("id") or tenant.get("tenantId")
    ]
    logger.info("Discovered %d tenant(s): %s", len(tenant_ids), tenant_ids)
    return tenant_ids


def bootstrap_database() -> dict[str, Any]:
    """Create or reconcile the warehouse schema from the repository DDL."""
    return platform_bootstrap_database()


def run_extract_raw(
    modules: str = "all",
    full_refresh: bool = False,
    include_reference: bool = True,
    intraday_current_day_only: bool = False,
) -> dict[str, Any]:
    """Run the VALD raw extraction stage."""
    provider_config = load_provider_config("vald")
    selected_modules = resolve_modules(modules, provider_config)
    if full_refresh and intraday_current_day_only:
        logger.info(
            "Ignoring intraday_current_day_only for full-refresh VALD extraction."
        )
        intraday_current_day_only = False
    if not selected_modules and not include_reference:
        return {
            "reference": {},
            "modules": {},
            "tenant_ids": [],
            "total_extracted": 0,
            "total_loaded": 0,
            "has_new_data": False,
            "errors": ["No modules enabled for raw extraction."],
        }

    db = DatabaseManager(get_db_config())
    watermark_mgr = WatermarkManager(db)
    batch_manager = BatchManager(db)
    summary: dict[str, Any] = {
        "reference": {},
        "modules": {},
        "tenant_ids": [],
        "total_extracted": 0,
        "total_loaded": 0,
        "has_new_data": False,
        "errors": [],
    }
    created_batch_ids: list[str] = []

    # Phase 8.8.A: extraction (~32m baseline) is the lightest stage and
    # was intentionally not wrapped in track_stage — its body is too
    # branchy to wrap cleanly without 100+ lines of re-indentation, and
    # raw_to_bronze + bronze_to_silver + silver_to_gold (which ARE
    # wrapped) cover ~99% of the wall-clock budget. If extraction time
    # ever becomes interesting we can extract its body to a helper and
    # wrap that.
    vald_client: ValdClient | None = None
    try:
        vald_client = ValdClient(provider_config)
        tenant_ids = discover_tenant_ids(vald_client)
        if not tenant_ids:
            raise ValueError("No tenant IDs available for raw extraction.")
        summary["tenant_ids"] = tenant_ids

        if full_refresh:
            _reset_module_watermarks(
                watermark_mgr=watermark_mgr,
                modules=selected_modules,
                tenant_ids=tenant_ids,
            )

        if include_reference:
            ref_batch_id = batch_manager.start_batch(
                provider=_PROVIDER,
                source_account=_SOURCE_ACCOUNT,
                api_name="reference_raw",
            )
            created_batch_ids.append(ref_batch_id)
            try:
                raw_loader = ValdRawLoader(db, ref_batch_id, _SOURCE_ACCOUNT)
                ref_extractor = ValdReferenceExtractor(
                    vald_client=vald_client,
                    raw_loader=raw_loader,
                    batch_manager=batch_manager,
                )
                ref_summary = ref_extractor.extract_all(tenant_ids)
                summary["reference"] = ref_summary
                ref_seen = int(ref_summary.get("profiles_seen", 0))
                ref_written = int(ref_summary.get("snapshots_written", 0))
                summary["total_extracted"] += ref_seen
                summary["total_loaded"] += ref_written
                batch_manager.complete_batch(ref_batch_id, ref_seen, ref_written)
            except Exception as exc:
                batch_manager.fail_batch(ref_batch_id, str(exc)[:1000])
                msg = f"Reference raw extraction failed: {exc}"
                logger.error(msg)
                logger.error(traceback.format_exc())
                summary["errors"].append(msg)

        tenant_workers = max(1, min(_resolve_tenant_workers(), len(tenant_ids)))
        for module_name in selected_modules:
            module_results: dict[str, Any] = {
                "tenants": {},
                "total_extracted": 0,
                "total_loaded": 0,
                "errors": [],
            }

            def _extract_one_tenant(
                tenant_id: str,
                module_name: str = module_name,
            ) -> tuple[str, str, dict[str, Any] | None, str | None]:
                """Extract one tenant; return (tenant, batch_id, result, error)."""
                batch_id = batch_manager.start_batch(
                    provider=_PROVIDER,
                    source_account=_SOURCE_ACCOUNT,
                    api_name=f"{module_name}_raw",
                )
                try:
                    raw_loader = ValdRawLoader(db, batch_id, _SOURCE_ACCOUNT)
                    extractor = _build_module_extractor(
                        module_name=module_name,
                        vald_client=vald_client,
                        raw_loader=raw_loader,
                        watermark_mgr=watermark_mgr,
                        batch_manager=batch_manager,
                        intraday_current_day_only=intraday_current_day_only,
                    )
                    result = extractor.extract(tenant_id)
                    return tenant_id, batch_id, result, None
                except Exception as exc:
                    return tenant_id, batch_id, None, str(exc)

            module_workers = max(1, min(tenant_workers, len(tenant_ids)))
            logger.info(
                "VALD raw extraction: module=%s tenants=%d workers=%d",
                module_name,
                len(tenant_ids),
                module_workers,
            )
            with ThreadPoolExecutor(
                max_workers=module_workers,
                thread_name_prefix=f"vald-{module_name}-tenant",
            ) as pool:
                futures = [
                    pool.submit(_extract_one_tenant, tenant_id)
                    for tenant_id in tenant_ids
                ]
                for future in as_completed(futures):
                    tenant_id, batch_id, result, error = future.result()
                    created_batch_ids.append(batch_id)
                    if error is not None or result is None:
                        try:
                            batch_manager.fail_batch(
                                batch_id, (error or "unknown")[:1000]
                            )
                        except Exception:
                            logger.exception(
                                "Failed to mark batch %s as failed", batch_id
                            )
                        msg = (
                            f"Raw extraction failed for module '{module_name}' "
                            f"tenant '{tenant_id}': {error}"
                        )
                        logger.error(msg)
                        module_results["errors"].append(msg)
                        continue
                    module_results["tenants"][tenant_id] = result
                    module_results["total_extracted"] += result["records_extracted"]
                    module_results["total_loaded"] += result["records_loaded"]
                    batch_manager.complete_batch(
                        batch_id=batch_id,
                        records_extracted=result["records_extracted"],
                        records_loaded=result["records_loaded"],
                    )
            summary["modules"][module_name] = module_results
            summary["total_extracted"] += module_results["total_extracted"]
            summary["total_loaded"] += module_results["total_loaded"]

        summary["has_new_data"] = summary["total_loaded"] > 0
        _validate_batch_integrity(db, created_batch_ids)
        return summary
    finally:
        if vald_client is not None:
            try:
                vald_client.close()
            except Exception:
                pass
        db.close()


def run_raw_to_bronze_stage(
    modules: str = "all",
    include_reference: bool = True,
    *,
    table_overrides: dict[str, str] | None = None,
    full_replay: bool = False,
    replay_cursor_table: str | None = _VALD_REPLAY_CURSOR_TABLE,
    include_only_source_tables: list[str] | tuple[str, ...] | None = None,
    exclude_source_tables: list[str] | tuple[str, ...] | None = None,
    ingested_at_start: datetime | None = None,
    ingested_at_end: datetime | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Replay raw VALD payloads into bronze."""
    provider_config = load_provider_config("vald")
    selected_modules = resolve_modules(modules, provider_config)
    db = DatabaseManager(get_db_config())
    try:
        required_tables = ["raw.ingestion_batch_log", "raw.sync_watermark"]
        if not full_replay and replay_cursor_table is not None:
            required_tables.append(replay_cursor_table)
        _require_tables(db, required_tables)
        # Phase 8.8.A: instrument the bronze replay so we can attribute
        # the ~4h24m wall-clock to specific tables / chunk sizes.
        with track_stage(
            "vald", "raw_to_bronze", db=db,
            extra={
                "modules": list(selected_modules),
                "full_replay": bool(full_replay),
                "include_reference": bool(include_reference),
            },
        ) as _metrics:
            summary = replay_raw_to_bronze(
                db=db,
                modules=selected_modules,
                include_reference=include_reference,
                table_overrides=table_overrides,
                replay_cursor_table=replay_cursor_table,
                full_replay=full_replay,
                include_only_source_tables=include_only_source_tables,
                exclude_source_tables=exclude_source_tables,
                ingested_at_start=ingested_at_start,
                ingested_at_end=ingested_at_end,
                deadline=deadline,
            )
            _metrics["rows_read"] = summary.get("processed_raw_rows")
            _metrics["rows_written"] = summary.get("loaded_rows")
        if summary["processed_raw_rows"] < 0 or summary["loaded_rows"] < 0:
            raise ValueError("Raw->bronze replay reported negative counts.")
        return summary
    finally:
        db.close()


def run_intraday_raw_to_bronze_stage(
    modules: str = "all",
    include_reference: bool = True,
) -> dict[str, Any]:
    """Replay intraday VALD raw payloads into bronze, deferring heavy tables."""
    summary = run_raw_to_bronze_stage(
        modules=modules,
        include_reference=include_reference,
        exclude_source_tables=tuple(INTRADAY_DEFERRED_RAW_TABLES),
    )
    summary["deferred_source_tables"] = list(INTRADAY_DEFERRED_RAW_TABLES)
    db = DatabaseManager(get_db_config())
    try:
        summary["incremental_scope"] = _build_intraday_incremental_scope(db, summary)
    finally:
        db.close()
    return summary


def run_intraday_deferred_raw_to_bronze_stage(
    modules: str = "all",
) -> dict[str, Any]:
    """Replay deferred heavy VALD raw tables after the intraday critical path."""
    return _run_intraday_deferred_raw_to_bronze_stage_unlocked(modules)


_DEFERRED_BRONZE_MAX_RUNTIME_SECONDS = 20 * 60  # 20 minutes per intraday slot


def _run_intraday_deferred_raw_to_bronze_stage_unlocked(modules: str) -> dict[str, Any]:
    """Replay deferred heavy VALD raw tables without acquiring the shared write lock.

    Runs for at most ``_DEFERRED_BRONZE_MAX_RUNTIME_SECONDS`` seconds per call.
    The replay cursor is saved after every commit batch so the next call resumes
    from the last checkpoint rather than re-processing already-committed rows.
    """
    deadline = time.monotonic() + _DEFERRED_BRONZE_MAX_RUNTIME_SECONDS
    summary = run_raw_to_bronze_stage(
        modules=modules,
        include_reference=False,
        include_only_source_tables=tuple(INTRADAY_DEFERRED_RAW_TABLES),
        deadline=deadline,
    )
    summary["deferred_mode"] = True
    summary["source_tables"] = list(INTRADAY_DEFERRED_RAW_TABLES)
    return summary


def _build_historical_day_incremental_scope(
    db: DatabaseManager,
    ingested_at_start: datetime,
    ingested_at_end: datetime,
) -> dict[str, Any]:
    """Return impacted VALD test_ids for all raw rows ingested in the given UTC window.

    Looks up bronze test_ids whose raw_id matches rows in the source raw table
    with ``ingested_at`` falling inside ``[ingested_at_start, ingested_at_end)``.
    """
    test_ids_by_family: dict[str, set[str]] = defaultdict(set)
    source_tables_summary: dict[str, dict[str, Any]] = {}

    for source_table, mapping in _INTRADAY_INCREMENTAL_SCOPE_TABLES.items():
        family, bronze_table = mapping

        rows = db.fetch_all_dict(
            f"""
            SELECT DISTINCT b.test_id::text AS test_id
            FROM {bronze_table} b
            WHERE b.raw_id IN (
                SELECT raw_id
                FROM {source_table}
                WHERE ingested_at >= %s
                  AND ingested_at < %s
            )
              AND b.test_id IS NOT NULL
            ORDER BY b.test_id::text
            """,
            (ingested_at_start, ingested_at_end),
        )
        test_ids = sorted({str(row["test_id"]) for row in rows if row.get("test_id")})
        test_ids_by_family[family].update(test_ids)
        source_tables_summary[source_table] = {
            "family": family,
            "bronze_table": bronze_table,
            "test_ids": test_ids,
            "test_count": len(test_ids),
        }

    normalized_by_family = {
        family: sorted(test_ids)
        for family, test_ids in test_ids_by_family.items()
    }
    total_test_ids = sum(len(test_ids) for test_ids in normalized_by_family.values())
    return {
        "by_family": normalized_by_family,
        "counts_by_family": {
            family: len(test_ids)
            for family, test_ids in normalized_by_family.items()
        },
        "source_tables": source_tables_summary,
        "total_test_ids": total_test_ids,
        "has_impacted_tests": total_test_ids > 0,
    }


def run_historical_day_raw_to_bronze(replay_date_str: str) -> dict[str, Any]:
    """Replay all raw VALD rows ingested on the given Lisbon calendar day into live bronze.

    Args:
        replay_date_str: ISO date string in ``YYYY-MM-DD`` format (Lisbon calendar day).

    Returns:
        Replay summary including ``incremental_scope`` with affected test_ids, suitable
        for passing directly to :func:`run_intraday_bronze_to_silver_stage`.
    """
    from ingestion.vald.day_window import resolve_lisbon_day_window_from_date

    replay_date = date.fromisoformat(replay_date_str)
    window = resolve_lisbon_day_window_from_date(replay_date)

    summary = run_raw_to_bronze_stage(
        modules="all",
        include_reference=True,
        full_replay=True,
        replay_cursor_table=None,
        ingested_at_start=window.day_start_utc,
        ingested_at_end=window.day_end_utc,
    )

    db = DatabaseManager(get_db_config())
    try:
        summary["replay_date"] = replay_date_str
        summary["day_window"] = window.as_summary()
        summary["incremental_scope"] = _build_historical_day_incremental_scope(
            db,
            window.day_start_utc,
            window.day_end_utc,
        )
    finally:
        db.close()

    logger.info(
        "Historical day reprocess complete for %s: %s",
        replay_date_str,
        summary,
    )
    return summary


def _build_intraday_incremental_scope(
    db: DatabaseManager,
    replay_summary: dict[str, Any],
) -> dict[str, Any]:
    """Return the impacted VALD test_ids inferred from replayed bronze raw_id ranges."""
    test_ids_by_family: dict[str, set[str]] = defaultdict(set)
    source_tables_summary: dict[str, dict[str, Any]] = {}

    for source_table, table_summary in replay_summary.get("tables", {}).items():
        mapping = _INTRADAY_INCREMENTAL_SCOPE_TABLES.get(source_table)
        if mapping is None:
            continue

        family, bronze_table = mapping
        processed_raw_rows = int(table_summary.get("processed_raw_rows", 0) or 0)
        start_raw_id = int(table_summary.get("start_raw_id", 0) or 0)
        last_raw_id = int(table_summary.get("last_raw_id", start_raw_id) or start_raw_id)

        if processed_raw_rows <= 0 or last_raw_id <= start_raw_id:
            source_tables_summary[source_table] = {
                "family": family,
                "bronze_table": bronze_table,
                "test_ids": [],
                "test_count": 0,
            }
            continue

        rows = db.fetch_all_dict(
            f"""
            SELECT DISTINCT test_id::text AS test_id
            FROM {bronze_table}
            WHERE raw_id > %s
              AND raw_id <= %s
              AND test_id IS NOT NULL
            ORDER BY test_id::text
            """,
            (start_raw_id, last_raw_id),
        )
        test_ids = sorted({str(row["test_id"]) for row in rows if row.get("test_id")})
        test_ids_by_family[family].update(test_ids)
        source_tables_summary[source_table] = {
            "family": family,
            "bronze_table": bronze_table,
            "test_ids": test_ids,
            "test_count": len(test_ids),
        }

    normalized_by_family = {
        family: sorted(test_ids)
        for family, test_ids in test_ids_by_family.items()
    }
    total_test_ids = sum(len(test_ids) for test_ids in normalized_by_family.values())
    return {
        "by_family": normalized_by_family,
        "counts_by_family": {
            family: len(test_ids)
            for family, test_ids in normalized_by_family.items()
        },
        "source_tables": source_tables_summary,
        "total_test_ids": total_test_ids,
        "has_impacted_tests": total_test_ids > 0,
    }


def _normalize_incremental_scope(
    incremental_scope: dict[str, Any] | None,
) -> dict[str, list[str]] | None:
    """Return the normalized per-family impacted test scope."""
    if incremental_scope is None:
        return None
    by_family = incremental_scope.get("by_family") or {}
    return {
        str(family): [str(test_id) for test_id in test_ids]
        for family, test_ids in by_family.items()
    }


def _incremental_scope_has_impacted_tests(
    incremental_scope: dict[str, Any] | None,
) -> bool:
    """Return True when an incremental scope contains any affected test ids."""
    if incremental_scope is None:
        return False
    if "has_impacted_tests" in incremental_scope:
        return bool(incremental_scope.get("has_impacted_tests"))
    by_family = incremental_scope.get("by_family") or {}
    return any(test_ids for test_ids in by_family.values())


def run_bronze_to_silver_stage() -> dict[str, Any]:
    """Run the bronze->silver VALD stage."""
    return _run_with_live_write_lock(
        "bronze_to_silver",
        _run_bronze_to_silver_stage_unlocked,
    )


def _run_bronze_to_silver_stage_unlocked() -> dict[str, Any]:
    """Run the bronze->silver VALD stage without acquiring the shared write lock."""
    db = DatabaseManager(get_db_config())
    try:
        _require_tables(db, REFERENCE_BRONZE_TABLES)
        # Phase 8.8.A: bronze->silver is the second-heaviest stage
        # (~4h09m in the pre-cleanup baseline). Track it so we can drill
        # into per-family timings via run_silver_etl's child stages.
        with track_stage("vald", "bronze_to_silver", db=db) as _metrics:
            summary = run_silver_etl(db)
            _metrics["rows_written"] = (
                summary.get("assessment_metrics", {}).get("total_inserted")
            )
        metrics_total = summary.get("assessment_metrics", {}).get("total_inserted", 0)
        if metrics_total < 0:
            raise ValueError("Silver ETL reported a negative inserted metric count.")
        return summary
    finally:
        db.close()


def run_intraday_bronze_to_silver_stage(
    reference_time: datetime | None = None,
    *,
    incremental_scope: dict[str, Any] | None = None,
    refresh_reference_entities: bool = True,
) -> dict[str, Any]:
    """Run the bronze->silver VALD stage for the current Europe/Lisbon day only."""
    window = resolve_lisbon_day_window_utc(reference_time)
    if (
        incremental_scope is not None
        and not _incremental_scope_has_impacted_tests(incremental_scope)
        and not refresh_reference_entities
    ):
        return {
            "assessment_metrics": {"total_inserted": 0},
            "day_window": window.as_summary(),
            "incremental_scope": incremental_scope,
            "reference_entities_refreshed": False,
            "skipped": True,
            "skip_reason": _NO_INTRADAY_CHANGES_SKIP_REASON,
        }
    return _run_with_live_write_lock(
        "intraday_bronze_to_silver",
        lambda: _run_intraday_bronze_to_silver_stage_unlocked(
            window,
            incremental_scope,
            refresh_reference_entities=refresh_reference_entities,
        ),
        skip_when_busy=True,
        skipped_summary={
            "assessment_metrics": {"total_inserted": 0},
            "day_window": window.as_summary(),
        },
    )


def _run_intraday_bronze_to_silver_stage_unlocked(
    window: Any,
    incremental_scope: dict[str, Any] | None = None,
    *,
    refresh_reference_entities: bool = True,
) -> dict[str, Any]:
    """Run the intraday bronze->silver stage without acquiring the shared write lock."""
    db = DatabaseManager(get_db_config())
    try:
        _require_tables(db, REFERENCE_BRONZE_TABLES)
        scoped_test_ids_by_family = _normalize_incremental_scope(incremental_scope)
        use_incremental_scope = scoped_test_ids_by_family is not None
        summary = run_silver_etl(
            db,
            day_start_utc=None if use_incremental_scope else window.day_start_utc,
            day_end_utc=None if use_incremental_scope else window.day_end_utc,
            scoped_test_ids_by_family=scoped_test_ids_by_family,
            refresh_reference_entities=refresh_reference_entities,
        )
        summary["day_window"] = window.as_summary()
        if incremental_scope is not None:
            summary["incremental_scope"] = incremental_scope
        metrics_total = summary.get("assessment_metrics", {}).get("total_inserted", 0)
        if metrics_total < 0:
            raise ValueError("Silver ETL reported a negative inserted metric count.")
        return summary
    finally:
        db.close()


def run_silver_to_gold_stage() -> dict[str, Any]:
    """Run the silver->gold VALD stage."""
    return _run_with_live_write_lock(
        "silver_to_gold",
        _run_silver_to_gold_stage_unlocked,
    )


def _run_silver_to_gold_stage_unlocked() -> dict[str, Any]:
    """Run the silver->gold VALD stage without acquiring the shared write lock."""
    db = DatabaseManager(_get_stage_db_config(statement_timeout_env_key="POSTGRES_GOLD_STATEMENT_TIMEOUT_MS"))
    try:
        _require_tables(
            db,
            [
                "silver.vald_assessment_metric",
                DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
                *ACTIVE_GOLD_TABLES,
            ],
        )
        # Phase 8.8.A: Gold is currently fast (~7m baseline) but tracked
        # for completeness. If perf changes here we want to know.
        with track_stage("vald", "silver_to_gold", db=db) as _metrics:
            summary = run_gold_etl(db)
            _metrics["rows_written"] = summary.get("total_published_rows")
        if summary.get("total_excluded_outside_threshold_rows", 0) < 0:
            raise ValueError("Gold ETL reported a negative excluded row count.")
        return summary
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Phase 1 (2026-05-09) — VALD IQR outlier audit entry point
# ---------------------------------------------------------------------------

def run_vald_quality_audit(
    family: str | None = None,
    *,
    incremental: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run the per-family VALD IQR outlier audit.

    Phase 1 (2026-05-09): produces ``silver.data_quality_flag`` rows
    scoped per ``(team_group_id, test_type, metric_name)`` for the
    chosen assessment family (or all 5 families when ``family`` is
    ``None``).

    Each invocation records its lifecycle in
    ``silver.data_quality_audit_run`` so subsequent incremental runs
    pick up where the previous one stopped.

    Parameters
    ----------
    family : str, optional
        One of 'forcedecks', 'forceframe', 'nordics', 'speed', 'dynamo'.
        ``None`` audits all 5 in sequence.
    incremental : bool
        When True (default), audit only rows ``updated_at`` > last
        successful run for the same family. ``False`` re-audits
        everything (useful after a gold rebuild or when resetting
        baselines).
    limit : int, optional
        Per-family row cap for testing. Production runs leave this
        ``None``.

    Returns
    -------
    dict — summary across families, suitable for logging or surfacing
    in the Airflow task XCom payload.
    """
    import uuid

    from ingestion.common.quality import QualityEngine
    from ingestion.vald import quality_rules

    if family is not None and family not in quality_rules.ALL_RULE_SET_BUILDERS:
        raise ValueError(
            f"Unknown VALD family {family!r}. "
            f"Known: {sorted(quality_rules.ALL_RULE_SET_BUILDERS.keys())}"
        )

    families = (
        [family] if family else list(quality_rules.ALL_RULE_SET_BUILDERS.keys())
    )

    db = DatabaseManager(get_db_config())
    summary: dict[str, Any] = {
        "incremental": incremental,
        "families": {},
        "total_records_checked": 0,
        "total_flags": 0,
        "errors": [],
    }
    try:
        engine = QualityEngine(db)
        for fam in families:
            run_id = str(uuid.uuid4())
            started_at_sql = """
                INSERT INTO silver.data_quality_audit_run
                    (run_id, pipeline, family, incremental, started_at, status)
                VALUES (%s, 'vald', %s, %s, now(), 'running')
            """
            db.execute(started_at_sql, (run_id, fam, incremental))

            with track_stage(
                "vald", "quality_audit", sub_stage=fam, db=db,
                extra={"family": fam, "incremental": incremental},
            ) as metrics:
                try:
                    rule_set = quality_rules.build_rule_set(fam)
                    family_summary = engine.audit_long_form_table(
                        rule_set,
                        incremental=incremental,
                        limit=limit,
                    )
                    metrics["rows_read"] = family_summary.get("records_checked")
                    metrics["rows_written"] = family_summary.get("flags")
                    summary["families"][fam] = family_summary
                    summary["total_records_checked"] += family_summary.get(
                        "records_checked", 0
                    )
                    summary["total_flags"] += family_summary.get("flags", 0)

                    db.execute(
                        """
                        UPDATE silver.data_quality_audit_run
                           SET finished_at = now(),
                               status = 'success',
                               records_audited = %s,
                               flags_written = %s
                         WHERE run_id = %s
                        """,
                        (
                            family_summary.get("records_checked", 0),
                            family_summary.get("flags", 0),
                            run_id,
                        ),
                    )
                except Exception as exc:
                    error_message = repr(exc)[:1000]
                    summary["errors"].append(f"{fam}: {error_message}")
                    db.execute(
                        """
                        UPDATE silver.data_quality_audit_run
                           SET finished_at = now(),
                               status = 'failed',
                               error_message = %s
                         WHERE run_id = %s
                        """,
                        (error_message, run_id),
                    )
                    logger.exception("VALD quality audit failed for family=%s", fam)
                    # Continue with the next family — one failed audit
                    # shouldn't prevent the others from running.

        logger.info(
            "VALD quality audit complete: %d families, %d records checked, %d flags",
            len(families),
            summary["total_records_checked"],
            summary["total_flags"],
        )
        return summary
    finally:
        db.close()


def run_intraday_silver_to_gold_stage(
    reference_time: datetime | None = None,
    *,
    incremental_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the silver->gold VALD stage for the current Europe/Lisbon day only."""
    window = resolve_lisbon_day_window_utc(reference_time)
    if incremental_scope is not None and not _incremental_scope_has_impacted_tests(incremental_scope):
        return {
            "day_window": window.as_summary(),
            "coverage": {
                "rows_written": 0,
                "covered_count": 0,
                "unmapped_count": 0,
                "unmapped_test_names": [],
            },
            "total_rows": 0,
            "total_source_rows": 0,
            "total_excluded_above_threshold_rows": 0,
            "total_excluded_below_threshold_rows": 0,
            "total_excluded_outside_threshold_rows": 0,
            "incremental_scope": incremental_scope,
            "skipped": True,
            "skip_reason": _NO_INTRADAY_CHANGES_SKIP_REASON,
        }
    return _run_with_live_write_lock(
        "intraday_silver_to_gold",
        lambda: _run_intraday_silver_to_gold_stage_unlocked(window, incremental_scope),
        skip_when_busy=True,
        skipped_summary={
            "day_window": window.as_summary(),
            "coverage": {
                "rows_written": 0,
                "covered_count": 0,
                "unmapped_count": 0,
                "unmapped_test_names": [],
            },
            "total_rows": 0,
            "total_source_rows": 0,
            "total_excluded_above_threshold_rows": 0,
            "total_excluded_below_threshold_rows": 0,
            "total_excluded_outside_threshold_rows": 0,
        },
    )


def run_historical_day_bronze_to_silver_stage(
    *,
    incremental_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the bronze->silver stage for a historical day reprocess.

    Delegates to ``run_intraday_bronze_to_silver_stage`` but raises
    ``ValdPipelineBusyError`` instead of silently skipping when another
    pipeline holds the live write lock.  The caller (Airflow task) then
    fails visibly so the operator knows to re-trigger once the lock is free.
    """
    result = run_intraday_bronze_to_silver_stage(incremental_scope=incremental_scope)
    if result.get("skipped") and result.get("skip_reason") != _NO_INTRADAY_CHANGES_SKIP_REASON:
        raise ValdPipelineBusyError(
            "Historical silver stage skipped: another pipeline holds the live write lock. "
            "Re-trigger vald_historical_day_reprocess once the concurrent run completes."
        )
    return result


def run_historical_day_silver_to_gold_stage(
    *,
    incremental_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the silver->gold stage for a historical day reprocess.

    Delegates to ``run_intraday_silver_to_gold_stage`` but raises
    ``ValdPipelineBusyError`` instead of silently skipping when another
    pipeline holds the live write lock.
    """
    result = run_intraday_silver_to_gold_stage(incremental_scope=incremental_scope)
    if result.get("skipped") and result.get("skip_reason") != _NO_INTRADAY_CHANGES_SKIP_REASON:
        raise ValdPipelineBusyError(
            "Historical gold stage skipped: another pipeline holds the live write lock. "
            "Re-trigger vald_historical_day_reprocess once the concurrent run completes."
        )
    return result


def _run_intraday_silver_to_gold_stage_unlocked(
    window: Any,
    incremental_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the intraday silver->gold stage without acquiring the shared write lock."""
    db = DatabaseManager(_get_stage_db_config(statement_timeout_env_key="POSTGRES_GOLD_STATEMENT_TIMEOUT_MS"))
    try:
        _require_tables(
            db,
            [
                "silver.vald_assessment_metric",
                DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
                *ACTIVE_GOLD_TABLES,
            ],
        )
        scoped_test_ids_by_family = _normalize_incremental_scope(incremental_scope)
        use_incremental_scope = scoped_test_ids_by_family is not None
        summary = run_gold_etl(
            db,
            day_start_utc=None if use_incremental_scope else window.day_start_utc,
            day_end_utc=None if use_incremental_scope else window.day_end_utc,
            scoped_test_ids_by_family=scoped_test_ids_by_family,
        )
        summary["day_window"] = window.as_summary()
        if incremental_scope is not None:
            summary["incremental_scope"] = incremental_scope
        if summary.get("total_excluded_outside_threshold_rows", 0) < 0:
            raise ValueError("Gold ETL reported a negative excluded row count.")
        return summary
    finally:
        db.close()


def _build_stage_table_name(live_table: str) -> str:
    """Return the persistent staging-table name for a live warehouse table."""
    schema_name, table_name = live_table.split(".", 1)
    return f"{_FULL_REBUILD_STAGE_SCHEMA}.{schema_name}_{table_name}"


def _build_bronze_stage_specs() -> list[_BronzePublishSpec]:
    """Return stage-table specs for the bronze snapshot rebuild."""
    bronze_publish_modes: dict[str, tuple[str, tuple[str, ...]]] = {
        "bronze.vald_profiles": ("merge", ("vald_profile_id",)),
        "bronze.vald_profile_categories": ("replace_group", ("vald_profile_id",)),
        "bronze.vald_forcedecks_result_definitions": ("merge", ("result_id",)),
        "bronze.vald_forcedecks_tests": ("merge", ("test_id",)),
        "bronze.vald_forcedecks_trials": ("merge", ("trial_id",)),
        "bronze.vald_forcedecks_trial_results": ("replace_group", ("trial_id",)),
        "bronze.vald_forceframe_tests": ("merge", ("test_id",)),
        "bronze.vald_forceframe_test_metrics": ("merge", ("test_id",)),
        "bronze.vald_forceframe_force_traces": ("replace_group", ("test_id",)),
        "bronze.vald_nordbord_tests": ("merge", ("test_id",)),
        "bronze.vald_nordbord_test_metrics": ("merge", ("test_id",)),
        "bronze.vald_nordbord_ecc_exercises": ("merge", ("exercise_id",)),
        "bronze.vald_nordbord_ecc_repetitions": ("merge", ("repetition_id",)),
        "bronze.vald_smartspeed_test_summaries": ("merge", ("test_id",)),
        "bronze.vald_smartspeed_test_details": ("merge", ("test_id",)),
        "bronze.vald_smartspeed_rep_results": ("replace_group", ("test_id",)),
        "bronze.vald_dynamo_tests": ("merge", ("test_id",)),
        "bronze.vald_dynamo_rep_summaries": ("replace_group", ("test_id",)),
        "bronze.vald_dynamo_repetitions": ("replace_group", ("test_id",)),
        "bronze.vald_dynamo_traces": ("replace_group", ("test_id",)),
    }
    return [
        _BronzePublishSpec(
            live_table=live_table,
            stage_table=_build_stage_table_name(live_table),
            mode=bronze_publish_modes[live_table][0],
            key_columns=bronze_publish_modes[live_table][1],
        )
        for live_table in ACTIVE_BRONZE_TABLES
    ]


def _build_full_rebuild_stage_specs() -> tuple[list[_StageTableSpec], list[_StageTableSpec]]:
    """Return stage-table specs for the full silver and gold rebuild."""
    silver_specs = [
        _StageTableSpec(
            live_table=SILVER_TABLES["membership"],
            stage_table=_build_stage_table_name(SILVER_TABLES["membership"]),
            columns=tuple(MEMBERSHIP_COLUMNS),
        ),
        _StageTableSpec(
            live_table=SILVER_TABLES["profile"],
            stage_table=_build_stage_table_name(SILVER_TABLES["profile"]),
            columns=tuple(PROFILE_COLUMNS),
        ),
        _StageTableSpec(
            live_table=SILVER_TABLES["assessment"],
            stage_table=_build_stage_table_name(SILVER_TABLES["assessment"]),
            columns=tuple(ASSESSMENT_COLUMNS),
        ),
        _StageTableSpec(
            live_table=DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE,
            stage_table=_build_stage_table_name(DEFAULT_REFERENCE_METRIC_COVERAGE_TABLE),
            columns=tuple(REFERENCE_METRIC_COVERAGE_COLUMNS),
        ),
    ]
    gold_specs = [
        _StageTableSpec(
            live_table=live_table,
            stage_table=_build_stage_table_name(live_table),
            columns=tuple(GOLD_COLUMNS_BY_FAMILY[family]),
        )
        for family, live_table in GOLD_TABLES.items()
    ]
    return silver_specs, gold_specs


# Raw tables that are too large to replay from scratch every night.
# During a full refresh, these tables are skipped in the raw→bronze replay and
# their current live bronze data is copied directly into the stage table instead.
# The intraday incremental already keeps them up to date between full refreshes.
_FULL_REFRESH_STAGE_COPY_TABLES: dict[str, str] = {
    "raw.vald_forceframe_force_traces": "bronze.vald_forceframe_force_traces",
}


def _build_full_refresh_stage_context() -> dict[str, Any]:
    """Return the deterministic stage-table context for authoritative full refreshes."""
    bronze_stage_specs = _build_bronze_stage_specs()
    silver_stage_specs, gold_stage_specs = _build_full_rebuild_stage_specs()
    bronze_stage_tables = {
        spec.live_table: spec.stage_table
        for spec in bronze_stage_specs
    }
    silver_stage_tables = {
        "membership": silver_stage_specs[0].stage_table,
        "profile": silver_stage_specs[1].stage_table,
        "assessment": silver_stage_specs[2].stage_table,
        "coverage": silver_stage_specs[3].stage_table,
    }
    gold_stage_tables = {
        family: _build_stage_table_name(live_table)
        for family, live_table in GOLD_TABLES.items()
    }
    return {
        "bronze_stage_specs": bronze_stage_specs,
        "silver_stage_specs": silver_stage_specs,
        "gold_stage_specs": gold_stage_specs,
        "bronze_stage_tables": bronze_stage_tables,
        "silver_stage_tables": silver_stage_tables,
        "gold_stage_tables": gold_stage_tables,
    }


def _prepare_stage_tables(
    db: DatabaseManager,
    specs: list[_StageTableSpec] | list[_BronzePublishSpec],
) -> None:
    """Recreate the persistent staging tables from their live definitions."""
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {_FULL_REBUILD_STAGE_SCHEMA}")
    for spec in specs:
        db.execute(f"DROP TABLE IF EXISTS {spec.stage_table} CASCADE")
        db.execute(
            f"CREATE TABLE {spec.stage_table} (LIKE {spec.live_table} INCLUDING ALL)"
        )


def _publish_stage_tables(
    db: DatabaseManager,
    specs: list[_StageTableSpec],
) -> dict[str, Any]:
    """Replace live tables atomically from staged full-build tables.

    Phase 8.7.A (2026-05-09): the previous implementation
    ``TRUNCATE live; INSERT live FROM stage`` left the live table empty
    between the two statements, and the TRUNCATE violated locked decision #7.
    We now publish via :func:`atomic_publish_table` (rename swap inside one
    txn) so the live table is never empty and no TRUNCATE/DELETE touches it.

    The caller is expected to have already populated each
    ``spec.stage_table`` to its full target shape; this function only swaps.
    """
    from ingestion.common.atomic_publish import atomic_publish_table

    summary: dict[str, Any] = {
        "tables": {},
        "total_published_rows": 0,
    }
    for spec in specs:
        result = atomic_publish_table(
            db,
            live_table=spec.live_table,
            stage_table=spec.stage_table,
        )
        published_rows = int(result.get("rows_in_new_live", 0))
        summary["tables"][spec.live_table] = {
            "stage_table": spec.stage_table,
            "published_rows": published_rows,
            "archived_table": result.get("archived_table"),
            "archived_dropped": result.get("archived_dropped"),
        }
        summary["total_published_rows"] += published_rows
    logger.info("Published staged VALD tables into live schemas: %s", summary)
    return summary


def _get_table_columns_metadata(
    db: DatabaseManager,
    table_name: str,
) -> list[tuple[str, bool]]:
    schema_name, relation_name = table_name.split(".", 1)
    rows = db.fetch_all(
        """
        SELECT column_name, is_identity, column_default
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema_name, relation_name),
    )
    if not rows:
        raise ValueError(f"Could not discover columns for {table_name}")

    return [
        (
            str(row[0]),
            str(row[1]).upper() == "YES" or str(row[2] or "").startswith("nextval("),
        )
        for row in rows
    ]


def _get_copyable_columns(
    db: DatabaseManager,
    table_name: str,
) -> tuple[str, ...]:
    return tuple(
        column_name
        for column_name, is_identity in _get_table_columns_metadata(db, table_name)
        if not is_identity
    )


def _build_key_predicate(
    left_alias: str,
    right_alias: str,
    key_columns: tuple[str, ...],
) -> str:
    return " AND ".join(
        f"{left_alias}.{column_name} IS NOT DISTINCT FROM {right_alias}.{column_name}"
        for column_name in key_columns
    )


def _publish_bronze_stage_tables(
    db: DatabaseManager,
    specs: list[_BronzePublishSpec],
) -> dict[str, Any]:
    """Reconcile live bronze tables from their staged full-snapshot copies."""
    summary: dict[str, Any] = {
        "tables": {},
        "total_inserted_rows": 0,
        "total_deleted_rows": 0,
    }

    with db.connection() as conn:
        with conn.cursor() as cur:
            for spec in specs:
                copyable_columns = _get_copyable_columns(db, spec.stage_table)
                column_list = ", ".join(copyable_columns)
                key_predicate = _build_key_predicate("stage_keys", "live", spec.key_columns)

                cur.execute(
                    f"""
                    DELETE FROM {spec.live_table} AS live
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM (
                            SELECT DISTINCT {", ".join(spec.key_columns)}
                            FROM {spec.stage_table}
                        ) AS stage_keys
                        WHERE {key_predicate}
                    )
                    """
                )
                deleted_missing = cur.rowcount
                inserted_rows = 0
                deleted_replaced = 0

                # Phase 8.7.B (2026-05-09): both spec.mode branches now use
                # ``INSERT … ON CONFLICT (key_columns) DO UPDATE``. The previous
                # "non-merge" branch did ``DELETE live USING stage_keys; INSERT``
                # which is functionally equivalent (every column is sourced from
                # stage in both modes) but violated locked decision #7.
                # ``mode`` is preserved on the summary for backwards compat /
                # observability but no longer drives different SQL.
                update_columns = [
                    column_name
                    for column_name in copyable_columns
                    if column_name not in spec.key_columns and column_name != "created_at"
                ]
                update_sql = ", ".join(
                    f"{column_name} = EXCLUDED.{column_name}"
                    for column_name in update_columns
                )
                cur.execute(
                    f"""
                    INSERT INTO {spec.live_table} ({column_list})
                    SELECT {column_list}
                    FROM {spec.stage_table}
                    ON CONFLICT ({", ".join(spec.key_columns)}) DO UPDATE
                    SET {update_sql}
                    """
                )
                inserted_rows = cur.rowcount

                summary["tables"][spec.live_table] = {
                    "stage_table": spec.stage_table,
                    "mode": spec.mode,
                    "deleted_missing_rows": deleted_missing,
                    "deleted_replaced_rows": deleted_replaced,
                    "inserted_rows": inserted_rows,
                }
                summary["total_deleted_rows"] += deleted_missing + deleted_replaced
                summary["total_inserted_rows"] += inserted_rows

    logger.info("Published staged VALD bronze tables into live schema: %s", summary)
    return summary


def prepare_full_refresh_bronze_stage_tables() -> dict[str, Any]:
    """Prepare the authoritative bronze stage tables used by midnight rebuilds.

    Heavy tables listed in ``_FULL_REFRESH_STAGE_COPY_TABLES`` are populated by
    copying their live bronze data directly into the stage (fast INSERT SELECT)
    rather than being rebuilt from raw during the subsequent replay step.
    """
    context = _build_full_refresh_stage_context()
    # Use the silver statement timeout (0 = unlimited) because the live→stage
    # copy for large tables (e.g. force traces) can take several minutes.
    db = DatabaseManager(_get_stage_db_config(statement_timeout_env_key="POSTGRES_SILVER_STATEMENT_TIMEOUT_MS"))
    live_copy_summary: dict[str, int] = {}
    try:
        _prepare_stage_tables(db, context["bronze_stage_specs"])
        bronze_stage_tables = context["bronze_stage_tables"]
        for _raw_table, live_bronze_table in _FULL_REFRESH_STAGE_COPY_TABLES.items():
            stage_table = bronze_stage_tables.get(live_bronze_table)
            if stage_table is None:
                continue
            copyable_columns = _get_copyable_columns(db, live_bronze_table)
            col_list = ", ".join(copyable_columns)
            db.execute(
                f"INSERT INTO {stage_table} ({col_list}) "
                f"SELECT {col_list} FROM {live_bronze_table}"
            )
            count_row = db.fetch_one(f"SELECT COUNT(*) FROM {stage_table}")
            copied_rows = int(count_row[0]) if count_row else 0
            live_copy_summary[live_bronze_table] = copied_rows
            logger.info(
                "Copied %d rows from live %s into stage %s (raw replay skipped)",
                copied_rows, live_bronze_table, stage_table,
            )
    finally:
        db.close()
    summary = {
        "stage": "bronze_prepare",
        "stage_table_count": len(context["bronze_stage_tables"]),
        "stage_tables": dict(context["bronze_stage_tables"]),
        "live_copy_tables": live_copy_summary,
    }
    logger.info("Prepared full-refresh bronze stage tables: %s", summary)
    return summary


def run_full_refresh_raw_to_bronze_stage(
    *,
    modules: str = "all",
    include_reference: bool = True,
) -> dict[str, Any]:
    """Replay full-refresh raw VALD payloads into the authoritative bronze stage tables.

    Tables in ``_FULL_REFRESH_STAGE_COPY_TABLES`` are excluded from raw replay
    because their stage data was already populated by ``prepare_full_refresh_bronze_stage_tables``
    via a direct live→stage copy.
    """
    context = _build_full_refresh_stage_context()
    return run_raw_to_bronze_stage(
        modules=modules,
        include_reference=include_reference,
        table_overrides=context["bronze_stage_tables"],
        full_replay=True,
        replay_cursor_table=None,
        exclude_source_tables=tuple(_FULL_REFRESH_STAGE_COPY_TABLES.keys()),
    )


def run_full_refresh_bronze_to_silver_stage() -> dict[str, Any]:
    """Publish staged bronze data and rebuild staged silver tables for the full refresh."""
    context = _build_full_refresh_stage_context()
    summary: dict[str, Any] = {
        "bronze_publish": {},
        "silver": {},
    }
    with _hold_vald_live_write_lock(owner="full_refresh_bronze_to_silver"):
        db = DatabaseManager(_get_stage_db_config(statement_timeout_env_key="POSTGRES_SILVER_STATEMENT_TIMEOUT_MS"))
        try:
            summary["bronze_publish"] = _publish_bronze_stage_tables(
                db,
                context["bronze_stage_specs"],
            )
            _prepare_stage_tables(db, context["silver_stage_specs"])
            summary["silver"] = run_silver_etl(
                db,
                table_overrides={
                    key: context["silver_stage_tables"][key]
                    for key in ("membership", "profile", "assessment")
                },
                sync_quality_flags=False,
            )
        finally:
            db.close()
    logger.info("Full-refresh bronze->silver stage complete: %s", summary)
    return summary


def run_full_refresh_silver_to_gold_stage(
    *,
    runtime_validation: bool = False,
) -> dict[str, Any]:
    """Build staged gold tables, publish silver/gold live tables, and refresh quality flags."""
    context = _build_full_refresh_stage_context()
    summary: dict[str, Any] = {
        "gold": {},
        "publish": {},
        "quality_refresh": {},
        "validation": {},
    }
    with _hold_vald_live_write_lock(owner="full_refresh_silver_to_gold"):
        db = DatabaseManager(
            _get_stage_db_config(statement_timeout_env_key="POSTGRES_GOLD_STATEMENT_TIMEOUT_MS")
        )
        try:
            _prepare_stage_tables(db, context["gold_stage_specs"])
            summary["gold"] = run_gold_etl(
                db,
                assessment_source_table=context["silver_stage_tables"]["assessment"],
                target_tables=context["gold_stage_tables"],
                coverage_table=context["silver_stage_tables"]["coverage"],
            )
        finally:
            db.close()

        db = DatabaseManager(get_db_config())
        try:
            summary["publish"] = _publish_stage_tables(
                db,
                [*context["silver_stage_specs"], *context["gold_stage_specs"]],
            )
            summary["quality_refresh"] = rebuild_overlap_quality_flags(db)
        finally:
            db.close()

    if runtime_validation:
        summary["validation"] = run_validation(runtime_only=True, run_pytest_suite=False)

    logger.info("Full-refresh silver->gold stage complete: %s", summary)
    return summary


def run_reset_rebuild(
    runtime_validation: bool = False,
) -> dict[str, Any]:
    """Run the midnight authoritative rebuild from full raw extract through gold."""
    bootstrap_database()

    summary: dict[str, Any] = {
        "raw": {},
        "raw_to_bronze": {},
        "silver": {},
        "gold": {},
        "publish": {},
        "quality_refresh": {},
        "validation": {},
        "errors": [],
    }

    raw_summary = run_extract_raw(
        modules="all",
        full_refresh=True,
        include_reference=True,
    )
    summary["raw"] = raw_summary
    summary["errors"].extend(raw_summary.get("errors", []))
    for module_data in raw_summary.get("modules", {}).values():
        summary["errors"].extend(module_data.get("errors", []))

    prepare_full_refresh_bronze_stage_tables()

    summary["raw_to_bronze"] = run_full_refresh_raw_to_bronze_stage(
        modules="all",
        include_reference=True,
    )

    if _summary_has_errors(summary):
        raise ValueError("Reset rebuild aborted before publish due to raw extraction or bronze replay errors.")

    silver_stage_summary = run_full_refresh_bronze_to_silver_stage()
    summary["silver"] = silver_stage_summary.get("silver", {})
    summary["publish"] = {
        "bronze": silver_stage_summary.get("bronze_publish", {}),
        "silver_gold": {},
    }

    gold_stage_summary = run_full_refresh_silver_to_gold_stage(
        runtime_validation=runtime_validation,
    )
    summary["gold"] = gold_stage_summary.get("gold", {})
    summary["publish"]["silver_gold"] = gold_stage_summary.get("publish", {})
    summary["quality_refresh"] = gold_stage_summary.get("quality_refresh", {})
    summary["validation"] = gold_stage_summary.get("validation", {})
    summary["errors"].extend(summary["validation"].get("errors", []))

    return summary


def run_validation(
    runtime_only: bool = False,
    run_pytest_suite: bool = True,
) -> dict[str, Any]:
    """Validate the VALD pipeline environment, schema, and assets."""
    provider_config = load_provider_config("vald")
    errors: list[str] = []

    _validate_required_environment(provider_config, errors)
    _validate_compile_targets(errors)
    _validate_docker_assets(errors)

    db = DatabaseManager(get_db_config())
    try:
        _validate_schema_state(db, errors)
    finally:
        db.close()

    if run_pytest_suite and not runtime_only:
        _run_validation_pytest(errors)

    summary = {
        "ok": not errors,
        "errors": errors,
        "runtime_only": runtime_only,
        "pytest_ran": run_pytest_suite and not runtime_only,
    }
    if errors:
        logger.error("VALD pipeline validation failed: %s", errors)
    else:
        logger.info("VALD pipeline validation passed.")
    return summary


def run_end_to_end(
    modules: str = "all",
    full_refresh: bool = False,
    include_reference: bool = True,
    runtime_validation: bool = False,
) -> dict[str, Any]:
    """Run the complete stage-based VALD pipeline."""
    summary: dict[str, Any] = {
        "raw": {},
        "raw_to_bronze": {},
        "silver": {},
        "gold": {},
        "validation": {},
        "errors": [],
    }

    raw_summary = run_extract_raw(
        modules=modules,
        full_refresh=full_refresh,
        include_reference=include_reference,
    )
    summary["raw"] = raw_summary
    summary["errors"].extend(raw_summary.get("errors", []))
    for module_data in raw_summary.get("modules", {}).values():
        summary["errors"].extend(module_data.get("errors", []))

    summary["raw_to_bronze"] = run_raw_to_bronze_stage(
        modules=modules,
        include_reference=include_reference,
    )
    with _hold_vald_live_write_lock(owner="end_to_end"):
        summary["silver"] = _run_bronze_to_silver_stage_unlocked()
        summary["gold"] = _run_silver_to_gold_stage_unlocked()
    if runtime_validation:
        summary["validation"] = run_validation(runtime_only=True, run_pytest_suite=False)
        summary["errors"].extend(summary["validation"].get("errors", []))

    return summary


def run_resume_pipeline(
    from_stage: str,
    *,
    modules: str = "all",
    full_refresh: bool = False,
    include_reference: bool = True,
    runtime_validation: bool = False,
) -> dict[str, Any]:
    """Resume the VALD pipeline from a specific stage through gold."""
    canonical_stage = _normalize_pipeline_stage(from_stage)
    summary: dict[str, Any] = {
        "resumed_from": canonical_stage,
        "raw": {},
        "raw_to_bronze": {},
        "silver": {},
        "gold": {},
        "validation": {},
        "errors": [],
    }

    if full_refresh and canonical_stage != "raw":
        logger.info(
            "--full-refresh only applies when resuming from the raw stage; ignoring it for %s",
            canonical_stage,
        )

    if _should_run_stage("raw", canonical_stage):
        raw_summary = run_extract_raw(
            modules=modules,
            full_refresh=full_refresh if canonical_stage == "raw" else False,
            include_reference=include_reference,
        )
        summary["raw"] = raw_summary
        summary["errors"].extend(raw_summary.get("errors", []))
        for module_data in raw_summary.get("modules", {}).values():
            summary["errors"].extend(module_data.get("errors", []))

    if _should_run_stage("raw_to_bronze", canonical_stage):
        summary["raw_to_bronze"] = run_raw_to_bronze_stage(
            modules=modules,
            include_reference=include_reference,
        )

    if _should_run_stage("bronze_to_silver", canonical_stage) or _should_run_stage(
        "silver_to_gold",
        canonical_stage,
    ):
        with _hold_vald_live_write_lock(owner=f"resume_pipeline:{canonical_stage}"):
            if _should_run_stage("bronze_to_silver", canonical_stage):
                summary["silver"] = _run_bronze_to_silver_stage_unlocked()

            if _should_run_stage("silver_to_gold", canonical_stage):
                summary["gold"] = _run_silver_to_gold_stage_unlocked()

    if runtime_validation:
        summary["validation"] = run_validation(runtime_only=True, run_pytest_suite=False)
        summary["errors"].extend(summary["validation"].get("errors", []))

    return summary


def main_bootstrap_database(argv: list[str] | None = None) -> int:
    """CLI entrypoint for database bootstrap."""
    return platform_main_bootstrap_database(argv)


def main_run_extract_raw(argv: list[str] | None = None) -> int:
    """CLI entrypoint for raw extraction."""
    parser = argparse.ArgumentParser(description="Run the VALD raw extraction stage.")
    _add_module_args(parser)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Reset VALD module watermarks before extracting raw payloads.",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip VALD reference endpoint capture.",
    )
    parser.add_argument(
        "--intraday-current-day-only",
        action="store_true",
        help=(
            "Clamp VALD modifiedFromUtc requests to the start of the current "
            "Europe/Lisbon day. Intended for intraday fast extraction only."
        ),
    )
    args = parser.parse_args(argv)
    summary = run_extract_raw(
        modules=args.modules,
        full_refresh=args.full_refresh,
        include_reference=not args.skip_reference,
        intraday_current_day_only=args.intraday_current_day_only,
    )
    _log_stage_summary("VALD raw extraction", summary)
    return 1 if _summary_has_errors(summary) else 0


def main_run_reset_rebuild(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the safe staged full rebuild used by midnight."""
    parser = argparse.ArgumentParser(
        description="Run the safe staged VALD full rebuild without wiping raw or bronze.",
    )
    parser.add_argument(
        "--runtime-validate",
        action="store_true",
        help="Run runtime-only validation after the gold stage.",
    )
    args = parser.parse_args(argv)
    summary = run_reset_rebuild(runtime_validation=args.runtime_validate)
    _print_pipeline_summary(summary)
    return 1 if _summary_has_errors(summary) else 0


def main_run_raw_to_bronze(argv: list[str] | None = None) -> int:
    """CLI entrypoint for raw->bronze replay."""
    parser = argparse.ArgumentParser(description="Replay VALD raw payloads into bronze.")
    _add_module_args(parser)
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip replaying VALD reference raw tables.",
    )
    parser.add_argument(
        "--defer-heavy-tables",
        action="store_true",
        help="Skip heavy raw tables such as ForceFrame traces for a faster critical-path replay.",
    )
    parser.add_argument(
        "--heavy-tables-only",
        action="store_true",
        help="Replay only the deferred heavy raw tables such as ForceFrame traces.",
    )
    args = parser.parse_args(argv)
    if args.defer_heavy_tables and args.heavy_tables_only:
        parser.error("--defer-heavy-tables and --heavy-tables-only are mutually exclusive.")

    replay_kwargs: dict[str, Any] = {
        "modules": args.modules,
        "include_reference": not args.skip_reference,
    }
    if args.defer_heavy_tables:
        replay_kwargs["exclude_source_tables"] = tuple(INTRADAY_DEFERRED_RAW_TABLES)
    if args.heavy_tables_only:
        replay_kwargs["include_reference"] = False
        replay_kwargs["include_only_source_tables"] = tuple(INTRADAY_DEFERRED_RAW_TABLES)

    summary = run_raw_to_bronze_stage(
        **replay_kwargs,
    )
    _log_stage_summary("VALD raw->bronze", summary)
    return 0


def main_run_bronze_to_silver(argv: list[str] | None = None) -> int:
    """CLI entrypoint for bronze->silver stage."""
    parser = argparse.ArgumentParser(description="Run the VALD bronze->silver stage.")
    parser.parse_args(argv)
    summary = run_bronze_to_silver_stage()
    _log_stage_summary("VALD bronze->silver", summary)
    return 0


def main_run_silver_to_gold(argv: list[str] | None = None) -> int:
    """CLI entrypoint for silver->gold stage."""
    parser = argparse.ArgumentParser(description="Run the VALD silver->gold stage.")
    parser.parse_args(argv)
    summary = run_silver_to_gold_stage()
    _log_stage_summary("VALD silver->gold", summary)
    return 0


def main_run_resume_pipeline(argv: list[str] | None = None) -> int:
    """CLI entrypoint for resuming the VALD pipeline from a specific stage."""
    parser = argparse.ArgumentParser(description="Resume the VALD pipeline from a specific stage.")
    parser.add_argument(
        "--from-stage",
        required=True,
        choices=_PIPELINE_STAGE_ORDER,
        help="Stage to resume from.",
    )
    _add_module_args(parser)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Reset VALD module watermarks before extracting raw payloads when resuming from raw.",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip VALD reference endpoint capture and replay.",
    )
    parser.add_argument(
        "--runtime-validate",
        action="store_true",
        help="Run runtime-only validation after the gold stage.",
    )
    args = parser.parse_args(argv)
    summary = run_resume_pipeline(
        args.from_stage,
        modules=args.modules,
        full_refresh=args.full_refresh,
        include_reference=not args.skip_reference,
        runtime_validation=args.runtime_validate,
    )
    _print_pipeline_summary(summary)
    return 1 if _summary_has_errors(summary) else 0


def main_validate_pipeline(argv: list[str] | None = None) -> int:
    """CLI entrypoint for pipeline validation."""
    parser = argparse.ArgumentParser(description="Validate the VALD pipeline environment and schema.")
    parser.add_argument(
        "--runtime-only",
        action="store_true",
        help="Run runtime-safe validation only and skip pytest.",
    )
    parser.add_argument(
        "--skip-pytest",
        action="store_true",
        help="Skip the focused pytest suite.",
    )
    args = parser.parse_args(argv)
    summary = run_validation(
        runtime_only=args.runtime_only,
        run_pytest_suite=not args.skip_pytest,
    )
    _log_stage_summary("VALD validation", summary)
    return 0 if summary["ok"] else 1


def main_run_ingestion(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the full VALD pipeline."""
    parser = argparse.ArgumentParser(description="Run the end-to-end VALD pipeline.")
    _add_module_args(parser)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Reset VALD module watermarks before extracting raw payloads.",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip VALD reference endpoint capture and replay.",
    )
    parser.add_argument(
        "--runtime-validate",
        action="store_true",
        help="Run runtime-only validation after the gold stage.",
    )
    parser.add_argument(
        "--skip-quality",
        action="store_true",
        help="Deprecated no-op retained for backward compatibility.",
    )
    args = parser.parse_args(argv)
    if args.skip_quality:
        logger.info("--skip-quality is deprecated and now has no effect.")

    summary = run_end_to_end(
        modules=args.modules,
        full_refresh=args.full_refresh,
        include_reference=not args.skip_reference,
        runtime_validation=args.runtime_validate,
    )
    _print_pipeline_summary(summary)
    return 1 if _summary_has_errors(summary) else 0


def _build_module_extractor(
    module_name: str,
    vald_client: ValdClient,
    raw_loader: ValdRawLoader,
    watermark_mgr: WatermarkManager,
    batch_manager: BatchManager,
    intraday_current_day_only: bool = False,
) -> Any:
    extractors = {
        "forcedecks": ForceDecksExtractor,
        "forceframe": ForceFrameExtractor,
        "nordbord": NordBordExtractor,
        "smartspeed": SmartSpeedExtractor,
        "dynamo": DynaMoExtractor,
    }
    extractor_cls = extractors[module_name]
    return extractor_cls(
        vald_client=vald_client,
        raw_loader=raw_loader,
        bronze_loader=None,
        watermark_mgr=watermark_mgr,
        batch_manager=batch_manager,
        intraday_current_day_only=intraday_current_day_only,
    )


def _reset_module_watermarks(
    watermark_mgr: WatermarkManager,
    modules: list[str],
    tenant_ids: list[str],
) -> None:
    for module_name in modules:
        for tenant_id in tenant_ids:
            watermark_mgr.update_watermark(
                provider=_PROVIDER,
                source_account=_SOURCE_ACCOUNT,
                api_name=f"{module_name}_tests",
                watermark_value=VALD_CUTOFF_UTC,
                records_synced=0,
                tenant_id=tenant_id,
            )
            logger.info(
                "Full refresh: reset watermark for %s tenant=%s",
                module_name,
                tenant_id,
            )


def _validate_required_environment(
    provider_config: dict[str, Any],
    errors: list[str],
) -> None:
    db_config = get_db_config()
    required_db = ["host", "port", "dbname", "user"]
    for key in required_db:
        value = db_config.get(key)
        if value in (None, ""):
            errors.append(f"Missing database configuration value: {key}")

    auth_cfg = provider_config.get("auth", {})
    for key_name in ("token_url_env", "client_id_env", "client_secret_env"):
        env_key = auth_cfg.get(key_name)
        if env_key and not get_env(env_key):
            errors.append(f"Missing required VALD environment variable: {env_key}")

    region_env = provider_config.get("region_env")
    if region_env and not get_env(region_env):
        errors.append(f"Missing required VALD environment variable: {region_env}")


def _validate_schema_state(db: DatabaseManager, errors: list[str]) -> None:
    required_tables = [
        "raw.sync_watermark",
        "raw.ingestion_batch_log",
        _VALD_REPLAY_CURSOR_TABLE,
        *REFERENCE_RAW_TABLES,
        *REFERENCE_BRONZE_TABLES,
        *CATAPULT_RAW_TABLES,
        *CATAPULT_BRONZE_TABLES,
        *ACTIVE_SILVER_TABLES,
        *ACTIVE_GOLD_TABLES,
    ]
    for tables in MODULE_RAW_TABLES.values():
        required_tables.extend(tables)
    for tables in MODULE_BRONZE_TABLES.values():
        required_tables.extend(tables)

    for table_name in required_tables:
        if not _table_exists(db, table_name):
            errors.append(f"Missing required table: {table_name}")

    for table_name, column_name in _REQUIRED_VALD_COLUMNS:
        if not _column_exists(db, table_name, column_name):
            errors.append(f"Missing required column: {table_name}.{column_name}")

    for table_name, column_name in _FORBIDDEN_VALD_COLUMNS:
        if _column_exists(db, table_name, column_name):
            errors.append(f"Removed VALD column still exists: {table_name}.{column_name}")

    for table_name in OBSOLETE_VALD_TABLES:
        if _table_exists(db, table_name):
            errors.append(f"Obsolete VALD table still exists: {table_name}")

    for table_name in CATAPULT_PARTITIONED_BRONZE_TABLES:
        if _count_partitions(db, table_name) == 0:
            errors.append(f"Catapult partitioned table has no partitions: {table_name}")

    allowed_silver_tables = {table_name.split(".", 1)[1] for table_name in ACTIVE_SILVER_TABLES}
    silver_rows = db.fetch_all(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'silver'
        ORDER BY table_name
        """
    )
    for row in silver_rows:
        table_name = str(row[0])
        if table_name not in allowed_silver_tables:
            errors.append(f"Unexpected non-VALD silver table still exists: silver.{table_name}")

    allowed_gold_tables = {
        table_name.split(".", 1)[1]
        for table_name in [*ACTIVE_GOLD_TABLES, *_SUPPORTED_NON_VALD_GOLD_TABLES]
    }
    gold_rows = db.fetch_all(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
        ORDER BY table_name
        """
    )
    for row in gold_rows:
        table_name = str(row[0])
        if table_name not in allowed_gold_tables:
            errors.append(f"Unexpected non-VALD gold table still exists: gold.{table_name}")

    for table_name in UNSUPPORTED_CATAPULT_TABLES:
        if _table_exists(db, table_name):
            errors.append(f"Unsupported Catapult silver/gold table exists: {table_name}")


def _validate_compile_targets(errors: list[str]) -> None:
    for relative_path in _VALIDATION_COMPILE_TARGETS:
        path = _PROJECT_ROOT / relative_path
        if not path.exists():
            errors.append(f"Missing required pipeline asset: {relative_path}")
            continue
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append(f"Python compile failed for {relative_path}: {exc.msg}")


def _validate_docker_assets(errors: list[str]) -> None:
    for relative_path in ("Dockerfile", "docker-compose.yml"):
        if not (_PROJECT_ROOT / relative_path).exists():
            errors.append(f"Missing required Docker asset: {relative_path}")


def _run_validation_pytest(errors: list[str]) -> None:
    cmd = ["pytest", *_VALIDATION_PYTEST_TARGETS, "-q"]
    try:
        subprocess.run(
            cmd,
            cwd=_PROJECT_ROOT,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        errors.append(f"Focused pytest suite failed with exit code {exc.returncode}")


def _table_exists(db: DatabaseManager, table_name: str) -> bool:
    row = db.fetch_one("SELECT to_regclass(%s)", (table_name,))
    return bool(row and row[0] is not None)


def _column_exists(db: DatabaseManager, table_name: str, column_name: str) -> bool:
    schema_name, relation_name = table_name.split(".", 1)
    row = db.fetch_one(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        """,
        (schema_name, relation_name, column_name),
    )
    return bool(row)


def _count_partitions(db: DatabaseManager, table_name: str) -> int:
    row = db.fetch_one(
        """
        SELECT COUNT(*)
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        WHERE parent_ns.nspname || '.' || parent.relname = %s
        """,
        (table_name,),
    )
    return int(row[0]) if row else 0


def _require_tables(db: DatabaseManager, table_names: list[str]) -> None:
    missing = [table_name for table_name in table_names if not _table_exists(db, table_name)]
    if missing:
        raise ValueError(f"Missing required source tables: {missing}")


def _validate_batch_integrity(db: DatabaseManager, batch_ids: list[str]) -> None:
    if not batch_ids:
        return
    row = db.fetch_one(
        """
        SELECT COUNT(*)
        FROM raw.ingestion_batch_log
        WHERE batch_id = ANY(%s::uuid[])
          AND status = 'running'
        """,
        (batch_ids,),
    )
    if row and int(row[0]) > 0:
        raise ValueError("One or more current VALD ingestion batches remain in running state.")


@contextmanager
def _hold_vald_live_write_lock(
    *,
    owner: str,
    db_config: dict[str, Any] | None = None,
    wait: bool = False,
) -> Generator[None, None, None]:
    """Hold a shared warehouse advisory lock across VALD silver/gold live writes."""
    lock_db = DatabaseManager(
        dict(db_config or get_db_config()),
        min_conn=1,
        max_conn=1,
    )
    conn = lock_db.get_connection()
    locked = False
    try:
        with conn.cursor() as cur:
            if wait:
                cur.execute(
                    "SELECT pg_advisory_lock(%s, %s)",
                    (_VALD_LIVE_WRITE_LOCK_NAMESPACE, _VALD_LIVE_WRITE_LOCK_RESOURCE),
                )
                locked = True
            else:
                cur.execute(
                    "SELECT pg_try_advisory_lock(%s, %s)",
                    (_VALD_LIVE_WRITE_LOCK_NAMESPACE, _VALD_LIVE_WRITE_LOCK_RESOURCE),
                )
                row = cur.fetchone()
                locked = bool(row and row[0])
        conn.commit()
        if not locked:
            raise ValdPipelineBusyError(
                f"{owner} could not acquire the VALD live-write lock because another silver/gold writer is already running."
            )
        logger.info("Acquired VALD live-write lock for %s", owner)
        yield
    finally:
        try:
            if locked:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(%s, %s)",
                        (_VALD_LIVE_WRITE_LOCK_NAMESPACE, _VALD_LIVE_WRITE_LOCK_RESOURCE),
                    )
                    unlocked_row = cur.fetchone()
                conn.commit()
                logger.info(
                    "Released VALD live-write lock for %s (unlocked=%s)",
                    owner,
                    bool(unlocked_row and unlocked_row[0]),
                )
        finally:
            lock_db.put_connection(conn)
            lock_db.close()


def _run_with_live_write_lock(
    owner: str,
    operation: Callable[[], dict[str, Any]],
    *,
    skip_when_busy: bool = False,
    skipped_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a live-write stage under the shared advisory lock."""
    try:
        with _hold_vald_live_write_lock(owner=owner):
            return operation()
    except ValdPipelineBusyError as exc:
        if not skip_when_busy:
            raise
        logger.warning("Skipping %s because %s", owner, exc)
        summary = dict(skipped_summary or {})
        summary["skipped"] = True
        summary["skip_reason"] = str(exc)
        return summary


def _summary_has_errors(summary: dict[str, Any]) -> bool:
    if summary.get("errors"):
        return True
    for module_data in summary.get("modules", {}).values():
        if module_data.get("errors"):
            return True
    if summary.get("validation", {}).get("errors"):
        return True
    return False


def _get_stage_db_config(
    *,
    statement_timeout_env_key: str | None = None,
) -> dict[str, Any]:
    """Return DB config with an optional stage-specific statement timeout."""
    config = get_db_config()
    if statement_timeout_env_key is not None:
        override = get_env(statement_timeout_env_key)
        if override not in (None, ""):
            config["statement_timeout_ms"] = int(override)
    return config


def _normalize_pipeline_stage(stage_name: str) -> str:
    """Return the canonical stage name accepted by resume helpers."""
    normalized = stage_name.strip().lower()
    normalized = _PIPELINE_STAGE_ALIASES.get(normalized, normalized)
    if normalized not in _PIPELINE_STAGE_ORDER:
        raise ValueError(
            f"Unknown pipeline stage '{stage_name}'. Expected one of: {_PIPELINE_STAGE_ORDER}"
        )
    return normalized


def _should_run_stage(stage_name: str, from_stage: str) -> bool:
    """Return True when a stage should execute for the requested resume point."""
    return _PIPELINE_STAGE_ORDER.index(stage_name) >= _PIPELINE_STAGE_ORDER.index(from_stage)


def _add_module_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--modules",
        type=str,
        default="all",
        help='Comma-separated VALD modules to run, or "all".',
    )


def _log_stage_summary(stage_name: str, summary: dict[str, Any]) -> None:
    logger.info("%s summary: %s", stage_name, summary)


def _print_pipeline_summary(summary: dict[str, Any]) -> None:
    logger.info("=" * 72)
    logger.info("VALD Ingestion Pipeline Summary")
    logger.info("=" * 72)

    raw_summary = summary.get("raw", {})
    if raw_summary:
        logger.info(
            "Raw extraction: total_extracted=%d total_loaded=%d has_new_data=%s",
            raw_summary.get("total_extracted", 0),
            raw_summary.get("total_loaded", 0),
            raw_summary.get("has_new_data", False),
        )

    replay_summary = summary.get("raw_to_bronze", {})
    if replay_summary:
        logger.info(
            "Raw->Bronze: processed_raw_rows=%d loaded_rows=%d",
            replay_summary.get("processed_raw_rows", 0),
            replay_summary.get("loaded_rows", 0),
        )

    silver_summary = summary.get("silver", {})
    if silver_summary:
        metrics_info = silver_summary.get("assessment_metrics", {})
        logger.info(
            "Silver ETL: metrics=%d",
            metrics_info.get("total_inserted", 0),
        )

    gold_summary = summary.get("gold", {})
    if gold_summary:
        logger.info(
            "Gold ETL: total_rows=%d source_rows=%d excluded_above_threshold=%d "
            "excluded_below_threshold=%d excluded_outside_threshold=%d",
            gold_summary.get("total_rows", 0),
            gold_summary.get("total_source_rows", 0),
            gold_summary.get("total_excluded_above_threshold_rows", 0),
            gold_summary.get("total_excluded_below_threshold_rows", 0),
            gold_summary.get("total_excluded_outside_threshold_rows", 0),
        )

    publish_summary = summary.get("publish", {})
    if publish_summary:
        logger.info(
            "Publish: total_published_rows=%d",
            publish_summary.get("total_published_rows", 0),
        )

    quality_refresh_summary = summary.get("quality_refresh", {})
    if quality_refresh_summary:
        logger.info(
            "Quality refresh: flags_written=%d ambiguous_profiles=%d",
            quality_refresh_summary.get("flags_written", 0),
            quality_refresh_summary.get("ambiguous_profiles", 0),
        )

    validation_summary = summary.get("validation", {})
    if validation_summary:
        logger.info(
            "Validation: ok=%s runtime_only=%s pytest_ran=%s",
            validation_summary.get("ok"),
            validation_summary.get("runtime_only"),
            validation_summary.get("pytest_ran"),
        )

    all_errors = list(summary.get("errors", []))
    if all_errors:
        logger.info("-" * 72)
        logger.info("Errors (%d):", len(all_errors))
        for index, err in enumerate(all_errors, start=1):
            logger.info("  %d. %s", index, err)
    logger.info("=" * 72)
