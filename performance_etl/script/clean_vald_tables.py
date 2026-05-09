"""
Clean VALD tables across raw, bronze, silver, and gold layers.

Truncates active VALD tables so the pipeline can be re-run from scratch.
Optionally drops retired and non-VALD warehouse tables that are no longer used.

Usage::

    # Show what would be cleaned (dry run)
    python script/clean_vald_tables.py --dry-run

    # Clean only raw VALD tables
    python script/clean_vald_tables.py --layers raw

    # Clean only bronze
    python script/clean_vald_tables.py --layers bronze

    # Clean bronze + silver
    python script/clean_vald_tables.py --layers bronze,silver

    # Clean everything (raw + bronze + silver + gold + watermarks + batches)
    python script/clean_vald_tables.py --layers all

    # Clean everything including watermarks (forces full re-sync)
    python script/clean_vald_tables.py --layers all --reset-watermarks

    # Clean only a specific module
    python script/clean_vald_tables.py --layers bronze --module forcedecks

    # Drop retired or non-VALD tables that are no longer used
    python script/clean_vald_tables.py --layers raw,bronze,silver,gold --drop-obsolete
"""

from __future__ import annotations

import argparse
import sys

from ingestion.common.config import get_db_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.vald.cutoff import VALD_CUTOFF_UTC

logger = get_logger(__name__)

RAW_TABLES = [
    "raw.vald_profiles",
    "raw.vald_forcedecks_tests",
    "raw.vald_forcedecks_trials",
    "raw.vald_forcedecks_result_definitions",
    "raw.vald_forceframe_tests",
    "raw.vald_forceframe_test_metrics",
    "raw.vald_forceframe_force_traces",
    "raw.vald_nordbord_tests",
    "raw.vald_nordbord_test_metrics",
    "raw.vald_nordbord_ecc_exercises",
    "raw.vald_nordbord_ecc_repetitions",
    "raw.vald_smartspeed_test_summaries",
    "raw.vald_smartspeed_test_details",
    "raw.vald_dynamo_tests",
    "raw.vald_dynamo_test_details",
    "raw.vald_dynamo_traces",
]

BRONZE_TABLES = [
    "bronze.vald_profiles",
    "bronze.vald_profile_categories",
    "bronze.vald_forcedecks_tests",
    "bronze.vald_forcedecks_trials",
    "bronze.vald_forcedecks_trial_results",
    "bronze.vald_forcedecks_result_definitions",
    "bronze.vald_forceframe_tests",
    "bronze.vald_forceframe_test_metrics",
    "bronze.vald_forceframe_force_traces",
    "bronze.vald_nordbord_tests",
    "bronze.vald_nordbord_test_metrics",
    "bronze.vald_nordbord_ecc_exercises",
    "bronze.vald_nordbord_ecc_repetitions",
    "bronze.vald_smartspeed_test_summaries",
    "bronze.vald_smartspeed_test_details",
    "bronze.vald_smartspeed_rep_results",
    "bronze.vald_dynamo_tests",
    "bronze.vald_dynamo_rep_summaries",
    "bronze.vald_dynamo_repetitions",
    "bronze.vald_dynamo_traces",
]

OBSOLETE_RAW_TABLES = [
    "raw.pipeline_stage_cursor",
    "raw.vald_tenants",
    "raw.vald_categories",
    "raw.vald_groups",
    "raw.vald_forceframe_training_exercises",
    "raw.vald_forceframe_training_repetitions",
    "raw.vald_nordbord_force_traces",
    "raw.vald_nordbord_iso_sessions",
    "raw.vald_nordbord_iso_exercises",
    "raw.vald_nordbord_iso_repetitions",
    "raw.vald_humantrak_tests",
    "raw.vald_humantrak_repetitions",
]

OBSOLETE_BRONZE_TABLES = [
    "bronze.vald_tenants",
    "bronze.vald_categories",
    "bronze.vald_groups",
    "bronze.vald_forceframe_training_exercises",
    "bronze.vald_forceframe_training_repetitions",
    "bronze.vald_nordbord_force_traces",
    "bronze.vald_nordbord_iso_sessions",
    "bronze.vald_nordbord_iso_exercises",
    "bronze.vald_nordbord_iso_repetitions",
    "bronze.vald_humantrak_tests",
    "bronze.vald_humantrak_metric_groups",
    "bronze.vald_humantrak_metric_summaries",
    "bronze.vald_humantrak_metric_asymmetries",
    "bronze.vald_humantrak_repetitions",
]

OBSOLETE_SILVER_TABLES = [
    "silver.master_athlete",
    "silver.athlete_provider_link",
    "silver.athlete_match_candidate",
    "silver.athlete_match_rejection",
    "silver.athlete_mapping_audit",
    "silver.athlete_team_membership",
    "silver.dim_team",
    "silver.dim_season",
    "silver.dim_position",
    "silver.microcycle",
    "silver.master_tag",
    "silver.tag_account_mapping",
    "silver.tag_approval_request",
    "silver.tag_mismatch_log",
    "silver.data_quality_baseline",
    "silver.data_quality_threshold",
    "silver.vald_metric_quality_baseline",
]

OBSOLETE_GOLD_TABLES = [
    "gold.athlete_profile",
    "gold.daily_monitoring",
    "gold.velocity_benchmark",
    "gold.team_history",
    "gold.rtp_support",
    "gold.vald_jumps",
    "gold.vald_forcedecks_other",
]

OBSOLETE_TABLES = set(
    OBSOLETE_RAW_TABLES + OBSOLETE_BRONZE_TABLES + OBSOLETE_SILVER_TABLES + OBSOLETE_GOLD_TABLES
)

SILVER_TABLES = [
    "silver.vald_athlete_profile",
    "silver.vald_target_group_membership",
    "silver.vald_assessment_metric",
    "silver.data_quality_flag",
]

GOLD_TABLES = [
    "gold.vald_nordics",
    "gold.vald_forceframe",
    "gold.vald_forcedecks",
    "gold.vald_dynamo",
    "gold.vald_speed",
]

REPLAY_CURSOR_TABLE = "raw.vald_replay_cursor"

MODULE_PATTERNS = {
    "forcedecks": "forcedecks",
    "forceframe": "forceframe",
    "nordbord": "nordbord",
    "humantrak": "humantrak",
    "smartspeed": "smartspeed",
    "dynamo": "dynamo",
}

ALL_LAYERS = ["raw", "bronze", "silver", "gold"]


def _get_tables_for_layer(layer: str) -> list[str]:
    """Return the active table list for a given layer."""
    return {
        "raw": RAW_TABLES,
        "bronze": BRONZE_TABLES,
        "silver": SILVER_TABLES,
        "gold": GOLD_TABLES,
    }[layer]


def _get_obsolete_tables_for_layer(layer: str) -> list[str]:
    """Return the retired table list for a given layer."""
    return {
        "raw": OBSOLETE_RAW_TABLES,
        "bronze": OBSOLETE_BRONZE_TABLES,
        "silver": OBSOLETE_SILVER_TABLES,
        "gold": OBSOLETE_GOLD_TABLES,
    }.get(layer, [])


def _filter_by_module(tables: list[str], module: str | None) -> list[str]:
    """Filter table list to only those matching a specific module."""
    if not module:
        return tables

    pattern = MODULE_PATTERNS.get(module)
    if not pattern:
        logger.error("Unknown module: %s. Options: %s", module, list(MODULE_PATTERNS.keys()))
        sys.exit(1)

    return [table for table in tables if pattern in table]


def _table_exists(db: DatabaseManager, full_name: str) -> bool:
    """Check if a table exists in the database."""
    schema, table = full_name.split(".", 1)
    row = db.fetch_one(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return row is not None


def _get_row_count(db: DatabaseManager, full_name: str) -> int:
    """Get the exact row count for a table when available."""
    try:
        row = db.fetch_one(f"SELECT COUNT(*) FROM {full_name}")
        return row[0] if row else 0
    except Exception:
        return -1


def _drop_obsolete_tables(
    db: DatabaseManager,
    layers: list[str],
    module: str | None,
    dry_run: bool,
) -> dict[str, int]:
    """Drop retired or non-VALD warehouse tables when requested."""
    results: dict[str, int] = {}

    for layer in layers:
        tables = _get_obsolete_tables_for_layer(layer)
        tables = _filter_by_module(tables, module)

        for table in tables:
            if not _table_exists(db, table):
                logger.info("  SKIP  %s (obsolete table does not exist)", table)
                continue

            count = _get_row_count(db, table)
            if dry_run:
                logger.info("  [DRY] DROP %s - obsolete table with %d rows", table, count)
            else:
                db.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                logger.info("  DROP  %s - removed obsolete table (%d rows)", table, count)
            results[table] = count

    return results


def clean_tables(
    db: DatabaseManager,
    layers: list[str],
    module: str | None = None,
    reset_watermarks: bool = False,
    reset_batches: bool = False,
    drop_obsolete: bool = False,
    dry_run: bool = False,
) -> dict[str, int]:
    """Clean VALD tables for the specified layers.

    Args:
        db: Database connection.
        layers: List of layers to clean (raw, bronze, silver, gold).
        module: Optional module filter (forcedecks, forceframe, nordbord, humantrak, smartspeed, dynamo).
        reset_watermarks: Also reset VALD sync watermarks.
        reset_batches: Also clean VALD ingestion batch log entries.
        drop_obsolete: Also drop retired or non-VALD warehouse tables.
        dry_run: Only show what would be cleaned, do not execute.

    Returns:
        Dict mapping table name to row counts observed before cleanup.
    """
    results: dict[str, int] = {}

    for layer in layers:
        tables = _get_tables_for_layer(layer)
        tables = _filter_by_module(tables, module)

        for table in tables:
            if not _table_exists(db, table):
                logger.info("  SKIP  %s (does not exist)", table)
                continue

            count = _get_row_count(db, table)
            if dry_run:
                logger.info("  [DRY] %s - %d rows would be deleted", table, count)
            else:
                if count > 0:
                    db.execute(f"TRUNCATE TABLE {table} CASCADE")
                    logger.info("  CLEAN %s - %d rows deleted", table, count)
                else:
                    logger.info("  EMPTY %s - already empty", table)
            results[table] = count

    if drop_obsolete:
        results.update(_drop_obsolete_tables(db, layers, module, dry_run))

    if any(layer in layers for layer in ("raw", "bronze")) and _table_exists(db, REPLAY_CURSOR_TABLE):
        replay_tables = _filter_by_module(RAW_TABLES, module)
        if dry_run:
            cursor_count = db.fetch_one(
                f"SELECT COUNT(*) FROM {REPLAY_CURSOR_TABLE} WHERE source_table = ANY(%s::text[])",
                (replay_tables,),
            )
            logger.info(
                "  [DRY] %s - %d VALD replay cursors would be deleted",
                REPLAY_CURSOR_TABLE,
                int(cursor_count[0]) if cursor_count else 0,
            )
        else:
            db.execute(
                f"DELETE FROM {REPLAY_CURSOR_TABLE} WHERE source_table = ANY(%s::text[])",
                (replay_tables,),
            )
            logger.info("  CLEAN %s (%d source tables)", REPLAY_CURSOR_TABLE, len(replay_tables))

    if "silver" in layers and not dry_run:
        if module:
            pattern = MODULE_PATTERNS.get(module, "")
            db.execute(
                "DELETE FROM silver.data_quality_flag WHERE source_table LIKE %s",
                (f"%{pattern}%",),
            )
            logger.info("  CLEAN silver.data_quality_flag (VALD %s flags)", module)
        else:
            db.execute(
                "DELETE FROM silver.data_quality_flag WHERE source_table LIKE %s",
                ("%vald%",),
            )
            logger.info("  CLEAN silver.data_quality_flag (all VALD flags)")

    if reset_watermarks:
        if dry_run:
            wm_count = db.fetch_one(
                "SELECT COUNT(*) FROM raw.sync_watermark WHERE provider = 'vald'"
            )
            logger.info(
                "  [DRY] raw.sync_watermark - %d VALD watermarks would be reset to %s",
                wm_count[0],
                VALD_CUTOFF_UTC,
            )
        else:
            if module:
                db.execute(
                    """
                    UPDATE raw.sync_watermark
                    SET last_watermark = %s,
                        last_sync_started = now(),
                        last_sync_completed = now(),
                        last_sync_status = 'completed',
                        records_synced = 0,
                        updated_at = now()
                    WHERE provider = 'vald'
                      AND api_name LIKE %s
                    """,
                    (VALD_CUTOFF_UTC, f"%{module}%"),
                )
                logger.info("  RESET raw.sync_watermark (VALD %s -> %s)", module, VALD_CUTOFF_UTC)
            else:
                db.execute(
                    """
                    UPDATE raw.sync_watermark
                    SET last_watermark = %s,
                        last_sync_started = now(),
                        last_sync_completed = now(),
                        last_sync_status = 'completed',
                        records_synced = 0,
                        updated_at = now()
                    WHERE provider = 'vald'
                    """,
                    (VALD_CUTOFF_UTC,),
                )
                logger.info("  RESET raw.sync_watermark (all VALD -> %s)", VALD_CUTOFF_UTC)

    if reset_batches:
        if dry_run:
            batch_count = db.fetch_one(
                "SELECT COUNT(*) FROM raw.ingestion_batch_log WHERE provider = 'vald'"
            )
            logger.info(
                "  [DRY] raw.ingestion_batch_log - %d VALD batches would be deleted",
                batch_count[0],
            )
        else:
            db.execute("DELETE FROM raw.ingestion_batch_log WHERE provider = 'vald'")
            logger.info("  CLEAN raw.ingestion_batch_log (all VALD)")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean VALD tables across data layers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python script/clean_vald_tables.py --dry-run                     # Preview what would be cleaned
  python script/clean_vald_tables.py --layers raw                  # Clean only raw layer
  python script/clean_vald_tables.py --layers bronze,silver        # Clean bronze + silver
  python script/clean_vald_tables.py --layers all                  # Clean everything
  python script/clean_vald_tables.py --layers all --reset-watermarks  # Clean all + reset watermarks
  python script/clean_vald_tables.py --layers bronze --module forcedecks  # Clean only ForceDecks bronze
  python script/clean_vald_tables.py --layers raw,bronze --drop-obsolete  # Drop retired VALD tables
        """,
    )
    parser.add_argument(
        "--layers",
        type=str,
        default="all",
        help='Comma-separated layers to clean: raw, bronze, silver, gold, or "all" (default: all)',
    )
    parser.add_argument(
        "--module",
        type=str,
        default=None,
        help="Only clean tables for a specific module (forcedecks, forceframe, nordbord, humantrak, smartspeed, dynamo)",
    )
    parser.add_argument(
        "--reset-watermarks",
        action="store_true",
        help="Also delete VALD sync watermarks (forces full re-sync on next run)",
    )
    parser.add_argument(
        "--reset-batches",
        action="store_true",
        help="Also delete VALD ingestion batch log entries",
    )
    parser.add_argument(
        "--drop-obsolete",
        action="store_true",
        help="Also drop retired VALD raw/bronze tables that are no longer ingested",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be cleaned without actually deleting anything",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )
    args = parser.parse_args()

    if args.layers.strip().lower() == "all":
        layers = ALL_LAYERS
    else:
        layers = [layer.strip().lower() for layer in args.layers.split(",")]
        invalid = [layer for layer in layers if layer not in ALL_LAYERS]
        if invalid:
            logger.error("Invalid layer(s): %s. Options: %s", invalid, ALL_LAYERS)
            sys.exit(1)

    logger.info("=" * 60)
    logger.info("VALD Table Cleaner")
    logger.info("=" * 60)
    logger.info("Layers:     %s", ", ".join(layers))
    logger.info("Module:     %s", args.module or "(all)")
    logger.info("Watermarks: %s", "RESET" if args.reset_watermarks else "keep")
    logger.info("Batches:    %s", "RESET" if args.reset_batches else "keep")
    logger.info("Obsolete:   %s", "DROP" if args.drop_obsolete else "keep")
    logger.info("Mode:       %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("-" * 60)

    if not args.dry_run and not args.yes:
        answer = input(
            f"\n  This will PERMANENTLY DELETE data from {', '.join(layers)} layers. Continue? [y/N] "
        )
        if answer.strip().lower() not in ("y", "yes"):
            logger.info("Aborted.")
            return

    db = DatabaseManager(get_db_config())

    try:
        results = clean_tables(
            db=db,
            layers=layers,
            module=args.module,
            reset_watermarks=args.reset_watermarks,
            reset_batches=args.reset_batches,
            drop_obsolete=args.drop_obsolete,
            dry_run=args.dry_run,
        )

        total_rows = sum(count for count in results.values() if count > 0)
        tables_cleaned = sum(1 for count in results.values() if count > 0)
        obsolete_tables = sum(1 for table in results if table in OBSOLETE_TABLES)

        logger.info("-" * 60)
        if args.dry_run:
            logger.info(
                "DRY RUN: %d tables with %d total rows would be cleaned; %d obsolete tables would be dropped",
                tables_cleaned,
                total_rows,
                obsolete_tables,
            )
        else:
            logger.info(
                "Done: %d tables cleaned, %d total rows deleted, %d obsolete tables dropped",
                tables_cleaned,
                total_rows,
                obsolete_tables,
            )
        logger.info("=" * 60)
    finally:
        db.close()


if __name__ == "__main__":
    main()
