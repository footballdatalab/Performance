"""
Drop Catapult tables across raw, bronze, silver, and gold.

This is a destructive cleanup utility that removes every Catapult-prefixed
table in the raw, bronze, silver, and gold schemas, including monthly bronze
partitions. Optionally clears Catapult-scoped rows from the shared ingestion
metadata tables (``raw.sync_watermark`` and ``raw.ingestion_batch_log``).

After running, recreate the Catapult schema with ``bootstrap_database``.

Usage::

    python script/clean_catapult_tables.py --dry-run
    python script/clean_catapult_tables.py --yes
    python script/clean_catapult_tables.py --yes --reset-metadata
"""

from __future__ import annotations

import argparse

from ingestion.common.config import get_db_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_CATAPULT_PROVIDER = "catapult"


def discover_catapult_tables(db: DatabaseManager) -> list[str]:
    """Return Catapult tables that should be dropped."""
    rows = db.fetch_all(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('raw', 'bronze', 'silver', 'gold')
          AND table_name LIKE 'catapult%%'
        ORDER BY
            CASE table_schema
                WHEN 'gold' THEN 1
                WHEN 'silver' THEN 2
                WHEN 'bronze' THEN 3
                WHEN 'raw' THEN 4
                ELSE 999
            END,
            length(table_name) DESC,
            table_name
        """
    )
    return [f"{row[0]}.{row[1]}" for row in rows]


def reset_catapult_metadata(db: DatabaseManager, dry_run: bool = False) -> dict[str, int]:
    """Delete Catapult-scoped rows from the shared ingestion metadata tables."""
    results: dict[str, int] = {}
    for table in ("raw.sync_watermark", "raw.ingestion_batch_log"):
        try:
            row = db.fetch_one(
                f"SELECT COUNT(*) FROM {table} WHERE provider = %s",
                (_CATAPULT_PROVIDER,),
            )
            count = int(row[0]) if row else 0
        except Exception:
            logger.info("  SKIP  %s - table missing or not queryable", table)
            results[table] = -1
            continue
        results[table] = count
        if dry_run:
            logger.info("  [DRY] DELETE FROM %s WHERE provider='catapult' - %d rows", table, count)
            continue
        db.execute(
            f"DELETE FROM {table} WHERE provider = %s",
            (_CATAPULT_PROVIDER,),
        )
        logger.info("  DELETE %s - removed %d Catapult rows", table, count)
    return results


def _get_row_count(db: DatabaseManager, full_name: str) -> int:
    """Return the row count for a table when possible."""
    try:
        row = db.fetch_one(f"SELECT COUNT(*) FROM {full_name}")
        return int(row[0]) if row else 0
    except Exception:
        return -1


def drop_catapult_tables(
    db: DatabaseManager,
    dry_run: bool = False,
) -> dict[str, int]:
    """Drop all discovered Catapult tables and return their pre-drop counts."""
    results: dict[str, int] = {}
    tables = discover_catapult_tables(db)

    for table in tables:
        count = _get_row_count(db, table)
        results[table] = count
        if dry_run:
            logger.info("  [DRY] DROP %s - %d rows", table, count)
            continue
        db.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        logger.info("  DROP  %s - removed (%d rows)", table, count)

    return results


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Drop Catapult tables from raw, bronze, silver, and gold",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python script/clean_catapult_tables.py --dry-run
  python script/clean_catapult_tables.py --yes
  python script/clean_catapult_tables.py --yes --reset-metadata
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be dropped without executing it",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )
    parser.add_argument(
        "--reset-metadata",
        action="store_true",
        help=(
            "Also delete Catapult-scoped rows from raw.sync_watermark and "
            "raw.ingestion_batch_log so the next run starts from a fresh watermark."
        ),
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Catapult Table Drop")
    logger.info("=" * 60)
    logger.info("Mode: %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("Reset metadata: %s", args.reset_metadata)
    logger.info("-" * 60)

    if not args.dry_run and not args.yes:
        answer = input(
            "\n  This will PERMANENTLY DROP all Catapult tables in raw, bronze, silver, and gold. Continue? [y/N] "
        )
        if answer.strip().lower() not in {"y", "yes"}:
            logger.info("Aborted.")
            return

    db = DatabaseManager(get_db_config())
    try:
        results = drop_catapult_tables(db=db, dry_run=args.dry_run)
        if args.reset_metadata:
            logger.info("-" * 60)
            reset_catapult_metadata(db=db, dry_run=args.dry_run)
        logger.info("-" * 60)
        if not results:
            logger.info("No Catapult tables found.")
        elif args.dry_run:
            logger.info("DRY RUN: %d Catapult tables would be dropped.", len(results))
        else:
            logger.info("Dropped %d Catapult tables.", len(results))
        logger.info("=" * 60)
    finally:
        db.close()


if __name__ == "__main__":
    main()
