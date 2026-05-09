"""
Reset all pipeline-managed tables across the raw, bronze, silver, and gold schemas.

This script is intended for destructive end-to-end reset workflows, such as
re-running ingestion from a clean database state to verify duplicate handling.
It truncates every base table discovered in the selected schemas, including
ingestion metadata tables like ``raw.sync_watermark`` and
``raw.ingestion_batch_log`` by default.

Usage::

    python script/clean_database.py --dry-run
    python script/clean_database.py --yes
    python script/clean_database.py --schemas raw,bronze --count-mode exact --yes
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

from ingestion.common.config import get_db_config
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

DEFAULT_SCHEMA_ORDER = ("gold", "silver", "bronze", "raw")
VALID_SCHEMAS = set(DEFAULT_SCHEMA_ORDER)
VALID_COUNT_MODES = {"estimate", "exact", "none"}


@dataclass(frozen=True)
class TableInfo:
    """Describe a table targeted by the reset script."""

    schema: str
    name: str
    row_count: int | None = None

    @property
    def qualified_name(self) -> str:
        return qualify_identifier(self.schema, self.name)


def qualify_identifier(schema: str, name: str) -> str:
    """Return a safely quoted fully-qualified SQL identifier."""
    safe_schema = schema.replace('"', '""')
    safe_name = name.replace('"', '""')
    return f'"{safe_schema}"."{safe_name}"'


def parse_schemas(raw_value: str) -> list[str]:
    """Parse and validate the requested schema list."""
    value = raw_value.strip().lower()
    if value == "all":
        return list(DEFAULT_SCHEMA_ORDER)

    schemas: list[str] = []
    seen: set[str] = set()
    for chunk in raw_value.split(","):
        schema = chunk.strip().lower()
        if not schema:
            continue
        if schema not in VALID_SCHEMAS:
            msg = f"Invalid schema '{schema}'. Options: {list(DEFAULT_SCHEMA_ORDER)}"
            raise ValueError(msg)
        if schema not in seen:
            seen.add(schema)
            schemas.append(schema)

    if not schemas:
        msg = "No schemas selected. Use 'all' or a comma-separated list."
        raise ValueError(msg)

    return schemas


def order_tables(tables: list[TableInfo]) -> list[TableInfo]:
    """Sort tables in downstream-to-upstream schema order."""
    rank = {schema: index for index, schema in enumerate(DEFAULT_SCHEMA_ORDER)}
    return sorted(
        tables,
        key=lambda table: (rank.get(table.schema, len(rank)), table.schema, table.name),
    )


def build_truncate_sql(
    tables: list[TableInfo],
    restart_identity: bool = True,
    cascade: bool = True,
) -> str:
    """Build a single TRUNCATE statement for the selected tables."""
    if not tables:
        raise ValueError("No tables provided for truncation")

    qualified_tables = ", ".join(table.qualified_name for table in order_tables(tables))
    parts = [f"TRUNCATE TABLE {qualified_tables}"]
    if restart_identity:
        parts.append("RESTART IDENTITY")
    if cascade:
        parts.append("CASCADE")
    return " ".join(parts)


def fetch_tables(db: Any, schemas: list[str], count_mode: str) -> list[TableInfo]:
    """Discover base tables in the requested schemas."""
    placeholders = ", ".join(["%s"] * len(schemas))
    rank_cases = " ".join(
        f"WHEN %s THEN {index}"
        for index, _ in enumerate(DEFAULT_SCHEMA_ORDER, start=1)
    )

    sql = f"""
        SELECT
            t.table_schema,
            t.table_name,
            COALESCE(s.n_live_tup::bigint, 0) AS estimated_rows
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s
            ON s.schemaname = t.table_schema
           AND s.relname = t.table_name
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema IN ({placeholders})
        ORDER BY
            CASE t.table_schema {rank_cases} ELSE 999 END,
            t.table_name
    """
    params: tuple[Any, ...] = tuple(schemas) + tuple(DEFAULT_SCHEMA_ORDER)
    rows = db.fetch_all(sql, params)

    tables = [
        TableInfo(
            schema=str(row[0]),
            name=str(row[1]),
            row_count=None if count_mode == "none" else int(row[2]),
        )
        for row in rows
    ]

    if count_mode == "exact":
        tables = with_exact_counts(db, tables)

    return order_tables(tables)


def with_exact_counts(db: Any, tables: list[TableInfo]) -> list[TableInfo]:
    """Replace approximate counts with exact ``COUNT(*)`` values."""
    exact_tables: list[TableInfo] = []
    for table in tables:
        row = db.fetch_one(f"SELECT COUNT(*) FROM {table.qualified_name}")
        exact_tables.append(
            TableInfo(
                schema=table.schema,
                name=table.name,
                row_count=int(row[0]) if row else 0,
            )
        )
    return exact_tables


def format_row_count(row_count: int | None) -> str:
    """Render a human-readable row-count label."""
    if row_count is None:
        return "rows=?"
    return f"rows={row_count}"


def log_plan(tables: list[TableInfo], dry_run: bool) -> None:
    """Log the tables targeted by the reset."""
    prefix = "[DRY]" if dry_run else "PLAN"
    for table in tables:
        logger.info("  %s %s %s", prefix, table.qualified_name, format_row_count(table.row_count))


def reset_database(db: Any, tables: list[TableInfo], dry_run: bool) -> None:
    """Truncate all selected tables inside a single transaction."""
    if dry_run or not tables:
        return

    sql = build_truncate_sql(tables)
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the CLI parser for the reset script."""
    parser = argparse.ArgumentParser(
        description="Reset all pipeline-managed database tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python script/clean_database.py --dry-run
  python script/clean_database.py --yes
  python script/clean_database.py --schemas raw,bronze --count-mode exact --yes
        """,
    )
    parser.add_argument(
        "--schemas",
        type=str,
        default="all",
        help='Schemas to clean: "all" or a comma-separated subset of raw, bronze, silver, gold',
    )
    parser.add_argument(
        "--count-mode",
        type=str,
        default="estimate",
        help="Row-count mode: estimate, exact, or none (default: estimate)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be truncated without executing it",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip the confirmation prompt",
    )
    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        schemas = parse_schemas(args.schemas)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    count_mode = args.count_mode.strip().lower()
    if count_mode not in VALID_COUNT_MODES:
        logger.error("Invalid count mode '%s'. Options: %s", count_mode, sorted(VALID_COUNT_MODES))
        sys.exit(1)

    logger.info("=" * 72)
    logger.info("Database Reset")
    logger.info("=" * 72)
    logger.info("Schemas:    %s", ", ".join(schemas))
    logger.info("Count mode: %s", count_mode)
    logger.info("Mode:       %s", "DRY RUN" if args.dry_run else "LIVE")
    logger.info("-" * 72)

    from ingestion.common.db import DatabaseManager

    db = DatabaseManager(get_db_config())
    try:
        tables = fetch_tables(db, schemas, count_mode)
        if not tables:
            logger.warning("No tables found in the selected schemas.")
            return

        total_rows = sum(table.row_count or 0 for table in tables if table.row_count is not None)
        logger.info("Discovered %d table(s); total %s", len(tables), format_row_count(total_rows if count_mode != "none" else None))
        log_plan(tables, dry_run=args.dry_run)

        if not args.dry_run and not args.yes:
            answer = input(
                "\n  This will PERMANENTLY TRUNCATE the selected schemas and reset identities. Continue? [y/N] "
            )
            if answer.strip().lower() not in {"y", "yes"}:
                logger.info("Aborted.")
                return

        reset_database(db, tables, dry_run=args.dry_run)
        if args.dry_run:
            logger.info("Dry run complete.")
        else:
            logger.info("Database reset complete. Truncated %d table(s).", len(tables))
    finally:
        db.close()


if __name__ == "__main__":
    main()
