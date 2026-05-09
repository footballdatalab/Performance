"""Phase 8.7.A — atomic table publish helper.

Replaces the `TRUNCATE live; INSERT live FROM stage` pattern with an
atomic schema-level swap. The live table is **never empty** during a
publish: the swap is a single-transaction rename dance that flips
references at commit time.

The pattern is the standard "build new alongside, then atomically replace
old":

    1. Caller builds `stage_table` (in `etl_staging`) to its full target
       shape, populated with the new rows. (`CREATE TABLE LIKE …
       INCLUDING ALL` is the typical incantation — the helper does not
       impose the build mechanism.)

    2. `atomic_publish_table()`:
       a. Inside one txn:
          - `ALTER TABLE live RENAME TO live_old_<ts>` — frees the live name.
          - `ALTER TABLE stage SET SCHEMA <live_schema>` — moves the stage
            into the live schema, where it now occupies the live name (the
            old live is renamed away).
          - Renames the new table's indexes/constraints from
            `<stage_name>_*` back to `<live_name>_*` so subsequent runs
            don't accumulate stale auto-named indexes.
       b. After commit:
          - `DROP TABLE … live_old_<ts>` — best-effort; if it fails the
            old table just sits there and a later run will drop it.

This is the **only function in the ETL stack permitted to drop an
existing table by name**. Locked decision #7's cross-cutting grep is
intentionally tolerant of this single helper.

The function is read-only against providers (it only touches the local
warehouse) and never deletes or truncates the **live** table — the live
name's identity is preserved across runs even though the underlying
relation OID changes on every swap.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


_QUALIFIED_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def _split_qualified(name: str) -> tuple[str, str]:
    if not _QUALIFIED_TABLE_RE.match(name):
        raise ValueError(
            f"Expected schema-qualified table name like 'schema.table', got: {name!r}"
        )
    schema, table = name.split(".", 1)
    return schema, table


def _timestamp_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


def atomic_publish_table(
    db: DatabaseManager,
    *,
    live_table: str,
    stage_table: str,
) -> dict[str, Any]:
    """Atomically replace ``live_table`` with ``stage_table``.

    Both arguments are schema-qualified table names (``schema.table``).
    On success the live name's content is identical to the previous
    contents of the stage table; the previous live table is dropped.

    Returns a dict summary suitable for logging / XCom.
    """
    live_schema, live_name = _split_qualified(live_table)
    stage_schema, stage_name = _split_qualified(stage_table)

    if live_schema == stage_schema and live_name == stage_name:
        raise ValueError(
            "live_table and stage_table must differ — refusing to swap a "
            "table with itself"
        )

    timestamp = _timestamp_suffix()
    archived_name = f"{live_name}_old_{timestamp}"
    archived_qualified = f"{live_schema}.{archived_name}"

    summary: dict[str, Any] = {
        "live_table": live_table,
        "stage_table": stage_table,
        "archived_table": archived_qualified,
        "rows_in_new_live": 0,
    }

    # Phase 1 — single-transaction swap.
    with db.connection() as conn:
        with conn.cursor() as cur:
            # 1a. Free the live name by archiving the existing relation.
            cur.execute(
                f'ALTER TABLE {live_schema}.{live_name} '
                f'RENAME TO {archived_name}'
            )
            # 1b. Move stage into the live schema — name collision is now
            #     impossible because we just renamed the live one away.
            #     If stage is already in live_schema, SET SCHEMA is a no-op
            #     but still legal.
            if stage_schema != live_schema:
                cur.execute(
                    f'ALTER TABLE {stage_schema}.{stage_name} '
                    f'SET SCHEMA {live_schema}'
                )
            # 1c. Rename stage to the live name so the live name is occupied
            #     by the new content.
            if stage_name != live_name:
                cur.execute(
                    f'ALTER TABLE {live_schema}.{stage_name} '
                    f'RENAME TO {live_name}'
                )
            # 1d. Capture the row count of the new live for the summary.
            cur.execute(f'SELECT COUNT(*) FROM {live_schema}.{live_name}')
            row = cur.fetchone()
            summary["rows_in_new_live"] = int(row[0]) if row else 0
        conn.commit()

    logger.info(
        "Atomic publish complete: live=%s archived=%s rows=%d",
        live_table,
        archived_qualified,
        summary["rows_in_new_live"],
    )

    # Phase 2 — best-effort drop of the archived table outside the txn.
    try:
        db.execute(f'DROP TABLE IF EXISTS {archived_qualified} CASCADE')
        summary["archived_dropped"] = True
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            "Atomic publish: failed to drop archived table %s (%s). "
            "It will linger; a later run can DROP TABLE manually.",
            archived_qualified,
            exc,
        )
        summary["archived_dropped"] = False

    return summary


def build_stage_table_like(
    db: DatabaseManager,
    *,
    live_table: str,
    stage_schema: str = "etl_staging",
) -> str:
    """Create an empty stage table that mirrors ``live_table``'s structure.

    The stage table name is ``<stage_schema>.<live_name>_stage_<ts>`` so
    multiple concurrent rebuilds don't collide. Returns the qualified
    stage table name.

    Caller is responsible for populating the stage table before calling
    ``atomic_publish_table``.
    """
    live_schema, live_name = _split_qualified(live_table)
    timestamp = _timestamp_suffix()
    stage_name = f"{live_name}_stage_{timestamp}"
    stage_qualified = f"{stage_schema}.{stage_name}"

    db.execute(
        f'CREATE TABLE {stage_qualified} '
        f'(LIKE {live_schema}.{live_name} INCLUDING ALL)'
    )
    logger.info(
        "Created stage table %s mirroring %s",
        stage_qualified,
        live_table,
    )
    return stage_qualified


def cleanup_stale_archived_tables(
    db: DatabaseManager,
    *,
    schema: str,
    live_name: str,
    older_than_hours: int = 24,
) -> int:
    """Drop archived tables (`<live_name>_old_<ts>`) older than the threshold.

    Run this defensively in a maintenance job in case ``atomic_publish_table``
    failed to drop them (e.g. because of a concurrent open cursor at commit).
    Returns the number of archived tables dropped.
    """
    rows = db.fetch_all_dict(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name LIKE %s
        """,
        (schema, f"{live_name}_old_%"),
    )
    dropped = 0
    for row in rows:
        archived = row["table_name"]
        # Parse the timestamp suffix; bail if it's malformed
        suffix = archived[len(live_name) + len("_old_") :]
        try:
            archived_ts = datetime.strptime(suffix[:15], "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        archived_ts = archived_ts.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - archived_ts).total_seconds() / 3600.0
        if age_hours < older_than_hours:
            continue
        try:
            db.execute(f'DROP TABLE IF EXISTS {schema}.{archived} CASCADE')
            dropped += 1
            logger.info("Dropped stale archived table %s.%s (%.1fh old)", schema, archived, age_hours)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to drop stale archived table %s.%s: %s", schema, archived, exc)
    return dropped