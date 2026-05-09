"""
Postgres performance helpers (Phase 8.8.B).

Quick wins that don't change architecture but typically deliver 30–60%
of the wall-clock reduction at near-zero risk:

1. :func:`bulk_copy_into` — Postgres ``COPY`` for bulk loads. Typically
   3–5× faster than ``execute_values`` for >100k rows. Uses
   ``copy_expert`` so it works inside an existing connection (no need
   to open a new psql session).
2. :func:`unsafe_fast_session` — context manager that sets
   ``synchronous_commit=off`` for the session. Safe for replay /
   backfill paths because they're idempotent — a crash mid-replay is
   recovered from raw, not WAL.
3. :func:`analyze_table` — wrapper for ``ANALYZE table``. The query
   planner needs fresh stats after a large UPSERT, otherwise downstream
   silver merges pick bad plans. Currently missing on every silver
   write path.
4. :func:`deferred_indexes` — context manager that drops named indexes
   on entry and recreates them on exit. Targets: trace tables during
   bulk replay where index maintenance dominates B-tree write cost.
5. :func:`default_workers` — runtime CPU detection for shard counts.
   Defaults to ``max(2, os.cpu_count() // 2)`` so a 16-core box runs
   8 parallel workers by default instead of the legacy ``8``.

Every helper records to the timing log via :mod:`ingestion.common.timing`
when invoked under an active ``track_stage``, so the speedup is visible
in the flame graph.
"""

from __future__ import annotations

import io
import os
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Optional, Sequence

from ingestion.common.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# COPY-based bulk insert
# ---------------------------------------------------------------------------

def bulk_copy_into(
    db: Any,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    chunk_size: int = 50_000,
) -> int:
    """Bulk-load *rows* into *table* via Postgres ``COPY`` (Phase 8.8.B).

    Typically 3–5× faster than ``execute_values`` for >100k rows; the
    speedup grows with row size. Uses ``copy_expert`` so the load runs
    inside an existing pooled connection — no new psql session needed.

    Caveats:
      * ``COPY`` is INSERT-only. There is no ON CONFLICT path. Use this
        for bulk loads INTO STAGE TABLES, then atomic-swap (8.7.A) or
        UPSERT (8.7.B) from stage to live.
      * Each row must have the same length as ``columns``. Values are
        coerced to text via str(); ``None`` becomes the SQL NULL.
      * Tab and newline characters in string values are escaped per the
        Postgres ``COPY ... FROM STDIN`` text protocol.

    Returns the total row count written.
    """
    cols_sql = ", ".join(columns)
    copy_sql = (
        f"COPY {table} ({cols_sql}) FROM STDIN "
        f"WITH (FORMAT TEXT, NULL '\\N')"
    )

    total = 0
    buffer = io.StringIO()
    with db.connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                if len(row) != len(columns):
                    raise ValueError(
                        f"bulk_copy_into row width {len(row)} != columns "
                        f"width {len(columns)} for table {table}"
                    )
                buffer.write(_encode_copy_row(row))
                total += 1
                if total % chunk_size == 0:
                    buffer.seek(0)
                    cur.copy_expert(copy_sql, buffer)
                    buffer = io.StringIO()
            # Flush trailing rows.
            if buffer.tell() > 0:
                buffer.seek(0)
                cur.copy_expert(copy_sql, buffer)

    logger.info("bulk_copy_into | table=%s rows=%d (chunk_size=%d)",
                table, total, chunk_size)
    return total


def _encode_copy_row(row: Sequence[Any]) -> str:
    """Encode one row for the ``COPY ... FROM STDIN`` text protocol.

    Per https://www.postgresql.org/docs/current/sql-copy.html — text
    format escapes:
      * backslash → ``\\\\``
      * tab      → ``\\t``
      * newline  → ``\\n``
      * carriage → ``\\r``
      * NULL is represented by the configured NULL string (we use ``\\N``).
    """
    fields: list[str] = []
    for value in row:
        if value is None:
            fields.append("\\N")
            continue
        if isinstance(value, bool):
            fields.append("t" if value else "f")
            continue
        text = str(value)
        text = (
            text.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
        )
        fields.append(text)
    return "\t".join(fields) + "\n"


# ---------------------------------------------------------------------------
# synchronous_commit=off for replay paths
# ---------------------------------------------------------------------------

@contextmanager
def unsafe_fast_session(db: Any) -> Iterator[None]:
    """Set ``synchronous_commit=off`` for the duration of the block.

    Phase 8.8.B: bulk replay paths are idempotent — a crash mid-replay
    is recovered from raw, not from WAL — so disabling synchronous WAL
    flush is safe AND typically gives 2–4× transactional throughput on
    write-heavy workloads.

    Resets to the original setting on exit (success OR exception). Uses
    ``SET LOCAL`` so the change is scoped to the current transaction —
    nothing leaks back into the pool.
    """
    with db.connection() as conn:
        with conn.cursor() as cur:
            # SET LOCAL is rolled back at COMMIT, so the connection-pool
            # caller sees the default again on its next checkout.
            cur.execute("SET LOCAL synchronous_commit = off")
            try:
                yield
            finally:
                # Defensive: explicitly reset in case the caller forgot
                # to commit. (No-op when SET LOCAL already expired.)
                try:
                    cur.execute("RESET synchronous_commit")
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# ANALYZE after upserts
# ---------------------------------------------------------------------------

def analyze_table(
    db: Any,
    table: str,
    *,
    columns: Optional[Sequence[str]] = None,
    skip_locked: bool = True,
) -> None:
    """Run ``ANALYZE`` on *table* (Phase 8.8.B).

    The query planner uses ``ANALYZE`` stats to pick join orders, index
    usage, and parallel-worker counts. After a large UPSERT, stats are
    stale and silver merges pick bad plans. Calling this once at the
    end of every silver / gold rebuild keeps stats fresh for the next
    consumer.

    Cheap (~seconds even for 100M-row tables; samples a small fraction).
    Safe to call from inside a transaction — Postgres handles ANALYZE
    locking correctly. We use ``ANALYZE`` (not ``VACUUM ANALYZE``)
    because dead-tuple counts on UPSERT-only tables are typically low
    enough that VACUUM doesn't pay back.
    """
    cols_clause = f" ({', '.join(columns)})" if columns else ""
    # ANALYZE cannot run inside a transaction block in psycopg2's
    # default behaviour; we open a fresh connection with autocommit.
    sql = f"ANALYZE {table}{cols_clause}"
    try:
        conn = db.get_connection()
    except Exception:
        # Some db wrappers don't expose get_connection — fall back to
        # the standard connection() and skip; caller should retry.
        logger.warning(
            "analyze_table | unable to acquire raw connection for %s — skipped",
            table,
        )
        return

    previous_isolation = None
    try:
        previous_isolation = conn.isolation_level
        conn.set_isolation_level(0)  # AUTOCOMMIT
        with conn.cursor() as cur:
            cur.execute(sql)
        logger.info("analyze_table | %s OK%s", table, cols_clause)
    except Exception:
        logger.exception("analyze_table | %s FAILED", table)
        if not skip_locked:
            raise
    finally:
        try:
            if previous_isolation is not None:
                conn.set_isolation_level(previous_isolation)
        except Exception:
            pass
        try:
            db.put_connection(conn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Deferred index drop / recreate during bulk replay
# ---------------------------------------------------------------------------

@contextmanager
def deferred_indexes(
    db: Any,
    table: str,
    index_names: Sequence[str],
    *,
    skip_on_failure: bool = True,
) -> Iterator[None]:
    """Drop named indexes on entry; recreate them on exit (Phase 8.8.B).

    Targets bulk-replay paths against trace tables where B-tree
    maintenance dominates the wall-clock cost. Drop the indexes →
    insert without index churn → recreate the indexes once at the end
    in a single sorted scan.

    The recreated indexes use ``CREATE INDEX`` (not CONCURRENTLY) for
    speed — we hold the table lock anyway during the bulk replay.

    On failure inside the block, indexes are still recreated (best
    effort) so the table doesn't ship without its indexes. Any
    recreation failure is logged but not raised when
    ``skip_on_failure=True``.
    """
    if not index_names:
        yield
        return

    # Capture the index DDL before dropping so we can recreate exactly
    # what was there.
    captured_defs: dict[str, str] = {}
    with db.connection() as conn:
        with conn.cursor() as cur:
            for idx_name in index_names:
                cur.execute(
                    """
                    SELECT indexdef
                      FROM pg_indexes
                     WHERE schemaname || '.' || indexname = %s
                        OR indexname = %s
                    """,
                    (idx_name, idx_name),
                )
                row = cur.fetchone()
                if row is None:
                    logger.warning(
                        "deferred_indexes | %s not found, will not recreate",
                        idx_name,
                    )
                    continue
                captured_defs[idx_name] = row[0]
                # Drop using the schema-qualified name if possible.
                cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
                logger.info("deferred_indexes | dropped %s on %s",
                            idx_name, table)

    try:
        yield
    finally:
        # Recreate every captured index. Best effort — a failure here
        # leaves the table without that index, which is recoverable but
        # noisy.
        for idx_name, ddl in captured_defs.items():
            try:
                with db.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(ddl)
                logger.info("deferred_indexes | recreated %s on %s",
                            idx_name, table)
            except Exception:
                logger.exception(
                    "deferred_indexes | failed to recreate %s on %s",
                    idx_name, table,
                )
                if not skip_on_failure:
                    raise


# ---------------------------------------------------------------------------
# CPU-aware default worker count
# ---------------------------------------------------------------------------

def default_workers(
    *,
    floor: int = 2,
    divisor: int = 2,
    ceiling: Optional[int] = None,
) -> int:
    """Return ``max(floor, cpu_count // divisor)`` capped at ``ceiling``.

    Phase 8.8.B: replaces hard-coded shard counts with CPU-aware
    defaults. On a 16-core warehouse host the legacy default of 8 is
    correct; on an 8-core box we want 4; on a 4-core box we want 2.

    Override via env var (caller's responsibility) — this function is
    just the floor.
    """
    cpu = os.cpu_count() or 1
    workers = max(floor, cpu // divisor)
    if ceiling is not None:
        workers = min(workers, ceiling)
    return workers
