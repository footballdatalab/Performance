"""
ETL stage timing instrumentation (Phase 8.8.A).

Every long-running ETL stage wraps its body in :func:`track_stage` so we can
build a flame-graph-equivalent breakdown of where wall-clock time goes.
The recorded rows live in ``silver.etl_run_timings`` and are the
before/after measurement substrate for sub-phases 8.8.B–8.8.F.

Usage::

    from ingestion.common.timing import make_run_id, track_stage

    run_id = make_run_id()
    with track_stage("vald", "silver.assessment_metric",
                     sub_stage="forcedecks",
                     db=db, run_id=run_id) as metrics:
        rows = build_assessment_metrics(...)
        metrics["rows_written"] = rows

The yielded ``metrics`` dict is mutable; the wrapped block fills in
``rows_read``, ``rows_written``, ``bytes_read``, ``peak_memory_mb``, or
extra free-form keys (these are persisted in the JSONB ``extra``
column). The context manager always persists a row, even if the wrapped
block raises (status='failed'), so failed stages still appear in the
flame graph.

Design notes
------------
* The DB write is best-effort: if the timing INSERT itself fails we log
  and swallow — we never want timing instrumentation to break a
  production ETL run.
* When ``db`` is ``None`` the context manager only logs to stdout. This
  is the right behaviour for unit tests where there's no real DB.
* ``run_id`` is generated once at the top of a pipeline invocation. All
  child stages share it so a SQL ``SELECT … WHERE run_id = $1`` returns
  the full breakdown.
"""

from __future__ import annotations

import contextvars
import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional
from uuid import UUID, uuid4

from ingestion.common.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Run-id helpers
# ---------------------------------------------------------------------------

# Phase 8.8.A: contextvar-threaded run_id so child stages inherit the
# parent's run_id without every function signature having to take it.
# The pattern is:
#
#   with pipeline_run("vald", db=db) as run_id:        # sets contextvar
#       run_silver_etl(db)                              # children fetch via current_run_id()
#
# Child code can either:
#   - pass `run_id=current_run_id()` to track_stage (explicit), or
#   - call track_stage(...) with run_id=None and let it fall back.
_CURRENT_RUN_ID: contextvars.ContextVar[Optional[UUID]] = contextvars.ContextVar(
    "etl_run_id", default=None
)


def make_run_id() -> UUID:
    """Generate a fresh ``run_id`` for one ETL invocation.

    Call this once at the top of your pipeline entry point and thread it
    through every stage. All stages of the same run share the same id so
    the flame graph query (``WHERE run_id = $1``) returns the full
    breakdown.
    """
    return uuid4()


def current_run_id() -> Optional[UUID]:
    """Return the run_id set by the enclosing :func:`pipeline_run`, or None.

    Useful for nested code that wants to record extra timings against
    the same run without threading the id through every signature.
    """
    return _CURRENT_RUN_ID.get()


@contextmanager
def pipeline_run(
    pipeline: str,
    *,
    db: Any = None,
    run_id: Optional[UUID] = None,
    extra: Optional[dict[str, Any]] = None,
) -> Iterator[UUID]:
    """Top-level wrapper for one pipeline invocation.

    Generates a fresh ``run_id`` (or uses the one passed in), sets the
    contextvar so child :func:`track_stage` calls inherit it, and emits
    one timing row tagged ``stage='pipeline.run'`` covering the whole
    invocation.

    Yields the ``run_id`` so the caller can include it in their summary
    payload.
    """
    if run_id is None:
        run_id = make_run_id()
    token = _CURRENT_RUN_ID.set(run_id)
    try:
        with track_stage(
            pipeline,
            "pipeline.run",
            db=db,
            run_id=run_id,
            extra=extra,
        ):
            yield run_id
    finally:
        _CURRENT_RUN_ID.reset(token)


# ---------------------------------------------------------------------------
# Stage tracking
# ---------------------------------------------------------------------------

@contextmanager
def track_stage(
    pipeline: str,
    stage: str,
    *,
    sub_stage: Optional[str] = None,
    db: Any = None,
    run_id: Optional[UUID] = None,
    extra: Optional[dict[str, Any]] = None,
) -> Iterator[dict[str, Any]]:
    """Time a stage and persist the result to ``silver.etl_run_timings``.

    Yields a mutable metrics dict. The wrapped block can fill in
    ``rows_read``, ``rows_written``, ``bytes_read``, ``peak_memory_mb``,
    or arbitrary extra keys (which end up in the JSONB ``extra``
    column). Any exception is re-raised after the timing row is
    persisted with ``status='failed'``.

    Parameters
    ----------
    pipeline : str
        One of 'vald', 'catapult', 'zerozero', 'common'. Validated by
        the table CHECK constraint.
    stage : str
        Stable identifier (e.g. 'silver.assessment_metric').
    sub_stage : str, optional
        Per-family / per-table / per-shard granularity.
    db : DatabaseManager, optional
        The connection pool to write to. When ``None`` the timing is
        only logged. Tests should pass ``None``.
    run_id : UUID, optional
        Run identifier from :func:`make_run_id`. When ``None`` a fresh
        one is generated for this stage only — meaning the stage won't
        be groupable with siblings. Always pass an explicit run_id in
        production.
    extra : dict, optional
        Pre-populated extra fields. The wrapped block can add to it.
    """
    if run_id is None:
        # Phase 8.8.A: fall back to the contextvar set by pipeline_run() so
        # child stages inherit the parent's run_id without threading.
        run_id = _CURRENT_RUN_ID.get() or make_run_id()
    if extra is None:
        extra = {}

    metrics: dict[str, Any] = {
        "rows_read": None,
        "rows_written": None,
        "bytes_read": None,
        "peak_memory_mb": None,
        "extra": extra,
    }

    start_perf = time.perf_counter()
    started_at = datetime.now(tz=timezone.utc)
    status = "success"
    error_message: Optional[str] = None
    try:
        yield metrics
    except BaseException as exc:  # propagate after we record the failure
        status = "failed"
        error_message = repr(exc)[:1000]  # truncate to keep TEXT writes bounded
        raise
    finally:
        elapsed_ms = int((time.perf_counter() - start_perf) * 1000)
        finished_at = datetime.now(tz=timezone.utc)
        _persist_timing(
            db=db,
            run_id=run_id,
            pipeline=pipeline,
            stage=stage,
            sub_stage=sub_stage,
            started_at=started_at,
            finished_at=finished_at,
            elapsed_ms=elapsed_ms,
            status=status,
            metrics=metrics,
            error_message=error_message,
        )


def _persist_timing(
    *,
    db: Any,
    run_id: UUID,
    pipeline: str,
    stage: str,
    sub_stage: Optional[str],
    started_at: datetime,
    finished_at: datetime,
    elapsed_ms: int,
    status: str,
    metrics: dict[str, Any],
    error_message: Optional[str],
) -> None:
    """Best-effort persistence: log + DB write. Never raises."""
    rows_read = metrics.get("rows_read")
    rows_written = metrics.get("rows_written")
    bytes_read = metrics.get("bytes_read")
    peak_memory_mb = metrics.get("peak_memory_mb")
    extra_payload = metrics.get("extra") or {}

    logger.info(
        "etl_timing | run_id=%s pipeline=%s stage=%s sub_stage=%s "
        "elapsed_ms=%d status=%s rows_read=%s rows_written=%s",
        run_id, pipeline, stage, sub_stage, elapsed_ms, status,
        rows_read, rows_written,
    )

    if db is None:
        return

    try:
        db.execute(
            """
            INSERT INTO silver.etl_run_timings (
                run_id, pipeline, stage, sub_stage,
                started_at, finished_at, elapsed_ms, status,
                rows_read, rows_written, bytes_read, peak_memory_mb,
                error_message, extra
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(run_id),
                pipeline,
                stage,
                sub_stage,
                started_at,
                finished_at,
                elapsed_ms,
                status,
                rows_read,
                rows_written,
                bytes_read,
                peak_memory_mb,
                error_message,
                json.dumps(extra_payload, default=str),
            ),
        )
    except Exception:  # pragma: no cover — defensive; instrumentation must not break ETL
        logger.exception(
            "etl_timing | failed to persist row for pipeline=%s stage=%s",
            pipeline, stage,
        )


# ---------------------------------------------------------------------------
# Read helpers (flame-graph queries)
# ---------------------------------------------------------------------------

def summarize_run(db: Any, run_id: UUID | str) -> list[dict[str, Any]]:
    """Return per-stage breakdown for ``run_id`` ordered by ``elapsed_ms`` desc.

    Used as a quick "where did the time go?" question after a pipeline
    run completes. Equivalent SQL::

        SELECT pipeline, stage, sub_stage, elapsed_ms, status,
               rows_read, rows_written
          FROM silver.etl_run_timings
         WHERE run_id = $1
         ORDER BY elapsed_ms DESC;
    """
    rows = db.fetch_all_dict(
        """
        SELECT pipeline, stage, sub_stage, elapsed_ms, status,
               rows_read, rows_written, started_at, finished_at,
               error_message, extra
          FROM silver.etl_run_timings
         WHERE run_id = %s
         ORDER BY elapsed_ms DESC
        """,
        (str(run_id),),
    )
    return rows


def recent_pipeline_summary(
    db: Any,
    pipeline: str,
    *,
    limit_runs: int = 10,
) -> list[dict[str, Any]]:
    """Return p50 / p95 / max elapsed_ms per stage across the last N runs.

    Used as a baseline / regression check: did this run land in the
    expected percentile, or is something off?
    """
    rows = db.fetch_all_dict(
        """
        WITH recent_runs AS (
            SELECT DISTINCT run_id
              FROM silver.etl_run_timings
             WHERE pipeline = %s
             ORDER BY run_id DESC
             LIMIT %s
        )
        SELECT t.stage,
               t.sub_stage,
               COUNT(*)                                    AS run_count,
               PERCENTILE_DISC(0.5)
                   WITHIN GROUP (ORDER BY t.elapsed_ms)    AS p50_ms,
               PERCENTILE_DISC(0.95)
                   WITHIN GROUP (ORDER BY t.elapsed_ms)    AS p95_ms,
               MAX(t.elapsed_ms)                           AS max_ms,
               SUM(t.rows_written)                         AS total_rows_written
          FROM silver.etl_run_timings t
          JOIN recent_runs r ON r.run_id = t.run_id
         WHERE t.pipeline = %s
           AND t.status = 'success'
         GROUP BY t.stage, t.sub_stage
         ORDER BY p95_ms DESC
        """,
        (pipeline, limit_runs, pipeline),
    )
    return rows
