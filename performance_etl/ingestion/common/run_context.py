"""
RunContext + ReferenceCache (Phase 8.8.C).

A ``RunContext`` carries shared state for a single ETL invocation:
  * ``run_id`` — the Phase 8.8.A timing identifier.
  * ``db`` — the connection pool.
  * ``cache`` — process-local in-memory snapshots of reference data
    (target_groups, profile metadata, parameter definitions, etc).
    Built lazily on first read; reused across every stage of the run.

Pattern
-------
::

    from ingestion.common.run_context import RunContext

    with RunContext.start("vald", db=db) as ctx:
        # Children of the same run share the cache:
        target_groups = ctx.cache.get_or_load(
            "vald.target_groups",
            lambda: db.fetch_all_dict("SELECT * FROM bronze.vald_target_groups"),
        )
        # Subsequent calls return the cached value immediately.

The cache is **process-local**: there is no Redis, no shared
inter-process cache. Memory footprint is bounded by the largest single
loader's payload (each cached snapshot is one Python list / dict). For
FC Porto-scale data (thousands of profiles, 100s of target groups, low
thousands of parameter definitions) this is well under 50 MB even at
peak.

The cache is **per-run**: a new run always starts with an empty cache.
This guarantees freshness — we never serve stale reference data from a
prior invocation. The trade-off is a small re-load cost at the start
of every run, but those loads are quick (milliseconds for the dim
tables, single-digit seconds for the largest).

Threadsafe enough for our usage: the load functions are guarded by a
per-key ``threading.Lock`` so multiple workers asking for the same key
concurrently get one DB hit (not N).

Reading the cache through the context manager + ``get_or_load`` pattern
also makes 8.8.A timings see the cached-vs-fetched distinction: a cache
miss fires a ``track_stage("cache.load")`` row, a hit doesn't fire
anything. The difference between runs becomes visible.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional
from uuid import UUID

from ingestion.common.logging import get_logger
from ingestion.common.timing import current_run_id, make_run_id, pipeline_run, track_stage

logger = get_logger(__name__)


class ReferenceCache:
    """Process-local cache for reference data within a single ETL run.

    Loaders are lazy: nothing is fetched until ``get_or_load`` is called.
    Subsequent calls with the same key return the cached value without
    invoking the loader again.

    Internally guarded by per-key locks so concurrent workers don't
    duplicate the DB read for the same key.
    """

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()
        self._stats: dict[str, dict[str, int]] = {}

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Any],
        *,
        db: Any = None,
    ) -> Any:
        """Return the cached value for *key*, loading via *loader* on miss.

        ``db`` is used only for the timing log. Without it we still
        cache, just without per-load Postgres row attribution.
        """
        # Fast path: already cached. No locking needed because dict
        # reads are atomic in CPython.
        if key in self._values:
            self._record_hit(key)
            return self._values[key]

        # Slow path: take per-key lock so duplicate concurrent loads are
        # collapsed into one.
        lock = self._get_lock(key)
        with lock:
            if key in self._values:
                # Another thread won the race.
                self._record_hit(key)
                return self._values[key]
            with track_stage(
                "common", "cache.load", sub_stage=key, db=db,
            ) as metrics:
                value = loader()
                self._values[key] = value
                # Best-effort row count for the timing log.
                try:
                    metrics["rows_read"] = len(value) if hasattr(value, "__len__") else None
                except Exception:
                    metrics["rows_read"] = None
            self._record_miss(key)
            return value

    def invalidate(self, key: str) -> None:
        """Drop a single key — useful between stages that mutate refs."""
        self._values.pop(key, None)

    def stats(self) -> dict[str, dict[str, int]]:
        """Per-key hit/miss counts (useful in run summaries)."""
        return {k: dict(v) for k, v in self._stats.items()}

    def _get_lock(self, key: str) -> threading.Lock:
        with self._global_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def _record_hit(self, key: str) -> None:
        bucket = self._stats.setdefault(key, {"hits": 0, "misses": 0})
        bucket["hits"] += 1

    def _record_miss(self, key: str) -> None:
        bucket = self._stats.setdefault(key, {"hits": 0, "misses": 0})
        bucket["misses"] += 1


@dataclass
class RunContext:
    """One per ETL invocation. Carries db + cache + run_id together.

    Stages can either accept a RunContext explicitly or pull the cache
    from a thread-local sidecar (see :func:`current_run_context`).
    """

    pipeline: str
    run_id: UUID
    db: Any
    cache: ReferenceCache = field(default_factory=ReferenceCache)

    @classmethod
    @contextmanager
    def start(
        cls,
        pipeline: str,
        *,
        db: Any,
        run_id: Optional[UUID] = None,
    ) -> Iterator["RunContext"]:
        """Start a new RunContext under a fresh ``pipeline_run``.

        Yields the context. On exit, the contextvar is reset and the
        run's top-level ``pipeline.run`` timing row is persisted.
        """
        with pipeline_run(pipeline, db=db, run_id=run_id) as resolved_run_id:
            ctx = cls(pipeline=pipeline, run_id=resolved_run_id, db=db)
            token = _CURRENT_RUN_CONTEXT.set(ctx)
            try:
                yield ctx
            finally:
                _CURRENT_RUN_CONTEXT.reset(token)


# ---------------------------------------------------------------------------
# Contextvar for ambient access (avoid threading the RunContext through
# every signature).
# ---------------------------------------------------------------------------

import contextvars  # noqa: E402

_CURRENT_RUN_CONTEXT: contextvars.ContextVar[Optional[RunContext]] = contextvars.ContextVar(
    "etl_run_context", default=None
)


def current_run_context() -> Optional[RunContext]:
    """Return the active :class:`RunContext`, or ``None`` outside a run."""
    return _CURRENT_RUN_CONTEXT.get()


# ---------------------------------------------------------------------------
# Convenience loader factory: VALD reference data
# ---------------------------------------------------------------------------

def load_vald_target_groups(db: Any) -> list[dict[str, Any]]:
    """Loader for ``cache.get_or_load('vald.target_groups', ...)``.

    Cached snapshot of every active VALD target group + category. Used
    by silver_etl's membership and profile builders.
    """
    return db.fetch_all_dict(
        """
        SELECT
            target_group_id,
            target_group_name,
            tenant_id,
            modified_date_utc,
            ingested_at,
            updated_at
          FROM bronze.vald_target_groups
        """
    )


def load_vald_target_group_categories(db: Any) -> list[dict[str, Any]]:
    """Loader for ``cache.get_or_load('vald.target_group_categories', ...)``."""
    return db.fetch_all_dict(
        """
        SELECT
            category_id,
            target_group_id,
            category_name,
            tenant_id,
            ingested_at,
            updated_at
          FROM bronze.vald_target_group_categories
        """
    )


def load_vald_forcedecks_result_definitions(db: Any) -> list[dict[str, Any]]:
    """Loader for FD result-definition lookups used by the FD silver build."""
    return db.fetch_all_dict(
        """
        SELECT
            result_id,
            result_key,
            result_name,
            result_unit,
            result_unit_name
          FROM bronze.vald_forcedecks_result_definitions
        """
    )
