"""Unit tests for ingestion.common.run_context (Phase 8.8.C)."""

from __future__ import annotations

import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

if "psycopg2" not in sys.modules:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.extensions = types.ModuleType("psycopg2.extensions")
    psycopg2_stub.extensions.connection = object
    psycopg2_stub.extensions.cursor = object
    psycopg2_stub.extras = types.ModuleType("psycopg2.extras")
    psycopg2_stub.extras.execute_values = lambda *args, **kwargs: None
    psycopg2_stub.extras.RealDictCursor = object
    psycopg2_pool_stub = types.ModuleType("psycopg2.pool")
    psycopg2_pool_stub.ThreadedConnectionPool = object
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extensions"] = psycopg2_stub.extensions
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras
    sys.modules["psycopg2.pool"] = psycopg2_pool_stub

from ingestion.common.run_context import (
    ReferenceCache,
    RunContext,
    current_run_context,
)


class _CapturingDb:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = None) -> None:
        self.executed.append((sql, params))

    def fetch_all_dict(self, sql: str, params: tuple = None) -> list[dict]:
        self.executed.append((sql, params))
        return []


# ---------------------------------------------------------------------------
# ReferenceCache
# ---------------------------------------------------------------------------

def test_get_or_load_caches_after_first_call() -> None:
    cache = ReferenceCache()
    call_count = 0

    def loader() -> list[int]:
        nonlocal call_count
        call_count += 1
        return [1, 2, 3]

    first = cache.get_or_load("test.key", loader)
    second = cache.get_or_load("test.key", loader)

    assert first == [1, 2, 3]
    assert second is first  # same object: real cache, not a re-load
    assert call_count == 1


def test_get_or_load_records_hit_miss_stats() -> None:
    cache = ReferenceCache()
    cache.get_or_load("k1", lambda: [1])  # miss
    cache.get_or_load("k1", lambda: [1])  # hit
    cache.get_or_load("k1", lambda: [1])  # hit
    cache.get_or_load("k2", lambda: [2])  # miss

    stats = cache.stats()
    assert stats["k1"] == {"hits": 2, "misses": 1}
    assert stats["k2"] == {"hits": 0, "misses": 1}


def test_invalidate_drops_a_single_key() -> None:
    cache = ReferenceCache()
    call_count = 0

    def loader() -> list[int]:
        nonlocal call_count
        call_count += 1
        return [call_count]

    cache.get_or_load("k", loader)
    cache.invalidate("k")
    cache.get_or_load("k", loader)
    assert call_count == 2


def test_get_or_load_collapses_concurrent_misses_via_per_key_lock() -> None:
    """When N threads ask for the same missing key, only one loader fires.

    The lock is what causes that collapse: thread #1 acquires the lock,
    runs the loader; threads #2..N wait on the lock; when thread #1
    releases, threads #2..N see the value already in self._values and
    return without calling the loader again.
    """
    cache = ReferenceCache()
    call_count = 0
    call_count_lock = threading.Lock()
    started = threading.Event()
    proceed = threading.Event()

    def slow_loader() -> list[int]:
        nonlocal call_count
        # Tell the orchestrator that the loader started, then wait
        # for permission to continue (gives sibling threads a chance
        # to queue up on the per-key lock).
        started.set()
        proceed.wait(timeout=2.0)
        with call_count_lock:
            call_count += 1
        return [call_count]

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(cache.get_or_load, "k", slow_loader) for _ in range(8)]
        # Wait until at least one thread is inside the loader, then
        # release it. The other 7 will already be queued on the lock.
        assert started.wait(timeout=2.0), "loader did not start"
        proceed.set()
        results = [f.result(timeout=5.0) for f in futures]

    assert call_count == 1, "loader should fire exactly once across all 8 threads"
    # Every thread saw the same value.
    assert all(r == results[0] for r in results)


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------

def test_run_context_start_sets_contextvar_and_emits_pipeline_run_row() -> None:
    db = _CapturingDb()
    captured_ctx = None

    assert current_run_context() is None

    with RunContext.start("vald", db=db) as ctx:
        captured_ctx = ctx
        # Inside the context, current_run_context() returns this ctx.
        assert current_run_context() is ctx
        # The cache is empty initially.
        assert ctx.cache.stats() == {}
        # ctx.run_id is a UUID.
        assert ctx.run_id is not None

    # After exit, the contextvar is reset.
    assert current_run_context() is None

    # And one 'pipeline.run' timing row was persisted.
    insert_calls = [s for s, _ in db.executed if "INSERT INTO silver.etl_run_timings" in s]
    assert len(insert_calls) == 1


def test_run_context_cache_is_shared_within_the_run() -> None:
    """Two stages of the same run see the same cached value."""
    db = _CapturingDb()
    call_count = 0

    def loader() -> list[int]:
        nonlocal call_count
        call_count += 1
        return list(range(10))

    with RunContext.start("vald", db=db) as ctx:
        # Stage 1
        v1 = ctx.cache.get_or_load("group_data", loader, db=db)
        # Stage 2 (later in the same run)
        v2 = ctx.cache.get_or_load("group_data", loader, db=db)

    assert call_count == 1
    assert v1 is v2


def test_separate_runs_get_separate_caches() -> None:
    db = _CapturingDb()
    call_count = 0

    def loader() -> list[int]:
        nonlocal call_count
        call_count += 1
        return [call_count]

    with RunContext.start("vald", db=db) as ctx1:
        ctx1.cache.get_or_load("k", loader)

    with RunContext.start("vald", db=db) as ctx2:
        ctx2.cache.get_or_load("k", loader)

    # Two distinct runs → loader fired twice.
    assert call_count == 2
    # And the two contexts are distinct objects with distinct caches.
    assert ctx1 is not ctx2
    assert ctx1.cache is not ctx2.cache


def test_get_or_load_emits_cache_load_timing_on_miss() -> None:
    db = _CapturingDb()
    cache = ReferenceCache()
    cache.get_or_load("k", lambda: [1, 2, 3], db=db)

    insert_sqls = [s for s, _ in db.executed if "INSERT INTO silver.etl_run_timings" in s]
    assert len(insert_sqls) == 1
    # Sub_stage encodes the cache key
    insert_params = [p for s, p in db.executed if "INSERT INTO silver.etl_run_timings" in s]
    assert insert_params[0][2] == "cache.load"
    assert insert_params[0][3] == "k"  # sub_stage is the cache key
