"""
Replay raw VALD payloads into bronze tables.
"""

from __future__ import annotations

import json
import time
from collections.abc import Collection
from datetime import datetime
from typing import Any, Callable

from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger
from ingestion.vald.catalog import MODULE_RAW_TABLES, REFERENCE_RAW_TABLES
from ingestion.vald.loaders.bronze_loader import ValdBronzeLoader

logger = get_logger(__name__)

_PROVIDER = "vald"
_SOURCE_ACCOUNT = "vald_default"
_REPLAY_CURSOR_TABLE = "raw.vald_replay_cursor"


def _resolve_positive_int_env(env_var: str, default: int) -> int:
    """Return a positive int from an env var, falling back to ``default``."""
    import os

    raw_value = os.environ.get(env_var)
    if raw_value in (None, ""):
        return default
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


_REPLAY_CHUNK_SIZE = _resolve_positive_int_env("VALD_REPLAY_CHUNK_SIZE", 5000)
_REPLAY_COMMIT_BATCH_SIZE = _resolve_positive_int_env(
    "VALD_REPLAY_COMMIT_BATCH_SIZE", 5000
)
_FORCEFRAME_TRACE_SOURCE_TABLE = "raw.vald_forceframe_force_traces"
_FORCEFRAME_TRACE_REPLAY_CHUNK_SIZE = _resolve_positive_int_env(
    "VALD_FORCEFRAME_TRACE_REPLAY_CHUNK_SIZE",
    100,
)
_FORCEFRAME_TRACE_REPLAY_COMMIT_BATCH_SIZE = _resolve_positive_int_env(
    "VALD_FORCEFRAME_TRACE_REPLAY_COMMIT_BATCH_SIZE",
    25,
)
_DYNAMO_TRACE_SOURCE_TABLE = "raw.vald_dynamo_traces"
_DYNAMO_TRACE_REPLAY_CHUNK_SIZE = _resolve_positive_int_env(
    "VALD_DYNAMO_TRACE_REPLAY_CHUNK_SIZE",
    50,
)
_DYNAMO_TRACE_REPLAY_COMMIT_BATCH_SIZE = _resolve_positive_int_env(
    "VALD_DYNAMO_TRACE_REPLAY_COMMIT_BATCH_SIZE",
    25,
)


def _get_replay_chunk_size(source_table: str) -> int:
    """Return the replay SELECT chunk size for a source table."""
    if source_table == _FORCEFRAME_TRACE_SOURCE_TABLE:
        return _FORCEFRAME_TRACE_REPLAY_CHUNK_SIZE
    if source_table == _DYNAMO_TRACE_SOURCE_TABLE:
        return _DYNAMO_TRACE_REPLAY_CHUNK_SIZE
    return _REPLAY_CHUNK_SIZE


def _get_replay_commit_batch_size(source_table: str) -> int:
    """Return the replay commit batch size for a source table."""
    if source_table == _FORCEFRAME_TRACE_SOURCE_TABLE:
        return _FORCEFRAME_TRACE_REPLAY_COMMIT_BATCH_SIZE
    if source_table == _DYNAMO_TRACE_SOURCE_TABLE:
        return _DYNAMO_TRACE_REPLAY_COMMIT_BATCH_SIZE
    return _REPLAY_COMMIT_BATCH_SIZE


ReplayHandler = Callable[[ValdBronzeLoader, dict[str, Any]], int]

_MARKER_TABLES: dict[str, str] = {
    "raw.vald_profiles": "bronze.vald_profiles",
    "raw.vald_forcedecks_tests": "bronze.vald_forcedecks_tests",
    "raw.vald_forcedecks_result_definitions": "bronze.vald_forcedecks_result_definitions",
    "raw.vald_forcedecks_trials": "bronze.vald_forcedecks_trials",
    "raw.vald_forceframe_tests": "bronze.vald_forceframe_tests",
    "raw.vald_forceframe_test_metrics": "bronze.vald_forceframe_test_metrics",
    "raw.vald_forceframe_force_traces": "bronze.vald_forceframe_force_traces",
    "raw.vald_nordbord_tests": "bronze.vald_nordbord_tests",
    "raw.vald_nordbord_ecc_exercises": "bronze.vald_nordbord_ecc_exercises",
    "raw.vald_nordbord_ecc_repetitions": "bronze.vald_nordbord_ecc_repetitions",
    "raw.vald_nordbord_test_metrics": "bronze.vald_nordbord_test_metrics",
    "raw.vald_smartspeed_test_summaries": "bronze.vald_smartspeed_test_summaries",
    "raw.vald_smartspeed_test_details": "bronze.vald_smartspeed_test_details",
    "raw.vald_dynamo_tests": "bronze.vald_dynamo_tests",
    "raw.vald_dynamo_test_details": "bronze.vald_dynamo_repetitions",
    "raw.vald_dynamo_traces": "bronze.vald_dynamo_traces",
}


def replay_raw_to_bronze(
    db: Any,
    modules: list[str],
    include_reference: bool = True,
    *,
    table_overrides: dict[str, str] | None = None,
    replay_cursor_table: str | None = _REPLAY_CURSOR_TABLE,
    full_replay: bool = False,
    include_only_source_tables: Collection[str] | None = None,
    exclude_source_tables: Collection[str] | None = None,
    ingested_at_start: datetime | None = None,
    ingested_at_end: datetime | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Replay unprocessed raw VALD rows into bronze.

    ``deadline`` is an optional ``time.monotonic()`` value.  When set, the
    replay loop stops gracefully before starting the next raw row after the
    deadline is exceeded.  The cursor is saved at the last committed checkpoint
    so the next call resumes where this one left off.
    """
    batch_manager = BatchManager(db)
    ordered_tables: list[str] = []
    if include_reference:
        ordered_tables.extend(REFERENCE_RAW_TABLES)
    for module in modules:
        ordered_tables.extend(MODULE_RAW_TABLES[module])

    if include_only_source_tables is not None:
        allowed_tables = {table for table in include_only_source_tables}
        ordered_tables = [table for table in ordered_tables if table in allowed_tables]
    if exclude_source_tables is not None:
        excluded_tables = {table for table in exclude_source_tables}
        ordered_tables = [table for table in ordered_tables if table not in excluded_tables]

    summary: dict[str, Any] = {
        "tables": {},
        "processed_raw_rows": 0,
        "loaded_rows": 0,
        "has_new_data": False,
        "full_replay": full_replay,
        "deadline_reached": False,
    }

    for source_table in ordered_tables:
        last_raw_id = 0 if full_replay else _get_last_replayed_raw_id(
            db,
            source_table,
            replay_cursor_table,
        )

        if not db.fetch_one(
            f"SELECT 1 FROM {source_table} WHERE raw_id > %s LIMIT 1",
            (last_raw_id,),
        ):
            summary["tables"][source_table] = {
                "start_raw_id": last_raw_id,
                "processed_raw_rows": 0,
                "loaded_rows": 0,
                "last_raw_id": last_raw_id,
            }
            continue

        batch_id = batch_manager.start_batch(
            provider=_PROVIDER,
            source_account=_SOURCE_ACCOUNT,
            api_name=f"replay:{source_table}",
        )
        processed = 0
        loaded = 0
        previous_raw_id = last_raw_id
        conn = db.get_connection()
        try:
            handler = _REPLAY_HANDLERS[source_table]
            bronze_loader = ValdBronzeLoader(
                db,
                batch_id,
                conn=conn,
                table_overrides=table_overrides,
            )
            chunk_size = _get_replay_chunk_size(source_table)
            commit_batch_size = _get_replay_commit_batch_size(source_table)
            chunk_cursor_id = last_raw_id
            pending_commits = 0
            deadline_reached_for_table = False
            while True:
                # Each chunk is a fresh, short-lived query (keyset pagination).
                # This avoids server-side cursors (fragile over unreliable
                # connections) and keeps each SELECT well under the statement
                # timeout regardless of table size.
                if ingested_at_start is not None and ingested_at_end is not None:
                    chunk = db.fetch_all_dict(
                        f"""
                        SELECT raw_id, request_params, response_payload
                        FROM {source_table}
                        WHERE raw_id > %s
                          AND ingested_at >= %s
                          AND ingested_at < %s
                        ORDER BY raw_id
                        LIMIT %s
                        """,
                        (chunk_cursor_id, ingested_at_start, ingested_at_end, chunk_size),
                    )
                else:
                    chunk = db.fetch_all_dict(
                        f"""
                        SELECT raw_id, request_params, response_payload
                        FROM {source_table}
                        WHERE raw_id > %s
                        ORDER BY raw_id
                        LIMIT %s
                        """,
                        (chunk_cursor_id, chunk_size),
                    )
                if not chunk:
                    break
                if source_table == _FORCEFRAME_TRACE_SOURCE_TABLE:
                    chunk_test_ids = [
                        str(_as_params(r)["testId"])
                        for r in chunk
                        if _as_params(r).get("testId")
                    ]
                    bronze_loader.prefetch_forceframe_profile_ids(chunk_test_ids)
                if source_table == _DYNAMO_TRACE_SOURCE_TABLE:
                    chunk_test_ids = [
                        str(_as_params(r)["testId"])
                        for r in chunk
                        if _as_params(r).get("testId")
                    ]
                    bronze_loader.prefetch_dynamo_test_context(chunk_test_ids)
                for row in chunk:
                    if deadline is not None and time.monotonic() >= deadline:
                        if pending_commits:
                            conn.commit()
                            pending_commits = 0
                            if not full_replay and replay_cursor_table is not None:
                                _update_last_replayed_raw_id(
                                    db,
                                    source_table,
                                    previous_raw_id,
                                    replay_cursor_table,
                                )
                        summary["deadline_reached"] = True
                        logger.info(
                            "Replay deadline reached after %d rows for %s; "
                            "cursor saved at raw_id=%d - next run will resume here.",
                            processed,
                            source_table,
                            previous_raw_id,
                        )
                        deadline_reached_for_table = True
                        break
                    raw_id = int(row["raw_id"])
                    if raw_id <= previous_raw_id:
                        raise ValueError(
                            f"Non-monotonic raw replay order for {source_table}: "
                            f"{raw_id} <= {previous_raw_id}"
                        )
                    try:
                        loaded += handler(bronze_loader, row)
                    except Exception:
                        # Rollback may itself fail if the connection is broken;
                        # swallow that so the original exception propagates.
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        raise
                    processed += 1
                    previous_raw_id = raw_id
                    pending_commits += 1
                    if pending_commits >= commit_batch_size:
                        conn.commit()
                        pending_commits = 0
                        if not full_replay and replay_cursor_table is not None:
                            _update_last_replayed_raw_id(
                                db,
                                source_table,
                                previous_raw_id,
                                replay_cursor_table,
                            )
                if deadline_reached_for_table:
                    break
                chunk_cursor_id = previous_raw_id
                if deadline is not None and time.monotonic() >= deadline:
                    summary["deadline_reached"] = True
                    logger.info(
                        "Replay deadline reached after %d rows for %s; "
                        "cursor saved at raw_id=%d - next run will resume here.",
                        processed,
                        source_table,
                        previous_raw_id,
                    )
                    break
            if pending_commits:
                conn.commit()
                if not full_replay and replay_cursor_table is not None:
                    _update_last_replayed_raw_id(
                        db,
                        source_table,
                        previous_raw_id,
                        replay_cursor_table,
                    )

            batch_manager.complete_batch(batch_id, processed, loaded)
            if not full_replay and replay_cursor_table is not None:
                _update_last_replayed_raw_id(
                    db,
                    source_table,
                    previous_raw_id,
                    replay_cursor_table,
                )
        except Exception as exc:
            try:
                batch_manager.fail_batch(batch_id, str(exc)[:1000])
            except Exception:
                logger.exception("Failed to mark batch %s as failed", batch_id)
            raise
        finally:
            # put_connection discards the connection if it is broken, so the
            # pool never hands out a dead socket to the next caller.
            db.put_connection(conn)

        summary["tables"][source_table] = {
            "start_raw_id": last_raw_id,
            "processed_raw_rows": processed,
            "loaded_rows": loaded,
            "last_raw_id": previous_raw_id,
        }
        summary["processed_raw_rows"] += processed
        summary["loaded_rows"] += loaded
        summary["has_new_data"] = True
        logger.info(
            "Replayed %d raw rows from %s into bronze (%d loaded)",
            processed,
            source_table,
            loaded,
        )
        if summary["deadline_reached"]:
            break

    logger.info("VALD raw->bronze replay summary: %s", summary)
    return summary


def _get_last_replayed_raw_id(
    db: Any,
    source_table: str,
    replay_cursor_table: str | None = _REPLAY_CURSOR_TABLE,
) -> int:
    """Return the latest committed raw_id for a raw source table."""
    if replay_cursor_table is not None:
        row = db.fetch_one(
            f"SELECT last_raw_id FROM {replay_cursor_table} WHERE source_table = %s",
            (source_table,),
        )
        if row and row[0] is not None:
            return int(row[0])

    marker_table = _MARKER_TABLES[source_table]
    row = db.fetch_one(f"SELECT COALESCE(MAX(raw_id), 0) FROM {marker_table}")
    return int(row[0]) if row and row[0] is not None else 0


def _update_last_replayed_raw_id(
    db: Any,
    source_table: str,
    raw_id: int,
    replay_cursor_table: str,
) -> None:
    db.execute(
        f"""
        INSERT INTO {replay_cursor_table} (source_table, last_raw_id, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (source_table) DO UPDATE
        SET last_raw_id = EXCLUDED.last_raw_id,
            updated_at = EXCLUDED.updated_at
        """,
        (source_table, raw_id),
    )


def _as_payload(value: Any) -> Any:
    """Return a decoded JSON-like payload."""
    if isinstance(value, str):
        return json.loads(value)
    return value


def _as_params(row: dict[str, Any]) -> dict[str, Any]:
    params = row.get("request_params")
    if params is None:
        return {}
    if isinstance(params, str):
        return json.loads(params)
    return dict(params)


def _replay_profiles(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_profiles(list(payload), int(row["raw_id"]), tenant_id=params.get("teamId"))


def _replay_forcedecks_tests(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_forcedecks_tests(list(payload), int(row["raw_id"]), tenant_id=params.get("tenantId"))


def _replay_forcedecks_result_definitions(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    if isinstance(payload, dict):
        payload = payload.get("resultDefinitions") or payload.get("items") or [payload]
    return loader.load_forcedecks_result_definitions(list(payload), int(row["raw_id"]))


def _replay_forcedecks_trials(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = list(_as_payload(row["response_payload"]))
    params = _as_params(row)
    test_id = params.get("testId")
    for trial in payload:
        if test_id and not trial.get("testId"):
            trial["testId"] = test_id
    return loader.load_forcedecks_trials(payload, int(row["raw_id"]))


def _replay_forceframe_tests(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_forceframe_tests(list(payload), int(row["raw_id"]), tenant_id=params.get("tenantId"))


def _replay_forceframe_metrics(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_forceframe_test_metrics(
        str(params["testId"]),
        str(params["tenantId"]),
        payload,
        int(row["raw_id"]),
    )


def _replay_forceframe_traces(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    params = _as_params(row)
    payload = _as_payload(row["response_payload"])
    return loader.load_forceframe_force_traces(str(params["testId"]), payload, int(row["raw_id"]))


def _replay_nordbord_tests(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_nordbord_tests(list(payload), int(row["raw_id"]), tenant_id=params.get("tenantId"))


def _replay_nordbord_ecc_exercises(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    return loader.load_nordbord_ecc_exercises(list(payload), int(row["raw_id"]))


def _replay_nordbord_ecc_repetitions(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    return loader.load_nordbord_ecc_repetitions(list(payload), int(row["raw_id"]))


def _replay_nordbord_metrics(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_nordbord_test_metrics(
        str(params["testId"]),
        str(params["tenantId"]),
        payload,
        int(row["raw_id"]),
    )


def _replay_smartspeed_summaries(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_smartspeed_summaries(list(payload), int(row["raw_id"]), tenant_id=params.get("teamId"))


def _replay_smartspeed_details(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    if isinstance(payload, dict) and params.get("testId") and not payload.get("testId"):
        payload["testId"] = params["testId"]
    details = payload if isinstance(payload, list) else [payload]
    return loader.load_smartspeed_test_details(details, int(row["raw_id"]), tenant_id=params.get("teamId"))


def _replay_dynamo_tests(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    items = payload.get("items", []) if isinstance(payload, dict) else payload
    return loader.load_dynamo_tests(list(items), int(row["raw_id"]))


def _replay_dynamo_details(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_dynamo_repetitions(
        str(params["testId"]),
        list(payload.get("repetitions", [])),
        int(row["raw_id"]),
    )


def _replay_dynamo_traces(loader: ValdBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    params = _as_params(row)
    return loader.load_dynamo_traces(
        str(params["testId"]),
        str(params["tenantId"]),
        payload,
        int(row["raw_id"]),
    )


_REPLAY_HANDLERS: dict[str, ReplayHandler] = {
    "raw.vald_profiles": _replay_profiles,
    "raw.vald_forcedecks_tests": _replay_forcedecks_tests,
    "raw.vald_forcedecks_result_definitions": _replay_forcedecks_result_definitions,
    "raw.vald_forcedecks_trials": _replay_forcedecks_trials,
    "raw.vald_forceframe_tests": _replay_forceframe_tests,
    "raw.vald_forceframe_test_metrics": _replay_forceframe_metrics,
    "raw.vald_forceframe_force_traces": _replay_forceframe_traces,
    "raw.vald_nordbord_tests": _replay_nordbord_tests,
    "raw.vald_nordbord_ecc_exercises": _replay_nordbord_ecc_exercises,
    "raw.vald_nordbord_ecc_repetitions": _replay_nordbord_ecc_repetitions,
    "raw.vald_nordbord_test_metrics": _replay_nordbord_metrics,
    "raw.vald_smartspeed_test_summaries": _replay_smartspeed_summaries,
    "raw.vald_smartspeed_test_details": _replay_smartspeed_details,
    "raw.vald_dynamo_tests": _replay_dynamo_tests,
    "raw.vald_dynamo_test_details": _replay_dynamo_details,
    "raw.vald_dynamo_traces": _replay_dynamo_traces,
}
