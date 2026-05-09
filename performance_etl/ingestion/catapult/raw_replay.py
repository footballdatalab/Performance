"""
Replay raw Catapult payloads into bronze tables.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Callable

from ingestion.catapult.catalog import RAW_TO_BRONZE_REPLAY_ORDER, RAW_TO_BRONZE_TABLE_MAP
from ingestion.catapult.loaders.bronze_loader import CatapultBronzeLoader
from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_PROVIDER = "catapult"
_SENSOR_DATA_SOURCE_TABLE = "raw.catapult_sensor_data"


def _resolve_positive_int_env(env_var: str, default: int) -> int:
    """Return a positive int from an env var, falling back to ``default``."""
    raw_value = os.environ.get(env_var)
    if raw_value in (None, ""):
        return default
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


_REPLAY_CHUNK_SIZE = _resolve_positive_int_env("CATAPULT_REPLAY_CHUNK_SIZE", 1000)
_SENSOR_DATA_REPLAY_CHUNK_SIZE = _resolve_positive_int_env(
    "CATAPULT_SENSOR_DATA_REPLAY_CHUNK_SIZE",
    100,
)

ReplayHandler = Callable[[CatapultBronzeLoader, dict[str, Any]], int]


def replay_raw_to_bronze(
    db: Any,
    *,
    batch_ids_by_source_table: dict[str, list[str]] | None = None,
    endpoints: set[str] | None = None,
    full_replay: bool = False,
    ingested_at_start: datetime | None = None,
    ingested_at_end: datetime | None = None,
) -> dict[str, Any]:
    """Replay unprocessed Catapult raw rows into bronze."""
    if (ingested_at_start is not None or ingested_at_end is not None) and not full_replay:
        raise ValueError("ingested_at filters require full_replay=True.")

    batch_manager = BatchManager(db)
    summary: dict[str, Any] = {
        "tables": {},
        "processed_raw_rows": 0,
        "loaded_rows": 0,
        "skipped_rows": 0,
        "has_new_data": False,
        "endpoints_allowlist": sorted(endpoints) if endpoints is not None else None,
        "full_replay": full_replay,
        "ingested_at_start": ingested_at_start.isoformat() if ingested_at_start else None,
        "ingested_at_end": ingested_at_end.isoformat() if ingested_at_end else None,
    }

    allowed_source_tables: set[str] | None = None
    if endpoints is not None:
        allowed_source_tables = {f"raw.catapult_{name}" for name in endpoints}

    for source_table in RAW_TO_BRONZE_REPLAY_ORDER:
        if allowed_source_tables is not None and source_table not in allowed_source_tables:
            logger.info("Skipping replay of %s (not in --endpoints allowlist)", source_table)
            summary["tables"][source_table] = {
                "processed_raw_rows": 0,
                "loaded_rows": 0,
                "skipped_rows": 0,
                "skip_reasons": {},
                "skipped": True,
                "skip_reason": "Source table not in --endpoints allowlist.",
            }
            continue

        scoped_batch_ids = (
            sorted({str(batch_id) for batch_id in batch_ids_by_source_table.get(source_table, [])})
            if batch_ids_by_source_table
            else []
        )
        if batch_ids_by_source_table is not None and not scoped_batch_ids:
            summary["tables"][source_table] = {
                "processed_raw_rows": 0,
                "loaded_rows": 0,
                "skipped_rows": 0,
                "skip_reasons": {},
                "last_raw_id": None,
                "batch_ids": [],
                "skipped": True,
                "skip_reason": "No scoped extraction batch IDs for source table.",
            }
            continue

        marker_table = RAW_TO_BRONZE_TABLE_MAP[source_table]
        is_batch_scoped = bool(scoped_batch_ids)
        marker_watermarks = (
            {}
            if full_replay or is_batch_scoped
            else _get_last_replayed_raw_ids_by_source_account(db, marker_table)
        )
        source_accounts = _fetch_source_accounts(
            db,
            source_table=source_table,
            batch_ids=scoped_batch_ids if is_batch_scoped else None,
            ingested_at_start=ingested_at_start,
            ingested_at_end=ingested_at_end,
        )
        last_raw_id = 0 if full_replay or is_batch_scoped else max(marker_watermarks.values(), default=0)

        if not source_accounts:
            summary["tables"][source_table] = {
                "processed_raw_rows": 0,
                "loaded_rows": 0,
                "skipped_rows": 0,
                "skip_reasons": {},
                "last_raw_id": last_raw_id,
                "batch_ids": scoped_batch_ids,
                "full_replay": full_replay,
            }
            continue

        processed = 0
        loaded = 0
        skipped = 0
        skip_reasons: dict[str, int] = {}
        raw_id_min: int | None = None
        raw_id_max: int | None = None
        raw_batch_ids: set[str] = set()
        replay_batch_id: str | None = None
        chunk_size = _get_replay_chunk_size(source_table)
        logger.info(
            "Starting chunked replay from %s (source_accounts=%d, max_last_raw_id=%d, chunk_size=%d)",
            source_table,
            len(source_accounts),
            last_raw_id,
            chunk_size,
        )
        try:
            handler = _REPLAY_HANDLERS[source_table]
            for source_account in source_accounts:
                previous_raw_id = 0 if full_replay or is_batch_scoped else marker_watermarks.get(source_account, 0)
                while True:
                    raw_rows = _fetch_raw_rows(
                        db,
                        source_table=source_table,
                        marker_table=None,
                        batch_ids=scoped_batch_ids if is_batch_scoped else None,
                        ingested_at_start=ingested_at_start,
                        ingested_at_end=ingested_at_end,
                        source_account=source_account,
                        raw_id_after=previous_raw_id,
                        limit=chunk_size,
                    )
                    if not raw_rows:
                        break
                    if replay_batch_id is None:
                        replay_batch_id = batch_manager.start_batch(
                            provider=_PROVIDER,
                            source_account=source_account,
                            api_name=f"replay:{source_table}",
                        )
                    for row in raw_rows:
                        raw_id = int(row["raw_id"])
                        if raw_id <= previous_raw_id:
                            raise ValueError(
                                f"Non-monotonic raw replay order for {source_table}: "
                                f"{raw_id} <= {previous_raw_id}"
                            )
                        with db.connection() as conn:
                            loader = CatapultBronzeLoader(
                                db=db,
                                batch_id=replay_batch_id,
                                source_account=str(row["source_account"]),
                                conn=conn,
                            )
                            loaded += handler(loader, row)
                            loader_stats = loader.last_load_stats or {}
                            skipped += int(loader_stats.get("skipped_rows", 0))
                            _merge_skip_reasons(skip_reasons, dict(loader_stats.get("skip_reasons", {})))
                        processed += 1
                        previous_raw_id = raw_id
                        raw_id_min = raw_id if raw_id_min is None else min(raw_id_min, raw_id)
                        raw_id_max = raw_id if raw_id_max is None else max(raw_id_max, raw_id)
                        raw_batch_ids.add(str(row["batch_id"]))
                    logger.info(
                        "  %s/%s: replayed chunk through raw_id=%d "
                        "(processed=%d, loaded=%d, skipped=%d)",
                        source_table,
                        source_account,
                        previous_raw_id,
                        processed,
                        loaded,
                        skipped,
                    )

            if replay_batch_id is None:
                summary["tables"][source_table] = {
                    "processed_raw_rows": 0,
                    "loaded_rows": 0,
                    "skipped_rows": 0,
                    "skip_reasons": {},
                    "last_raw_id": last_raw_id,
                    "batch_ids": scoped_batch_ids,
                    "full_replay": full_replay,
                }
                continue

            batch_manager.complete_batch(replay_batch_id, processed, loaded)
        except Exception as exc:
            if replay_batch_id is not None:
                batch_manager.fail_batch(replay_batch_id, str(exc)[:1000])
            raise

        summary["tables"][source_table] = {
            "processed_raw_rows": processed,
            "loaded_rows": loaded,
            "skipped_rows": skipped,
            "skip_reasons": skip_reasons,
            "last_raw_id": _get_last_replayed_raw_id(db, source_table)
            if full_replay
            else max(last_raw_id, raw_id_max or 0),
            "raw_id_min": raw_id_min,
            "raw_id_max": raw_id_max,
            "batch_ids": sorted(raw_batch_ids),
            "full_replay": full_replay,
        }
        summary["processed_raw_rows"] += processed
        summary["loaded_rows"] += loaded
        summary["skipped_rows"] += skipped
        summary["has_new_data"] = True
        logger.info(
            "Replayed %d raw rows from %s into bronze (%d loaded, %d skipped)",
            processed,
            source_table,
            loaded,
            skipped,
        )

    logger.info("Catapult raw->bronze replay summary: %s", summary)
    return summary


def _get_last_replayed_raw_id(db: Any, source_table: str) -> int:
    marker_table = RAW_TO_BRONZE_TABLE_MAP[source_table]
    row = db.fetch_one(f"SELECT COALESCE(MAX(raw_id), 0) FROM {marker_table}")
    return int(row[0]) if row and row[0] is not None else 0


def _get_last_replayed_raw_ids_by_source_account(db: Any, marker_table: str) -> dict[str, int]:
    rows = db.fetch_all_dict(
        f"""
        SELECT source_account, COALESCE(MAX(raw_id), 0) AS last_raw_id
        FROM {marker_table}
        GROUP BY source_account
        """,
        tuple(),
    )
    return {str(row["source_account"]): int(row["last_raw_id"] or 0) for row in rows}


def _get_replay_chunk_size(source_table: str) -> int:
    if source_table == _SENSOR_DATA_SOURCE_TABLE:
        return _SENSOR_DATA_REPLAY_CHUNK_SIZE
    return _REPLAY_CHUNK_SIZE


def _fetch_source_accounts(
    db: Any,
    *,
    source_table: str,
    batch_ids: list[str] | None,
    ingested_at_start: datetime | None,
    ingested_at_end: datetime | None,
) -> list[str]:
    sql = f"""
        SELECT DISTINCT raw_row.source_account
        FROM {source_table} AS raw_row
        WHERE 1 = 1
    """
    params: list[Any] = []
    if batch_ids:
        sql += " AND raw_row.batch_id = ANY(%s::uuid[])"
        params.append(batch_ids)
    if ingested_at_start is not None:
        sql += " AND raw_row.ingested_at >= %s"
        params.append(ingested_at_start)
    if ingested_at_end is not None:
        sql += " AND raw_row.ingested_at < %s"
        params.append(ingested_at_end)
    sql += " ORDER BY raw_row.source_account"
    rows = db.fetch_all_dict(sql, tuple(params))
    return [str(row["source_account"]) for row in rows if row.get("source_account") is not None]


def _fetch_raw_rows(
    db: Any,
    *,
    source_table: str,
    marker_table: str | None,
    batch_ids: list[str] | None,
    ingested_at_start: datetime | None,
    ingested_at_end: datetime | None,
    source_account: str | None = None,
    raw_id_after: int = 0,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    marker_cte = ""
    if marker_table is not None and source_account is None:
        marker_cte = f"""
        WITH marker_watermarks AS (
            SELECT source_account, COALESCE(MAX(raw_id), 0) AS last_raw_id
            FROM {marker_table}
            GROUP BY source_account
        )
        """

    sql = f"""
        {marker_cte}
        SELECT raw_row.raw_id, raw_row.source_account, raw_row.batch_id,
               raw_row.request_params, raw_row.response_payload
        FROM {source_table} AS raw_row
    """
    if marker_table is not None and source_account is None:
        sql += """
        LEFT JOIN marker_watermarks AS marker
               ON marker.source_account = raw_row.source_account
        """
    sql += """
        WHERE 1 = 1
    """
    params: list[Any] = [raw_id_after]
    sql += " AND raw_row.raw_id > %s"
    if source_account is not None:
        sql += " AND raw_row.source_account = %s"
        params.append(source_account)
    elif marker_table is not None:
        sql += " AND raw_row.raw_id > COALESCE(marker.last_raw_id, 0)"
    if batch_ids:
        sql += " AND raw_row.batch_id = ANY(%s::uuid[])"
        params.append(batch_ids)
    if ingested_at_start is not None:
        sql += " AND raw_row.ingested_at >= %s"
        params.append(ingested_at_start)
    if ingested_at_end is not None:
        sql += " AND raw_row.ingested_at < %s"
        params.append(ingested_at_end)
    sql += " ORDER BY raw_row.raw_id"
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    return db.fetch_all_dict(sql, tuple(params))


def _merge_skip_reasons(target: dict[str, int], additions: dict[str, Any]) -> None:
    for reason, count in additions.items():
        target[reason] = target.get(reason, 0) + int(count)


def _as_payload(value: Any) -> Any:
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


def _replay_teams(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_teams(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_athletes(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_athletes(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_positions(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_positions(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_parameters(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_parameters(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_venues(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_venues(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_tag_types(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_tag_types(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_tags(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_tags(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_entity_tags(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    payload = _as_payload(row["response_payload"])
    entity_tags = payload if isinstance(payload, list) else payload.get("items", [])
    return loader.load_entity_tags(list(entity_tags), int(row["raw_id"]))


def _replay_activities(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_activities(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_periods(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    params = _as_params(row)
    return loader.load_periods(
        list(_as_payload(row["response_payload"])),
        int(row["raw_id"]),
        activity_id=_coerce_optional_text(params.get("activity_id")),
    )


def _replay_annotations(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    params = _as_params(row)
    return loader.load_annotations(
        list(_as_payload(row["response_payload"])),
        int(row["raw_id"]),
        annotation_scope=str(params.get("annotation_scope")),
        target_id=_coerce_optional_text(params.get("target_id")),
    )


def _replay_stats(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    return loader.load_stats(list(_as_payload(row["response_payload"])), int(row["raw_id"]))


def _replay_efforts(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    params = _as_params(row)
    return loader.load_efforts(
        _as_payload(row["response_payload"]),
        int(row["raw_id"]),
        activity_id=str(params["activity_id"]),
        athlete_id=str(params["athlete_id"]),
    )


def _replay_events(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    params = _as_params(row)
    return loader.load_events(
        _as_payload(row["response_payload"]),
        int(row["raw_id"]),
        activity_id=str(params["activity_id"]),
        athlete_id=str(params["athlete_id"]),
    )


def _replay_sensor_data(loader: CatapultBronzeLoader, row: dict[str, Any]) -> int:
    params = _as_params(row)
    return loader.load_sensor_data(
        _as_payload(row["response_payload"]),
        int(row["raw_id"]),
        activity_id=str(params["activity_id"]),
        athlete_id=str(params["athlete_id"]),
    )


def _coerce_optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


_REPLAY_HANDLERS: dict[str, ReplayHandler] = {
    "raw.catapult_teams": _replay_teams,
    "raw.catapult_athletes": _replay_athletes,
    "raw.catapult_positions": _replay_positions,
    "raw.catapult_parameters": _replay_parameters,
    "raw.catapult_venues": _replay_venues,
    "raw.catapult_tag_types": _replay_tag_types,
    "raw.catapult_tags": _replay_tags,
    "raw.catapult_entity_tags": _replay_entity_tags,
    "raw.catapult_activities": _replay_activities,
    "raw.catapult_periods": _replay_periods,
    "raw.catapult_annotations": _replay_annotations,
    "raw.catapult_stats": _replay_stats,
    "raw.catapult_efforts": _replay_efforts,
    "raw.catapult_events": _replay_events,
    "raw.catapult_sensor_data": _replay_sensor_data,
}
