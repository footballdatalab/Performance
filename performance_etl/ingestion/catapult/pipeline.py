"""
Catapult raw and bronze pipeline orchestration.
"""

from __future__ import annotations

import argparse
import traceback
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from ingestion.catapult.client import (
    CatapultAccountConfig,
    CatapultClient,
    CatapultRuntimeConfig,
    build_catapult_runtime_config,
)
from ingestion.catapult.cutoff import (
    CATAPULT_CUTOFF_UTC,
    clamp_catapult_start_time,
    is_on_or_after_catapult_cutoff,
)
from ingestion.catapult.loaders.raw_loader import CatapultRawLoader
from ingestion.catapult.raw_replay import replay_raw_to_bronze
from ingestion.common.batch import BatchManager
from ingestion.common.config import get_db_config
from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger
from ingestion.common.watermark import WatermarkManager

logger = get_logger(__name__)

_PROVIDER = "catapult"
_DEFAULT_STATS_BATCH_SIZE = 50
_VALID_PAIR_SOURCES = {"stats", "activity_athletes"}
_VALID_ENDPOINTS = frozenset(
    {
        "teams",
        "athletes",
        "positions",
        "parameters",
        "venues",
        "tag_types",
        "tags",
        "entity_tags",
        "activities",
        "periods",
        "annotations",
        "stats",
        "efforts",
        "events",
        "sensor_data",
    }
)
_CATAPULT_BRONZE_REPLAY_LOCK_NAMESPACE = 40291
_CATAPULT_BRONZE_REPLAY_LOCK_RESOURCE = 1
_ENTITY_TAGS_SKIP_REASON = (
    "Skipped raw capture because the provider references in this repo do not document a read endpoint for entity tags."
)
_STATS_DIMENSION_PARAMETERS = [
    "athlete_id",
    "athlete_name",
    "activity_id",
    "activity_name",
    "period_id",
    "period_name",
]
_STATS_PARAMETERS = [
    "total_distance",
    "player_load",
    "max_velocity",
    "high_speed_running_distance",
    "sprint_distance",
    "velocity_band_1_distance",
    "velocity_band_2_distance",
    "velocity_band_3_distance",
    "velocity_band_4_distance",
    "velocity_band_5_distance",
    "velocity_band_6_distance",
    "velocity_band_7_distance",
    "velocity_band_8_distance",
    "player_load_per_minute",
    "acceleration_efforts",
    "deceleration_efforts",
    "high_intensity_accelerations",
    "high_intensity_decelerations",
    "heart_rate_average",
    "heart_rate_max",
    "metabolic_power_average",
    "high_metabolic_load_distance",
]
_EVENT_TYPES = [
    "ima_acceleration",
    "ima_jump",
    "ima_impact",
    "goalkeeping_v1",
    "goalkeeping_v2",
    "cricket_delivery_au",
    "cricket_delivery",
    "running_symmetry",
    "ice_hockey_stride",
    "ice_hockey_bout",
    "ice_hockey_mp",
    "baseball_pitch_v1",
    "baseball_swing_v1",
    "baseball_pitch",
    "baseball_swing",
    "baseball_throw",
    "free_running",
    "football_movement_analysis",
    "rugby_union_scrum",
    "rugby_union_contact_involvement",
    "rugby_union_kick",
    "rugby_union_lineout",
    "rugby_league_tackle",
    "us_football_lineman_contact",
    "us_football_throw",
    "us_football_impact",
    "ice_hockey_goaltender_movement",
    "basketball",
    "tennis",
]
_VELOCITY_BANDS = "1,2,3,4,5,6,7,8"
_ACCELERATION_BANDS = "-3,-2,-1,1,2,3"


class CatapultPipelineBusyError(RuntimeError):
    """Raised when another Catapult bronze replay already holds the write lock."""


def run_extract_raw(
    *,
    accounts: str = "all",
    full_refresh: bool = False,
    include_reference: bool = True,
    include_sensor_data: bool = False,
    include_activity_athlete_enumeration: bool = False,
    days: int | None = None,
    pair_source: str = "stats",
    endpoints: set[str] | None = None,
) -> dict[str, Any]:
    """Run Catapult raw extraction for the selected accounts."""
    if days is not None and days <= 0:
        raise ValueError("days must be greater than zero when provided.")
    if pair_source not in _VALID_PAIR_SOURCES:
        raise ValueError(f"Unsupported Catapult pair source '{pair_source}'.")

    allowlist = _normalize_endpoints(endpoints)

    runtime_config = build_catapult_runtime_config()
    selected_accounts = _select_accounts(accounts, runtime_config)
    db = DatabaseManager(get_db_config())
    batch_manager = BatchManager(db)
    watermark_mgr = WatermarkManager(db)
    summary: dict[str, Any] = {
        "accounts": {},
        "total_extracted": 0,
        "total_loaded": 0,
        "has_new_data": False,
        "errors": [],
        "days": days,
        "pair_source": pair_source,
        "endpoints_allowlist": sorted(allowlist) if allowlist is not None else None,
    }

    try:
        if full_refresh and days is None and _endpoint_enabled(allowlist, "activities"):
            _reset_activity_watermarks(watermark_mgr, selected_accounts)

        logger.info(
            "Starting Catapult raw extraction for %d account(s): %s (allowlist=%s)",
            len(selected_accounts),
            ", ".join(a.name for a in selected_accounts),
            sorted(allowlist) if allowlist is not None else "all",
        )

        for account_index, account in enumerate(selected_accounts, start=1):
            logger.info(
                "[%d/%d] Extracting Catapult account '%s' (team_code=%s)",
                account_index,
                len(selected_accounts),
                account.name,
                account.team_code,
            )
            client = CatapultClient(runtime_config, account)
            account_summary: dict[str, Any] = {
                "reference": {},
                "activities": {},
                "periods": {},
                "annotations": {},
                "activity_athlete_enumeration": {},
                "activity_devices": {},
                "stats": {},
                "efforts": {},
                "events": {},
                "sensor_data": {},
                "errors": [],
                "total_extracted": 0,
                "total_loaded": 0,
            }
            try:
                if include_reference:
                    account_summary["reference"] = _extract_reference_endpoints(
                        db=db,
                        batch_manager=batch_manager,
                        account=account,
                        client=client,
                        allowlist=allowlist,
                    )
                    _accumulate_totals(account_summary, account_summary["reference"])

                if not _endpoint_enabled(allowlist, "activities"):
                    _apply_activities_skip(
                        account_summary,
                        "Endpoint 'activities' not in --endpoints allowlist.",
                    )
                    summary["accounts"][account.name] = account_summary
                    summary["total_extracted"] += account_summary["total_extracted"]
                    summary["total_loaded"] += account_summary["total_loaded"]
                    continue

                activity_result = _extract_activities(
                    db=db,
                    batch_manager=batch_manager,
                    watermark_mgr=watermark_mgr,
                    account=account,
                    client=client,
                    days=days,
                )
                account_summary["activities"] = activity_result["summary"]
                _accumulate_totals(account_summary, activity_result["summary"])

                activities = activity_result["activities"]
                if activities:
                    needs_stats = _endpoint_enabled(allowlist, "stats")
                    needs_efforts = _endpoint_enabled(allowlist, "efforts")
                    needs_events = _endpoint_enabled(allowlist, "events")
                    needs_sensor = _endpoint_enabled(allowlist, "sensor_data")
                    needs_details = needs_efforts or needs_events or needs_sensor

                    athlete_activity_pairs: list[tuple[str, str]] = []
                    activity_device_pairs: list[tuple[str, str]] = []
                    if include_activity_athlete_enumeration or pair_source == "activity_athletes":
                        pair_result = _enumerate_activity_athletes(
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            activities=activities,
                        )
                        account_summary["activity_athlete_enumeration"] = {
                            **pair_result["summary"],
                            "pairs": pair_result["pairs"],
                        }
                        if pair_source == "activity_athletes":
                            athlete_activity_pairs = pair_result["pairs"]
                    else:
                        account_summary["activity_athlete_enumeration"] = {
                            "records_extracted": 0,
                            "raw_rows_written": 0,
                            "skipped": True,
                            "skip_reason": (
                                "Activity-athlete enumeration is disabled unless "
                                "pair_source=activity_athletes."
                            ),
                        }

                    if needs_details:
                        device_result = _enumerate_activity_devices(
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            activities=activities,
                        )
                        account_summary["activity_devices"] = {
                            **device_result["summary"],
                            "pairs": device_result["pairs"],
                        }
                        activity_device_pairs = device_result["pairs"]
                    else:
                        account_summary["activity_devices"] = {
                            "records_extracted": 0,
                            "raw_rows_written": 0,
                            "skipped": True,
                            "skip_reason": (
                                "Activity-device enumeration is skipped because no detail endpoints "
                                "(efforts, events, sensor_data) are in the --endpoints allowlist."
                            ),
                        }

                    if _endpoint_enabled(allowlist, "periods"):
                        periods_summary = _extract_activity_children(
                            db=db,
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            activities=activities,
                            child_name="periods",
                        )
                        account_summary["periods"] = periods_summary
                        _accumulate_totals(account_summary, periods_summary)
                    else:
                        account_summary["periods"] = _endpoint_not_in_allowlist_skip("periods")

                    if _endpoint_enabled(allowlist, "annotations"):
                        annotations_summary = _extract_activity_children(
                            db=db,
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            activities=activities,
                            child_name="annotations",
                        )
                        account_summary["annotations"] = annotations_summary
                        _accumulate_totals(account_summary, annotations_summary)
                    else:
                        account_summary["annotations"] = _endpoint_not_in_allowlist_skip("annotations")

                    if needs_stats:
                        stats_result = _extract_stats(
                            db=db,
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            activities=activities,
                        )
                        account_summary["stats"] = stats_result["summary"]
                        _accumulate_totals(account_summary, stats_result["summary"])

                        if pair_source == "stats":
                            athlete_activity_pairs = stats_result["athlete_activity_pairs"]
                    else:
                        account_summary["stats"] = _endpoint_not_in_allowlist_skip("stats")

                    detail_pairs = activity_device_pairs or athlete_activity_pairs
                    if not account_summary["stats"].get("skipped"):
                        if activity_device_pairs:
                            account_summary["stats"]["detail_pair_source"] = "activity_devices"
                        elif athlete_activity_pairs:
                            account_summary["stats"]["detail_pair_source"] = pair_source

                    if not needs_efforts:
                        account_summary["efforts"] = _endpoint_not_in_allowlist_skip("efforts")
                    elif detail_pairs:
                        efforts_summary = _extract_athlete_activity_details(
                            db=db,
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            athlete_activity_pairs=detail_pairs,
                            detail_name="efforts",
                        )
                        account_summary["efforts"] = efforts_summary
                        _accumulate_totals(account_summary, efforts_summary)
                    else:
                        account_summary["efforts"] = {
                            "records_extracted": 0,
                            "raw_rows_written": 0,
                            "skipped": True,
                            "skip_reason": "No athlete-activity pairs were available for detail endpoint extraction.",
                        }

                    if not needs_events:
                        account_summary["events"] = _endpoint_not_in_allowlist_skip("events")
                    elif detail_pairs:
                        events_summary = _extract_athlete_activity_details(
                            db=db,
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            athlete_activity_pairs=detail_pairs,
                            detail_name="events",
                        )
                        account_summary["events"] = events_summary
                        _accumulate_totals(account_summary, events_summary)
                    else:
                        account_summary["events"] = {
                            "records_extracted": 0,
                            "raw_rows_written": 0,
                            "skipped": True,
                            "skip_reason": (
                                "No device-mapped athlete-activity pairs were available for detail endpoint extraction."
                            ),
                        }

                    if not needs_sensor:
                        account_summary["sensor_data"] = _endpoint_not_in_allowlist_skip("sensor_data")
                    elif not include_sensor_data:
                        account_summary["sensor_data"] = {
                            "records_extracted": 0,
                            "raw_rows_written": 0,
                            "skipped": True,
                            "skip_reason": "Sensor data capture is disabled unless --include-sensor-data is set.",
                        }
                    elif detail_pairs:
                        sensor_summary = _extract_athlete_activity_details(
                            db=db,
                            batch_manager=batch_manager,
                            account=account,
                            client=client,
                            athlete_activity_pairs=detail_pairs,
                            detail_name="sensor_data",
                        )
                        account_summary["sensor_data"] = sensor_summary
                        _accumulate_totals(account_summary, sensor_summary)
                    else:
                        account_summary["sensor_data"] = {
                            "records_extracted": 0,
                            "raw_rows_written": 0,
                            "skipped": True,
                            "skip_reason": (
                                "No device-mapped athlete-activity pairs were available for detail endpoint extraction."
                            ),
                        }
                else:
                    account_summary["activity_athlete_enumeration"] = {
                        "records_extracted": 0,
                        "raw_rows_written": 0,
                        "skipped": True,
                        "skip_reason": "No activities discovered for this account in the current window.",
                    }
                    account_summary["activity_devices"] = {
                        "records_extracted": 0,
                        "raw_rows_written": 0,
                        "skipped": True,
                        "skip_reason": "No activities discovered for this account in the current window.",
                    }
                    account_summary["efforts"] = {
                        "records_extracted": 0,
                        "raw_rows_written": 0,
                        "skipped": True,
                        "skip_reason": "No activities discovered for this account in the current window.",
                    }
                    account_summary["events"] = {
                        "records_extracted": 0,
                        "raw_rows_written": 0,
                        "skipped": True,
                        "skip_reason": "No activities discovered for this account in the current window.",
                    }
                    account_summary["sensor_data"] = {
                        "records_extracted": 0,
                        "raw_rows_written": 0,
                        "skipped": True,
                        "skip_reason": "No activities discovered for this account in the current window.",
                    }
            except Exception as exc:
                message = f"Catapult extraction failed for account '{account.name}': {exc}"
                logger.error(message)
                logger.error(traceback.format_exc())
                account_summary["errors"].append(message)
                summary["errors"].append(message)
            finally:
                client.close()

            summary["accounts"][account.name] = account_summary
            summary["total_extracted"] += account_summary["total_extracted"]
            summary["total_loaded"] += account_summary["total_loaded"]
            logger.info(
                "[%d/%d] Finished account '%s': extracted=%d, raw_rows_written=%d",
                account_index,
                len(selected_accounts),
                account.name,
                account_summary["total_extracted"],
                account_summary["total_loaded"],
            )

        summary["has_new_data"] = summary["total_loaded"] > 0
        logger.info(
            "Catapult raw extraction complete: total_extracted=%d, total_loaded=%d, errors=%d",
            summary["total_extracted"],
            summary["total_loaded"],
            len(summary["errors"]),
        )
        return summary
    finally:
        db.close()


def run_raw_to_bronze_stage(
    *,
    batch_ids_by_source_table: dict[str, list[str]] | None = None,
    endpoints: set[str] | None = None,
    full_replay: bool = False,
    ingested_at_start: datetime | None = None,
    ingested_at_end: datetime | None = None,
) -> dict[str, Any]:
    """Replay raw Catapult payloads into bronze."""
    allowlist = _normalize_endpoints(endpoints)
    # Phase 8.4: expand the allowlist with FK-required parents so a partial replay
    # (e.g. --endpoints tags) never violates fk_catapult_tags_tag_type.
    allowlist = _expand_replay_dependencies(allowlist)
    logger.info(
        "Starting Catapult raw->bronze replay (allowlist=%s)",
        sorted(allowlist) if allowlist is not None else "all",
    )
    with _hold_catapult_bronze_replay_lock(owner="raw_to_bronze"):
        db = DatabaseManager(get_db_config())
        try:
            result = replay_raw_to_bronze(
                db,
                batch_ids_by_source_table=batch_ids_by_source_table,
                endpoints=allowlist,
                full_replay=full_replay,
                ingested_at_start=ingested_at_start,
                ingested_at_end=ingested_at_end,
            )
            logger.info(
                "Catapult raw->bronze replay complete: processed_raw_rows=%d, loaded_rows=%d, skipped_rows=%d",
                result.get("processed_raw_rows", 0),
                result.get("loaded_rows", 0),
                result.get("skipped_rows", 0),
            )
            return result
        finally:
            db.close()


def run_intraday_raw_to_bronze_stage(
    *,
    batch_ids_by_source_table: dict[str, list[str]] | None = None,
    endpoints: set[str] | None = None,
) -> dict[str, Any]:
    """Replay incremental Catapult raw payloads into bronze."""
    return run_raw_to_bronze_stage(
        batch_ids_by_source_table=batch_ids_by_source_table,
        endpoints=endpoints,
    )


def run_full_refresh_raw_to_bronze_stage(
    *,
    endpoints: set[str] | None = None,
) -> dict[str, Any]:
    """Replay Catapult raw payloads after a full-refresh extraction."""
    return run_raw_to_bronze_stage(endpoints=endpoints)


def run_historical_day_raw_to_bronze(replay_date_str: str) -> dict[str, Any]:
    """Replay all raw Catapult rows ingested on a Lisbon calendar day into bronze."""
    from ingestion.vald.day_window import resolve_lisbon_day_window_from_date

    replay_date = date.fromisoformat(replay_date_str)
    window = resolve_lisbon_day_window_from_date(replay_date)

    summary = run_raw_to_bronze_stage(
        full_replay=True,
        ingested_at_start=window.day_start_utc,
        ingested_at_end=window.day_end_utc,
    )
    summary["replay_date"] = replay_date_str
    summary["day_window"] = window.as_summary()

    logger.info("Catapult historical day raw->bronze replay complete for %s: %s", replay_date_str, summary)
    return summary


def run_ingestion(
    *,
    accounts: str = "all",
    full_refresh: bool = False,
    include_reference: bool = True,
    include_sensor_data: bool = False,
    include_activity_athlete_enumeration: bool = False,
    days: int | None = None,
    pair_source: str = "stats",
    endpoints: set[str] | None = None,
) -> dict[str, Any]:
    """Run the Catapult raw extraction and replay stages end to end."""
    raw_summary = run_extract_raw(
        accounts=accounts,
        full_refresh=full_refresh,
        include_reference=include_reference,
        include_sensor_data=include_sensor_data,
        include_activity_athlete_enumeration=include_activity_athlete_enumeration,
        days=days,
        pair_source=pair_source,
        endpoints=endpoints,
    )
    replay_summary = run_raw_to_bronze_stage(endpoints=endpoints)
    return {
        "raw": raw_summary,
        "raw_to_bronze": replay_summary,
        "errors": list(raw_summary.get("errors", [])),
    }


def main_run_extract_raw(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Catapult raw extraction.")
    _add_catapult_args(parser)
    args = parser.parse_args(argv)
    summary = run_extract_raw(
        accounts=args.accounts,
        full_refresh=args.full_refresh,
        include_reference=not args.skip_reference,
        include_sensor_data=args.include_sensor_data,
        include_activity_athlete_enumeration=False,
        days=args.days,
        endpoints=_parse_endpoints_arg(args.endpoints),
    )
    _log_stage_summary("Catapult raw extraction", summary)
    return 1 if summary.get("errors") else 0


@contextmanager
def _hold_catapult_bronze_replay_lock(
    *,
    owner: str,
    db_config: dict[str, Any] | None = None,
    wait: bool = False,
) -> Generator[None, None, None]:
    """Hold a shared warehouse advisory lock across Catapult bronze replay writes."""
    lock_db = DatabaseManager(
        dict(db_config or get_db_config()),
        min_conn=1,
        max_conn=1,
    )
    conn = lock_db.get_connection()
    locked = False
    try:
        with conn.cursor() as cur:
            if wait:
                cur.execute(
                    "SELECT pg_advisory_lock(%s, %s)",
                    (
                        _CATAPULT_BRONZE_REPLAY_LOCK_NAMESPACE,
                        _CATAPULT_BRONZE_REPLAY_LOCK_RESOURCE,
                    ),
                )
                locked = True
            else:
                cur.execute(
                    "SELECT pg_try_advisory_lock(%s, %s)",
                    (
                        _CATAPULT_BRONZE_REPLAY_LOCK_NAMESPACE,
                        _CATAPULT_BRONZE_REPLAY_LOCK_RESOURCE,
                    ),
                )
                row = cur.fetchone()
                locked = bool(row and row[0])
        conn.commit()
        if not locked:
            raise CatapultPipelineBusyError(
                f"{owner} could not acquire the Catapult bronze-replay lock because another replay is already running."
            )
        logger.info("Acquired Catapult bronze-replay lock for %s", owner)
        yield
    finally:
        try:
            if locked:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(%s, %s)",
                        (
                            _CATAPULT_BRONZE_REPLAY_LOCK_NAMESPACE,
                            _CATAPULT_BRONZE_REPLAY_LOCK_RESOURCE,
                        ),
                    )
                    unlocked_row = cur.fetchone()
                conn.commit()
                logger.info(
                    "Released Catapult bronze-replay lock for %s (unlocked=%s)",
                    owner,
                    bool(unlocked_row and unlocked_row[0]),
                )
        finally:
            lock_db.put_connection(conn)
            lock_db.close()


def main_run_raw_to_bronze(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay Catapult raw payloads into bronze.")
    parser.add_argument(
        "--endpoints",
        type=str,
        default=None,
        help=(
            "Comma-separated allowlist of endpoints to replay. When omitted, all raw tables "
            "with pending rows are replayed. Valid values: "
            "teams, athletes, positions, parameters, venues, tag_types, tags, entity_tags, "
            "activities, periods, annotations, stats, efforts, events, sensor_data."
        ),
    )
    args = parser.parse_args(argv)
    summary = run_raw_to_bronze_stage(endpoints=_parse_endpoints_arg(args.endpoints))
    _log_stage_summary("Catapult raw->bronze", summary)
    return 0


def main_run_ingestion(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the end-to-end Catapult raw and bronze pipeline.")
    _add_catapult_args(parser)
    args = parser.parse_args(argv)
    summary = run_ingestion(
        accounts=args.accounts,
        full_refresh=args.full_refresh,
        include_reference=not args.skip_reference,
        include_sensor_data=args.include_sensor_data,
        include_activity_athlete_enumeration=False,
        days=args.days,
        endpoints=_parse_endpoints_arg(args.endpoints),
    )
    _log_stage_summary("Catapult ingestion", summary)
    return 1 if summary.get("errors") else 0


def _extract_reference_endpoints(
    *,
    db: DatabaseManager,
    batch_manager: BatchManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    allowlist: set[str] | None = None,
) -> dict[str, Any]:
    def _action(batch_id: str) -> dict[str, Any]:
        loader = CatapultRawLoader(db, batch_id, account.name)
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
            "endpoints": {},
            "entity_tags": {
                "records_extracted": 0,
                "raw_rows_written": 0,
                "skipped": True,
                "skip_reason": _ENTITY_TAGS_SKIP_REASON,
            },
        }
        endpoint_defs = [
            ("teams", "/teams", "catapult_teams"),
            ("athletes", "/athletes", "catapult_athletes"),
            ("positions", "/positions", "catapult_positions"),
            ("parameters", "/parameters", "catapult_parameters"),
            ("venues", "/venues", "catapult_venues"),
            ("tag_types", "/tagtype", "catapult_tag_types"),
            ("tags", "/tags", "catapult_tags"),
        ]
        for endpoint_name, path, table_name in endpoint_defs:
            if not _endpoint_enabled(allowlist, endpoint_name):
                summary["endpoints"][endpoint_name] = _endpoint_not_in_allowlist_skip(endpoint_name)
                continue
            logger.info("  GET %s (account=%s)", path, account.name)
            response = client.get(path)
            payload = response.json()
            record_count = _count_payload_rows(payload)
            _, inserted = loader.load_raw_if_changed_with_status(
                table_name=table_name,
                api_endpoint=path,
                response_payload=payload,
                response_status=response.status_code,
                api_version=client.api_version,
            )
            logger.info(
                "  %s: %d record(s), raw_row_written=%s",
                endpoint_name,
                record_count,
                bool(inserted),
            )
            summary["endpoints"][endpoint_name] = {
                "records_extracted": record_count,
                "raw_rows_written": 1 if inserted else 0,
            }
            summary["records_extracted"] += record_count
            summary["raw_rows_written"] += 1 if inserted else 0
        return summary

    return _execute_batch(
        batch_manager=batch_manager,
        account_name=account.name,
        api_name="reference_raw",
        action=_action,
    )


def _extract_activities(
    *,
    db: DatabaseManager,
    batch_manager: BatchManager,
    watermark_mgr: WatermarkManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    days: int | None = None,
) -> dict[str, Any]:
    discovered_activities: list[dict[str, Any]] = []

    def _action(batch_id: str) -> dict[str, Any]:
        loader = CatapultRawLoader(db, batch_id, account.name)
        now_utc = datetime.now(timezone.utc)
        if days is None:
            watermark = watermark_mgr.get_watermark(_PROVIDER, account.name, "activities")
            start_time_utc = _parse_watermark(watermark)
        else:
            watermark = None
            start_time_utc = now_utc - timedelta(days=days)
        start_time_utc = clamp_catapult_start_time(start_time_utc)
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
            "window_start": start_time_utc.isoformat(),
            "window_end": now_utc.isoformat(),
            "watermark_start": start_time_utc.isoformat() if watermark is not None else None,
            "watermark_end": watermark,
            "watermark_updated": days is None,
            "cutoff_utc": CATAPULT_CUTOFF_UTC,
            "pre_cutoff_dropped": 0,
        }

        logger.info(
            "  GET /activities (account=%s, window=%s -> %s)",
            account.name,
            start_time_utc.isoformat(),
            now_utc.isoformat(),
        )
        page = 1
        max_start_time = start_time_utc
        while True:
            params = client.build_activity_params(
                page=page,
                extra_params={
                    "sort": "start_time",
                    "start_time": int(start_time_utc.timestamp()),
                    "end_time": int(now_utc.timestamp()),
                },
            )
            response = client.get("/activities", params=params)
            payload = response.json()
            if not payload:
                logger.info("    /activities page %d: empty, stopping", page)
                break
            filtered_payload, dropped = _filter_activities_before_cutoff(payload)
            if dropped:
                summary["pre_cutoff_dropped"] += dropped
                logger.info(
                    "    /activities page %d: dropped %d activities before cutoff %s",
                    page,
                    dropped,
                    CATAPULT_CUTOFF_UTC,
                )
            if not filtered_payload:
                page += 1
                continue
            loader.load_raw(
                table_name="catapult_activities",
                api_endpoint="/activities",
                response_payload=filtered_payload,
                request_params=params,
                response_status=response.status_code,
                page_number=page,
                api_version=client.api_version,
            )
            logger.info(
                "    /activities page %d: %d activities kept (cumulative=%d)",
                page,
                len(filtered_payload),
                summary["records_extracted"] + len(filtered_payload),
            )
            discovered_activities.extend(filtered_payload)
            summary["records_extracted"] += len(filtered_payload)
            summary["raw_rows_written"] += 1
            max_start_time = max(
                max_start_time,
                _max_activity_start_time(filtered_payload, fallback=max_start_time),
            )
            page += 1

        watermark_value = max_start_time.isoformat()
        if days is None:
            watermark_mgr.update_watermark(
                provider=_PROVIDER,
                source_account=account.name,
                api_name="activities",
                watermark_value=watermark_value,
                records_synced=summary["records_extracted"],
            )
            summary["watermark_end"] = watermark_value
        return summary

    return {
        "summary": _execute_batch(
            batch_manager=batch_manager,
            account_name=account.name,
            api_name="activities_raw",
            action=_action,
        ),
        "activities": _dedupe_activities(discovered_activities),
    }


def _extract_activity_children(
    *,
    db: DatabaseManager,
    batch_manager: BatchManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    activities: list[dict[str, Any]],
    child_name: str,
) -> dict[str, Any]:
    def _action(batch_id: str) -> dict[str, Any]:
        loader = CatapultRawLoader(db, batch_id, account.name)
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
        }
        for activity in activities:
            activity_id = _normalize_identifier(activity.get("id"))
            if activity_id is None:
                continue
            path = f"/activities/{activity_id}/{child_name}"
            request_params = {
                "activity_id": activity_id,
            }
            if child_name == "annotations":
                request_params["annotation_scope"] = "activity"
                request_params["target_id"] = activity_id
            response = client.get(path)
            payload = response.json()
            loader.load_raw(
                table_name=f"catapult_{child_name}",
                api_endpoint=path,
                response_payload=payload,
                request_params=request_params,
                response_status=response.status_code,
                api_version=client.api_version,
            )
            summary["records_extracted"] += _count_payload_rows(payload)
            summary["raw_rows_written"] += 1
        return summary

    return _execute_batch(
        batch_manager=batch_manager,
        account_name=account.name,
        api_name=f"{child_name}_raw",
        action=_action,
    )


def _extract_stats(
    *,
    db: DatabaseManager,
    batch_manager: BatchManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    activities: list[dict[str, Any]],
) -> dict[str, Any]:
    athlete_activity_pairs: set[tuple[str, str]] = set()

    def _action(batch_id: str) -> dict[str, Any]:
        loader = CatapultRawLoader(db, batch_id, account.name)
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
            "request_batches": 0,
        }
        activity_ids = sorted(
            {
                activity_id
                for activity in activities
                if (activity_id := _normalize_identifier(activity.get("id"))) is not None
            }
        )
        for activity_id_batch in _chunked(activity_ids, _DEFAULT_STATS_BATCH_SIZE):
            request_payload = {
                "parameters": [*_STATS_DIMENSION_PARAMETERS, *_STATS_PARAMETERS],
                "filters": [
                    {
                        "name": "activity_id",
                        "comparison": "=",
                        "values": activity_id_batch,
                    }
                ],
                "group_by": ["athlete", "period", "activity"],
                "source": "cached_stats",
            }
            response = client.post("/stats", json=request_payload, params={"requested_only": "TRUE"})
            payload = response.json()
            loader.load_raw(
                table_name="catapult_stats",
                api_endpoint="/stats",
                response_payload=payload,
                request_params=request_payload,
                response_status=response.status_code,
                api_version=client.api_version,
            )
            summary["records_extracted"] += _count_payload_rows(payload)
            summary["raw_rows_written"] += 1
            summary["request_batches"] += 1
            for row in payload:
                athlete_id = _normalize_stats_dimension_identifier(row, "athlete_id")
                activity_id = _normalize_stats_dimension_identifier(row, "activity_id")
                if athlete_id is None or activity_id is None:
                    continue
                athlete_activity_pairs.add((athlete_id, activity_id))
        return summary

    return {
        "summary": _execute_batch(
            batch_manager=batch_manager,
            account_name=account.name,
            api_name="stats_raw",
            action=_action,
        ),
        "athlete_activity_pairs": sorted(athlete_activity_pairs),
    }


def _extract_athlete_activity_details(
    *,
    db: DatabaseManager,
    batch_manager: BatchManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    athlete_activity_pairs: list[tuple[str, str]],
    detail_name: str,
) -> dict[str, Any]:
    def _action(batch_id: str) -> dict[str, Any]:
        loader = CatapultRawLoader(db, batch_id, account.name)
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
            "not_found_requests": 0,
        }
        for athlete_id, activity_id in athlete_activity_pairs:
            requests_for_pair = _build_detail_requests(
                detail_name=detail_name,
                athlete_id=athlete_id,
                activity_id=activity_id,
            )
            pair_not_found = 0
            for detail_request in requests_for_pair:
                try:
                    response = client.get(detail_request["path"], params=detail_request["params"])
                except requests.HTTPError as exc:
                    response = exc.response
                    if response is not None and response.status_code == 404:
                        pair_not_found += 1
                        continue
                    raise
                payload = response.json()
                loader.load_raw(
                    table_name=f"catapult_{detail_name}",
                    api_endpoint=detail_request["path"],
                    response_payload=payload,
                    request_params={
                        "athlete_id": athlete_id,
                        "activity_id": activity_id,
                        **detail_request["params"],
                    },
                    response_status=response.status_code,
                    api_version=client.api_version,
                )
                summary["records_extracted"] += _count_detail_rows(detail_name, payload)
                summary["raw_rows_written"] += 1
            if pair_not_found == len(requests_for_pair):
                summary["not_found_requests"] += 1
        return summary

    return _execute_batch(
        batch_manager=batch_manager,
        account_name=account.name,
        api_name=f"{detail_name}_raw",
        action=_action,
    )


def _enumerate_activity_athletes(
    *,
    batch_manager: BatchManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    activities: list[dict[str, Any]],
) -> dict[str, Any]:
    athlete_activity_pairs: set[tuple[str, str]] = set()

    def _action(batch_id: str) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
            "activities_covered": 0,
        }
        for activity in activities:
            activity_id = _normalize_identifier(activity.get("id"))
            if activity_id is None:
                continue
            response = client.get(f"/activities/{activity_id}/athletes")
            payload = response.json()
            summary["activities_covered"] += 1
            for athlete in payload:
                athlete_id = _normalize_identifier(athlete.get("id") or athlete.get("athlete_id"))
                if athlete_id is None:
                    continue
                athlete_activity_pairs.add((athlete_id, activity_id))
            summary["records_extracted"] = len(athlete_activity_pairs)
        return summary

    return {
        "summary": _execute_batch(
            batch_manager=batch_manager,
            account_name=account.name,
            api_name="activity_athlete_enumeration",
            action=_action,
        ),
        "pairs": sorted(athlete_activity_pairs),
    }


def _enumerate_activity_devices(
    *,
    batch_manager: BatchManager,
    account: CatapultAccountConfig,
    client: CatapultClient,
    activities: list[dict[str, Any]],
) -> dict[str, Any]:
    athlete_activity_pairs: set[tuple[str, str]] = set()

    def _action(batch_id: str) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "records_extracted": 0,
            "raw_rows_written": 0,
            "activities_covered": 0,
        }
        for activity in activities:
            activity_id = _normalize_identifier(activity.get("id"))
            if activity_id is None:
                continue
            response = client.get(f"/activities/{activity_id}/devices")
            payload = response.json()
            summary["activities_covered"] += 1
            for device_mapping in payload:
                athlete_id = _normalize_identifier(device_mapping.get("athlete_id"))
                if athlete_id is None:
                    continue
                athlete_activity_pairs.add((athlete_id, activity_id))
            summary["records_extracted"] = len(athlete_activity_pairs)
        return summary

    return {
        "summary": _execute_batch(
            batch_manager=batch_manager,
            account_name=account.name,
            api_name="activity_device_enumeration",
            action=_action,
        ),
        "pairs": sorted(athlete_activity_pairs),
    }


def _execute_batch(
    *,
    batch_manager: BatchManager,
    account_name: str,
    api_name: str,
    action: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    batch_id = batch_manager.start_batch(
        provider=_PROVIDER,
        source_account=account_name,
        api_name=api_name,
    )
    try:
        summary = action(batch_id)
        summary.setdefault("batch_id", batch_id)
        batch_manager.complete_batch(
            batch_id=batch_id,
            records_extracted=int(summary.get("records_extracted", 0)),
            records_loaded=int(summary.get("raw_rows_written", 0)),
        )
        return summary
    except Exception as exc:
        batch_manager.fail_batch(batch_id, str(exc)[:1000])
        raise


def _select_accounts(
    accounts: str,
    runtime_config: CatapultRuntimeConfig,
) -> list[CatapultAccountConfig]:
    if accounts.strip().lower() == "all":
        return list(runtime_config.accounts)

    requested = {part.strip().lower() for part in accounts.split(",") if part.strip()}
    selected: list[CatapultAccountConfig] = []
    for account in runtime_config.accounts:
        aliases = {account.name.lower(), account.team_code.lower()}
        if requested & aliases:
            selected.append(account)

    if not selected:
        raise ValueError(f"No Catapult accounts matched '{accounts}'.")
    return selected


def _reset_activity_watermarks(
    watermark_mgr: WatermarkManager,
    accounts: list[CatapultAccountConfig],
) -> None:
    for account in accounts:
        watermark_mgr.update_watermark(
            provider=_PROVIDER,
            source_account=account.name,
            api_name="activities",
            watermark_value=CATAPULT_CUTOFF_UTC,
            records_synced=0,
        )


def _parse_watermark(value: str | None) -> datetime:
    if not value:
        value = CATAPULT_CUTOFF_UTC
    if value.isdigit():
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _dedupe_activities(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for activity in activities:
        activity_id = _normalize_identifier(activity.get("id"))
        if activity_id is None:
            continue
        deduped[activity_id] = activity
    return list(deduped.values())


def _filter_activities_before_cutoff(
    payload: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Drop activities whose start_time is before the Catapult cutoff."""
    kept: list[dict[str, Any]] = []
    dropped = 0
    for activity in payload:
        start_time = activity.get("start_time")
        if start_time is None:
            kept.append(activity)
            continue
        candidate = _parse_watermark(str(start_time))
        if is_on_or_after_catapult_cutoff(candidate):
            kept.append(activity)
        else:
            dropped += 1
    return kept, dropped


def _max_activity_start_time(payload: list[dict[str, Any]], *, fallback: datetime) -> datetime:
    resolved = fallback
    for activity in payload:
        start_time = activity.get("start_time")
        if not start_time:
            continue
        candidate = _parse_watermark(str(start_time))
        if candidate > resolved:
            resolved = candidate
    return resolved


def _count_payload_rows(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return len(items)
        return 1
    return 0


def _count_detail_rows(detail_name: str, payload: Any) -> int:
    if detail_name == "sensor_data":
        return len(_extract_sensor_rows(payload))
    if detail_name == "efforts":
        return sum(len(rows) for rows in _extract_effort_groups(payload).values())
    if detail_name == "events":
        return sum(len(rows) for rows in _extract_event_groups(payload).values())
    if isinstance(payload, dict):
        return sum(len(value) for value in payload.values() if isinstance(value, list))
    return _count_payload_rows(payload)


def _build_detail_requests(
    *,
    detail_name: str,
    athlete_id: str,
    activity_id: str,
) -> list[dict[str, Any]]:
    if detail_name == "sensor_data":
        return [
            {
                "path": f"/activities/{activity_id}/athletes/{athlete_id}/sensor",
                "params": {"nulls": 1, "parameters": "ts,cs,lat,long,v,hr"},
            }
        ]
    if detail_name == "events":
        return [
            {
                "path": f"/activities/{activity_id}/athletes/{athlete_id}/events",
                "params": {"event_types": ",".join(_EVENT_TYPES)},
            }
        ]
    if detail_name == "efforts":
        return [
            {
                "path": f"/activities/{activity_id}/athletes/{athlete_id}/efforts",
                "params": {
                    "effort_types": "velocity",
                    "velocity_bands": _VELOCITY_BANDS,
                },
            },
            {
                "path": f"/activities/{activity_id}/athletes/{athlete_id}/efforts",
                "params": {
                    "effort_types": "acceleration",
                    "acceleration_bands": _ACCELERATION_BANDS,
                },
            },
        ]
    raise ValueError(f"Unsupported Catapult detail name '{detail_name}'.")


def _extract_effort_groups(payload: Any) -> dict[str, list[dict[str, Any]]]:
    if isinstance(payload, dict):
        if any(key in payload for key in ("velocity", "acceleration")):
            return {
                "velocity": list(payload.get("velocity", [])),
                "acceleration": list(payload.get("acceleration", [])),
            }
        data = payload.get("data")
        if isinstance(data, dict):
            return {
                "velocity": list(data.get("velocity_efforts", [])),
                "acceleration": list(data.get("acceleration_efforts", [])),
            }
    if isinstance(payload, list):
        velocity_rows: list[dict[str, Any]] = []
        acceleration_rows: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            groups = _extract_effort_groups(item)
            velocity_rows.extend(groups.get("velocity", []))
            acceleration_rows.extend(groups.get("acceleration", []))
        return {"velocity": velocity_rows, "acceleration": acceleration_rows}
    return {"velocity": [], "acceleration": []}


def _extract_event_groups(payload: Any) -> dict[str, list[dict[str, Any]]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return {str(key): list(value) for key, value in data.items() if isinstance(value, list)}
        return {str(key): list(value) for key, value in payload.items() if isinstance(value, list)}
    if isinstance(payload, list):
        groups: dict[str, list[dict[str, Any]]] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            for key, rows in _extract_event_groups(item).items():
                groups.setdefault(key, []).extend(rows)
        return groups
    return {}


def _extract_sensor_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [dict(row) for row in data if isinstance(row, dict)]
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            rows.extend(_extract_sensor_rows(item))
        return rows
    return []


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _parse_endpoints_arg(value: str | None) -> set[str] | None:
    if value is None:
        return None
    parsed = {part.strip().lower() for part in value.split(",") if part.strip()}
    return _normalize_endpoints(parsed)


def _normalize_endpoints(endpoints: set[str] | None) -> set[str] | None:
    if endpoints is None:
        return None
    lowered = {str(name).strip().lower() for name in endpoints if str(name).strip()}
    if not lowered:
        return None
    unknown = lowered - _VALID_ENDPOINTS
    if unknown:
        raise ValueError(
            f"Unsupported Catapult endpoints: {sorted(unknown)}. "
            f"Valid values: {sorted(_VALID_ENDPOINTS)}."
        )
    return lowered


# Phase 8.4: replay-time FK dependencies between Catapult bronze tables.
# When a child endpoint is in the replay allowlist, its parent(s) MUST be replayed
# first to avoid FK violations like the ones recorded on 2026-04-30:
#   "insert or update on table 'catapult_tags' violates foreign key constraint
#    'fk_catapult_tags_tag_type'"
# Keys are child endpoints; values are the parent endpoints that must precede them.
# Order within the value tuple is not enforced — RAW_TO_BRONZE_REPLAY_ORDER does that.
_REPLAY_PARENT_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "tags": ("tag_types",),
    "entity_tags": ("tag_types", "tags", "athletes", "teams", "positions", "venues"),
    "athletes": ("teams", "positions"),
    "periods": ("activities",),
    "annotations": ("activities",),
    "stats": ("activities", "athletes"),
    "efforts": ("activities", "athletes"),
    "events": ("activities", "athletes"),
    "sensor_data": ("activities", "athletes"),
}


def _expand_replay_dependencies(endpoints: set[str] | None) -> set[str] | None:
    """Expand a replay endpoint allowlist with FK-required parent endpoints.

    Pure / idempotent: applying it twice produces the same set.

    Returns None unchanged (None means "all endpoints" — no expansion needed).
    """
    if endpoints is None:
        return None
    expanded = set(endpoints)
    # Iterative closure: a parent may itself have parents.
    while True:
        added = set()
        for child in list(expanded):
            for parent in _REPLAY_PARENT_DEPENDENCIES.get(child, ()):
                if parent not in expanded:
                    added.add(parent)
        if not added:
            break
        expanded |= added
    return expanded


def _endpoint_enabled(allowlist: set[str] | None, endpoint_name: str) -> bool:
    return allowlist is None or endpoint_name in allowlist


def _endpoint_not_in_allowlist_skip(endpoint_name: str) -> dict[str, Any]:
    return {
        "records_extracted": 0,
        "raw_rows_written": 0,
        "skipped": True,
        "skip_reason": f"Endpoint '{endpoint_name}' not in --endpoints allowlist.",
    }


def _apply_activities_skip(account_summary: dict[str, Any], reason: str) -> None:
    skip_payload = {
        "records_extracted": 0,
        "raw_rows_written": 0,
        "skipped": True,
        "skip_reason": reason,
    }
    for key in (
        "activities",
        "activity_athlete_enumeration",
        "activity_devices",
        "periods",
        "annotations",
        "stats",
        "efforts",
        "events",
        "sensor_data",
    ):
        account_summary[key] = dict(skip_payload)


def _normalize_identifier(value: Any) -> str | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _normalize_stats_dimension_identifier(row: dict[str, Any], key: str) -> str | None:
    value = _normalize_identifier(row.get(key))
    if value not in {None, "0"}:
        return value
    fallback = _normalize_identifier(row.get(f"{key}_id"))
    if fallback == "0":
        return None
    return fallback


def _accumulate_totals(parent_summary: dict[str, Any], child_summary: dict[str, Any]) -> None:
    parent_summary["total_extracted"] += int(child_summary.get("records_extracted", 0))
    parent_summary["total_loaded"] += int(child_summary.get("raw_rows_written", 0))


def _add_catapult_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--accounts",
        type=str,
        default="all",
        help='Comma-separated Catapult account names/team codes to run, or "all".',
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Reset the Catapult activities watermark before extracting raw payloads.",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip Catapult reference endpoint capture.",
    )
    parser.add_argument(
        "--include-sensor-data",
        action="store_true",
        help="Capture Catapult sensor-data endpoints as part of the raw stage.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Limit Catapult activities to the latest rolling N days without touching the incremental watermark.",
    )
    parser.add_argument(
        "--endpoints",
        type=str,
        default=None,
        help=(
            "Comma-separated allowlist of endpoints to run. When omitted, all endpoints run. "
            "Valid values: "
            "teams, athletes, positions, parameters, venues, tag_types, tags, entity_tags, "
            "activities, periods, annotations, stats, efforts, events, sensor_data."
        ),
    )


def _log_stage_summary(stage_name: str, summary: dict[str, Any]) -> None:
    logger.info("%s summary: %s", stage_name, summary)
