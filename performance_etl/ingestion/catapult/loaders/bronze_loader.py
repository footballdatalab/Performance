"""
Catapult bronze-layer loader.

Parses raw Catapult payloads into the typed ``bronze.catapult_*`` tables.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg2.extras

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_JSON_EXCLUDE_KEYS = {
    "activity_id",
    "activity_name",
    "athlete_id",
    "athlete_name",
    "end_time",
    "period_id",
    "start_time",
}


class CatapultBronzeLoader:
    """Insert typed Catapult rows into bronze."""

    def __init__(
        self,
        db: DatabaseManager,
        batch_id: str,
        source_account: str,
        *,
        conn: psycopg2.extensions.connection | None = None,
    ) -> None:
        self.db = db
        self.batch_id = batch_id
        self.source_account = source_account
        self.conn = conn
        self.last_load_stats: dict[str, Any] | None = None
        self._period_exists_cache: dict[str, bool] = {}

    def load_teams(self, teams: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for team in teams:
            team_id = _coerce_identifier(team.get("id") or team.get("team_id"))
            if team_id is None:
                _increment_reason(skip_reasons, "missing_team_id")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "team_id": team_id,
                    "team_name": _coerce_text(team.get("name")),
                    "team_code": _coerce_text(team.get("slug") or team.get("code")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_teams",
            records,
            conflict_columns=["source_account", "team_id"],
            attempted_rows=len(teams),
            skip_reasons=skip_reasons,
        )

    def load_positions(self, positions: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for position in positions:
            position_id = _coerce_identifier(position.get("id") or position.get("position_id"))
            if position_id is None:
                _increment_reason(skip_reasons, "missing_position_id")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "position_id": position_id,
                    "position_name": _coerce_text(position.get("name")),
                    "position_slug": _coerce_text(position.get("slug")),
                    "sport_name": _coerce_text(position.get("sport_name")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_positions",
            records,
            conflict_columns=["source_account", "position_id"],
            attempted_rows=len(positions),
            skip_reasons=skip_reasons,
        )

    def load_parameters(self, parameters: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for parameter in parameters:
            parameter_id = _coerce_identifier(parameter.get("id") or parameter.get("parameter_id"))
            if parameter_id is None:
                _increment_reason(skip_reasons, "missing_parameter_id")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "parameter_id": parameter_id,
                    "parameter_name": _coerce_text(parameter.get("name")),
                    "parameter_slug": _coerce_text(parameter.get("slug")),
                    "parameter_unit": _coerce_text(
                        parameter.get("unit")
                        or parameter.get("unit_type")
                        or _nested(parameter, "unit", "name")
                    ),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_parameters",
            records,
            conflict_columns=["source_account", "parameter_id"],
            attempted_rows=len(parameters),
            skip_reasons=skip_reasons,
        )

    def load_venues(self, venues: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for venue in venues:
            venue_id = _coerce_identifier(venue.get("id") or venue.get("venue_id"))
            if venue_id is None:
                _increment_reason(skip_reasons, "missing_venue_id")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "venue_id": venue_id,
                    "venue_name": _coerce_text(venue.get("name")),
                    "venue_city": _coerce_text(
                        venue.get("city")
                        or _nested(venue, "location", "city")
                        or _nested(venue, "address", "city")
                    ),
                    "venue_country": _coerce_text(
                        venue.get("country")
                        or _nested(venue, "location", "country")
                        or _nested(venue, "address", "country")
                    ),
                    "latitude": _coerce_numeric(venue.get("latitude") or _nested(venue, "location", "latitude")),
                    "longitude": _coerce_numeric(venue.get("longitude") or _nested(venue, "location", "longitude")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_venues",
            records,
            conflict_columns=["source_account", "venue_id"],
            attempted_rows=len(venues),
            skip_reasons=skip_reasons,
        )

    def load_tag_types(self, tag_types: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for tag_type in tag_types:
            tag_type_id = _coerce_identifier(tag_type.get("id") or tag_type.get("tag_type_id"))
            if tag_type_id is None:
                _increment_reason(skip_reasons, "missing_tag_type_id")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "tag_type_id": tag_type_id,
                    "tag_type_name": _coerce_text(tag_type.get("name")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_tag_types",
            records,
            conflict_columns=["source_account", "tag_type_id"],
            attempted_rows=len(tag_types),
            skip_reasons=skip_reasons,
        )

    def load_tags(self, tags: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for tag in tags:
            tag_id = _coerce_identifier(tag.get("id") or tag.get("tag_id"))
            if tag_id is None:
                _increment_reason(skip_reasons, "missing_tag_id")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "tag_id": tag_id,
                    "tag_type_id": _coerce_identifier(tag.get("tag_type_id") or _nested(tag, "tag_type", "id")),
                    "tag_name": _coerce_text(tag.get("name")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        self._ensure_tag_type_placeholders(
            {
                str(record["tag_type_id"])
                for record in records
                if record.get("tag_type_id") is not None
            }
        )
        return self._upsert(
            "bronze.catapult_tags",
            records,
            conflict_columns=["source_account", "tag_id"],
            attempted_rows=len(tags),
            skip_reasons=skip_reasons,
        )

    def _ensure_tag_type_placeholders(self, tag_type_ids: set[str]) -> None:
        """Seed missing tag-type keys so tags can preserve their provider FK."""
        if not tag_type_ids:
            return

        existing_ids = self._fetch_existing_tag_type_ids(tag_type_ids)
        missing_ids = sorted(tag_type_ids - existing_ids)
        if not missing_ids:
            return

        self._upsert(
            "bronze.catapult_tag_types",
            [
                {
                    "source_account": self.source_account,
                    "tag_type_id": tag_type_id,
                    "tag_type_name": None,
                    "raw_id": None,
                    "batch_id": self.batch_id,
                }
                for tag_type_id in missing_ids
            ],
            conflict_columns=["source_account", "tag_type_id"],
            attempted_rows=len(missing_ids),
            skip_reasons={},
        )

    def _fetch_existing_tag_type_ids(self, tag_type_ids: set[str]) -> set[str]:
        sql = """
            SELECT tag_type_id
            FROM bronze.catapult_tag_types
            WHERE source_account = %s
              AND tag_type_id = ANY(%s)
        """
        params = (self.source_account, sorted(tag_type_ids))
        if self.conn is None:
            rows = self.db.fetch_all_dict(sql, params)
        else:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = [dict(row) for row in cur.fetchall()]
        return {str(row["tag_type_id"]) for row in rows}

    def load_athletes(self, athletes: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for athlete in athletes:
            athlete_id = _coerce_identifier(athlete.get("id") or athlete.get("athlete_id"))
            if athlete_id is None:
                _increment_reason(skip_reasons, "missing_athlete_id")
                continue
            first_name = _coerce_text(athlete.get("first_name"))
            last_name = _coerce_text(athlete.get("last_name"))
            full_name = _coerce_text(
                athlete.get("name")
                or athlete.get("full_name")
                or " ".join(part for part in (first_name, last_name) if part)
            )
            records.append(
                {
                    "source_account": self.source_account,
                    "athlete_id": athlete_id,
                    "current_team_id": _coerce_identifier(
                        athlete.get("current_team_id")
                        or athlete.get("team_id")
                        or _nested(athlete, "current_team", "id")
                    ),
                    "position_id": _coerce_identifier(
                        athlete.get("position_id") or _nested(athlete, "position", "id")
                    ),
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": full_name,
                    "gender": _coerce_text(athlete.get("gender")),
                    "nickname": _coerce_text(athlete.get("nickname")),
                    "height": _coerce_integer(athlete.get("height")),
                    "weight": _coerce_integer(athlete.get("weight")),
                    "date_of_birth": _coerce_date(
                        athlete.get("date_of_birth_date")
                        or athlete.get("date_of_birth")
                        or athlete.get("dob")
                    ),
                    "jersey_number": _coerce_text(athlete.get("jersey") or athlete.get("jersey_number")),
                    "velocity_max": _coerce_numeric(athlete.get("velocity_max")),
                    "acceleration_max": _coerce_numeric(athlete.get("acceleration_max")),
                    "heart_rate_max": _coerce_numeric(athlete.get("heart_rate_max")),
                    "player_load_max": _coerce_numeric(athlete.get("player_load_max")),
                    "max_player_load_per_minute": _coerce_numeric(athlete.get("max_player_load_per_minute")),
                    "image": _coerce_text(athlete.get("image")),
                    "icon": _coerce_text(athlete.get("icon")),
                    "stroke_colour": _coerce_text(athlete.get("stroke_colour")),
                    "fill_colour": _coerce_text(athlete.get("fill_colour")),
                    "trail_colour_start": _coerce_text(athlete.get("trail_colour_start")),
                    "trail_colour_end": _coerce_text(athlete.get("trail_colour_end")),
                    "is_synced": _coerce_boolean(athlete.get("is_synced")),
                    "is_deleted": _coerce_boolean(athlete.get("is_deleted")),
                    "is_demo": _coerce_boolean(athlete.get("is_demo")),
                    "provider_created_at": _coerce_datetime(athlete.get("created_at")),
                    "provider_modified_at": _coerce_datetime(athlete.get("modified_at")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_athletes",
            records,
            conflict_columns=["source_account", "athlete_id"],
            attempted_rows=len(athletes),
            skip_reasons=skip_reasons,
        )

    def load_entity_tags(self, entity_tags: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for entity_tag in entity_tags:
            entity_type = _coerce_text(
                entity_tag.get("entity_type") or entity_tag.get("table_name") or entity_tag.get("type")
            )
            entity_id = _coerce_text(
                entity_tag.get("entity_id") or entity_tag.get("table_id") or entity_tag.get("id")
            )
            tag_id = _coerce_identifier(entity_tag.get("tag_id") or _nested(entity_tag, "tag", "id"))
            if not entity_type or not entity_id or tag_id is None:
                _increment_reason(skip_reasons, "missing_entity_tag_keys")
                continue
            tagged_at = _coerce_datetime(
                entity_tag.get("tagged_at") or entity_tag.get("created_at") or entity_tag.get("updated_at")
            )
            records.append(
                {
                    "source_account": self.source_account,
                    "record_hash": _hash_value(self.source_account, entity_type, entity_id, tag_id),
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "tag_id": tag_id,
                    "tagged_at": tagged_at,
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_entity_tags",
            records,
            conflict_columns=["record_hash"],
            attempted_rows=len(entity_tags),
            skip_reasons=skip_reasons,
        )

    def load_activities(self, activities: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for activity in activities:
            activity_id = _coerce_identifier(activity.get("id") or activity.get("activity_id"))
            start_time = _coerce_datetime(activity.get("start_time") or activity.get("start"))
            if activity_id is None:
                _increment_reason(skip_reasons, "missing_activity_id")
                continue
            if start_time is None:
                _increment_reason(skip_reasons, "missing_start_time")
                continue
            end_time = _coerce_datetime(activity.get("end_time") or activity.get("end"))
            duration_seconds = _coerce_numeric(activity.get("duration") or activity.get("duration_seconds"))
            if duration_seconds is None and end_time is not None:
                duration_seconds = Decimal(str((end_time - start_time).total_seconds()))
            records.append(
                {
                    "source_account": self.source_account,
                    "activity_id": activity_id,
                    "activity_name": _coerce_text(activity.get("name") or activity.get("activity_name")),
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": duration_seconds,
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_activities",
            records,
            conflict_columns=["source_account", "activity_id"],
            attempted_rows=len(activities),
            skip_reasons=skip_reasons,
        )

    def load_periods(
        self,
        periods: list[dict[str, Any]],
        raw_id: int,
        *,
        activity_id: str | None = None,
    ) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for period in periods:
            period_id = _coerce_identifier(period.get("id") or period.get("period_id"))
            resolved_activity_id = _coerce_identifier(period.get("activity_id")) or activity_id
            if period_id is None:
                _increment_reason(skip_reasons, "missing_period_id")
                continue
            if resolved_activity_id is None:
                _increment_reason(skip_reasons, "missing_activity_id")
                continue
            start_time = _coerce_datetime(period.get("start_time") or period.get("start"))
            end_time = _coerce_datetime(period.get("end_time") or period.get("end"))
            duration_seconds = _coerce_numeric(period.get("duration") or period.get("duration_seconds"))
            if duration_seconds is None and start_time is not None and end_time is not None:
                duration_seconds = Decimal(str((end_time - start_time).total_seconds()))
            records.append(
                {
                    "source_account": self.source_account,
                    "period_id": period_id,
                    "activity_id": resolved_activity_id,
                    "period_name": _coerce_text(period.get("name") or period.get("period_name")),
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": duration_seconds,
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_periods",
            records,
            conflict_columns=["source_account", "period_id"],
            attempted_rows=len(periods),
            skip_reasons=skip_reasons,
        )

    def load_annotations(
        self,
        annotations: list[dict[str, Any]],
        raw_id: int,
        *,
        annotation_scope: str,
        target_id: str | None,
    ) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for annotation in annotations:
            annotation_id = _coerce_identifier(annotation.get("id") or annotation.get("annotation_id"))
            if annotation_id is None:
                _increment_reason(skip_reasons, "missing_annotation_id")
                continue
            activity_id = _coerce_identifier(annotation.get("activity_id"))
            period_id = _coerce_identifier(annotation.get("period_id"))
            athlete_id = _coerce_identifier(annotation.get("athlete_id"))
            if annotation_scope == "activity":
                activity_id = activity_id or target_id
                period_id = None
                athlete_id = None
            elif annotation_scope == "period":
                period_id = period_id or target_id
                activity_id = None
                athlete_id = None
            elif annotation_scope == "athlete":
                athlete_id = athlete_id or target_id
                activity_id = None
                period_id = None
            records.append(
                {
                    "source_account": self.source_account,
                    "annotation_id": annotation_id,
                    "annotation_scope": annotation_scope,
                    "activity_id": activity_id,
                    "period_id": period_id,
                    "athlete_id": athlete_id,
                    "annotation_text": _coerce_text(
                        annotation.get("text") or annotation.get("name") or annotation.get("description")
                    ),
                    "created_by": _coerce_text(
                        annotation.get("created_by")
                        or _nested(annotation, "owner", "name")
                        or _nested(annotation, "created_by", "name")
                    ),
                    "recorded_at": _coerce_datetime(
                        annotation.get("recorded_at")
                        or annotation.get("created_at")
                        or annotation.get("start_time")
                    ),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_annotations",
            records,
            conflict_columns=["source_account", "annotation_id"],
            attempted_rows=len(annotations),
            skip_reasons=skip_reasons,
        )

    def load_stats(self, stats_rows: list[dict[str, Any]], raw_id: int) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        for row in stats_rows:
            activity_id = _coerce_stats_dimension_identifier(row, "activity_id")
            athlete_id = _coerce_stats_dimension_identifier(row, "athlete_id")
            if activity_id is None:
                _increment_reason(skip_reasons, "missing_activity_id")
                continue
            if athlete_id is None:
                _increment_reason(skip_reasons, "missing_athlete_id")
                continue

            activity_context = self._get_activity_context(activity_id)
            start_time = _coerce_datetime(row.get("start_time")) or activity_context.get("start_time")
            if start_time is None:
                logger.warning(
                    "Skipping Catapult stats row without start_time: account=%s activity_id=%s athlete_id=%s raw_id=%s",
                    self.source_account,
                    activity_id,
                    athlete_id,
                    raw_id,
                )
                _increment_reason(skip_reasons, "missing_start_time")
                continue

            raw_period_id = _coerce_stats_dimension_identifier(row, "period_id")
            period_id, period_key = self._resolve_stats_period(raw_period_id)
            records.append(
                {
                    "source_account": self.source_account,
                    "activity_id": activity_id,
                    "athlete_id": athlete_id,
                    "period_id": period_id,
                    "period_key": period_key,
                    "start_time": start_time,
                    "end_time": _coerce_datetime(row.get("end_time")) or activity_context.get("end_time"),
                    "total_distance": _coerce_numeric(row.get("total_distance")),
                    "player_load": _coerce_numeric(row.get("player_load")),
                    "max_velocity": _coerce_numeric(row.get("max_velocity")),
                    "high_speed_running_distance": _coerce_numeric(row.get("high_speed_running_distance")),
                    "sprint_distance": _coerce_numeric(row.get("sprint_distance")),
                    "velocity_band_1_distance": _coerce_numeric(row.get("velocity_band_1_distance")),
                    "velocity_band_2_distance": _coerce_numeric(row.get("velocity_band_2_distance")),
                    "velocity_band_3_distance": _coerce_numeric(row.get("velocity_band_3_distance")),
                    "velocity_band_4_distance": _coerce_numeric(row.get("velocity_band_4_distance")),
                    "velocity_band_5_distance": _coerce_numeric(row.get("velocity_band_5_distance")),
                    "velocity_band_6_distance": _coerce_numeric(row.get("velocity_band_6_distance")),
                    "velocity_band_7_distance": _coerce_numeric(row.get("velocity_band_7_distance")),
                    "velocity_band_8_distance": _coerce_numeric(row.get("velocity_band_8_distance")),
                    "player_load_per_minute": _coerce_numeric(row.get("player_load_per_minute")),
                    "acceleration_efforts": _coerce_numeric(row.get("acceleration_efforts")),
                    "deceleration_efforts": _coerce_numeric(row.get("deceleration_efforts")),
                    "high_intensity_accelerations": _coerce_numeric(row.get("high_intensity_accelerations")),
                    "high_intensity_decelerations": _coerce_numeric(row.get("high_intensity_decelerations")),
                    "heart_rate_average": _coerce_numeric(row.get("heart_rate_average")),
                    "heart_rate_max": _coerce_numeric(row.get("heart_rate_max")),
                    "metabolic_power_average": _coerce_numeric(row.get("metabolic_power_average")),
                    "high_metabolic_load_distance": _coerce_numeric(row.get("high_metabolic_load_distance")),
                    "all_parameters": json.dumps(
                        {key: value for key, value in row.items() if key not in _JSON_EXCLUDE_KEYS},
                        default=_json_default,
                        sort_keys=True,
                    ),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_stats",
            records,
            conflict_columns=[
                "source_account",
                "activity_id",
                "athlete_id",
                "start_time",
                "period_key",
            ],
            attempted_rows=len(stats_rows),
            skip_reasons=skip_reasons,
        )

    def load_efforts(
        self,
        efforts_payload: Any,
        raw_id: int,
        *,
        activity_id: str,
        athlete_id: str,
    ) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        normalized_efforts = _normalize_efforts_payload(efforts_payload)
        for effort in normalized_efforts.get("velocity", []):
            record = self._build_effort_record(
                effort,
                raw_id=raw_id,
                activity_id=activity_id,
                athlete_id=athlete_id,
                effort_type=_resolve_effort_type("velocity", effort),
            )
            if record is None:
                _increment_reason(skip_reasons, "missing_start_time")
                continue
            records.append(record)
        for effort in normalized_efforts.get("acceleration", []):
            record = self._build_effort_record(
                effort,
                raw_id=raw_id,
                activity_id=activity_id,
                athlete_id=athlete_id,
                effort_type=_resolve_effort_type("acceleration", effort),
            )
            if record is None:
                _increment_reason(skip_reasons, "missing_start_time")
                continue
            records.append(record)
        attempted_rows = len(normalized_efforts.get("velocity", [])) + len(normalized_efforts.get("acceleration", []))
        return self._upsert(
            "bronze.catapult_efforts",
            records,
            conflict_columns=["record_hash"],
            attempted_rows=attempted_rows,
            skip_reasons=skip_reasons,
        )

    def load_events(
        self,
        events_payload: Any,
        raw_id: int,
        *,
        activity_id: str,
        athlete_id: str,
    ) -> int:
        records = []
        attempted_rows = 0
        skip_reasons: dict[str, int] = {}
        normalized_events = _normalize_events_payload(events_payload)
        for event_type, event_rows in normalized_events.items():
            if not isinstance(event_rows, list):
                continue
            attempted_rows += len(event_rows)
            for event_row in event_rows:
                occurred_at = _coerce_datetime(
                    event_row.get("start_time")
                    or event_row.get("timestamp")
                    or event_row.get("dt")
                    or event_row.get("time")
                    or event_row.get("occurred_at")
                )
                if occurred_at is None:
                    _increment_reason(skip_reasons, "missing_occurred_at")
                    continue
                event_payload = dict(event_row)
                records.append(
                    {
                        "source_account": self.source_account,
                        "record_hash": _hash_value(
                            self.source_account,
                            activity_id,
                            athlete_id,
                            event_type,
                            occurred_at,
                            event_payload,
                        ),
                        "activity_id": activity_id,
                        "athlete_id": athlete_id,
                        "event_type": _coerce_text(event_type),
                        "event_value": _pick_numeric_value(
                            event_row,
                            preferred_keys=(
                                "intensity",
                                "height",
                                "impact",
                                "delivery_velocity",
                                "confidence",
                                "basketball_load",
                                "jump_attribute",
                            ),
                        ),
                        "occurred_at": occurred_at,
                        "event_payload": json.dumps(event_payload, default=_json_default, sort_keys=True),
                        "raw_id": raw_id,
                        "batch_id": self.batch_id,
                    }
                )

        return self._upsert(
            "bronze.catapult_events",
            records,
            conflict_columns=["record_hash"],
            attempted_rows=attempted_rows,
            skip_reasons=skip_reasons,
        )

    def load_sensor_data(
        self,
        sensor_rows: Any,
        raw_id: int,
        *,
        activity_id: str,
        athlete_id: str,
    ) -> int:
        records = []
        skip_reasons: dict[str, int] = {}
        normalized_rows = _normalize_sensor_rows(sensor_rows)
        for row in normalized_rows:
            recorded_at = _coerce_sensor_recorded_at(row)
            if recorded_at is None:
                _increment_reason(skip_reasons, "missing_recorded_at")
                continue
            records.append(
                {
                    "source_account": self.source_account,
                    "record_hash": _hash_value(
                        self.source_account,
                        activity_id,
                        athlete_id,
                        recorded_at,
                        row.get("latitude") or row.get("lat"),
                        row.get("longitude") or row.get("long"),
                        row.get("velocity") or row.get("v"),
                    ),
                    "activity_id": activity_id,
                    "athlete_id": athlete_id,
                    "recorded_at": recorded_at,
                    "latitude": _coerce_numeric(row.get("latitude") or row.get("lat")),
                    "longitude": _coerce_numeric(row.get("longitude") or row.get("long")),
                    "velocity": _coerce_numeric(row.get("velocity") or row.get("v")),
                    "heart_rate": _coerce_numeric(row.get("heart_rate") or row.get("hr")),
                    "accel_x": _coerce_numeric(row.get("accel_x")),
                    "accel_y": _coerce_numeric(row.get("accel_y")),
                    "accel_z": _coerce_numeric(row.get("accel_z")),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                }
            )

        return self._upsert(
            "bronze.catapult_sensor_data",
            records,
            conflict_columns=["source_account", "recorded_at", "record_hash"],
            attempted_rows=len(normalized_rows),
            skip_reasons=skip_reasons,
        )

    def _build_effort_record(
        self,
        effort: dict[str, Any],
        *,
        raw_id: int,
        activity_id: str,
        athlete_id: str,
        effort_type: str,
    ) -> dict[str, Any] | None:
        start_time = _coerce_datetime(effort.get("dt") or effort.get("start_time"))
        if start_time is None:
            return None
        return {
            "source_account": self.source_account,
            "record_hash": _hash_value(
                self.source_account,
                activity_id,
                athlete_id,
                effort_type,
                start_time,
                effort,
            ),
            "activity_id": activity_id,
            "athlete_id": athlete_id,
            "effort_type": effort_type,
            "magnitude": _pick_numeric_value(
                effort,
                preferred_keys=("mval", "max_velocity", "acceleration", "distance"),
            ),
            "start_time": start_time,
            "end_time": _coerce_datetime(effort.get("et") or effort.get("end_time")),
            "raw_id": raw_id,
            "batch_id": self.batch_id,
        }

    def _get_activity_context(self, activity_id: str) -> dict[str, Any]:
        row = self._fetch_one_dict(
            """
            SELECT start_time, end_time
            FROM bronze.catapult_activities
            WHERE source_account = %s
              AND activity_id = %s
            """,
            (self.source_account, activity_id),
        )
        return row or {}

    def _resolve_stats_period(self, period_id: str | None) -> tuple[str | None, str]:
        if period_id is None:
            return None, ""
        if self._period_exists(period_id):
            return period_id, period_id
        return None, period_id

    def _period_exists(self, period_id: str) -> bool:
        cached = self._period_exists_cache.get(period_id)
        if cached is not None:
            return cached
        row = self._fetch_one_dict(
            """
            SELECT period_id
            FROM bronze.catapult_periods
            WHERE source_account = %s
              AND period_id = %s
            """,
            (self.source_account, period_id),
        )
        exists = row is not None
        self._period_exists_cache[period_id] = exists
        return exists

    def _fetch_one_dict(
        self,
        sql: str,
        params: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        if self.conn is None:
            return self.db.fetch_one_dict(sql, params)
        with self.db.cursor(conn=self.conn, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def _upsert(
        self,
        table: str,
        records: list[dict[str, Any]],
        *,
        conflict_columns: list[str],
        attempted_rows: int | None = None,
        skip_reasons: dict[str, int] | None = None,
    ) -> int:
        if not records:
            self._set_last_load_stats(
                table=table,
                attempted_rows=attempted_rows or 0,
                loaded_rows=0,
                skip_reasons=skip_reasons,
            )
            return 0

        deduplicated_records, duplicate_rows = self._deduplicate_records(records, conflict_columns)
        if duplicate_rows > 0:
            skip_reasons = dict(skip_reasons or {})
            skip_reasons["duplicate_conflict_key"] = skip_reasons.get("duplicate_conflict_key", 0) + duplicate_rows
        records = deduplicated_records

        update_columns = [
            column
            for column in records[0].keys()
            if column not in set(conflict_columns) and column != "created_at"
        ]
        if "updated_at" not in update_columns:
            update_columns.append("updated_at")
        for record in records:
            record["updated_at"] = datetime.now(timezone.utc)

        self.db.upsert_batch_bronze(
            table=table,
            records=records,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            conn=self.conn,
        )
        loaded_rows = len(records)
        self._set_last_load_stats(
            table=table,
            attempted_rows=attempted_rows if attempted_rows is not None else loaded_rows,
            loaded_rows=loaded_rows,
            skip_reasons=skip_reasons,
        )
        return loaded_rows

    @staticmethod
    def _deduplicate_records(
        records: list[dict[str, Any]],
        conflict_columns: list[str],
    ) -> tuple[list[dict[str, Any]], int]:
        deduplicated: dict[tuple[Any, ...], dict[str, Any]] = {}
        duplicate_rows = 0
        for record in records:
            key = tuple(record.get(column) for column in conflict_columns)
            if key in deduplicated:
                duplicate_rows += 1
            deduplicated[key] = record
        return list(deduplicated.values()), duplicate_rows

    def _set_last_load_stats(
        self,
        *,
        table: str,
        attempted_rows: int,
        loaded_rows: int,
        skip_reasons: dict[str, int] | None,
    ) -> None:
        normalized_reasons = {key: value for key, value in (skip_reasons or {}).items() if value > 0}
        skipped_rows = sum(normalized_reasons.values())
        if attempted_rows < loaded_rows:
            attempted_rows = loaded_rows
        if attempted_rows > loaded_rows and skipped_rows == 0:
            skipped_rows = attempted_rows - loaded_rows
            normalized_reasons = {"dropped_rows": skipped_rows}
        self.last_load_stats = {
            "table": table,
            "attempted_rows": attempted_rows,
            "loaded_rows": loaded_rows,
            "skipped_rows": skipped_rows,
            "skip_reasons": normalized_reasons,
        }


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _normalize_efforts_payload(payload: Any) -> dict[str, list[dict[str, Any]]]:
    if isinstance(payload, dict):
        if any(key in payload for key in ("velocity", "acceleration")):
            return {
                "velocity": [dict(row) for row in payload.get("velocity", []) if isinstance(row, dict)],
                "acceleration": [dict(row) for row in payload.get("acceleration", []) if isinstance(row, dict)],
            }
        data = payload.get("data")
        if isinstance(data, dict):
            return {
                "velocity": [dict(row) for row in data.get("velocity_efforts", []) if isinstance(row, dict)],
                "acceleration": [
                    dict(row) for row in data.get("acceleration_efforts", []) if isinstance(row, dict)
                ],
            }
    if isinstance(payload, list):
        velocity_rows: list[dict[str, Any]] = []
        acceleration_rows: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_efforts_payload(item)
            velocity_rows.extend(normalized["velocity"])
            acceleration_rows.extend(normalized["acceleration"])
        return {"velocity": velocity_rows, "acceleration": acceleration_rows}
    return {"velocity": [], "acceleration": []}


def _normalize_events_payload(payload: Any) -> dict[str, list[dict[str, Any]]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return {
                str(key): [dict(row) for row in value if isinstance(row, dict)]
                for key, value in data.items()
                if isinstance(value, list)
            }
        return {
            str(key): [dict(row) for row in value if isinstance(row, dict)]
            for key, value in payload.items()
            if isinstance(value, list)
        }
    if isinstance(payload, list):
        normalized: dict[str, list[dict[str, Any]]] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            for key, rows in _normalize_events_payload(item).items():
                normalized.setdefault(key, []).extend(rows)
        return normalized
    return {}


def _normalize_sensor_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [dict(row) for row in data if isinstance(row, dict)]
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            rows.extend(_normalize_sensor_rows(item))
        return rows
    return []


def _resolve_effort_type(kind: str, effort: dict[str, Any]) -> str:
    band = _coerce_integer(effort.get("bnum") or effort.get("band"))
    if band is None:
        return kind
    return f"{kind}_band_{band}"


def _coerce_sensor_recorded_at(row: dict[str, Any]) -> datetime | None:
    timestamp = row.get("timestamp") or row.get("recorded_at")
    if timestamp not in (None, ""):
        return _coerce_datetime(timestamp)
    ts_value = _coerce_numeric(row.get("ts"))
    if ts_value is None:
        return None
    cs_value = _coerce_numeric(row.get("cs")) or Decimal("0")
    return _coerce_datetime(float(ts_value + (cs_value / Decimal("100"))))


def _coerce_identifier(value: Any) -> str | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _coerce_stats_dimension_identifier(payload: dict[str, Any], key: str) -> str | None:
    value = _coerce_identifier(payload.get(key))
    if value not in {None, "0"}:
        return value
    fallback = _coerce_identifier(payload.get(f"{key}_id"))
    if fallback == "0":
        return None
    return fallback


def _coerce_integer(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _coerce_boolean(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _coerce_text(value: Any) -> str | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _increment_reason(skip_reasons: dict[str, int], reason: str) -> None:
    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1


def _coerce_numeric(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        if timestamp <= 0:
            return None
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        try:
            return _coerce_datetime(int(text))
        except (OSError, OverflowError, ValueError):
            return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _coerce_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        parsed = _coerce_datetime(value)
        return parsed.date() if parsed is not None else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        parsed = _coerce_datetime(int(text))
        return parsed.date() if parsed is not None else None
    try:
        return date.fromisoformat(text)
    except ValueError:
        parsed = _coerce_datetime(text)
        return parsed.date() if parsed is not None else None


def _pick_numeric_value(payload: dict[str, Any], *, preferred_keys: tuple[str, ...]) -> Decimal | None:
    for key in preferred_keys:
        value = _coerce_numeric(payload.get(key))
        if value is not None:
            return value
    for key, value in payload.items():
        if key in {"timestamp", "dt", "et", "time"}:
            continue
        numeric = _coerce_numeric(value)
        if numeric is not None:
            return numeric
    return None


def _hash_value(*parts: Any) -> str:
    serialized = json.dumps(parts, default=_json_default, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)
