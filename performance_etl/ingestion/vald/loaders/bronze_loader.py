"""
VALD bronze-layer loader.

Maps API response fields to typed bronze table columns and upserts records.
Handles field-name normalisation from camelCase API responses to snake_case
database columns and resolves VALD's inconsistent naming conventions
(e.g. athleteId / hubAthleteId / profileId all map to profile_id).
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import re
from typing import Any

from psycopg2 import extras, extensions

from ingestion.common.db import DatabaseManager
from ingestion.common.logging import get_logger

logger = get_logger(__name__)

_FORCEDECKS_TRIAL_RESULT_CHUNK_SIZE = 1000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _camel_to_snake(name: str) -> str:
    """Convert a camelCase or PascalCase string to snake_case."""
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _safe_json(value: Any) -> str | None:
    """Serialise a value to a JSON string, or return None if the value is None."""
    if value is None:
        return None
    return json.dumps(value)


def _coerce_optional_int(value: Any) -> int | None:
    """Return *value* as an integer when it represents a whole number."""
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value) if value.is_integer() else None

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            try:
                float_value = float(stripped)
            except ValueError:
                return None
            return int(float_value) if float_value.is_integer() else None

    return None


def _normalise_forceframe_tick(force: dict[str, Any], fallback_tick: int) -> tuple[int, bool]:
    """Resolve a ForceFrame trace tick, falling back to a deterministic sequence."""
    for key in ("tick", "sampleIndex", "index"):
        tick = _coerce_optional_int(force.get(key))
        if tick is not None:
            return tick, key != "tick"

    return fallback_tick, True


# ---------------------------------------------------------------------------
# Loader class
# ---------------------------------------------------------------------------


class _ConnectionBoundDatabase:
    """Bind bronze upserts to a caller-managed database transaction."""

    def __init__(
        self,
        db: DatabaseManager,
        conn: extensions.connection | None,
    ) -> None:
        self._db = db
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._db, name)

    @contextmanager
    def connection(self):
        if self._conn is None:
            with self._db.connection() as conn:
                yield conn
            return

        yield self._conn

    @contextmanager
    def cursor(
        self,
        cursor_factory: Any = None,
    ):
        if self._conn is None:
            with self._db.cursor(cursor_factory=cursor_factory) as cur:
                yield cur
            return

        cur = self._conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
        finally:
            cur.close()

    def upsert_bronze(
        self,
        table: str,
        data: dict[str, Any],
        conflict_columns: list[str],
        update_columns: list[str],
    ) -> None:
        self._db.upsert_bronze(
            table=table,
            data=data,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            conn=self._conn,
        )

    def upsert_batch_bronze(
        self,
        table: str,
        records: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: list[str],
    ) -> None:
        self._db.upsert_batch_bronze(
            table=table,
            records=records,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            conn=self._conn,
        )

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> None:
        if self._conn is None:
            self._db.execute(sql, params)
            return

        with self._conn.cursor() as cur:
            cur.execute(sql, params)

    def fetch_one(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> tuple[Any, ...] | None:
        if self._conn is None:
            return self._db.fetch_one(sql, params)

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


class ValdBronzeLoader:
    """Upsert parsed VALD API records into ``bronze.vald_*`` tables.

    Args:
        db: An initialised :class:`~ingestion.common.db.DatabaseManager`.
        batch_id: UUID string for the current ingestion batch.
        conn: Optional connection used to keep multiple bronze writes in a
            single caller-managed transaction.
    """

    def __init__(
        self,
        db: DatabaseManager,
        batch_id: str,
        conn: extensions.connection | None = None,
        table_overrides: dict[str, str] | None = None,
    ) -> None:
        self.db = _ConnectionBoundDatabase(db, conn)
        self.batch_id = batch_id
        self._table_overrides = table_overrides or {}
        self._profile_id_cache: dict[str, str] = {}
        self._dynamo_test_context_cache: dict[str, tuple[str | None, Any]] = {}

    def _table(self, live_table: str) -> str:
        return self._table_overrides.get(live_table, live_table)

    def prefetch_forceframe_profile_ids(self, test_ids: list[str]) -> None:
        """Pre-populate the profile_id cache for a batch of ForceFrame test_ids.

        Replaces N individual SELECTs (one per unseen test) with a single
        ``WHERE test_id = ANY(...)`` query, which is critical when replaying
        a large backlog of force-trace tests.
        """
        uncached = [tid for tid in test_ids if tid and tid not in self._profile_id_cache]
        if not uncached:
            return
        ff_tests_table = self._table("bronze.vald_forceframe_tests")
        rows = self.db.fetch_all_dict(
            f"SELECT test_id::text, profile_id::text FROM {ff_tests_table} WHERE test_id = ANY(%s::uuid[])",
            (uncached,),
        )
        for row in rows:
            self._profile_id_cache[row["test_id"]] = row["profile_id"]

    def prefetch_dynamo_test_context(self, test_ids: list[str]) -> None:
        """Pre-populate profile/start-time context for DynaMo trace test_ids."""
        uncached = [
            tid
            for tid in dict.fromkeys(test_ids)
            if tid and tid not in self._dynamo_test_context_cache
        ]
        if not uncached:
            return
        dynamo_tests_table = self._table("bronze.vald_dynamo_tests")
        rows = self.db.fetch_all_dict(
            f"""
            SELECT test_id::text, profile_id::text, start_time_utc
            FROM {dynamo_tests_table}
            WHERE test_id = ANY(%s::uuid[])
            """,
            (uncached,),
        )
        for row in rows:
            self._dynamo_test_context_cache[row["test_id"]] = (
                row.get("profile_id"),
                row.get("start_time_utc"),
            )

    # ------------------------------------------------------------------
    # Reference entities
    # ------------------------------------------------------------------

    def load_profiles(
        self, profiles: list[dict], raw_id: int, tenant_id: str | None = None,
    ) -> int:
        """Upsert profile records into ``bronze.vald_profiles``.

        Args:
            profiles: List of profile dicts from the API.
            raw_id: The ``raw_id`` of the source raw record.
            tenant_id: Tenant UUID to use if the API response doesn't include it.

        Returns:
            Number of records upserted.
        """
        if not profiles:
            return 0

        records = []
        for p in profiles:
            records.append({
                "vald_profile_id": p.get("id") or p.get("profileId"),
                "tenant_id": p.get("tenantId") or p.get("teamId") or tenant_id,
                "given_name": p.get("givenName"),
                "family_name": p.get("familyName"),
                "external_id": p.get("externalId"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_profiles"),
            records=records,
            conflict_columns=["vald_profile_id"],
            update_columns=[
                "tenant_id", "given_name", "family_name", "external_id",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d profiles (raw_id=%s)", len(records), raw_id)

        # Also parse profile → category assignments from attributes
        self._load_profile_categories(profiles, raw_id, tenant_id)

        return len(records)

    def _load_profile_categories(
        self,
        profiles: list[dict],
        raw_id: int,
        tenant_id: str | None = None,
    ) -> int:
        """Parse profile ``attributes`` into ``bronze.vald_profile_categories``."""
        records = []
        seen: set[tuple[str, str]] = set()
        for p in profiles:
            pid = str(p.get("id") or p.get("profileId") or "")
            tid = str(p.get("tenantId") or p.get("teamId") or tenant_id or "")
            for attr in p.get("attributes", []):
                cat_id = str(attr.get("attributeTypeId") or "")
                key = (pid, cat_id)
                if not pid or not cat_id or key in seen:
                    continue
                seen.add(key)
                records.append({
                    "vald_profile_id": pid,
                    "tenant_id": tid,
                    "category_id": cat_id,
                    "category_name": attr.get("typeName"),
                    "group_id": attr.get("attributeValueId"),
                    "group_name": attr.get("valueName"),
                    "raw_id": raw_id,
                })

        if not records:
            return 0

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_profile_categories"),
            records=records,
            conflict_columns=["vald_profile_id", "category_id"],
            update_columns=[
                "category_name", "group_id", "group_name",
                "raw_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d profile-category assignments (raw_id=%s)",
            len(records), raw_id,
        )
        return len(records)

    # ------------------------------------------------------------------
    # ForceDecks
    # ------------------------------------------------------------------

    def load_forcedecks_tests(
        self, tests: list[dict], raw_id: int, tenant_id: str | None = None,
    ) -> int:
        """Upsert ForceDecks test records into ``bronze.vald_forcedecks_tests``.

        Args:
            tests: List of test dicts from the ForceDecks API.
            raw_id: The ``raw_id`` of the source raw record.
            tenant_id: Tenant UUID to use if the API response doesn't include it.

        Returns:
            Number of records upserted.
        """
        if not tests:
            return 0

        records = []
        for t in tests:
            records.append({
                "test_id": t.get("id") or t.get("testId"),
                "tenant_id": t.get("tenantId") or t.get("teamId") or tenant_id,
                "profile_id": (
                    t.get("profileId")
                    or t.get("athleteId")
                    or t.get("hubAthleteId")
                ),
                "recording_id": t.get("recordingId"),
                "modified_date_utc": t.get("modifiedDateUtc") or t.get("modifiedDateUTC"),
                "recorded_date_utc": t.get("recordedDateUtc") or t.get("recordedDateUTC"),
                "analysed_date_utc": t.get("analysedDateUtc") or t.get("analysedDateUTC"),
                "test_type": t.get("testType") or t.get("testTypeName"),
                "notes": t.get("notes"),
                "weight": t.get("weight"),
                "parameter": _safe_json(t.get("parameter") or t.get("parameters")),
                "extended_parameters": _safe_json(t.get("extendedParameters")),
                "attributes": _safe_json(t.get("attributes")),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_forcedecks_tests"),
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "profile_id", "recording_id",
                "modified_date_utc", "recorded_date_utc", "analysed_date_utc",
                "test_type", "notes", "weight",
                "parameter", "extended_parameters", "attributes",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d ForceDecks tests (raw_id=%s)", len(records), raw_id
        )
        return len(records)

    # ------------------------------------------------------------------
    # ForceFrame
    # ------------------------------------------------------------------

    def load_forceframe_tests(
        self, tests: list[dict], raw_id: int, tenant_id: str | None = None,
    ) -> int:
        """Upsert ForceFrame test records into ``bronze.vald_forceframe_tests``.

        Args:
            tests: List of test dicts from the ForceFrame API.
            raw_id: The ``raw_id`` of the source raw record.
            tenant_id: Tenant UUID to use if the API response doesn't include it.

        Returns:
            Number of records upserted.
        """
        if not tests:
            return 0

        records = []
        for t in tests:
            records.append({
                "test_id": t.get("id") or t.get("testId"),
                "tenant_id": t.get("tenantId") or t.get("teamId") or tenant_id,
                "profile_id": (
                    t.get("profileId")
                    or t.get("athleteId")
                    or t.get("hubAthleteId")
                ),
                "test_date_utc": t.get("testDateUtc") or t.get("testDateUTC"),
                "test_type_id": t.get("testTypeId"),
                "test_type_name": t.get("testTypeName"),
                "test_position_id": t.get("testPositionId"),
                "test_position_name": t.get("testPositionName"),
                "notes": t.get("notes"),
                "device": t.get("device"),
                "modified_date_utc": t.get("modifiedDateUtc") or t.get("modifiedDateUTC"),
                "inner_left_avg_force": t.get("innerLeftAvgForce"),
                "inner_left_impulse": t.get("innerLeftImpulse"),
                "inner_left_max_force": t.get("innerLeftMaxForce"),
                "inner_left_repetitions": t.get("innerLeftRepetitions"),
                "inner_right_avg_force": t.get("innerRightAvgForce"),
                "inner_right_impulse": t.get("innerRightImpulse"),
                "inner_right_max_force": t.get("innerRightMaxForce"),
                "inner_right_repetitions": t.get("innerRightRepetitions"),
                "outer_left_avg_force": t.get("outerLeftAvgForce"),
                "outer_left_impulse": t.get("outerLeftImpulse"),
                "outer_left_max_force": t.get("outerLeftMaxForce"),
                "outer_left_repetitions": t.get("outerLeftRepetitions"),
                "outer_right_avg_force": t.get("outerRightAvgForce"),
                "outer_right_impulse": t.get("outerRightImpulse"),
                "outer_right_max_force": t.get("outerRightMaxForce"),
                "outer_right_repetitions": t.get("outerRightRepetitions"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_forceframe_tests"),
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "profile_id", "test_date_utc",
                "test_type_id", "test_type_name",
                "test_position_id", "test_position_name",
                "notes", "device", "modified_date_utc",
                "inner_left_avg_force", "inner_left_impulse",
                "inner_left_max_force", "inner_left_repetitions",
                "inner_right_avg_force", "inner_right_impulse",
                "inner_right_max_force", "inner_right_repetitions",
                "outer_left_avg_force", "outer_left_impulse",
                "outer_left_max_force", "outer_left_repetitions",
                "outer_right_avg_force", "outer_right_impulse",
                "outer_right_max_force", "outer_right_repetitions",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d ForceFrame tests (raw_id=%s)", len(records), raw_id
        )
        return len(records)

    # ------------------------------------------------------------------
    # NordBord
    # ------------------------------------------------------------------

    def load_nordbord_tests(
        self, tests: list[dict], raw_id: int, tenant_id: str | None = None,
    ) -> int:
        """Upsert NordBord test records into ``bronze.vald_nordbord_tests``.

        Args:
            tests: List of test dicts from the NordBord API.
            raw_id: The ``raw_id`` of the source raw record.
            tenant_id: Tenant UUID to use if the API response doesn't include it.

        Returns:
            Number of records upserted.
        """
        if not tests:
            return 0

        records = []
        for t in tests:
            records.append({
                "test_id": t.get("id") or t.get("testId"),
                "tenant_id": t.get("tenantId") or t.get("teamId") or tenant_id,
                "profile_id": (
                    t.get("profileId")
                    or t.get("athleteId")
                    or t.get("hubAthleteId")
                ),
                "test_date_utc": t.get("testDateUtc") or t.get("testDateUTC"),
                "test_type_id": t.get("testTypeId"),
                "test_type_name": t.get("testTypeName"),
                "notes": t.get("notes"),
                "device": t.get("device"),
                "modified_date_utc": t.get("modifiedDateUtc") or t.get("modifiedDateUTC"),
                "left_avg_force": t.get("leftAvgForce"),
                "left_impulse": t.get("leftImpulse"),
                "left_max_force": t.get("leftMaxForce"),
                "left_torque": t.get("leftTorque"),
                "left_calibration": t.get("leftCalibration"),
                "left_repetitions": t.get("leftRepetitions"),
                "right_avg_force": t.get("rightAvgForce"),
                "right_impulse": t.get("rightImpulse"),
                "right_max_force": t.get("rightMaxForce"),
                "right_torque": t.get("rightTorque"),
                "right_calibration": t.get("rightCalibration"),
                "right_repetitions": t.get("rightRepetitions"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_nordbord_tests"),
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "profile_id", "test_date_utc",
                "test_type_id", "test_type_name",
                "notes", "device", "modified_date_utc",
                "left_avg_force", "left_impulse", "left_max_force",
                "left_torque", "left_calibration", "left_repetitions",
                "right_avg_force", "right_impulse", "right_max_force",
                "right_torque", "right_calibration", "right_repetitions",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d NordBord tests (raw_id=%s)", len(records), raw_id
        )
        return len(records)

    # ------------------------------------------------------------------
    # HumanTrak
    # ------------------------------------------------------------------

    def load_humantrak_tests(self, tests: list[dict], raw_id: int) -> int:
        """Upsert HumanTrak test records into ``bronze.vald_humantrak_tests``.

        Args:
            tests: List of test dicts from the HumanTrak API.
            raw_id: The ``raw_id`` of the source raw record.

        Returns:
            Number of records upserted.
        """
        if not tests:
            return 0

        records = []
        for t in tests:
            records.append({
                "test_id": t.get("id") or t.get("testId"),
                "tenant_id": t.get("tenantId") or t.get("teamId"),
                "profile_id": (
                    t.get("profileId")
                    or t.get("athleteId")
                    or t.get("hubAthleteId")
                ),
                "start_date_utc": t.get("startDateUtc") or t.get("startDateUTC"),
                "end_date_utc": t.get("endDateUtc") or t.get("endDateUTC"),
                "modified_date_utc": t.get("modifiedDateUtc") or t.get("modifiedDateUTC"),
                "test_type_code": t.get("testTypeCode") or t.get("testType"),
                "repetition_counts": _safe_json(t.get("repetitionCounts")),
                "metric_groups": _safe_json(t.get("metricGroups")),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table="bronze.vald_humantrak_tests",
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "profile_id",
                "start_date_utc", "end_date_utc", "modified_date_utc",
                "test_type_code", "repetition_counts", "metric_groups",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d HumanTrak tests (raw_id=%s)", len(records), raw_id
        )
        return len(records)

    # ------------------------------------------------------------------
    # SmartSpeed
    # ------------------------------------------------------------------

    def load_smartspeed_summaries(
        self, summaries: list[dict], raw_id: int, tenant_id: str | None = None,
    ) -> int:
        """Upsert SmartSpeed test summary records.

        Loads into ``bronze.vald_smartspeed_test_summaries``.

        Args:
            summaries: List of test summary dicts from the SmartSpeed API.
            raw_id: The ``raw_id`` of the source raw record.
            tenant_id: Tenant UUID to use if the API response doesn't include it.

        Returns:
            Number of records upserted.
        """
        if not summaries:
            return 0

        records = []
        for s in summaries:
            records.append({
                "test_id": s.get("id") or s.get("testId"),
                "test_result_id": s.get("testResultId"),
                "tenant_id": s.get("tenantId") or s.get("teamId") or tenant_id,
                "profile_id": (
                    s.get("profileId")
                    or s.get("athleteId")
                    or s.get("hubAthleteId")
                ),
                "group_under_test_id": s.get("groupUnderTestId"),
                "test_name": s.get("testName"),
                "test_type_name": s.get("testTypeName"),
                "rep_count": s.get("repCount"),
                "device_count": s.get("deviceCount"),
                "test_date_utc": s.get("testDateUtc") or s.get("testDateUTC"),
                "is_valid": s.get("isValid"),
                "all_groups": _safe_json(s.get("allGroups")),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_smartspeed_test_summaries"),
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "test_result_id", "tenant_id", "profile_id",
                "group_under_test_id", "test_name", "test_type_name",
                "rep_count", "device_count", "test_date_utc", "is_valid",
                "all_groups",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d SmartSpeed summaries (raw_id=%s)",
            len(records),
            raw_id,
        )
        return len(records)

    # ------------------------------------------------------------------
    # DynaMo
    # ------------------------------------------------------------------

    def load_dynamo_tests(self, tests: list[dict], raw_id: int) -> int:
        """Upsert DynaMo test records into ``bronze.vald_dynamo_tests``.

        Args:
            tests: List of test dicts from the DynaMo API.
            raw_id: The ``raw_id`` of the source raw record.

        Returns:
            Number of records upserted.
        """
        if not tests:
            return 0

        records = []
        for t in tests:
            records.append({
                "test_id": t.get("id") or t.get("testId"),
                "tenant_id": t.get("tenantId") or t.get("teamId"),
                "profile_id": (
                    t.get("profileId")
                    or t.get("athleteId")
                    or t.get("hubAthleteId")
                ),
                "test_category": t.get("testCategory"),
                "body_region": t.get("bodyRegion"),
                "movement": t.get("movement"),
                "position": t.get("position"),
                "laterality": t.get("laterality"),
                "attachments": _safe_json(t.get("attachments")),
                "start_time_utc": t.get("startTimeUtc") or t.get("startTimeUTC"),
                "duration_seconds": t.get("durationSeconds"),
                "hardware_info": _safe_json(t.get("hardwareInfo")),
                "software_info": _safe_json(t.get("softwareInfo")),
                "analysis_info": _safe_json(t.get("analysisInfo")),
                "analysed_date_utc": (
                    t.get("analysedDateUtc") or t.get("analysedDateUTC")
                ),
                "asymmetries": _safe_json(t.get("asymmetries")),
                "ratios": _safe_json(t.get("ratios")),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_dynamo_tests"),
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "profile_id",
                "test_category", "body_region", "movement",
                "position", "laterality", "attachments",
                "start_time_utc", "duration_seconds",
                "hardware_info", "software_info", "analysis_info",
                "analysed_date_utc", "asymmetries", "ratios",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d DynaMo tests (raw_id=%s)", len(records), raw_id
        )

        # Extract inline rep_summaries if present
        for t in tests:
            test_id = t.get("id") or t.get("testId")
            rep_summaries = (
                t.get("repetitionTypeSummaries")
                or t.get("repSummaries")
                or t.get("repetitionSummaries")
            )
            if test_id and rep_summaries:
                self.load_dynamo_rep_summaries(str(test_id), rep_summaries, raw_id)

        return len(records)

    # ------------------------------------------------------------------
    # ForceDecks sub-details
    # ------------------------------------------------------------------

    def load_forcedecks_result_definitions(
        self, definitions: list[dict], raw_id: int,
    ) -> int:
        """Upsert ForceDecks result definitions into bronze."""
        if not definitions:
            return 0

        records = []
        for d in definitions:
            records.append({
                "result_id": d.get("id") or d.get("resultId"),
                "result_id_string": d.get("resultIdString"),
                "result_name": d.get("resultName") or d.get("name"),
                "result_description": d.get("resultDescription") or d.get("description"),
                "result_group": d.get("resultGroup") or d.get("group"),
                "supports_asymmetry": d.get("supportsAsymmetry"),
                "is_repeat_result": d.get("isRepeatResult"),
                "result_unit": d.get("resultUnit") or d.get("unit"),
                "result_unit_name": d.get("resultUnitName") or d.get("unitName"),
                "result_unit_scale_factor": d.get("resultUnitScaleFactor"),
                "number_of_decimal_places": d.get("numberOfDecimalPlaces"),
                "trend_direction": d.get("trendDirection"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_forcedecks_result_definitions"),
            records=records,
            conflict_columns=["result_id"],
            update_columns=[
                "result_id_string", "result_name", "result_description",
                "result_group", "supports_asymmetry", "is_repeat_result",
                "result_unit", "result_unit_name", "result_unit_scale_factor",
                "number_of_decimal_places", "trend_direction",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d ForceDecks result definitions", len(records))
        return len(records)

    def load_forcedecks_trials(
        self, trials: list[dict], raw_id: int,
    ) -> int:
        """Upsert ForceDecks trials into bronze."""
        if not trials:
            return 0

        records = []
        for t in trials:
            trial_id = t.get("id") or t.get("trialId")
            test_id = t.get("testId")
            profile_id = (
                t.get("profileId") or t.get("athleteId")
                or t.get("hubAthleteId")
            )

            records.append({
                "trial_id": trial_id,
                "test_id": test_id,
                "profile_id": profile_id,
                "recorded_utc": t.get("recordedUtc") or t.get("recordedUTC"),
                "start_time": t.get("startTime"),
                "end_time": t.get("endTime"),
                "limb": t.get("limb"),
                "last_modified_utc": (
                    t.get("lastModifiedUtc") or t.get("lastModifiedUTC")
                ),
                "results": _safe_json(t.get("results")),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_forcedecks_trials"),
            records=records,
            conflict_columns=["trial_id"],
            update_columns=[
                "test_id", "profile_id", "recorded_utc",
                "start_time", "end_time", "limb",
                "last_modified_utc", "results",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        self._load_forcedecks_trial_results(trials, raw_id)
        logger.info("Upserted %d ForceDecks trials (raw_id=%s)", len(records), raw_id)
        return len(records)

    def _load_forcedecks_trial_results(
        self, trials: list[dict], raw_id: int,
    ) -> int:
        """Replace parsed ForceDecks trial results for the supplied trials."""
        table_name = self._table("bronze.vald_forcedecks_trial_results")
        result_rows: list[dict[str, Any]] = []
        trial_ids: list[str] = []

        for trial in trials:
            trial_id = trial.get("id") or trial.get("trialId")
            test_id = trial.get("testId")
            profile_id = (
                trial.get("profileId")
                or trial.get("athleteId")
                or trial.get("hubAthleteId")
            )
            if not trial_id or not test_id or not profile_id:
                continue

            trial_ids.append(str(trial_id))
            for result in trial.get("results", []) or []:
                result_id = result.get("resultId")
                if result_id is None:
                    definition = result.get("definition") or {}
                    result_id = definition.get("id") or definition.get("resultId")
                if result_id is None:
                    continue

                result_rows.append({
                    "trial_id": trial_id,
                    "test_id": test_id,
                    "profile_id": profile_id,
                    "result_id": result_id,
                    "value": result.get("value"),
                    "time": result.get("time"),
                    "limb": result.get("limb") or trial.get("limb"),
                    "repeat": result.get("repeat"),
                    "raw_id": raw_id,
                    "batch_id": self.batch_id,
                })

        if not trial_ids:
            return 0

        # Phase 8.7.B (2026-05-09): replaced the chunked DELETE+INSERT pattern
        # with INSERT … ON CONFLICT DO UPDATE per chunk. The natural-key UNIQUE
        # index uq_vald_forcedecks_trial_results_nk on
        # (trial_id, result_id, limb, repeat) NULLS NOT DISTINCT was added in
        # Phase 8.7.B.1 (sql/ddl/bronze/47). PG infers the arbiter index from
        # the conflict-target column list. Locked decision #7 satisfied:
        # no DELETE on the live table; UPSERT is a single atomic statement.
        if result_rows:
            columns = list(result_rows[0].keys())
            update_columns = [
                c for c in columns
                if c not in ("trial_id", "result_id", "limb", "repeat")
            ]
            update_clause = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in update_columns
            )
            sql = (
                f"INSERT INTO {table_name} "
                f"({', '.join(columns)}) VALUES %s "
                f"ON CONFLICT (trial_id, result_id, limb, repeat) "
                f"DO UPDATE SET {update_clause}"
            )
            template = "(" + ", ".join([f"%({c})s" for c in columns]) + ")"

            with self.db.connection() as conn:
                with conn.cursor() as cur:
                    for index in range(0, len(result_rows), _FORCEDECKS_TRIAL_RESULT_CHUNK_SIZE):
                        chunk_rows = result_rows[index : index + _FORCEDECKS_TRIAL_RESULT_CHUNK_SIZE]
                        extras.execute_values(
                            cur,
                            sql,
                            chunk_rows,
                            template=template,
                        )

        logger.info(
            "Rebuilt %d ForceDecks trial results across %d trials",
            len(result_rows),
            len(trial_ids),
        )
        return len(result_rows)

    # ------------------------------------------------------------------
    # ForceFrame sub-details
    # ------------------------------------------------------------------

    def load_forceframe_test_metrics(
        self, test_id: str, tenant_id: str,
        metrics: dict, raw_id: int,
    ) -> int:
        """Upsert ForceFrame test metrics (JSONB payload) into bronze."""
        if not metrics:
            return 0

        record = {
            "test_id": test_id,
            "tenant_id": tenant_id,
            "metrics_payload": _safe_json(metrics),
            "raw_id": raw_id,
            "batch_id": self.batch_id,
        }

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_forceframe_test_metrics"),
            records=[record],
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "metrics_payload",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        return 1

    # ------------------------------------------------------------------
    # NordBord sub-details
    # ------------------------------------------------------------------

    def load_nordbord_ecc_exercises(
        self, exercises: list[dict], raw_id: int,
    ) -> int:
        """Upsert NordBord eccentric exercises into bronze."""
        if not exercises:
            return 0

        records = []
        for e in exercises:
            records.append({
                "exercise_id": e.get("id") or e.get("exerciseId"),
                "session_id": e.get("sessionId"),
                "program_exercise_id": e.get("programExerciseId"),
                "profile_id": (
                    e.get("profileId") or e.get("athleteId")
                    or e.get("hubAthleteId")
                ),
                "tenant_id": e.get("tenantId") or e.get("teamId"),
                "exercise_date_utc": (
                    e.get("exerciseDateUtc") or e.get("exerciseDateUTC")
                ),
                "modified_date_utc": (
                    e.get("modifiedDateUtc") or e.get("modifiedDateUTC")
                    or e.get("modifiedUtc")
                ),
                "force_left": e.get("forceLeft"),
                "force_right": e.get("forceRight"),
                "impulse_left": e.get("impulseLeft"),
                "impulse_right": e.get("impulseRight"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_nordbord_ecc_exercises"),
            records=records,
            conflict_columns=["exercise_id"],
            update_columns=[
                "session_id", "program_exercise_id", "profile_id",
                "tenant_id", "exercise_date_utc", "modified_date_utc",
                "force_left", "force_right", "impulse_left", "impulse_right",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d NordBord ecc exercises", len(records))
        return len(records)

    def load_nordbord_ecc_repetitions(
        self, reps: list[dict], raw_id: int,
    ) -> int:
        """Upsert NordBord eccentric repetitions into bronze."""
        if not reps:
            return 0

        records = []
        for r in reps:
            records.append({
                "repetition_id": r.get("id") or r.get("repetitionId"),
                "profile_id": (
                    r.get("profileId") or r.get("athleteId")
                    or r.get("hubAthleteId")
                ),
                "tenant_id": r.get("tenantId") or r.get("teamId"),
                "session_id": r.get("sessionId"),
                "session_exercise_id": r.get("sessionExerciseId"),
                "program_exercise_id": r.get("programExerciseId"),
                "repetition_number": r.get("repetitionNumber"),
                "repetition_date_utc": (
                    r.get("repetitionDateUtc") or r.get("repetitionDateUTC")
                ),
                "modified_date_utc": (
                    r.get("modifiedDateUtc") or r.get("modifiedDateUTC")
                    or r.get("modifiedUtc")
                ),
                "force_left": r.get("forceLeft"),
                "force_right": r.get("forceRight"),
                "impulse_left": r.get("impulseLeft"),
                "impulse_right": r.get("impulseRight"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_nordbord_ecc_repetitions"),
            records=records,
            conflict_columns=["repetition_id"],
            update_columns=[
                "profile_id", "tenant_id", "session_id",
                "session_exercise_id", "program_exercise_id",
                "repetition_number", "repetition_date_utc",
                "modified_date_utc", "force_left", "force_right",
                "impulse_left", "impulse_right",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d NordBord ecc repetitions", len(records))
        return len(records)

    def load_nordbord_test_metrics(
        self, test_id: str, tenant_id: str,
        metrics: dict, raw_id: int,
    ) -> int:
        """Upsert NordBord test metrics (JSONB payload) into bronze."""
        if not metrics:
            return 0

        record = {
            "test_id": test_id,
            "tenant_id": tenant_id,
            "metrics_payload": _safe_json(metrics),
            "raw_id": raw_id,
            "batch_id": self.batch_id,
        }

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_nordbord_test_metrics"),
            records=[record],
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "metrics_payload",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        return 1

    # ------------------------------------------------------------------
    # SmartSpeed sub-details
    # ------------------------------------------------------------------

    def load_smartspeed_test_details(
        self, details: list[dict], raw_id: int,
        tenant_id: str | None = None,
    ) -> int:
        """Upsert SmartSpeed test detail records into bronze."""
        if not details:
            return 0

        records = []
        for d in details:
            test_id = d.get("testId") or d.get("id")
            rep_results = d.get("repResults", [])
            records.append({
                "test_id": test_id,
                "tenant_id": tenant_id,
                "profile_id": (
                    d.get("profileId") or d.get("athleteId")
                    or d.get("hubAthleteId")
                ),
                "session_id": d.get("sessionId"),
                "group_under_test_id": d.get("groupUnderTestId"),
                "test_date_utc": d.get("testDateUtc") or d.get("testDateUTC"),
                "trial_index": d.get("trialIndex"),
                "tag": d.get("tag"),
                "additional_test_result": _safe_json(
                    d.get("additionalTestResult")
                ),
                "rep_results": _safe_json(rep_results),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

            # Also load individual rep results
            if rep_results:
                self._load_smartspeed_rep_results(
                    test_id, rep_results, raw_id,
                )

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_smartspeed_test_details"),
            records=records,
            conflict_columns=["test_id"],
            update_columns=[
                "tenant_id", "profile_id", "session_id",
                "group_under_test_id", "test_date_utc", "trial_index",
                "tag", "additional_test_result", "rep_results",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d SmartSpeed test details", len(records))
        return len(records)

    def _load_smartspeed_rep_results(
        self, test_id: str, rep_results: list[dict], raw_id: int,
    ) -> int:
        """Insert SmartSpeed per-rep results into bronze."""
        if not rep_results:
            return 0

        records = []
        for i, rep in enumerate(rep_results, 1):
            records.append({
                "test_id": test_id,
                "rep_number": rep.get("repNumber", i),
                "rep_data": _safe_json(rep),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_smartspeed_rep_results"),
            records=records,
            conflict_columns=["test_id", "rep_number"],
            update_columns=["rep_data", "raw_id", "batch_id", "updated_at"],
        )
        return len(records)

    # ------------------------------------------------------------------
    # DynaMo sub-details
    # ------------------------------------------------------------------

    def load_dynamo_rep_summaries(
        self, test_id: str, summaries: list[dict], raw_id: int,
    ) -> int:
        """Insert DynaMo repetition summaries into bronze."""
        if not summaries:
            return 0

        records = []
        for s in summaries:
            records.append({
                "test_id": test_id,
                "movement_type": s.get("movement") or s.get("movementType"),
                "side": s.get("laterality") or s.get("side"),
                "max_force_newtons": s.get("maxForceNewtons"),
                "avg_force_newtons": s.get("avgForceNewtons"),
                "max_impulse_ns": s.get("maxImpulseNewtonSeconds") or s.get("maxImpulseNs"),
                "avg_impulse_ns": s.get("avgImpulseNewtonSeconds") or s.get("avgImpulseNs"),
                "max_rfd_nps": (
                    s.get("maxRateOfForceDevelopmentNewtonsPerSecond")
                    or s.get("maxRfdNps")
                ),
                "avg_rfd_nps": (
                    s.get("avgRateOfForceDevelopmentNewtonsPerSecond")
                    or s.get("avgRfdNps")
                ),
                "avg_time_to_peak_s": s.get("avgTimeToPeakForceSeconds") or s.get("avgTimeToPeakS"),
                "min_time_to_peak_s": s.get("minTimeToPeakForceSeconds") or s.get("minTimeToPeakS"),
                "max_rom_degrees": s.get("maxRangeOfMotionDegrees") or s.get("maxRomDegrees"),
                "avg_rom_degrees": s.get("avgRangeOfMotionDegrees") or s.get("avgRomDegrees"),
                "summary_payload": _safe_json(s),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_dynamo_rep_summaries"),
            records=records,
            conflict_columns=["test_id", "movement_type", "side"],
            update_columns=[
                "max_force_newtons", "avg_force_newtons",
                "max_impulse_ns", "avg_impulse_ns",
                "max_rfd_nps", "avg_rfd_nps",
                "avg_time_to_peak_s", "min_time_to_peak_s",
                "max_rom_degrees", "avg_rom_degrees",
                "summary_payload", "raw_id", "batch_id", "updated_at",
            ],
        )

        logger.info(
            "Inserted %d DynaMo rep summaries for test %s", len(records), test_id
        )
        return len(records)

    # ------------------------------------------------------------------
    # NordBord ISO training
    # ------------------------------------------------------------------

    def load_nordbord_iso_sessions(
        self, sessions: list[dict], raw_id: int,
    ) -> int:
        """Upsert NordBord isometric sessions into bronze."""
        if not sessions:
            return 0

        records = []
        for s in sessions:
            records.append({
                "session_id": s.get("id") or s.get("sessionId"),
                "profile_id": (
                    s.get("profileId") or s.get("athleteId")
                    or s.get("hubAthleteId")
                ),
                "tenant_id": s.get("tenantId") or s.get("teamId"),
                "session_date_utc": (
                    s.get("sessionDateUtc") or s.get("sessionDateUTC")
                ),
                "modified_date_utc": (
                    s.get("modifiedDateUtc") or s.get("modifiedDateUTC")
                    or s.get("modifiedUtc")
                ),
                "program_id": s.get("programId"),
                "raw_payload": _safe_json(s),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table="bronze.vald_nordbord_iso_sessions",
            records=records,
            conflict_columns=["session_id"],
            update_columns=[
                "profile_id", "tenant_id", "session_date_utc",
                "modified_date_utc", "program_id", "raw_payload",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d NordBord iso sessions", len(records))
        return len(records)

    def load_nordbord_iso_exercises(
        self, exercises: list[dict], raw_id: int,
    ) -> int:
        """Upsert NordBord isometric exercises into bronze."""
        if not exercises:
            return 0

        records = []
        for e in exercises:
            records.append({
                "exercise_id": e.get("id") or e.get("exerciseId"),
                "session_id": e.get("sessionId"),
                "program_exercise_id": e.get("programExerciseId"),
                "profile_id": (
                    e.get("profileId") or e.get("athleteId")
                    or e.get("hubAthleteId")
                ),
                "tenant_id": e.get("tenantId") or e.get("teamId"),
                "exercise_date_utc": (
                    e.get("exerciseDateUtc") or e.get("exerciseDateUTC")
                ),
                "modified_date_utc": (
                    e.get("modifiedDateUtc") or e.get("modifiedDateUTC")
                    or e.get("modifiedUtc")
                ),
                "force_left": e.get("forceLeft"),
                "force_right": e.get("forceRight"),
                "impulse_left": e.get("impulseLeft"),
                "impulse_right": e.get("impulseRight"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table="bronze.vald_nordbord_iso_exercises",
            records=records,
            conflict_columns=["exercise_id"],
            update_columns=[
                "session_id", "program_exercise_id", "profile_id",
                "tenant_id", "exercise_date_utc", "modified_date_utc",
                "force_left", "force_right", "impulse_left", "impulse_right",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d NordBord iso exercises", len(records))
        return len(records)

    def load_nordbord_iso_repetitions(
        self, reps: list[dict], raw_id: int,
    ) -> int:
        """Upsert NordBord isometric repetitions into bronze."""
        if not reps:
            return 0

        records = []
        for r in reps:
            records.append({
                "repetition_id": r.get("id") or r.get("repetitionId"),
                "profile_id": (
                    r.get("profileId") or r.get("athleteId")
                    or r.get("hubAthleteId")
                ),
                "tenant_id": r.get("tenantId") or r.get("teamId"),
                "session_id": r.get("sessionId"),
                "session_exercise_id": r.get("sessionExerciseId"),
                "program_exercise_id": r.get("programExerciseId"),
                "repetition_number": r.get("repetitionNumber"),
                "repetition_date_utc": (
                    r.get("repetitionDateUtc") or r.get("repetitionDateUTC")
                ),
                "modified_date_utc": (
                    r.get("modifiedDateUtc") or r.get("modifiedDateUTC")
                    or r.get("modifiedUtc")
                ),
                "force_left": r.get("forceLeft"),
                "force_right": r.get("forceRight"),
                "impulse_left": r.get("impulseLeft"),
                "impulse_right": r.get("impulseRight"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table="bronze.vald_nordbord_iso_repetitions",
            records=records,
            conflict_columns=["repetition_id"],
            update_columns=[
                "profile_id", "tenant_id", "session_id",
                "session_exercise_id", "program_exercise_id",
                "repetition_number", "repetition_date_utc",
                "modified_date_utc", "force_left", "force_right",
                "impulse_left", "impulse_right",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d NordBord iso repetitions", len(records))
        return len(records)

    # ------------------------------------------------------------------
    # NordBord force traces
    # ------------------------------------------------------------------

    def load_nordbord_force_traces(
        self, test_id: str, trace_data: dict, raw_id: int,
    ) -> int:
        """Insert NordBord force trace data points into bronze.

        The API returns a dict with a ``forces`` list of objects, each
        containing ``tick``, ``leftForce``, and ``rightForce``.
        We also need the profile_id from the test record.
        """
        forces = trace_data.get("forces", [])
        if not forces:
            return 0

        # Get profile_id from the test record
        row = self.db.fetch_one(
            "SELECT profile_id FROM bronze.vald_nordbord_tests WHERE test_id = %s",
            (test_id,),
        )
        profile_id = str(row[0]) if row else None

        records = []
        for f in forces:
            records.append({
                "test_id": test_id,
                "profile_id": profile_id,
                "tick": f.get("tick"),
                "left_force": f.get("leftForce"),
                "right_force": f.get("rightForce"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        # Batch insert (append-only, no upsert needed)
        if records:
            cols = list(records[0].keys())
            col_str = ", ".join(cols)
            placeholders = ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO bronze.vald_nordbord_force_traces ({col_str}) VALUES ({placeholders})"
            for rec in records:
                self.db.execute(sql, tuple(rec[c] for c in cols))

        logger.info(
            "Inserted %d NordBord force trace points for test %s",
            len(records), test_id,
        )
        return len(records)

    # ------------------------------------------------------------------
    # ForceFrame training
    # ------------------------------------------------------------------

    def load_forceframe_training_exercises(
        self, exercises: list[dict], raw_id: int,
    ) -> int:
        """Upsert ForceFrame training exercises into bronze."""
        if not exercises:
            return 0

        records = []
        for e in exercises:
            records.append({
                "exercise_id": e.get("id") or e.get("exerciseId"),
                "session_id": e.get("sessionId"),
                "program_exercise_id": e.get("programExerciseId"),
                "profile_id": (
                    e.get("profileId") or e.get("athleteId")
                    or e.get("hubAthleteId")
                ),
                "tenant_id": e.get("tenantId") or e.get("teamId"),
                "exercise_date_utc": (
                    e.get("exerciseDateUtc") or e.get("exerciseDateUTC")
                ),
                "modified_date_utc": (
                    e.get("modifiedDateUtc") or e.get("modifiedDateUTC")
                    or e.get("modifiedUtc")
                ),
                "time_in_zone_left": e.get("timeInZoneLeft"),
                "time_in_zone_right": e.get("timeInZoneRight"),
                "impulse_left": e.get("impulseLeft"),
                "impulse_right": e.get("impulseRight"),
                "stability_left": e.get("stabilityLeft"),
                "stability_right": e.get("stabilityRight"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table="bronze.vald_forceframe_training_exercises",
            records=records,
            conflict_columns=["exercise_id"],
            update_columns=[
                "session_id", "program_exercise_id", "profile_id",
                "tenant_id", "exercise_date_utc", "modified_date_utc",
                "time_in_zone_left", "time_in_zone_right",
                "impulse_left", "impulse_right",
                "stability_left", "stability_right",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d ForceFrame training exercises", len(records))
        return len(records)

    def load_forceframe_training_repetitions(
        self, reps: list[dict], raw_id: int,
    ) -> int:
        """Upsert ForceFrame training repetitions into bronze."""
        if not reps:
            return 0

        records = []
        for r in reps:
            records.append({
                "repetition_id": r.get("id") or r.get("repetitionId"),
                "profile_id": (
                    r.get("profileId") or r.get("athleteId")
                    or r.get("hubAthleteId")
                ),
                "tenant_id": r.get("tenantId") or r.get("teamId"),
                "session_id": r.get("sessionId"),
                "session_exercise_id": r.get("sessionExerciseId"),
                "program_exercise_id": r.get("programExerciseId"),
                "repetition_number": r.get("repetitionNumber"),
                "repetition_date_utc": (
                    r.get("repetitionDateUtc") or r.get("repetitionDateUTC")
                ),
                "modified_date_utc": (
                    r.get("modifiedDateUtc") or r.get("modifiedDateUTC")
                    or r.get("modifiedUtc")
                ),
                "time_in_zone_left": r.get("timeInZoneLeft"),
                "time_in_zone_right": r.get("timeInZoneRight"),
                "stability_left": r.get("stabilityLeft"),
                "stability_right": r.get("stabilityRight"),
                "impulse_left": r.get("impulseLeft"),
                "impulse_right": r.get("impulseRight"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table="bronze.vald_forceframe_training_repetitions",
            records=records,
            conflict_columns=["repetition_id"],
            update_columns=[
                "profile_id", "tenant_id", "session_id",
                "session_exercise_id", "program_exercise_id",
                "repetition_number", "repetition_date_utc",
                "modified_date_utc", "time_in_zone_left", "time_in_zone_right",
                "stability_left", "stability_right",
                "impulse_left", "impulse_right",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info("Upserted %d ForceFrame training repetitions", len(records))
        return len(records)

    # ------------------------------------------------------------------
    # ForceFrame force traces
    # ------------------------------------------------------------------

    def load_forceframe_force_traces(
        self, test_id: str, trace_data: dict[str, Any] | list[dict[str, Any]], raw_id: int,
    ) -> int:
        """Insert ForceFrame force trace data points into bronze.

        The API usually returns a dict with a ``forces`` list of objects,
        but some responses arrive as a bare list. Missing ``tick`` values
        are normalised to a deterministic sequence so a sparse payload does
        not fail the whole trace load.
        """
        if isinstance(trace_data, dict):
            forces = trace_data.get("forces", [])
        else:
            forces = trace_data

        if not forces:
            return 0

        if test_id not in self._profile_id_cache:
            row = self.db.fetch_one(
                f"SELECT profile_id FROM {self._table('bronze.vald_forceframe_tests')} WHERE test_id = %s",
                (test_id,),
            )
            if row is None or row[0] is None:
                msg = f"ForceFrame trace load missing profile_id for test {test_id}"
                raise ValueError(msg)
            self._profile_id_cache[test_id] = str(row[0])

        profile_id = self._profile_id_cache[test_id]

        records = []
        fallback_count = 0
        previous_tick: int | None = None
        for index, force in enumerate(forces):
            fallback_tick = previous_tick + 1 if previous_tick is not None else index
            tick, used_fallback = _normalise_forceframe_tick(force, fallback_tick)
            if used_fallback:
                fallback_count += 1

            records.append({
                "test_id": test_id,
                "profile_id": profile_id,
                "tick": tick,
                "inner_left_force": force.get("innerLeftForce"),
                "inner_right_force": force.get("innerRightForce"),
                "outer_left_force": force.get("outerLeftForce"),
                "outer_right_force": force.get("outerRightForce"),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })
            previous_tick = tick

        if records:
            cols = list(records[0].keys())
            col_str = ", ".join(cols)
            row_values = [tuple(rec[c] for c in cols) for rec in records]
            force_trace_table = self._table("bronze.vald_forceframe_force_traces")

            # Phase 8.7.B (2026-05-09): single-statement UPSERT replaces the
            # previous DELETE-then-INSERT pattern. The natural-key UNIQUE index
            # uq_vald_forceframe_force_traces_nk on (test_id, tick) was added
            # in Phase 8.7.B.1. Each tick is unique per test, and re-loading
            # the same test produces a deterministic set of ticks (the API
            # response is stable per test_id), so UPSERT is correct.
            update_columns = [c for c in cols if c not in ("test_id", "tick")]
            update_clause = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in update_columns
            )
            sql = (
                f"INSERT INTO {force_trace_table} ({col_str}) VALUES %s "
                f"ON CONFLICT (test_id, tick) DO UPDATE SET {update_clause}"
            )

            with self.db.connection() as conn:
                with conn.cursor() as cur:
                    # page_size=2000 reduces round-trips for large traces (default is 100)
                    extras.execute_values(cur, sql, row_values, page_size=2000)

        if fallback_count:
            logger.warning(
                "ForceFrame: normalised %d trace ticks for test %s",
                fallback_count,
                test_id,
            )

        logger.debug(
            "Inserted %d ForceFrame force trace points for test %s",
            len(records), test_id,
        )
        return len(records)

    # ------------------------------------------------------------------
    # DynaMo repetitions & traces
    # ------------------------------------------------------------------

    def load_dynamo_repetitions(
        self, test_id: str, reps: list[dict], raw_id: int,
    ) -> int:
        """Upsert DynaMo repetitions into bronze."""
        if not reps:
            return 0

        records = []
        for i, r in enumerate(reps):
            records.append({
                "test_id": test_id,
                "repetition_number": r.get("repetitionNumber", i + 1),
                "side": r.get("laterality") or r.get("side"),
                "impulse_ns": (
                    r.get("impulseNewtonSeconds")
                    or r.get("impulseNs")
                    or r.get("impulse")
                ),
                "rfd_nps": (
                    r.get("rateOfForceDevelopmentNewtonsPerSecond")
                    or r.get("rfdNps")
                    or r.get("rfd")
                ),
                "time_to_peak_s": (
                    r.get("timeToPeakForceSeconds")
                    or r.get("timeToPeakS")
                    or r.get("timeToPeak")
                ),
                "rom_degrees": (
                    r.get("rangeOfMotionDegrees")
                    or r.get("romDegrees")
                    or r.get("rom")
                ),
                "rep_payload": _safe_json(r),
                "raw_id": raw_id,
                "batch_id": self.batch_id,
            })

        self.db.upsert_batch_bronze(
            table=self._table("bronze.vald_dynamo_repetitions"),
            records=records,
            conflict_columns=["test_id", "repetition_number", "side"],
            update_columns=[
                "impulse_ns", "rfd_nps",
                "time_to_peak_s", "rom_degrees", "rep_payload",
                "raw_id", "batch_id", "updated_at",
            ],
        )
        logger.info(
            "Upserted %d DynaMo repetitions for test %s", len(records), test_id
        )
        return len(records)

    def load_dynamo_traces(
        self, test_id: str, tenant_id: str,
        trace_data: dict, raw_id: int,
    ) -> int:
        """Insert DynaMo trace data into bronze.

        The API returns a dict with ``forceTrace`` (list) and optionally
        ``imuTrace`` (list).  We store the entire arrays as JSONB in a
        single row per test.
        """
        if not trace_data:
            return 0

        if test_id in self._dynamo_test_context_cache:
            profile_id, start_time_utc = self._dynamo_test_context_cache[test_id]
        else:
            row = self.db.fetch_one(
                f"SELECT profile_id, start_time_utc FROM {self._table('bronze.vald_dynamo_tests')} WHERE test_id = %s",
                (test_id,),
            )
            profile_id = str(row[0]) if row and row[0] is not None else None
            start_time_utc = row[1] if row else None
            self._dynamo_test_context_cache[test_id] = (profile_id, start_time_utc)

        record = {
            "test_id": test_id,
            "profile_id": profile_id,
            "tenant_id": tenant_id,
            "start_time_utc": start_time_utc,
            "trace_type": "force",
            "force_trace": _safe_json(trace_data.get("forceTrace")),
            "imu_trace": _safe_json(trace_data.get("imuTrace")),
            "raw_id": raw_id,
            "batch_id": self.batch_id,
        }

        cols = list(record.keys())
        col_str = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        trace_table = self._table("bronze.vald_dynamo_traces")

        # Phase 8.7.B (2026-05-09): UPSERT replaces DELETE-then-INSERT. The
        # natural-key UNIQUE index uq_vald_dynamo_traces_nk on (test_id) was
        # added in Phase 8.7.B.1. The DynaMo loader writes one row per test
        # (force_trace + imu_trace as JSONB blobs) so test_id alone is unique.
        update_columns = [c for c in cols if c != "test_id"]
        update_clause = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in update_columns
        )
        sql = (
            f"INSERT INTO {trace_table} ({col_str}) VALUES ({placeholders}) "
            f"ON CONFLICT (test_id) DO UPDATE SET {update_clause}"
        )
        with self.db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(record[c] for c in cols))

        logger.info("Inserted DynaMo trace for test %s", test_id)
        return 1
