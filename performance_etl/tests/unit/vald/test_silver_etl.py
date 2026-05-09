from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from datetime import datetime, timezone

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

from ingestion.vald import silver_etl


class _FakeDatabase:
    """Phase 8.7.A: extended with `.connection()` so atomic_publish_table works.

    The mock cursor returns ``(0,)`` for the SELECT COUNT(*) query inside the
    swap helper. All SQL (including from inside the cursor) is captured into
    ``execute_calls`` so tests can still assert against the recorded statements.
    """

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.execute_calls.append((sql, params))

    @contextmanager
    def connection(self):
        yield _FakeDatabaseConn(self)


class _FakeDatabaseConn:
    def __init__(self, owner: "_FakeDatabase") -> None:
        self._owner = owner

    @contextmanager
    def cursor(self):
        yield _FakeDatabaseCursor(self._owner)

    def commit(self) -> None:
        return None


class _FakeDatabaseCursor:
    def __init__(self, owner: "_FakeDatabase") -> None:
        self._owner = owner
        self._scripted_count_rows: list[tuple[int]] = []
        # Phase 8.7.C site #13: _deactivate_excluded_profiles inspects
        # ``cur.rowcount`` after the UPDATE to log how many profiles were
        # soft-deleted. Default to 0 (mock test fixtures don't run real SQL).
        self.rowcount = 0

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self._owner.execute_calls.append((sql, params))
        # Scripted return for atomic_publish_table's SELECT COUNT(*).
        if sql.strip().upper().startswith("SELECT COUNT(*)"):
            self._scripted_count_rows.append((0,))

    def fetchone(self):
        return self._scripted_count_rows.pop(0) if self._scripted_count_rows else None


class _ForceDecksInsertCursor:
    def __init__(self, owner: "_ForceDecksInsertDb") -> None:
        self.owner = owner
        self.rowcount = 7

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.owner.executed_sql = sql
        self.owner.executed_params = params

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class _ForceDecksInsertConnection:
    def __init__(self, owner: "_ForceDecksInsertDb") -> None:
        self.owner = owner

    def cursor(self) -> _ForceDecksInsertCursor:
        return _ForceDecksInsertCursor(self.owner)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class _ForceDecksInsertDb:
    def __init__(self) -> None:
        self.executed_sql: str | None = None
        self.executed_params: tuple[object, ...] | None = None

    @contextmanager
    def connection(self):
        yield _ForceDecksInsertConnection(self)


class _FakeMembershipDatabase:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def fetch_all_dict(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> list[dict[str, object]]:
        return self.rows


class _SequentialRowsDatabase:
    def __init__(self, *responses: list[dict[str, object]]) -> None:
        self.responses = list(responses)
        self.queries: list[str] = []

    def fetch_all_dict(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> list[dict[str, object]]:
        self.queries.append(sql)
        return self.responses.pop(0)


def _patch_non_assessment_dependencies(
    monkeypatch,
    captured: dict[str, object],
) -> None:
    monkeypatch.setattr(
        silver_etl,
        "_load_target_groups",
        lambda: [{"group_id": "g1", "group_name": "Equipa A Active", "category_id": "c1"}],
    )
    monkeypatch.setattr(
        silver_etl,
        "_backfill_forcedecks_trial_results",
        lambda db: {"backfill_performed": False},
    )
    monkeypatch.setattr(
        silver_etl,
        "_build_target_group_membership",
        lambda db, target_groups: ([], {"membership_rows": 0}),
    )
    monkeypatch.setattr(silver_etl, "_insert_rows", lambda *args, **kwargs: 0)
    monkeypatch.setattr(silver_etl, "_sync_overlap_quality_flags", lambda db, rows: {})
    monkeypatch.setattr(
        silver_etl,
        "_build_scoped_profile_rows",
        lambda db, membership_table=silver_etl.SILVER_TABLES["membership"]: [],
    )
    monkeypatch.setattr(
        silver_etl,
        "_upsert_scoped_profiles",
        lambda db, rows, profile_table=silver_etl.SILVER_TABLES["profile"]: 0,
    )
    # Phase 8.7.C site #13: the function was renamed from
    # _delete_excluded_profiles → _deactivate_excluded_profiles (UPDATE not
    # DELETE). The old name remains as a backwards-compat alias but the
    # call site in run_silver_etl now uses the new name, so we patch the
    # new name here.
    monkeypatch.setattr(
        silver_etl,
        "_deactivate_excluded_profiles",
        lambda db,
        profile_table=silver_etl.SILVER_TABLES["profile"],
        membership_table=silver_etl.SILVER_TABLES["membership"]: 0,
    )
    monkeypatch.setattr(silver_etl, "_build_profile_lookup", lambda rows: {})

    def fake_load_assessment_metrics(
        db,
        profile_lookup,
        profile_table=silver_etl.SILVER_TABLES["profile"],
        assessment_table=silver_etl.SILVER_TABLES["assessment"],
        day_start_utc=None,
        day_end_utc=None,
        scoped_test_ids_by_family=None,
    ):
        captured["profile_table"] = profile_table
        captured["assessment_table"] = assessment_table
        captured["day_start_utc"] = day_start_utc
        captured["day_end_utc"] = day_end_utc
        captured["scoped_test_ids_by_family"] = scoped_test_ids_by_family
        return {"total_inserted": 2, "by_family": {"forcedecks": 2}}

    monkeypatch.setattr(silver_etl, "_load_assessment_metrics", fake_load_assessment_metrics)


def test_run_silver_etl_intraday_rebuilds_only_window(monkeypatch) -> None:
    db = _FakeDatabase()
    captured: dict[str, object] = {}
    _patch_non_assessment_dependencies(monkeypatch, captured)

    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    summary = silver_etl.run_silver_etl(
        db,
        day_start_utc=day_start,
        day_end_utc=day_end,
    )

    assert summary["assessment_scope"] == "day_window"
    assert captured == {
        "profile_table": "silver.vald_athlete_profile",
        "assessment_table": "silver.vald_assessment_metric",
        "day_start_utc": day_start,
        "day_end_utc": day_end,
        "scoped_test_ids_by_family": None,
    }
    # Phase 8.7.B (2026-05-09): the previous test asserted a DELETE FROM
    # silver.vald_assessment_metric; that's gone — _load_assessment_metrics
    # now uses ON CONFLICT (metric_row_key) DO UPDATE so scoped rebuilds no
    # longer delete the live rows. Locked decision #7 invariant: no DELETE
    # against the live silver assessment metric table from the scoped path.
    assert not any(
        "DELETE FROM silver.vald_assessment_metric" in sql
        for sql, _ in db.execute_calls
    ), "Phase 8.7.B: scoped silver rebuild must not DELETE from live"
    assert not any(
        "TRUNCATE TABLE silver.vald_assessment_metric RESTART IDENTITY" in sql
        for sql, _ in db.execute_calls
    )


def test_run_silver_etl_full_rebuild_atomic_publishes_assessment_table(monkeypatch) -> None:
    """Phase 8.7.A: full rebuild must use atomic stage→live swap, not TRUNCATE.

    The function used to ``TRUNCATE TABLE silver.vald_assessment_metric`` then
    INSERT in two separate statements. Phase 8.7.A replaces this with a stage
    table that's atomically swapped onto the live name — the live table is
    never empty during the rebuild. Locked decision #7 invariant.
    """
    db = _FakeDatabase()
    captured: dict[str, object] = {}
    _patch_non_assessment_dependencies(monkeypatch, captured)

    summary = silver_etl.run_silver_etl(db)

    assert summary["assessment_scope"] == "full"
    # The load now runs against a stage table, not the live name.
    captured_assessment = captured["assessment_table"]
    assert isinstance(captured_assessment, str)
    assert captured_assessment.startswith("etl_staging.vald_assessment_metric_stage_"), (
        "Phase 8.7.A: full rebuild must point _load_assessment_metrics at a stage table"
    )
    assert captured["day_start_utc"] is None
    assert captured["day_end_utc"] is None
    assert captured["scoped_test_ids_by_family"] is None
    assert captured["profile_table"] == "silver.vald_athlete_profile"

    # Locked decision #7 invariant: NO TRUNCATE against the live assessment table.
    executed_sqls = [sql for sql, _ in db.execute_calls]
    assert not any(
        "TRUNCATE" in sql and "silver.vald_assessment_metric" in sql and "_stage_" not in sql and "_old_" not in sql
        for sql in executed_sqls
    ), "Phase 8.7.A: must not TRUNCATE the live silver.vald_assessment_metric"

    # Atomic-swap fingerprint: stage created, live renamed, schema swapped, archived dropped.
    assert any("CREATE TABLE etl_staging.vald_assessment_metric_stage_" in sql for sql in executed_sqls)
    assert any("ALTER TABLE silver.vald_assessment_metric RENAME TO" in sql and "_old_" in sql for sql in executed_sqls)
    assert any("SET SCHEMA silver" in sql for sql in executed_sqls)
    assert any("DROP TABLE IF EXISTS silver.vald_assessment_metric_old_" in sql and "CASCADE" in sql for sql in executed_sqls)


def test_insert_forcedecks_family_derives_rep_number_from_trial_sequence() -> None:
    db = _ForceDecksInsertDb()

    inserted = silver_etl._insert_forcedecks_family(
        db,
        family="forcedecks",
    )

    assert inserted == 7
    assert db.executed_sql is not None
    assert "source_rows AS (" in db.executed_sql
    assert "raw_repeat_number" in db.executed_sql
    assert "DENSE_RANK() OVER" in db.executed_sql
    assert "PARTITION BY test_id, COALESCE(side, '__unsided__')" in db.executed_sql
    assert "ORDER BY test_date ASC, trial_id ASC" in db.executed_sql
    assert "deduplicated_prepared AS (" in db.executed_sql
    assert "ROW_NUMBER() OVER (" in db.executed_sql
    assert "normalized_metric_value" in db.executed_sql
    assert "WHERE duplicate_rank = 1" in db.executed_sql


def test_run_silver_etl_can_target_stage_tables_and_defer_quality_flags(monkeypatch) -> None:
    db = _FakeDatabase()
    captured: dict[str, object] = {}
    _patch_non_assessment_dependencies(monkeypatch, captured)

    monkeypatch.setattr(
        silver_etl,
        "_build_target_group_membership",
        lambda db, target_groups: ([], {"membership_rows": 0, "ambiguous_profiles": 3}),
    )
    sync_calls: list[object] = []
    monkeypatch.setattr(
        silver_etl,
        "_sync_overlap_quality_flags",
        lambda db, rows: sync_calls.append(rows) or {},
    )

    summary = silver_etl.run_silver_etl(
        db,
        table_overrides={
            "membership": "etl_staging.silver_vald_target_group_membership",
            "profile": "etl_staging.silver_vald_athlete_profile",
            "assessment": "etl_staging.silver_vald_assessment_metric",
        },
        sync_quality_flags=False,
    )

    assert summary["quality"] == {
        "deferred": True,
        "open_flags_deleted": 0,
        "flags_written": 0,
        "ambiguous_profiles": 3,
    }
    assert summary["tables"] == {
        "membership": "etl_staging.silver_vald_target_group_membership",
        "profile": "etl_staging.silver_vald_athlete_profile",
        "assessment": "etl_staging.silver_vald_assessment_metric",
    }
    assert sync_calls == []


def test_build_target_group_membership_keeps_one_gold_membership_for_ambiguous_profiles() -> None:
    db = _FakeMembershipDatabase(
        [
            {
                "provider_profile_id": "profile-1",
                "tenant_id": "tenant-1",
                "category_id": "cat-b",
                "category_name": "Equipa B",
                "group_id": "group-b",
                "group_name": "Equipa B Active",
                "raw_id": 10,
            },
            {
                "provider_profile_id": "profile-1",
                "tenant_id": "tenant-1",
                "category_id": "cat-u19",
                "category_name": "Equipa U19",
                "group_id": "group-u19",
                "group_name": "Equipa U19 Active",
                "raw_id": 11,
            },
        ]
    )

    membership_rows, summary = silver_etl._build_target_group_membership(
        db,
        [
            {"group_id": "group-b", "group_name": "Equipa B Active", "category_id": "cat-b"},
            {"group_id": "group-u19", "group_name": "Equipa U19 Active", "category_id": "cat-u19"},
        ],
    )

    assert summary["distinct_target_profiles"] == 1
    assert summary["included_profiles"] == 1
    assert summary["ambiguous_profiles"] == 1
    included_rows = [row for row in membership_rows if row["include_in_gold"]]
    excluded_rows = [row for row in membership_rows if not row["include_in_gold"]]
    assert len(included_rows) == 1
    assert len(excluded_rows) == 1
    assert included_rows[0]["target_group_id"] == "group-b"
    assert included_rows[0]["is_ambiguous"] is True


def test_build_scoped_profile_rows_prunes_empty_provider_fields() -> None:
    db = _FakeMembershipDatabase(
        [
            {
                "provider_profile_id": "profile-1",
                "tenant_id": "tenant-1",
                "given_name": "Martim",
                "family_name": "Fernandes",
                "date_of_birth": "2000-01-01",
                "sex": "M",
                "email": "m@example.com",
                "external_id": "ext-1",
                "sync_id": "sync-1",
                "source_created_at": datetime(2026, 3, 1, tzinfo=timezone.utc),
                "source_updated_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
                "target_group_id": "group-1",
                "target_group_name": "Equipa A Active",
                "target_category_id": "cat-1",
                "target_category_name": "Equipa A",
            }
        ]
    )

    profile_rows = silver_etl._build_scoped_profile_rows(db)

    assert silver_etl.PROFILE_COLUMNS == [
        "provider_profile_id",
        "tenant_id",
        "provider_full_name",
        "provider_given_name",
        "provider_family_name",
        "provider_status",
        "first_seen_at",
        "last_seen_at",
        "target_group_id",
        "target_group_name",
        "target_category_id",
        "target_category_name",
    ]
    assert profile_rows == [
        {
            "provider_profile_id": "profile-1",
            "tenant_id": "tenant-1",
            "provider_full_name": "Martim Fernandes",
            "provider_given_name": "Martim",
            "provider_family_name": "Fernandes",
            "provider_status": "active",
            "first_seen_at": datetime(2026, 3, 1, tzinfo=timezone.utc),
            "last_seen_at": datetime(2026, 3, 29, tzinfo=timezone.utc),
            "target_group_id": "group-1",
            "target_group_name": "Equipa A Active",
            "target_category_id": "cat-1",
            "target_category_name": "Equipa A",
        }
    ]


def test_load_smartspeed_metrics_ignores_removed_summary_columns(monkeypatch) -> None:
    db = _SequentialRowsDatabase(
        [
            {
                "test_id": "test-1",
                "profile_id": "profile-1",
                "test_name": "Sprint",
                "test_type_name": "Sprint",
                "summary_test_date_utc": datetime(2026, 3, 29, 10, 0, tzinfo=timezone.utc),
                "detail_test_date_utc": None,
                "additional_test_result": {"totalTime": 1.82},
                "rep_results": [
                    {
                        "repNumber": 1,
                        "reactionTime": 0.21,
                        "splitResults": [{"splitIndex": 1, "splitTime": 1.82}],
                    }
                ],
            }
        ],
        [],
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        silver_etl,
        "_insert_rows",
        lambda _db, _table, rows, columns: captured.update({"rows": rows, "columns": columns}) or len(rows),
    )

    counts = silver_etl._load_smartspeed_metrics(
        db,
        {
            "profile-1": {
                "provider_profile_id": "profile-1",
                "athlete_name": "Martim Fernandes",
                "team_name": "Equipa A",
                "team_group_name": "Equipa A Active",
                "team_group_id": "group-1",
                "category_id": "cat-1",
            }
        },
    )

    assert "running_summary" not in db.queries[0]
    assert "jumping_summary" not in db.queries[0]
    metric_names = {row["metric_name"] for row in captured["rows"]}
    assert {"total_time", "reaction_time", "split_1_split_time"} <= metric_names
    assert counts == {"speed": len(captured["rows"])}


def test_load_dynamo_metrics_ignores_pruned_force_and_modified_date_columns(monkeypatch) -> None:
    db = _SequentialRowsDatabase(
        [
            {
                "test_id": "test-1",
                "profile_id": "profile-1",
                "test_category": "Strength",
                "body_region": "Knee",
                "movement": "Extension",
                "position": "Seated",
                "start_time_utc": datetime(2026, 3, 29, 10, 0, tzinfo=timezone.utc),
                "analysed_date_utc": None,
                "movement_type": "Concentric",
                "side": "left",
                "max_force_newtons": 120.0,
                "avg_force_newtons": 100.0,
                "max_impulse_ns": None,
                "avg_impulse_ns": None,
                "max_rfd_nps": None,
                "avg_rfd_nps": None,
                "avg_time_to_peak_s": None,
                "min_time_to_peak_s": None,
                "max_rom_degrees": None,
                "avg_rom_degrees": None,
                "summary_payload": {},
            }
        ],
        [
            {
                "test_id": "test-1",
                "profile_id": "profile-1",
                "test_category": "Strength",
                "body_region": "Knee",
                "movement": "Extension",
                "position": "Seated",
                "start_time_utc": datetime(2026, 3, 29, 10, 0, tzinfo=timezone.utc),
                "analysed_date_utc": None,
                "repetition_number": 1,
                "side": "left",
                "impulse_ns": 5.0,
                "rfd_nps": 7.0,
                "time_to_peak_s": 0.4,
                "rom_degrees": 35.0,
                "rep_payload": {"forceNewtons": 999.0, "impulseNs": 5.0},
            }
        ],
        [],
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        silver_etl,
        "_insert_rows",
        lambda _db, _table, rows, columns: captured.update({"rows": rows, "columns": columns}) or len(rows),
    )

    inserted = silver_etl._load_dynamo_metrics(
        db,
        {
            "profile-1": {
                "provider_profile_id": "profile-1",
                "athlete_name": "Martim Fernandes",
                "team_name": "Equipa A",
                "team_group_name": "Equipa A Active",
                "team_group_id": "group-1",
                "category_id": "cat-1",
            }
        },
    )

    assert all("modified_date_utc" not in query for query in db.queries)
    assert all("r.force_newtons" not in query for query in db.queries)
    metric_names = {row["metric_name"] for row in captured["rows"]}
    assert "force_newtons" not in metric_names
    assert "impulse_ns" in metric_names
    assert inserted == len(captured["rows"])
