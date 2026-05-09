from __future__ import annotations

from pathlib import Path
import sys
import types

try:
    import psycopg2.extras  # noqa: F401
except ModuleNotFoundError:
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.extras = types.ModuleType("psycopg2.extras")
    psycopg2.extras.execute_values = lambda *args, **kwargs: None
    psycopg2.extras.RealDictCursor = object
    psycopg2.pool = types.ModuleType("psycopg2.pool")
    psycopg2.pool.ThreadedConnectionPool = object
    psycopg2.extensions = types.SimpleNamespace(connection=object, cursor=object)
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extras"] = psycopg2.extras
    sys.modules["psycopg2.pool"] = psycopg2.pool

from script.clean_vald_tables import (
    BRONZE_TABLES,
    GOLD_TABLES,
    OBSOLETE_GOLD_TABLES,
    OBSOLETE_BRONZE_TABLES,
    OBSOLETE_RAW_TABLES,
    OBSOLETE_SILVER_TABLES,
    RAW_TABLES,
    REPLAY_CURSOR_TABLE,
    SILVER_TABLES,
    clean_tables,
)


class _FakeDatabase:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def fetch_one(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> tuple[int] | None:
        if "information_schema.tables" in sql:
            assert params is not None
            full_name = f"{params[0]}.{params[1]}"
            return (1,) if full_name in self.counts else None

        if sql.startswith("SELECT COUNT(*) FROM "):
            table = sql.split("FROM ", 1)[1]
            if " WHERE " in table:
                table = table.split(" WHERE ", 1)[0]
            return (self.counts[table],)

        raise AssertionError(f"Unexpected SQL: {sql}")

    def execute(
        self,
        sql: str,
        params: tuple[object, ...] | None = None,
    ) -> None:
        self.executed.append((sql, params))


def test_active_table_lists_exclude_retired_vald_tables() -> None:
    assert not (set(RAW_TABLES) & set(OBSOLETE_RAW_TABLES))
    assert not (set(BRONZE_TABLES) & set(OBSOLETE_BRONZE_TABLES))
    assert not (set(SILVER_TABLES) & set(OBSOLETE_SILVER_TABLES))
    assert not (set(GOLD_TABLES) & set(OBSOLETE_GOLD_TABLES))
    assert "raw.pipeline_stage_cursor" not in RAW_TABLES
    assert "raw.vald_tenants" not in RAW_TABLES
    assert "raw.vald_categories" not in RAW_TABLES
    assert "raw.vald_groups" not in RAW_TABLES
    assert "bronze.vald_tenants" not in BRONZE_TABLES
    assert "bronze.vald_categories" not in BRONZE_TABLES
    assert "bronze.vald_groups" not in BRONZE_TABLES
    assert "silver.master_athlete" not in SILVER_TABLES
    assert "silver.data_quality_baseline" not in SILVER_TABLES
    assert "gold.daily_monitoring" not in GOLD_TABLES
    assert "gold.athlete_profile" not in GOLD_TABLES
    assert "gold.vald_jumps" not in GOLD_TABLES
    assert "gold.vald_forcedecks_other" not in GOLD_TABLES


def test_baseline_vald_ddl_excludes_retired_tables() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    creation_ddl_text = "\n".join(
        [
            (repo_root / "sql" / "ddl" / "raw" / "11_raw_vald_tables.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "30_bronze_vald_reference.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "31_bronze_vald_forcedecks.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "32_bronze_vald_forceframe.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "33_bronze_vald_nordbord.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "34_bronze_vald_humantrak.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "35_bronze_vald_smartspeed.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "bronze" / "36_bronze_vald_dynamo.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "silver" / "42_silver_vald_entities.sql").read_text(encoding="utf-8"),
            (repo_root / "sql" / "ddl" / "gold" / "55_gold_vald_assessment_marts.sql").read_text(encoding="utf-8"),
        ]
    )

    for table in (
        "raw.pipeline_stage_cursor",
        "raw.vald_tenants",
        "raw.vald_categories",
        "raw.vald_groups",
        "bronze.vald_tenants",
        "bronze.vald_categories",
        "bronze.vald_groups",
        "gold.vald_jumps",
        "gold.vald_forcedecks_other",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" not in creation_ddl_text


def test_baseline_vald_ddl_prunes_zero_value_columns_but_keeps_sparse_fields() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    raw_ddl = (repo_root / "sql" / "ddl" / "raw" / "11_raw_vald_tables.sql").read_text(encoding="utf-8")
    bronze_reference_ddl = (repo_root / "sql" / "ddl" / "bronze" / "30_bronze_vald_reference.sql").read_text(encoding="utf-8")
    bronze_forcedecks_ddl = (repo_root / "sql" / "ddl" / "bronze" / "31_bronze_vald_forcedecks.sql").read_text(encoding="utf-8")
    bronze_smartspeed_ddl = (repo_root / "sql" / "ddl" / "bronze" / "35_bronze_vald_smartspeed.sql").read_text(encoding="utf-8")
    bronze_dynamo_ddl = (repo_root / "sql" / "ddl" / "bronze" / "36_bronze_vald_dynamo.sql").read_text(encoding="utf-8")
    silver_ddl = (repo_root / "sql" / "ddl" / "silver" / "42_silver_vald_entities.sql").read_text(encoding="utf-8")
    gold_ddl = (repo_root / "sql" / "ddl" / "gold" / "55_gold_vald_assessment_marts.sql").read_text(encoding="utf-8")

    assert raw_ddl.count("page_number         INTEGER") == 2
    assert "external_id         VARCHAR(255)" in bronze_reference_ddl
    assert "date_of_birth       DATE" not in bronze_reference_ddl
    assert "sex                 VARCHAR(20)" not in bronze_reference_ddl
    assert "email               VARCHAR(255)" not in bronze_reference_ddl
    assert "sync_id             VARCHAR(255)" not in bronze_reference_ddl
    assert "being_merged_with   UUID" not in bronze_reference_ddl
    assert "merge_expiry        TIMESTAMPTZ" not in bronze_reference_ddl
    assert "notes" in bronze_forcedecks_ddl
    assert "parameter" in bronze_forcedecks_ddl
    assert "additional_options" not in bronze_smartspeed_ddl
    assert "running_summary" not in bronze_smartspeed_ddl
    assert "jumping_summary" not in bronze_smartspeed_ddl
    assert "modified_date_utc" not in bronze_dynamo_ddl
    assert "force_newtons           NUMERIC" not in bronze_dynamo_ddl
    assert "provider_birth_date" not in silver_ddl
    assert "provider_email" not in silver_ddl
    assert "provider_external_id" not in silver_ddl
    assert "provider_sex" not in silver_ddl
    assert "provider_sync_id" not in silver_ddl
    assert "raw_payload_hash" not in silver_ddl
    assert gold_ddl.count("rep_number                INTEGER") == 3
    assert gold_ddl.count("side                      VARCHAR(50)") == 4


def test_clean_tables_drops_obsolete_tables_when_requested() -> None:
    db = _FakeDatabase(
        counts={
            "raw.pipeline_stage_cursor": 0,
            "bronze.vald_humantrak_tests": 0,
            REPLAY_CURSOR_TABLE: 0,
        }
    )

    results = clean_tables(
        db=db,
        layers=["raw", "bronze"],
        drop_obsolete=True,
        dry_run=False,
    )

    assert results["raw.pipeline_stage_cursor"] == 0
    assert results["bronze.vald_humantrak_tests"] == 0
    assert ("DROP TABLE IF EXISTS raw.pipeline_stage_cursor CASCADE", None) in db.executed
    assert ("DROP TABLE IF EXISTS bronze.vald_humantrak_tests CASCADE", None) in db.executed
    assert any(sql.startswith(f"DELETE FROM {REPLAY_CURSOR_TABLE}") for sql, _ in db.executed)
