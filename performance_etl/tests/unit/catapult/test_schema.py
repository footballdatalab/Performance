from __future__ import annotations

from pathlib import Path

from ingestion.catapult.catalog import BRONZE_TABLES, RAW_TABLES

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_RAW_DDL = (_PROJECT_ROOT / "sql" / "ddl" / "raw" / "15_raw_catapult_tables.sql").read_text(encoding="utf-8")
_RAW_REPLAY_INDEX_DDL = (
    _PROJECT_ROOT / "sql" / "ddl" / "raw" / "16_raw_catapult_replay_indexes.sql"
).read_text(encoding="utf-8")
_BRONZE_DDL = (_PROJECT_ROOT / "sql" / "ddl" / "bronze" / "38_bronze_catapult_tables.sql").read_text(encoding="utf-8")
_BRONZE_REPLAY_INDEX_DDL = (
    _PROJECT_ROOT / "sql" / "ddl" / "bronze" / "46_bronze_catapult_replay_indexes.sql"
).read_text(encoding="utf-8")
_ATHLETES_DDL = _BRONZE_DDL.split(
    "CREATE TABLE IF NOT EXISTS bronze.catapult_athletes (", 1
)[1].split("CREATE INDEX IF NOT EXISTS ix_catapult_athletes_name", 1)[0]


def test_raw_ddl_declares_all_required_catapult_tables() -> None:
    for table_name in RAW_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in _RAW_DDL


def test_raw_replay_indexes_cover_catapult_source_account_keysets() -> None:
    for table_name in RAW_TABLES:
        assert f"ON {table_name} (source_account, raw_id)" in _RAW_REPLAY_INDEX_DDL


def test_raw_replay_indexes_cover_catapult_batch_scoped_keysets() -> None:
    for table_name in RAW_TABLES:
        assert f"ON {table_name} (batch_id, source_account, raw_id)" in _RAW_REPLAY_INDEX_DDL


def test_bronze_ddl_declares_all_required_catapult_tables() -> None:
    for table_name in BRONZE_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in _BRONZE_DDL


def test_bronze_replay_indexes_cover_catapult_source_account_watermarks() -> None:
    for table_name in BRONZE_TABLES:
        assert f"ON {table_name} (source_account, raw_id DESC)" in _BRONZE_REPLAY_INDEX_DDL


def test_bronze_ddl_uses_source_account_scoped_keys() -> None:
    assert "PRIMARY KEY (source_account, team_id)" in _BRONZE_DDL
    assert "PRIMARY KEY (source_account, athlete_id)" in _BRONZE_DDL
    assert "PRIMARY KEY (source_account, activity_id)" in _BRONZE_DDL
    assert "ON bronze.catapult_stats (" in _BRONZE_DDL
    assert "source_account," in _BRONZE_DDL


def test_bronze_ddl_enforces_annotation_and_entity_tag_constraints() -> None:
    assert "CONSTRAINT chk_catapult_annotations_scope" in _BRONZE_DDL
    assert "CONSTRAINT chk_catapult_annotations_target" in _BRONZE_DDL
    assert "CONSTRAINT fk_catapult_entity_tags_tag" in _BRONZE_DDL


def test_bronze_ddl_trims_provider_empty_catapult_columns() -> None:
    assert "athlete_status" not in _BRONZE_DDL
    assert "period_order" not in _BRONZE_DDL
    assert "fk_catapult_activities_team" not in _BRONZE_DDL
    assert "fk_catapult_activities_venue" not in _BRONZE_DDL
    assert "uq_catapult_efforts_row" not in _BRONZE_DDL
    assert "fk_catapult_efforts_period" not in _BRONZE_DDL
    assert "fk_catapult_events_period" not in _BRONZE_DDL
    assert "fk_catapult_sensor_data_period" not in _BRONZE_DDL


def test_bronze_athletes_ddl_keeps_profile_fields_but_not_lookup_duplicates() -> None:
    assert "current_team_id" in _ATHLETES_DDL
    assert "gender" in _ATHLETES_DDL
    assert "nickname" in _ATHLETES_DDL
    assert "height" in _ATHLETES_DDL
    assert "weight" in _ATHLETES_DDL
    assert "velocity_max" in _ATHLETES_DDL
    assert "provider_created_at" in _ATHLETES_DDL
    assert "provider_modified_at" in _ATHLETES_DDL
    assert "fk_catapult_athletes_current_team" in _ATHLETES_DDL
    assert "position_name" not in _ATHLETES_DDL
    assert "tag_list" not in _ATHLETES_DDL
    assert "tags" not in _ATHLETES_DDL
