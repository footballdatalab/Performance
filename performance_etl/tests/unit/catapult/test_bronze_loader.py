from __future__ import annotations

import json
from datetime import datetime, timezone

from ingestion.catapult.loaders.bronze_loader import CatapultBronzeLoader


class _FakeDatabase:
    def __init__(self, *, existing_period_ids: set[str] | None = None) -> None:
        self.upserts: list[dict[str, object]] = []
        self.existing_period_ids = existing_period_ids
        self.existing_tag_type_ids: set[str] = set()

    def upsert_batch_bronze(
        self,
        table: str,
        records: list[dict[str, object]],
        conflict_columns: list[str],
        update_columns: list[str],
        conn=None,
    ) -> None:
        self.upserts.append(
            {
                "table": table,
                "records": records,
                "conflict_columns": conflict_columns,
                "update_columns": update_columns,
            }
        )

    def fetch_one_dict(self, sql: str, params: tuple[object, ...]):
        if "FROM bronze.catapult_periods" in sql:
            period_id = str(params[1])
            if self.existing_period_ids is not None and period_id not in self.existing_period_ids:
                return None
            return {"period_id": period_id}
        return {
            "start_time": datetime(2026, 3, 31, 9, 0, tzinfo=timezone.utc),
            "end_time": datetime(2026, 3, 31, 10, 0, tzinfo=timezone.utc),
        }

    def fetch_all_dict(self, sql: str, params: tuple[object, ...]):
        if "FROM bronze.catapult_tag_types" in sql:
            requested_ids = set(params[1])
            return [
                {"tag_type_id": tag_type_id}
                for tag_type_id in sorted(self.existing_tag_type_ids & requested_ids)
            ]
        return []


def test_load_stats_uses_activity_context_and_preserves_extra_parameters() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_stats(
        [
            {
                "activity_id": "activity-100",
                "athlete_id": "athlete-200",
                "period_id": None,
                "team_id": "8",
                "total_distance": "5432.5",
                "player_load": "321.4",
                "max_velocity": "8.7",
                "custom_metric": "12.3",
            }
        ],
        raw_id=99,
    )

    assert loaded == 1
    call = db.upserts[0]
    assert call["table"] == "bronze.catapult_stats"
    record = call["records"][0]
    assert record["period_key"] == ""
    all_parameters = json.loads(record["all_parameters"])
    assert all_parameters["custom_metric"] == "12.3"
    assert all_parameters["team_id"] == "8"


def test_load_tags_seeds_missing_tag_type_placeholders_before_tags() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_tags(
        [
            {
                "id": "tag-1",
                "tag_type_id": "type-1",
                "name": "Training",
            }
        ],
        raw_id=98,
    )

    assert loaded == 1
    assert [call["table"] for call in db.upserts] == [
        "bronze.catapult_tag_types",
        "bronze.catapult_tags",
    ]
    placeholder = db.upserts[0]["records"][0]
    assert placeholder["source_account"] == "CATAPULT_A"
    assert placeholder["tag_type_id"] == "type-1"
    assert placeholder["tag_type_name"] is None
    assert placeholder["raw_id"] is None


def test_load_tags_does_not_overwrite_existing_tag_types_with_placeholders() -> None:
    db = _FakeDatabase()
    db.existing_tag_type_ids = {"type-1"}
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_tags(
        [
            {
                "id": "tag-1",
                "tag_type_id": "type-1",
                "name": "Training",
            }
        ],
        raw_id=98,
    )

    assert loaded == 1
    assert [call["table"] for call in db.upserts] == ["bronze.catapult_tags"]


def test_load_efforts_flattens_velocity_and_acceleration_efforts() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_efforts(
        {
            "velocity": [{"dt": 1711872000000, "et": 1711872002000, "bnum": 6, "mval": "8.1"}],
            "acceleration": [{"dt": 1711872005000, "et": 1711872007000, "bnum": 2, "mval": "3.4"}],
        },
        raw_id=100,
        activity_id=300,
        athlete_id=400,
    )

    assert loaded == 2
    records = db.upserts[0]["records"]
    assert records[0]["effort_type"] == "velocity_band_6"
    assert records[1]["effort_type"] == "acceleration_band_2"
    assert "period_id" not in records[0]


def test_load_efforts_accepts_catapult_api_shape() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_efforts(
        [
            {
                "athlete_id": "athlete-200",
                "data": {
                    "velocity_efforts": [{"start_time": 1774709101.35, "end_time": 1774709102.55, "band": "6"}],
                    "acceleration_efforts": [{"start_time": 1774709103.35, "end_time": 1774709104.55, "band": "-1"}],
                },
            }
        ],
        raw_id=100,
        activity_id="activity-100",
        athlete_id="athlete-200",
    )

    assert loaded == 2
    records = db.upserts[0]["records"]
    assert records[0]["effort_type"] == "velocity_band_6"
    assert records[1]["effort_type"] == "acceleration_band_-1"


def test_load_efforts_deduplicates_duplicate_record_hashes_within_a_single_payload() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_efforts(
        {
            "velocity": [
                {"dt": 1711872000000, "et": 1711872002000, "bnum": 6, "mval": "8.1"},
                {"dt": 1711872000000, "et": 1711872002000, "bnum": 6, "mval": "8.1"},
            ]
        },
        raw_id=100,
        activity_id=300,
        athlete_id=400,
    )

    assert loaded == 1
    assert len(db.upserts[0]["records"]) == 1
    assert loader.last_load_stats is not None
    assert loader.last_load_stats["skipped_rows"] == 1
    assert loader.last_load_stats["skip_reasons"] == {"duplicate_conflict_key": 1}


def test_load_stats_accepts_suffixed_identifier_fields() -> None:
    db = _FakeDatabase(existing_period_ids={"period-300"})
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_stats(
        [
            {
                "activity_id_id": "activity-100",
                "athlete_id_id": "athlete-200",
                "period_id_id": "period-300",
                "total_distance": "5432.5",
                "player_load": "321.4",
                "start_time": 1774708340.55,
            }
        ],
        raw_id=101,
    )

    assert loaded == 1
    record = db.upserts[0]["records"][0]
    assert record["activity_id"] == "activity-100"
    assert record["athlete_id"] == "athlete-200"
    assert record["period_id"] == "period-300"
    assert record["period_key"] == "period-300"


def test_load_stats_preserves_synthetic_period_key_when_period_dimension_is_missing() -> None:
    db = _FakeDatabase(existing_period_ids=set())
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_stats(
        [
            {
                "activity_id_id": "activity-100",
                "athlete_id_id": "athlete-200",
                "period_id_id": "synthetic-period-300",
                "start_time": 1774708340.55,
                "period_name": "AutoCreatedPeriod",
            }
        ],
        raw_id=101,
    )

    assert loaded == 1
    record = db.upserts[0]["records"][0]
    assert record["period_id"] is None
    assert record["period_key"] == "synthetic-period-300"


def test_load_stats_skips_zero_suffixed_identifier_fields() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_stats(
        [
            {
                "activity_id_id": 0,
                "athlete_id_id": "0",
                "period_id_id": "period-300",
                "start_time": 1774708340.55,
            }
        ],
        raw_id=102,
    )

    assert loaded == 0
    assert db.upserts == []
    assert loader.last_load_stats is not None
    assert loader.last_load_stats["skip_reasons"]["missing_activity_id"] == 1


def test_load_events_accepts_catapult_api_shape() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_events(
        [
            {
                "athlete_id": "athlete-200",
                "data": {
                    "ima_acceleration": [{"start_time": 1774708362.15, "intensity": 0.99}],
                    "football_movement_analysis": [{"start_time": 1774708340.75, "movement_type": 1}],
                },
            }
        ],
        raw_id=103,
        activity_id="activity-100",
        athlete_id="athlete-200",
    )

    assert loaded == 2
    records = db.upserts[0]["records"]
    assert {record["event_type"] for record in records} == {"ima_acceleration", "football_movement_analysis"}
    assert "period_id" not in records[0]


def test_load_athletes_enriches_profile_fields_without_lookup_duplicates() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_athletes(
        [
            {
                "id": "04239d1b-70a2-4dcb-9dc1-33eddeadbee7",
                "first_name": "Ivan",
                "last_name": "Marcano",
                "gender": "Unspecified",
                "jersey": "505",
                "nickname": "",
                "height": 190,
                "weight": 80,
                "date_of_birth": 551401200,
                "date_of_birth_date": "1987-06-23",
                "velocity_max": 9.13893,
                "acceleration_max": 0,
                "heart_rate_max": 191,
                "player_load_max": 0,
                "image": "acb19f388cda7e6416dc8f384affef45d151dc3b.png",
                "icon": "circle",
                "stroke_colour": "#FFFFFF",
                "fill_colour": "#0000ff",
                "trail_colour_start": "",
                "trail_colour_end": "",
                "is_synced": 0,
                "is_deleted": 0,
                "created_at": "2016-03-10 16:54:26",
                "modified_at": "2025-07-29 07:00:01",
                "is_demo": False,
                "tag_list": ["13157"],
                "tags": [{"id": "tag-1", "name": "13157"}],
                "current_team_id": "f1c72546-92ec-45cf-a4e6-19e65e502a20",
                "max_player_load_per_minute": 15,
                "position": "DC",
                "position_id": "e68cf550-89ac-440c-bfbf-1b91cd3d806d",
                "position_name": "DC",
            }
        ],
        raw_id=105,
    )

    assert loaded == 1
    record = db.upserts[0]["records"][0]
    assert record["current_team_id"] == "f1c72546-92ec-45cf-a4e6-19e65e502a20"
    assert record["position_id"] == "e68cf550-89ac-440c-bfbf-1b91cd3d806d"
    assert record["gender"] == "Unspecified"
    assert record["nickname"] is None
    assert record["height"] == 190
    assert record["weight"] == 80
    assert str(record["velocity_max"]) == "9.13893"
    assert str(record["heart_rate_max"]) == "191"
    assert record["is_synced"] is False
    assert record["is_deleted"] is False
    assert record["is_demo"] is False
    assert record["provider_created_at"].isoformat() == "2016-03-10T16:54:26+00:00"
    assert record["provider_modified_at"].isoformat() == "2025-07-29T07:00:01+00:00"
    assert "position_name" not in record
    assert "position" not in record
    assert "tag_list" not in record
    assert "tags" not in record


def test_load_sensor_data_accepts_catapult_api_shape() -> None:
    db = _FakeDatabase()
    loader = CatapultBronzeLoader(db=db, batch_id="batch-1", source_account="CATAPULT_A")

    loaded = loader.load_sensor_data(
        [
            {
                "athlete_id": "athlete-200",
                "data": [
                    {"ts": 1774708340, "cs": 5, "lat": 41.1, "long": -8.6, "v": 3.2, "hr": 102},
                    {"ts": 1774708340, "cs": 15, "lat": 41.1, "long": -8.6, "v": 3.4, "hr": 103},
                ],
            }
        ],
        raw_id=104,
        activity_id="activity-100",
        athlete_id="athlete-200",
    )

    assert loaded == 2
    records = db.upserts[0]["records"]
    assert records[0]["latitude"] is not None
    assert records[0]["velocity"] is not None
    assert records[0]["heart_rate"] is not None
    assert "period_id" not in records[0]
