from __future__ import annotations

from ingestion.catapult import review
from ingestion.catapult.client import CatapultAccountConfig
from ingestion.catapult.replay_scope import batch_ids_for_account, build_batch_ids_by_source_table


def test_build_batch_ids_by_source_table_expands_stage_batches() -> None:
    batch_ids = build_batch_ids_by_source_table(
        {
            "accounts": {
                "CATAPULT_U15": {
                    "reference": {
                        "batch_id": "batch-reference",
                        "endpoints": {
                            "teams": {"raw_rows_written": 1},
                            "athletes": {"raw_rows_written": 1},
                            "positions": {"raw_rows_written": 1},
                            "parameters": {"raw_rows_written": 1},
                            "venues": {"raw_rows_written": 1},
                            "tag_types": {"raw_rows_written": 1},
                            "tags": {"raw_rows_written": 1},
                        },
                        "entity_tags": {"raw_rows_written": 1},
                    },
                    "activities": {"batch_id": "batch-activities", "raw_rows_written": 1},
                    "periods": {"batch_id": "batch-periods", "raw_rows_written": 1},
                    "annotations": {"batch_id": "batch-annotations", "raw_rows_written": 1},
                    "stats": {"batch_id": "batch-stats", "raw_rows_written": 1},
                    "efforts": {"batch_id": "batch-efforts", "raw_rows_written": 1},
                    "events": {"batch_id": "batch-events", "raw_rows_written": 1},
                    "sensor_data": {"batch_id": "batch-sensor", "raw_rows_written": 1},
                }
            }
        }
    )

    assert batch_ids["raw.catapult_teams"] == ["batch-reference"]
    assert batch_ids["raw.catapult_athletes"] == ["batch-reference"]
    assert batch_ids["raw.catapult_activities"] == ["batch-activities"]
    assert batch_ids["raw.catapult_sensor_data"] == ["batch-sensor"]


def test_build_batch_ids_by_source_table_skips_reference_tables_with_zero_raw_writes() -> None:
    batch_ids = build_batch_ids_by_source_table(
        {
            "accounts": {
                "CATAPULT_U15": {
                    "reference": {
                        "batch_id": "batch-reference",
                        "endpoints": {
                            "teams": {"raw_rows_written": 1},
                            "athletes": {"raw_rows_written": 0},
                            "positions": {"raw_rows_written": 0},
                            "parameters": {"raw_rows_written": 0},
                            "venues": {"raw_rows_written": 0},
                            "tag_types": {"raw_rows_written": 0},
                            "tags": {"raw_rows_written": 0},
                        },
                        "entity_tags": {"raw_rows_written": 0},
                    }
                }
            }
        }
    )

    assert batch_ids["raw.catapult_teams"] == ["batch-reference"]
    assert "raw.catapult_athletes" not in batch_ids


def test_batch_ids_for_account_skip_reference_tables_with_zero_raw_writes() -> None:
    account_summary = {
        "reference": {
            "batch_id": "batch-reference",
            "endpoints": {
                "teams": {"raw_rows_written": 1},
                "athletes": {"raw_rows_written": 0},
            },
            "entity_tags": {"raw_rows_written": 0},
        }
    }

    assert batch_ids_for_account("raw.catapult_teams", account_summary) == ["batch-reference"]
    assert batch_ids_for_account("raw.catapult_athletes", account_summary) == []


def test_compact_activity_pair_output_replaces_pairs_with_count() -> None:
    raw_summary = {
        "accounts": {
            "CATAPULT_U15": {
                "activity_athlete_enumeration": {
                    "pairs": [("athlete-1", "activity-1"), ("athlete-2", "activity-1")]
                },
                "activity_devices": {
                    "pairs": [("athlete-1", "activity-1")]
                },
            }
        }
    }

    review._compact_activity_pair_output(raw_summary)

    assert raw_summary["accounts"]["CATAPULT_U15"]["activity_athlete_enumeration"] == {"pair_count": 2}
    assert raw_summary["accounts"]["CATAPULT_U15"]["activity_devices"] == {"pair_count": 1}


def test_expected_micro_pairs_prefers_device_pairs() -> None:
    account_summary = {
        "activity_athlete_enumeration": {
            "pairs": [("athlete-1", "activity-1"), ("athlete-2", "activity-1")]
        },
        "activity_devices": {
            "pairs": [("athlete-1", "activity-1")]
        },
    }

    assert review._expected_micro_pairs(account_summary) == {("athlete-1", "activity-1")}


def test_classify_zero_row_table_marks_entity_tags_as_unresolved() -> None:
    classification = review._classify_zero_row_table("raw.catapult_entity_tags", 0, 0)

    assert classification is not None
    assert classification["status"] == "unresolved"


def test_classify_identifier_shape_distinguishes_uuid_numeric_and_text() -> None:
    assert review._classify_identifier_shape("5e8a91e0-4ba2-4faf-b6e0-fe51117d8c84") == "uuid"
    assert review._classify_identifier_shape("123456") == "numeric"
    assert review._classify_identifier_shape("team-A") == "text"


def test_select_review_accounts_matches_names_and_team_codes() -> None:
    account = CatapultAccountConfig(
        name="CATAPULT_U15",
        api_key_env="CATAPULT_U15_API_KEY",
        api_key="secret",
        team_code="U15",
        team_level="academy",
    )

    assert review._select_review_accounts("u15", (account,)) == [account]
    assert review._select_review_accounts("CATAPULT_U15", (account,)) == [account]


def test_run_review_audit_only_uses_reconstructed_context(monkeypatch) -> None:
    raw_summary = {"accounts": {}, "errors": []}
    replay_summary = {"tables": {}}
    audit_summary = {"accounts": {}, "passed": True, "failures": []}

    class FakeDb:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(review, "DatabaseManager", lambda config: FakeDb())
    monkeypatch.setattr(review, "get_db_config", lambda: {})
    monkeypatch.setattr(
        review,
        "_build_audit_only_context",
        lambda **kwargs: (raw_summary, replay_summary),
    )
    monkeypatch.setattr(
        review,
        "audit_review",
        lambda **kwargs: audit_summary,
    )

    result = review.run_review(audit_only=True)

    assert result["raw"] is raw_summary
    assert result["raw_to_bronze"] is replay_summary
    assert result["audit"] is audit_summary
    assert result["mode"] == "audit_only"
    assert result["passed"] is True
