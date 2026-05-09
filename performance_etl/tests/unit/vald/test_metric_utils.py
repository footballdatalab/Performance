from ingestion.vald.metric_utils import (
    build_metric_row_key,
    normalise_metric_value_for_key,
)


def test_normalise_metric_value_for_key_removes_scale_noise() -> None:
    assert normalise_metric_value_for_key("001.2300") == "1.23"
    assert normalise_metric_value_for_key("-0.0") == "0"


def test_build_metric_row_key_is_deterministic_for_same_row() -> None:
    row = {
        "provider_profile_id": "profile-1",
        "team_group_id": "team-1",
        "test_id": "test-1",
        "assessment_family": "nordics",
        "source_module": "nordbord",
        "metric_name": "max_force",
        "side": "left",
        "rep_number": None,
        "metric_value": "450.000",
    }

    assert build_metric_row_key(**row) == build_metric_row_key(**row)


def test_build_metric_row_key_changes_when_metric_value_changes() -> None:
    base_kwargs = {
        "provider_profile_id": "profile-1",
        "team_group_id": "team-1",
        "test_id": "test-1",
        "assessment_family": "forcedecks",
        "source_module": "forcedecks",
        "metric_name": "general_bodyweight_in_kilograms",
        "side": None,
        "rep_number": None,
    }

    first = build_metric_row_key(metric_value="81.50", **base_kwargs)
    second = build_metric_row_key(metric_value="82.00", **base_kwargs)

    assert first != second
