from __future__ import annotations

import os
import types
import uuid
from datetime import datetime, timezone

import pytest

if not os.environ.get("VALD_SMOKE_POSTGRES_HOST"):
    pytest.skip("VALD smoke database is not configured.", allow_module_level=True)

try:
    import psycopg
except ModuleNotFoundError:  # pragma: no cover - smoke path only
    psycopg = None

from ingestion.vald import pipeline


def test_vald_stage_pipeline_smoke(monkeypatch) -> None:
    db_config = {
        "host": os.environ["VALD_SMOKE_POSTGRES_HOST"],
        "port": int(os.environ.get("VALD_SMOKE_POSTGRES_PORT", "5432")),
        "dbname": os.environ["VALD_SMOKE_POSTGRES_DB"],
        "user": os.environ["VALD_SMOKE_POSTGRES_USER"],
        "password": os.environ.get("VALD_SMOKE_POSTGRES_PASSWORD", ""),
    }
    if psycopg is None:
        pytest.skip("psycopg is required for the smoke database path.")

    monkeypatch.setattr(pipeline, "get_db_config", lambda: db_config)

    pipeline.bootstrap_database()

    conn = psycopg.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
    )

    provider_profile_id = str(uuid.uuid4())
    team_group_id = str(uuid.uuid4())
    category_id = str(uuid.uuid4())
    test_id = str(uuid.uuid4())

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE raw.vald_profiles,
                               raw.vald_forcedecks_tests,
                               bronze.vald_profiles,
                               bronze.vald_profile_categories,
                               bronze.vald_forcedecks_tests,
                               silver.vald_assessment_metric,
                               silver.vald_reference_metric_coverage,
                               gold.vald_forcedecks
                RESTART IDENTITY
                """
            )
            cur.execute(
                """
                INSERT INTO raw.vald_profiles (
                    source_account, api_endpoint, request_params, response_payload,
                    response_status, page_number, batch_id, api_version
                )
                VALUES (
                    'vald_default',
                    '/v2019q3/teams/tenant-1/athletes',
                    '{"teamId":"tenant-1"}'::jsonb,
                    %s::jsonb,
                    200,
                    NULL,
                    uuid_generate_v4(),
                    'v2019q3'
                )
                """,
                (f'[{{"id":"{provider_profile_id}","givenName":"Martim","familyName":"Fernandes","attributes":[]}}]',),
            )
            cur.execute(
                """
                INSERT INTO raw.vald_forcedecks_tests (
                    source_account, api_endpoint, request_params, response_payload,
                    response_status, page_number, batch_id, api_version
                )
                VALUES (
                    'vald_default',
                    '/tests',
                    '{"tenantId":"tenant-1"}'::jsonb,
                    %s::jsonb,
                    200,
                    NULL,
                    uuid_generate_v4(),
                    'v2020q1'
                )
                """,
                (f'[{{"id":"{test_id}","tenantId":"tenant-1","profileId":"{provider_profile_id}","modifiedDateUtc":"2026-03-28T00:00:00Z"}}]',),
            )
            for metric_value in list(range(1, 101)) + [1000]:
                cur.execute(
                    """
                    INSERT INTO silver.vald_assessment_metric (
                        provider_profile_id,
                        athlete_name,
                        team_name,
                        team_group_name,
                        team_group_id,
                        category_id,
                        test_date,
                        source_module,
                        assessment_family,
                        test_id,
                        test_name,
                        test_type,
                        metric_name,
                        metric_value,
                        metric_unit,
                        side,
                        rep_number,
                        metric_row_key
                    )
                    VALUES (
                        %s, 'Martim Fernandes', 'Equipa A', 'Equipa A Active', %s, %s,
                        '2026-03-28T00:00:00Z', 'forcedecks', 'forcedecks', %s,
                        'CMJ', 'CMJ', 'takeoff_jump_height_imp_mom', %s, 'cm', NULL, 1, md5(random()::text)
                    )
                    """,
                    (provider_profile_id, team_group_id, category_id, test_id, metric_value),
                )

    replay_summary = pipeline.run_raw_to_bronze_stage(modules="forcedecks", include_reference=True)
    gold_summary = pipeline.run_silver_to_gold_stage()

    assert replay_summary["processed_raw_rows"] >= 2
    assert gold_summary["total_rows"] > 0
    assert gold_summary["total_excluded_outside_threshold_rows"] >= 1


def test_vald_intraday_gold_only_rebuilds_current_day_slice(monkeypatch) -> None:
    db_config = {
        "host": os.environ["VALD_SMOKE_POSTGRES_HOST"],
        "port": int(os.environ.get("VALD_SMOKE_POSTGRES_PORT", "5432")),
        "dbname": os.environ["VALD_SMOKE_POSTGRES_DB"],
        "user": os.environ["VALD_SMOKE_POSTGRES_USER"],
        "password": os.environ.get("VALD_SMOKE_POSTGRES_PASSWORD", ""),
    }
    if psycopg is None:
        pytest.skip("psycopg is required for the smoke database path.")

    monkeypatch.setattr(pipeline, "get_db_config", lambda: db_config)

    pipeline.bootstrap_database()

    conn = psycopg.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
    )

    provider_profile_id = str(uuid.uuid4())
    team_group_id = str(uuid.uuid4())
    category_id = str(uuid.uuid4())
    historical_test_id = str(uuid.uuid4())
    current_test_id = str(uuid.uuid4())
    day_start = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        pipeline,
        "resolve_lisbon_day_window_utc",
        lambda reference_time=None: types.SimpleNamespace(
            day_start_utc=day_start,
            day_end_utc=day_end,
            as_summary=lambda: {
                "day_start_utc": "2026-03-29T00:00:00Z",
                "day_end_utc": "2026-03-30T00:00:00Z",
            },
        ),
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE silver.vald_assessment_metric,
                               silver.vald_reference_metric_coverage,
                               gold.vald_forcedecks
                RESTART IDENTITY
                """
            )
            for metric_value in range(1, 101):
                cur.execute(
                    """
                    INSERT INTO silver.vald_assessment_metric (
                        provider_profile_id,
                        athlete_name,
                        team_name,
                        team_group_name,
                        team_group_id,
                        category_id,
                        test_date,
                        source_module,
                        assessment_family,
                        test_id,
                        test_name,
                        test_type,
                        metric_name,
                        metric_value,
                        metric_unit,
                        side,
                        rep_number,
                        metric_row_key
                    )
                    VALUES (
                        %s, 'Martim Fernandes', 'Equipa A', 'Equipa A Active', %s, %s,
                        '2026-03-28T12:00:00Z', 'forcedecks', 'forcedecks', %s,
                        'CMJ', 'CMJ', 'takeoff_jump_height_imp_mom', %s, 'cm', NULL, 1, md5(random()::text)
                    )
                    """,
                    (provider_profile_id, team_group_id, category_id, historical_test_id, metric_value),
                )
            cur.execute(
                """
                INSERT INTO silver.vald_assessment_metric (
                    provider_profile_id,
                    athlete_name,
                    team_name,
                    team_group_name,
                    team_group_id,
                    category_id,
                    test_date,
                    source_module,
                    assessment_family,
                    test_id,
                    test_name,
                    test_type,
                    metric_name,
                    metric_value,
                    metric_unit,
                    side,
                    rep_number,
                    metric_row_key
                )
                VALUES (
                    %s, 'Martim Fernandes', 'Equipa A', 'Equipa A Active', %s, %s,
                    '2026-03-29T12:00:00Z', 'forcedecks', 'forcedecks', %s,
                    'CMJ', 'CMJ', 'takeoff_jump_height_imp_mom', 55, 'cm', NULL, 1, md5(random()::text)
                )
                """,
                (provider_profile_id, team_group_id, category_id, current_test_id),
            )
            cur.execute(
                """
                INSERT INTO gold.vald_forcedecks (
                    provider_profile_id,
                    athlete_name,
                    team_name,
                    team_group_name,
                    team_group_id,
                    category_id,
                    test_date,
                    source_module,
                    assessment_family,
                    test_id,
                    test_name,
                    test_type,
                    metric_name,
                    metric_value,
                    metric_unit,
                    side,
                    rep_number
                )
                VALUES (
                    %s, 'Martim Fernandes', 'Equipa A', 'Equipa A Active', %s, %s,
                    '2026-03-28T12:00:00Z', 'forcedecks', 'forcedecks', %s,
                    'CMJ', 'CMJ', 'takeoff_jump_height_imp_mom', 42, 'cm', NULL, 1
                )
                """,
                (provider_profile_id, team_group_id, category_id, historical_test_id),
            )
            cur.execute(
                """
                INSERT INTO gold.vald_forcedecks (
                    provider_profile_id,
                    athlete_name,
                    team_name,
                    team_group_name,
                    team_group_id,
                    category_id,
                    test_date,
                    source_module,
                    assessment_family,
                    test_id,
                    test_name,
                    test_type,
                    metric_name,
                    metric_value,
                    metric_unit,
                    side,
                    rep_number
                )
                VALUES (
                    %s, 'Martim Fernandes', 'Equipa A', 'Equipa A Active', %s, %s,
                    '2026-03-29T12:00:00Z', 'forcedecks', 'forcedecks', %s,
                    'CMJ', 'CMJ', 'takeoff_jump_height_imp_mom', 999, 'cm', NULL, 1
                )
                """,
                (provider_profile_id, team_group_id, category_id, current_test_id),
            )

    summary = pipeline.run_intraday_silver_to_gold_stage()

    assert summary["tables"]["gold.vald_forcedecks"]["inserted_rows"] == 1

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_value
                FROM gold.vald_forcedecks
                WHERE test_date = '2026-03-28T12:00:00Z'::timestamptz
                """
            )
            historical_values = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                SELECT metric_value
                FROM gold.vald_forcedecks
                WHERE test_date >= %s
                  AND test_date < %s
                ORDER BY metric_value
                """,
                (day_start, day_end),
            )
            current_day_values = [row[0] for row in cur.fetchall()]

    assert historical_values == [42]
    assert current_day_values == [55]
