from __future__ import annotations

import subprocess

from script import start_airflow_stack


def test_get_airflow_query_service_prefers_scheduler(monkeypatch) -> None:
    monkeypatch.setattr(
        start_airflow_stack,
        "_list_running_services",
        lambda: {"airflow-webserver", "airflow-scheduler"},
    )

    assert start_airflow_stack._get_airflow_query_service() == "airflow-scheduler"


def test_wait_for_vald_idle_returns_immediately_without_services(monkeypatch) -> None:
    monkeypatch.setattr(
        start_airflow_stack,
        "_get_airflow_query_service",
        lambda: None,
    )

    assert start_airflow_stack.wait_for_vald_idle(timeout_seconds=30, poll_seconds=1) == 0


def test_wait_for_vald_idle_polls_until_clear(monkeypatch) -> None:
    calls = {"count": 0}
    slept: list[int] = []

    def fake_query(service: str):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {
                    "dag_id": "vald_intraday_incremental",
                    "run_id": "scheduled__2026-03-29T12:30:00+00:00",
                    "task_id": "extract_task",
                    "state": "running",
                    "start_date": "2026-03-29T13:00:05+00:00",
                }
            ]
        return []

    monotonic_values = iter([0.0, 1.0])

    monkeypatch.setattr(
        start_airflow_stack,
        "_get_airflow_query_service",
        lambda: "airflow-scheduler",
    )
    monkeypatch.setattr(start_airflow_stack, "_query_active_vald_tasks", fake_query)
    monkeypatch.setattr(start_airflow_stack.time, "sleep", slept.append)
    monkeypatch.setattr(start_airflow_stack.time, "monotonic", lambda: next(monotonic_values))

    assert start_airflow_stack.wait_for_vald_idle(timeout_seconds=30, poll_seconds=5) == 0
    assert slept == [5]


def test_wait_for_vald_idle_times_out(monkeypatch) -> None:
    monotonic_values = iter([0.0, 31.0])

    monkeypatch.setattr(
        start_airflow_stack,
        "_get_airflow_query_service",
        lambda: "airflow-scheduler",
    )
    monkeypatch.setattr(
        start_airflow_stack,
        "_query_active_vald_tasks",
        lambda service: [
            {
                "dag_id": "vald_midnight_full_refresh",
                "run_id": "scheduled__2026-03-29T00:00:00+00:00",
                "task_id": "reset_rebuild_task",
                "state": "running",
                "start_date": "2026-03-29T12:17:25+00:00",
            }
        ],
    )
    monkeypatch.setattr(start_airflow_stack.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(start_airflow_stack.time, "monotonic", lambda: next(monotonic_values))

    assert start_airflow_stack.wait_for_vald_idle(timeout_seconds=30, poll_seconds=5) == 1


def test_main_runs_wait_then_init_and_start(monkeypatch) -> None:
    commands: list[list[str]] = []

    monkeypatch.setattr(
        start_airflow_stack,
        "wait_for_vald_idle",
        lambda timeout_seconds, poll_seconds: 0,
    )
    monkeypatch.setattr(
        start_airflow_stack,
        "_run",
        lambda command: commands.append(command) or 0,
    )
    monkeypatch.setattr(
        start_airflow_stack,
        "wait_for_service_healthy",
        lambda service: 0,
    )

    rc = start_airflow_stack.main([])

    assert rc == 0
    assert commands == [
        ["docker", "compose", "build", "airflow-webserver"],
        ["docker", "compose", "up", "-d", "airflow-metadata-db"],
        ["docker", "compose", "up", "--no-deps", "airflow-init"],
        [
            "docker",
            "compose",
            "up",
            "-d",
            "airflow-webserver",
            "airflow-scheduler",
        ],
    ]


def test_wait_for_service_healthy_returns_when_container_is_healthy(monkeypatch) -> None:
    monkeypatch.setattr(
        start_airflow_stack,
        "_get_service_container_id",
        lambda service: "container-123",
    )
    monkeypatch.setattr(
        start_airflow_stack,
        "_get_container_status",
        lambda container_id: "healthy",
    )

    assert start_airflow_stack.wait_for_service_healthy(
        "airflow-metadata-db",
        timeout_seconds=5,
        poll_seconds=1,
    ) == 0


def test_query_active_vald_tasks_raises_on_failed_exec(monkeypatch) -> None:
    monkeypatch.setattr(
        start_airflow_stack,
        "_run_capture",
        lambda command: subprocess.CompletedProcess(
            command,
            returncode=1,
            stdout="",
            stderr="boom",
        ),
    )

    try:
        start_airflow_stack._query_active_vald_tasks("airflow-scheduler")
    except RuntimeError as exc:
        assert "boom" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")
