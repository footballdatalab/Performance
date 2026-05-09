"""
Start the local Dockerized Airflow stack in the correct order.

This wrapper waits for active provider Airflow work to finish, runs the one-shot
``airflow-init`` service, and then starts the long-running webserver and
scheduler services.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DAG_IDS = (
    "vald_midnight_full_refresh",
    "vald_intraday_incremental",
    "catapult_daily_full_refresh",
    "catapult_intraday_incremental",
)
ACTIVE_TASK_STATES = ("queued", "running")
DEFAULT_WAIT_TIMEOUT_SECONDS = 7200
DEFAULT_POLL_SECONDS = 30
DEFAULT_SERVICE_HEALTH_TIMEOUT_SECONDS = 180
DEFAULT_SERVICE_HEALTH_POLL_SECONDS = 5

_ACTIVE_TASK_QUERY = """
import json
from airflow.models import TaskInstance
from airflow.settings import Session

DAG_IDS = (
    "vald_midnight_full_refresh",
    "vald_intraday_incremental",
    "catapult_daily_full_refresh",
    "catapult_intraday_incremental",
)
ACTIVE_STATES = ("queued", "running")

session = Session()
try:
    rows = (
        session.query(
            TaskInstance.dag_id,
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.state,
            TaskInstance.start_date,
        )
        .filter(
            TaskInstance.dag_id.in_(DAG_IDS),
            TaskInstance.state.in_(ACTIVE_STATES),
        )
        .order_by(
            TaskInstance.dag_id.asc(),
            TaskInstance.run_id.asc(),
            TaskInstance.task_id.asc(),
        )
        .all()
    )
    print(
        json.dumps(
            [
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "state": state,
                    "start_date": start_date.isoformat() if start_date else None,
                }
                for dag_id, run_id, task_id, state, start_date in rows
            ]
        )
    )
finally:
    session.close()
""".strip()


def _run(command: list[str]) -> int:
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    return completed.returncode


def _run_capture(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _build_shared_airflow_image() -> int:
    """Build the shared Airflow image once.

    Multiple services reuse the same ``image`` and ``build`` definition in the
    Compose file. Building a single canonical service avoids concurrent exports
    of the same tag.
    """
    return _run(["docker", "compose", "build", "airflow-webserver"])


def _get_service_container_id(service: str) -> str | None:
    completed = _run_capture(["docker", "compose", "ps", "-q", service])
    if completed.returncode != 0:
        return None
    container_id = completed.stdout.strip()
    return container_id or None


def _get_container_status(container_id: str) -> str | None:
    completed = _run_capture(
        [
            "docker",
            "inspect",
            "-f",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
            container_id,
        ]
    )
    if completed.returncode != 0:
        return None
    status = completed.stdout.strip()
    return status or None


def wait_for_service_healthy(
    service: str,
    *,
    timeout_seconds: int = DEFAULT_SERVICE_HEALTH_TIMEOUT_SECONDS,
    poll_seconds: int = DEFAULT_SERVICE_HEALTH_POLL_SECONDS,
) -> int:
    print(f"Waiting for {service} to become healthy...")
    deadline = time.monotonic() + timeout_seconds

    while True:
        container_id = _get_service_container_id(service)
        if container_id is not None:
            status = _get_container_status(container_id)
            if status == "healthy":
                print(f"{service} is healthy.")
                return 0
            if status == "running":
                print(f"{service} is running.")
                return 0

        if time.monotonic() >= deadline:
            print(f"Timed out waiting for {service} to become healthy.")
            return 1

        time.sleep(poll_seconds)


def _list_running_services() -> set[str]:
    completed = _run_capture(
        ["docker", "compose", "ps", "--services", "--status", "running"]
    )
    if completed.returncode != 0:
        return set()
    return {
        line.strip()
        for line in completed.stdout.splitlines()
        if line.strip()
    }


def _get_airflow_query_service() -> str | None:
    running_services = _list_running_services()
    if "airflow-scheduler" in running_services:
        return "airflow-scheduler"
    if "airflow-webserver" in running_services:
        return "airflow-webserver"
    return None


def _query_active_vald_tasks(service: str) -> list[dict[str, str | None]]:
    completed = _run_capture(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            service,
            "python",
            "-c",
            _ACTIVE_TASK_QUERY,
        ]
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or "Unknown error."
        raise RuntimeError(f"Failed to query active Airflow tasks via {service}: {message}")

    payload = completed.stdout.strip() or "[]"
    return json.loads(payload)


def _format_active_tasks(tasks: list[dict[str, str | None]]) -> str:
    lines = []
    for task in tasks:
        lines.append(
            "  - "
            f"{task['dag_id']} | {task['run_id']} | {task['task_id']} | "
            f"{task['state']} | start={task['start_date'] or 'n/a'}"
        )
    return "\n".join(lines)


def wait_for_vald_idle(
    *,
    timeout_seconds: int = DEFAULT_WAIT_TIMEOUT_SECONDS,
    poll_seconds: int = DEFAULT_POLL_SECONDS,
) -> int:
    service = _get_airflow_query_service()
    if service is None:
        print("No running Airflow service found; skipping provider idle wait.")
        return 0

    print(f"Checking active provider Airflow tasks via {service} before deploy...")
    deadline = time.monotonic() + timeout_seconds

    while True:
        active_tasks = _query_active_vald_tasks(service)
        if not active_tasks:
            print("No active provider Airflow tasks found. Safe to deploy.")
            return 0

        print("Active provider Airflow tasks detected:")
        print(_format_active_tasks(active_tasks))

        if time.monotonic() >= deadline:
            print(
                "Timed out waiting for active provider Airflow tasks to finish. "
                "Use --skip-wait to force a deploy anyway."
            )
            return 1

        print(f"Waiting {poll_seconds} seconds before checking again...")
        time.sleep(poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Safely initialize and start the Dockerized Airflow stack."
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip Docker image rebuilds during startup.",
    )
    parser.add_argument(
        "--skip-wait",
        action="store_true",
        help="Skip the safety wait for active provider Airflow tasks.",
    )
    parser.add_argument(
        "--wait-timeout-seconds",
        type=int,
        default=DEFAULT_WAIT_TIMEOUT_SECONDS,
        help=(
            "Maximum time to wait for active provider Airflow tasks before aborting "
            f"(default: {DEFAULT_WAIT_TIMEOUT_SECONDS})."
        ),
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help=f"Polling interval while waiting for VALD tasks to finish (default: {DEFAULT_POLL_SECONDS}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.wait_timeout_seconds < 1:
        print("--wait-timeout-seconds must be greater than 0.")
        return 2
    if args.poll_seconds < 1:
        print("--poll-seconds must be greater than 0.")
        return 2

    if not args.skip_wait:
        wait_rc = wait_for_vald_idle(
            timeout_seconds=args.wait_timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
        if wait_rc != 0:
            return wait_rc

    if not args.no_build:
        build_rc = _build_shared_airflow_image()
        if build_rc != 0:
            return build_rc

    metadata_command = ["docker", "compose", "up", "-d", "airflow-metadata-db"]
    init_command = ["docker", "compose", "up", "--no-deps", "airflow-init"]
    start_command = [
        "docker",
        "compose",
        "up",
        "-d",
        "airflow-webserver",
        "airflow-scheduler",
    ]

    metadata_rc = _run(metadata_command)
    if metadata_rc != 0:
        return metadata_rc
    metadata_wait_rc = wait_for_service_healthy("airflow-metadata-db")
    if metadata_wait_rc != 0:
        return metadata_wait_rc

    init_rc = _run(init_command)
    if init_rc != 0:
        return init_rc

    return _run(start_command)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
