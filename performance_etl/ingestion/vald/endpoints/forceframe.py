"""
Endpoint wrapper for the VALD ForceFrame API.

Provides methods to retrieve isometric-strength tests, metrics, force
traces, and training session data.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class ForceFrameEndpoint:
    """Methods for the VALD External ForceFrame API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``forceframe``
            product (see :meth:`ValdClient.forceframe_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def get_tests_v2(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve ForceFrame tests modified since a given timestamp (v2).

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of test objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /tests/v2?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching ForceFrame tests v2 — params=%s", params)
        response = self.client.get("/tests/v2", params=params)

        if response.status_code == 204:
            logger.debug("ForceFrame tests v2 returned 204 No Content")
            return []

        data = response.json()
        # API wraps tests in {"tests": [...]}
        if isinstance(data, dict):
            return data.get("tests", [])
        return data

    def get_test_metrics(self, tenant_id: str, test_id: str) -> dict:
        """Retrieve computed metrics for a specific ForceFrame test.

        Args:
            tenant_id: UUID of the tenant.
            test_id: UUID of the test.

        Returns:
            Dictionary of metric values.

        API:
            ``GET /tests/{test_id}/metrics?tenantId=...``
        """
        logger.debug(
            "Fetching ForceFrame test metrics — tenant=%s test=%s",
            tenant_id,
            test_id,
        )
        response = self.client.get(
            f"/tests/{test_id}/metrics",
            params={"tenantId": tenant_id},
        )
        return response.json()

    def get_force_trace(self, tenant_id: str, test_id: str) -> list[dict]:
        """Retrieve the raw force-trace data for a ForceFrame test.

        Args:
            tenant_id: UUID of the tenant.
            test_id: UUID of the test.

        Returns:
            List of force-trace data points.

        API:
            ``GET /tests/{test_id}/forceframetrace?tenantId=...``
        """
        logger.debug(
            "Fetching ForceFrame force trace — tenant=%s test=%s",
            tenant_id,
            test_id,
        )
        response = self.client.get(
            f"/tests/{test_id}/forceframetrace",
            params={"tenantId": tenant_id},
        )
        return response.json()

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def get_training_exercises(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve ForceFrame training-session exercises.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of exercise objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/exercises?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching ForceFrame training exercises — params=%s", params)
        response = self.client.get("/training/sessions/exercises", params=params)

        if response.status_code == 204:
            logger.debug("ForceFrame training exercises returned 204 No Content")
            return []

        return response.json()

    def get_training_repetitions(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve ForceFrame training-session exercise repetitions.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of repetition objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/exercises/repetitions?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching ForceFrame training repetitions — params=%s", params)
        response = self.client.get(
            "/training/sessions/exercises/repetitions", params=params
        )

        if response.status_code == 204:
            logger.debug("ForceFrame training repetitions returned 204 No Content")
            return []

        return response.json()
