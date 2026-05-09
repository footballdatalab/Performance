"""
Endpoint wrapper for the VALD NordBord API.

Provides methods to retrieve hamstring-strength tests, metrics, force
traces, and eccentric/isometric training session data.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class NordBordEndpoint:
    """Methods for the VALD External NordBord API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``nordbord``
            product (see :meth:`ValdClient.nordbord_client`).
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
        """Retrieve NordBord tests modified since a given timestamp (v2).

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

        logger.debug("Fetching NordBord tests v2 — params=%s", params)
        response = self.client.get("/tests/v2", params=params)

        if response.status_code == 204:
            logger.debug("NordBord tests v2 returned 204 No Content")
            return []

        data = response.json()
        # API wraps tests in {"tests": [...]}
        if isinstance(data, dict):
            return data.get("tests", [])
        return data

    def get_test_metrics(self, tenant_id: str, test_id: str) -> dict:
        """Retrieve computed metrics for a specific NordBord test.

        Args:
            tenant_id: UUID of the tenant.
            test_id: UUID of the test.

        Returns:
            Dictionary of metric values.

        API:
            ``GET /tests/{test_id}/metrics?tenantId=...``
        """
        logger.debug(
            "Fetching NordBord test metrics — tenant=%s test=%s",
            tenant_id,
            test_id,
        )
        response = self.client.get(
            f"/tests/{test_id}/metrics",
            params={"tenantId": tenant_id},
        )
        return response.json()

    def get_force_trace(self, tenant_id: str, test_id: str) -> list[dict]:
        """Retrieve the raw force-trace data for a NordBord test.

        Args:
            tenant_id: UUID of the tenant.
            test_id: UUID of the test.

        Returns:
            List of force-trace data points.

        API:
            ``GET /tests/{test_id}/nordbordtrace?tenantId=...``
        """
        logger.debug(
            "Fetching NordBord force trace — tenant=%s test=%s",
            tenant_id,
            test_id,
        )
        response = self.client.get(
            f"/tests/{test_id}/nordbordtrace",
            params={"tenantId": tenant_id},
        )
        return response.json()

    # ------------------------------------------------------------------
    # Eccentric training
    # ------------------------------------------------------------------

    def get_ecc_exercises(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve NordBord eccentric training exercises.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of exercise objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/eccentric/exercises?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching NordBord ecc exercises — params=%s", params)
        response = self.client.get(
            "/training/sessions/eccentric/exercises", params=params
        )

        if response.status_code == 204:
            logger.debug("NordBord ecc exercises returned 204 No Content")
            return []

        return response.json()

    def get_ecc_repetitions(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve NordBord eccentric training exercise repetitions.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of repetition objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/eccentric/exercises/repetitions?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching NordBord ecc repetitions — params=%s", params)
        response = self.client.get(
            "/training/sessions/eccentric/exercises/repetitions", params=params
        )

        if response.status_code == 204:
            logger.debug("NordBord ecc repetitions returned 204 No Content")
            return []

        return response.json()

    # ------------------------------------------------------------------
    # Isometric training
    # ------------------------------------------------------------------

    def get_iso_sessions(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve NordBord isometric training sessions.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of session objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/isometric?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching NordBord iso sessions — params=%s", params)
        response = self.client.get("/training/sessions/isometric", params=params)

        if response.status_code == 204:
            logger.debug("NordBord iso sessions returned 204 No Content")
            return []

        return response.json()

    def get_iso_exercises(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve NordBord isometric training exercises.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of exercise objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/isometric/exercises?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching NordBord iso exercises — params=%s", params)
        response = self.client.get(
            "/training/sessions/isometric/exercises", params=params
        )

        if response.status_code == 204:
            logger.debug("NordBord iso exercises returned 204 No Content")
            return []

        return response.json()

    def get_iso_repetitions(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve NordBord isometric training exercise repetitions.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.
            profile_id: Optional profile UUID filter.

        Returns:
            List of repetition objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /training/sessions/isometric/exercises/repetitions?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching NordBord iso repetitions — params=%s", params)
        response = self.client.get(
            "/training/sessions/isometric/exercises/repetitions", params=params
        )

        if response.status_code == 204:
            logger.debug("NordBord iso repetitions returned 204 No Content")
            return []

        return response.json()
