"""
Endpoint wrapper for the VALD HumanTrak API.

Provides methods to retrieve movement-assessment test types, tests, and
test repetitions.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class HumanTrakEndpoint:
    """Methods for the VALD External HumanTrak API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``humantrak``
            product (see :meth:`ValdClient.humantrak_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    def get_test_type_metrics(self) -> list[dict]:
        """Retrieve all available HumanTrak test-type metric definitions.

        Returns:
            List of test-type metric objects.

        API:
            ``GET /v2/test-type/metrics``
        """
        logger.debug("Fetching HumanTrak test-type metrics")
        response = self.client.get("/v2/test-type/metrics")
        return response.json()

    def get_tests(
        self,
        tenant_id: str,
        modified_from_utc: str,
    ) -> list[dict]:
        """Retrieve HumanTrak tests modified since a given timestamp.

        .. important::
           The HumanTrak API uses **case-sensitive** query parameter names:
           ``TenantId`` and ``ModifiedFromUtc`` (capital letters).

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp.

        Returns:
            List of test objects, or ``[]`` on ``204 No Content``.

        API:
            ``GET /v2/tests-by-modified-date?TenantId=...&ModifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "TenantId": tenant_id,
            "ModifiedFromUtc": modified_from_utc,
        }

        logger.debug("Fetching HumanTrak tests — params=%s", params)
        response = self.client.get("/v2/tests-by-modified-date", params=params)

        if response.status_code == 204:
            logger.debug("HumanTrak tests returned 204 No Content")
            return []

        return response.json()

    def get_test_repetitions(
        self,
        test_id: str,
        tenant_id: str,
        profile_id: str,
    ) -> list[dict]:
        """Retrieve repetitions for a specific HumanTrak test.

        Args:
            test_id: UUID of the test.
            tenant_id: UUID of the tenant.
            profile_id: UUID of the profile (athlete).

        Returns:
            List of repetition objects.

        API:
            ``GET /v2/test/{test_id}/repetitions?tenantId=...&profileId=...``
        """
        logger.debug(
            "Fetching HumanTrak test repetitions — test=%s tenant=%s profile=%s",
            test_id,
            tenant_id,
            profile_id,
        )
        response = self.client.get(
            f"/v2/test/{test_id}/repetitions",
            params={"tenantId": tenant_id, "profileId": profile_id},
        )
        return response.json()
