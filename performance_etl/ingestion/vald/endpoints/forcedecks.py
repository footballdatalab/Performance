"""
Endpoint wrapper for the VALD ForceDecks API.

Provides methods to retrieve force-plate tests, trials, and result
definitions.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class ForceDecksEndpoint:
    """Methods for the VALD External ForceDecks API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``forcedecks``
            product (see :meth:`ValdClient.forcedecks_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    def get_athletes(self, team_id: str) -> list[dict]:
        """Retrieve all athletes for a team.

        Uses the legacy ``v2019q3`` endpoint that works with old Auth0
        credentials.

        Args:
            team_id: UUID of the team (tenant).

        Returns:
            List of athlete/profile objects.

        API:
            ``GET /v2019q3/teams/{team_id}/athletes``
        """
        logger.debug("Fetching athletes for team=%s", team_id)
        response = self.client.get(f"/v2019q3/teams/{team_id}/athletes")

        if response.status_code == 204:
            return []

        return response.json()

    def get_tests(
        self,
        tenant_id: str,
        modified_from_utc: str,
        profile_id: str | None = None,
    ) -> list[dict]:
        """Retrieve ForceDecks tests modified since a given timestamp.

        Args:
            tenant_id: UUID of the tenant.
            modified_from_utc: ISO-8601 UTC timestamp. Only tests modified
                on or after this value are returned.
            profile_id: Optional profile UUID to filter tests for a single
                athlete.

        Returns:
            List of test objects, or an empty list when the API returns
            ``204 No Content`` (no more data).

        API:
            ``GET /tests?tenantId=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {
            "tenantId": tenant_id,
            "modifiedFromUtc": modified_from_utc,
        }
        if profile_id is not None:
            params["profileId"] = profile_id

        logger.debug("Fetching ForceDecks tests — params=%s", params)
        response = self.client.get("/tests", params=params)

        if response.status_code == 204:
            logger.debug("ForceDecks tests returned 204 No Content")
            return []

        data = response.json()
        # API wraps tests in {"tests": [...]}
        if isinstance(data, dict):
            return data.get("tests", [])
        return data

    def get_trials(self, team_id: str, test_id: str) -> list[dict]:
        """Retrieve trial-level data for a specific ForceDecks test.

        Args:
            team_id: UUID of the team (tenant).
            test_id: UUID of the test.

        Returns:
            List of trial objects.

        API:
            ``GET /v2019q3/teams/{team_id}/tests/{test_id}/trials``
        """
        logger.debug(
            "Fetching ForceDecks trials — team=%s test=%s", team_id, test_id
        )
        response = self.client.get(
            f"/v2019q3/teams/{team_id}/tests/{test_id}/trials"
        )
        return response.json()

    def get_result_definitions(self) -> list[dict]:
        """Retrieve the list of ForceDecks result (metric) definitions.

        Returns:
            List of result-definition objects describing available metrics.

        API:
            ``GET /resultdefinitions``
        """
        logger.debug("Fetching ForceDecks result definitions")
        response = self.client.get("/resultdefinitions")
        return response.json()
