"""
Endpoint wrapper for the VALD SmartSpeed API.

Provides methods to retrieve timing-gate test summaries and detailed
test results.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class SmartSpeedEndpoint:
    """Methods for the VALD External SmartSpeed API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``smartspeed``
            product (see :meth:`ValdClient.smartspeed_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    def get_test_summaries(
        self,
        team_id: str,
        page: int = 1,
        modified_from_utc: str | None = None,
        athlete_id: str | None = None,
    ) -> list[dict]:
        """Retrieve SmartSpeed test summaries for a team, paginated.

        Termination: an empty array signals no more data.

        Args:
            team_id: UUID of the team.
            page: 1-based page number (default ``1``).
            modified_from_utc: Optional ISO-8601 UTC timestamp to filter
                tests modified after this date.
            athlete_id: Optional athlete UUID to filter results.

        Returns:
            List of test-summary objects.  An empty list means there are
            no further pages.

        API:
            ``GET /v1/team/{team_id}/tests?page=...&modifiedFromUtc=...``
        """
        params: dict[str, Any] = {"page": page}
        if modified_from_utc is not None:
            params["modifiedFromUtc"] = modified_from_utc
        if athlete_id is not None:
            params["athleteId"] = athlete_id

        logger.debug(
            "Fetching SmartSpeed test summaries — team=%s params=%s",
            team_id,
            params,
        )
        response = self.client.get(f"/v1/team/{team_id}/tests", params=params)
        return response.json()

    def get_test_detail(self, team_id: str, test_id: str) -> dict:
        """Retrieve the full detail for a specific SmartSpeed test.

        Args:
            team_id: UUID of the team.
            test_id: UUID of the test.

        Returns:
            Test-detail object.

        API:
            ``GET /v1/team/{team_id}/tests/{test_id}/detail``
        """
        logger.debug(
            "Fetching SmartSpeed test detail — team=%s test=%s",
            team_id,
            test_id,
        )
        response = self.client.get(
            f"/v1/team/{team_id}/tests/{test_id}/detail"
        )
        return response.json()
