"""
Endpoint wrapper for the VALD Profiles API.

Provides methods to retrieve athlete/patient profiles for a tenant.
"""

from __future__ import annotations

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class ProfilesEndpoint:
    """Methods for the VALD External Profiles API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``profiles``
            product (see :meth:`ValdClient.profiles_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    def get_profiles(
        self,
        tenant_id: str,
        profile_ids: list[str] | None = None,
        group_id: str | None = None,
    ) -> list[dict]:
        """Retrieve profiles for a specific tenant.

        Args:
            tenant_id: UUID of the tenant (required).
            profile_ids: Optional list of profile UUIDs to filter.
            group_id: Optional group UUID to filter by.

        Returns:
            List of profile objects with profileId, syncId, givenName,
            familyName, dateOfBirth, externalId.

        API:
            ``GET /profiles?tenantId=<tenantId>``
        """
        logger.debug("Fetching profiles for tenant=%s", tenant_id)
        params: dict[str, str] = {"tenantId": tenant_id}
        if profile_ids:
            params["profileIds"] = ",".join(profile_ids)
        if group_id:
            params["groupId"] = group_id
        response = self.client.get("/profiles", params=params)
        data = response.json()
        # API returns {"profiles": [...]}
        if isinstance(data, dict) and "profiles" in data:
            return data["profiles"]
        return data
