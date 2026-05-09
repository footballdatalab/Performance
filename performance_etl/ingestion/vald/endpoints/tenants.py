"""
Endpoint wrapper for the VALD External Tenants API.

Provides methods to retrieve tenants, their categories, and their groups.

Reference: skills/vald-playbook/api-tenants.md
"""

from __future__ import annotations

from ingestion.common.http_client import BaseHttpClient
from ingestion.common.logging import get_logger

logger = get_logger(__name__)


class TenantsEndpoint:
    """Methods for the VALD External Tenants API.

    Args:
        client: A :class:`BaseHttpClient` configured for the ``tenants``
            product (see :meth:`ValdClient.tenants_client`).
    """

    def __init__(self, client: BaseHttpClient) -> None:
        self.client = client

    def get_tenants(self) -> list[dict]:
        """Retrieve all tenants accessible to the authenticated account.

        Returns:
            List of tenant objects.

        API:
            ``GET /tenants``
        """
        logger.debug("Fetching tenants")
        response = self.client.get("/tenants")
        data = response.json()
        # API returns {"tenants": [...]}
        if isinstance(data, dict) and "tenants" in data:
            return data["tenants"]
        return data

    def get_tenant(self, tenant_id: str) -> dict:
        """Retrieve details of a single tenant.

        Args:
            tenant_id: UUID of the tenant.

        Returns:
            Tenant object with id, name, sport, league, logoUri.

        API:
            ``GET /tenants/{tenantId}``
        """
        logger.debug("Fetching tenant details for %s", tenant_id)
        response = self.client.get(f"/tenants/{tenant_id}")
        data = response.json()
        return data

    def get_categories(self, tenant_id: str) -> list[dict]:
        """Retrieve categories for a specific tenant.

        Args:
            tenant_id: UUID of the tenant.

        Returns:
            List of category objects with id, name, syncId.

        API:
            ``GET /categories?tenantId=<tenantId>``
        """
        logger.debug("Fetching categories for tenant=%s", tenant_id)
        response = self.client.get("/categories", params={"tenantId": tenant_id})
        data = response.json()
        # API returns {"categories": [...]}
        if isinstance(data, dict) and "categories" in data:
            return data["categories"]
        return data

    def get_groups(self, tenant_id: str) -> list[dict]:
        """Retrieve groups for a specific tenant.

        Args:
            tenant_id: UUID of the tenant.

        Returns:
            List of group objects with id, name, categoryId, syncId.

        API:
            ``GET /groups?tenantId=<tenantId>``
        """
        logger.debug("Fetching groups for tenant=%s", tenant_id)
        response = self.client.get("/groups", params={"tenantId": tenant_id})
        data = response.json()
        # API returns {"groups": [...]}
        if isinstance(data, dict) and "groups" in data:
            return data["groups"]
        return data
