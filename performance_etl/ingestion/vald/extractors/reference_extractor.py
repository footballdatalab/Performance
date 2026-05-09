"""
VALD reference data extractor.

Captures the current athlete-profile snapshot using the ForceDecks
``/v2019q3/teams/{id}/athletes`` endpoint.
"""

from __future__ import annotations

from typing import Any

from ingestion.common.batch import BatchManager
from ingestion.common.logging import get_logger
from ingestion.vald.client import ValdClient
from ingestion.vald.endpoints.forcedecks import ForceDecksEndpoint
from ingestion.vald.loaders.raw_loader import ValdRawLoader

logger = get_logger(__name__)


class ValdReferenceExtractor:
    """Extract the VALD athlete-profile reference snapshot."""

    def __init__(
        self,
        vald_client: ValdClient,
        raw_loader: ValdRawLoader,
        batch_manager: BatchManager,
    ) -> None:
        self.vald_client = vald_client
        self.raw_loader = raw_loader
        self.batch_manager = batch_manager
        self._fd_ep = ForceDecksEndpoint(vald_client.forcedecks_client)

    def extract_all(self, tenant_ids: list[str] | None = None) -> dict[str, Any]:
        """Extract all reference entities."""
        summary: dict[str, Any] = {
            "profiles_seen": 0,
            "snapshots_written": 0,
            "errors": [],
        }

        if not tenant_ids:
            logger.warning("No tenant IDs provided for reference extraction")
            return summary

        for tenant_id in tenant_ids:
            try:
                athletes_data = self._fd_ep.get_athletes(tenant_id)
                if not athletes_data:
                    continue
                _, inserted = self.raw_loader.load_raw_if_changed_with_status(
                    table_name="vald_profiles",
                    api_endpoint=f"/v2019q3/teams/{tenant_id}/athletes",
                    response_payload=athletes_data,
                    request_params={"teamId": tenant_id},
                    api_version="v2019q3",
                )
                summary["profiles_seen"] += len(athletes_data)
                summary["snapshots_written"] += 1 if inserted else 0
                logger.info(
                    "Extracted %d profiles for tenant=%s via ForceDecks (snapshot_written=%s)",
                    len(athletes_data),
                    tenant_id,
                    inserted,
                )
            except Exception as exc:
                msg = f"Failed to extract profiles for tenant {tenant_id}: {exc}"
                logger.error(msg)
                summary["errors"].append(msg)

        logger.info(
            "Reference extraction complete: profiles_seen=%d snapshots_written=%d errors=%d",
            summary["profiles_seen"],
            summary["snapshots_written"],
            len(summary["errors"]),
        )
        return summary
