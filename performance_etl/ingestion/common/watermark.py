"""
Sync watermark management for incremental data ingestion.

Reads and writes the ``raw.sync_watermark`` table to track per-provider,
per-account, per-endpoint synchronisation state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ingestion.common.logging import get_logger

if TYPE_CHECKING:
    from ingestion.common.db import DatabaseManager

logger = get_logger(__name__)

DEFAULT_WATERMARK = "1970-01-01T00:00:00.0000000Z"

# Sentinel UUID used to make the unique index work with NULL tenant_id.
_NULL_TENANT_SENTINEL = "00000000-0000-0000-0000-000000000000"


class WatermarkManager:
    """Read and write sync watermarks stored in ``raw.sync_watermark``.

    Args:
        db: An initialised :class:`~ingestion.common.db.DatabaseManager`.
    """

    def __init__(self, db: "DatabaseManager") -> None:
        self._db = db

    def get_watermark(
        self,
        provider: str,
        source_account: str,
        api_name: str,
        tenant_id: str | None = None,
    ) -> str:
        """Retrieve the last watermark value for a sync key.

        Args:
            provider: Provider identifier (e.g. ``"catapult"``).
            source_account: Account name (e.g. ``"CATAPULT_A"``).
            api_name: API endpoint name (e.g. ``"activities"``).
            tenant_id: Optional tenant UUID (used by VALD multi-tenant setup).

        Returns:
            The stored watermark string, or the default
            ``"1970-01-01T00:00:00.0000000Z"`` if no row exists.
        """
        sql = """
            SELECT last_watermark
            FROM raw.sync_watermark
            WHERE provider       = %s
              AND source_account = %s
              AND api_name       = %s
              AND COALESCE(tenant_id, %s::UUID) = %s::UUID
        """
        coalesce_val = tenant_id if tenant_id else _NULL_TENANT_SENTINEL
        row = self._db.fetch_one(
            sql,
            (provider, source_account, api_name, _NULL_TENANT_SENTINEL, coalesce_val),
        )
        watermark = row[0] if row else None
        if watermark:
            logger.debug(
                "Watermark found: provider=%s account=%s api=%s -> %s",
                provider,
                source_account,
                api_name,
                watermark,
            )
            return watermark

        logger.info(
            "No watermark found for provider=%s account=%s api=%s; using default",
            provider,
            source_account,
            api_name,
        )
        return DEFAULT_WATERMARK

    def update_watermark(
        self,
        provider: str,
        source_account: str,
        api_name: str,
        watermark_value: str,
        records_synced: int,
        tenant_id: str | None = None,
    ) -> None:
        """Insert or update the watermark for a sync key.

        Uses the same ``COALESCE`` unique-index strategy defined in the DDL
        so that ``NULL`` tenant_id rows are handled correctly.

        Args:
            provider: Provider identifier.
            source_account: Account name.
            api_name: API endpoint name.
            watermark_value: New watermark value to persist.
            records_synced: Number of records synced in this run.
            tenant_id: Optional tenant UUID.
        """
        sql = """
            INSERT INTO raw.sync_watermark
                (provider, source_account, api_name, tenant_id,
                 last_watermark, last_sync_started, last_sync_completed,
                 last_sync_status, records_synced, updated_at)
            VALUES
                (%s, %s, %s, %s,
                 %s, now(), now(),
                 'completed', %s, now())
            ON CONFLICT (provider, source_account, api_name,
                         COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::UUID))
            DO UPDATE SET
                last_watermark      = EXCLUDED.last_watermark,
                last_sync_completed = EXCLUDED.last_sync_completed,
                last_sync_status    = EXCLUDED.last_sync_status,
                records_synced      = EXCLUDED.records_synced,
                updated_at          = EXCLUDED.updated_at
        """
        self._db.execute(
            sql,
            (
                provider,
                source_account,
                api_name,
                tenant_id,
                watermark_value,
                records_synced,
            ),
        )
        logger.info(
            "Watermark updated: provider=%s account=%s api=%s -> %s (%d records)",
            provider,
            source_account,
            api_name,
            watermark_value,
            records_synced,
        )
