"""
Ingestion batch lifecycle management.

Each ingestion run creates a batch row in ``raw.ingestion_batch_log``.  The
batch transitions through **running -> completed** or **running -> failed**.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from ingestion.common.logging import get_logger

if TYPE_CHECKING:
    from ingestion.common.db import DatabaseManager

logger = get_logger(__name__)


class BatchManager:
    """Manage the lifecycle of rows in ``raw.ingestion_batch_log``.

    Args:
        db: An initialised :class:`~ingestion.common.db.DatabaseManager`.
    """

    def __init__(self, db: "DatabaseManager") -> None:
        self._db = db

    def start_batch(
        self,
        provider: str,
        source_account: str,
        api_name: str,
    ) -> str:
        """Create a new batch record with status ``running``.

        Args:
            provider: Provider identifier (e.g. ``"catapult"``).
            source_account: Account name (e.g. ``"CATAPULT_A"``).
            api_name: API endpoint name (e.g. ``"activities"``).

        Returns:
            The generated ``batch_id`` as a string UUID.
        """
        batch_id = str(uuid.uuid4())
        sql = """
            INSERT INTO raw.ingestion_batch_log
                (batch_id, provider, source_account, api_name, status, started_at)
            VALUES
                (%s, %s, %s, %s, 'running', now())
        """
        self._db.execute(sql, (batch_id, provider, source_account, api_name))
        logger.info(
            "Batch started: id=%s provider=%s account=%s api=%s",
            batch_id,
            provider,
            source_account,
            api_name,
        )
        return batch_id

    def complete_batch(
        self,
        batch_id: str,
        records_extracted: int,
        records_loaded: int,
    ) -> None:
        """Mark a batch as successfully completed.

        Args:
            batch_id: UUID of the batch to update.
            records_extracted: Total records fetched from the API.
            records_loaded: Total records persisted to the database.
        """
        sql = """
            UPDATE raw.ingestion_batch_log
            SET status            = 'completed',
                completed_at      = now(),
                records_extracted  = %s,
                records_loaded     = %s
            WHERE batch_id = %s
        """
        self._db.execute(sql, (records_extracted, records_loaded, batch_id))
        logger.info(
            "Batch completed: id=%s extracted=%d loaded=%d",
            batch_id,
            records_extracted,
            records_loaded,
        )

    def fail_batch(self, batch_id: str, error_message: str) -> None:
        """Mark a batch as failed and store the error message.

        Args:
            batch_id: UUID of the batch to update.
            error_message: Human-readable error description (may be truncated
                by the caller for very long tracebacks).
        """
        sql = """
            UPDATE raw.ingestion_batch_log
            SET status        = 'failed',
                completed_at  = now(),
                error_message = %s
            WHERE batch_id = %s
        """
        self._db.execute(sql, (error_message, batch_id))
        logger.error(
            "Batch failed: id=%s error=%s",
            batch_id,
            error_message[:200],
        )
