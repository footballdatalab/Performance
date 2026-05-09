"""
CLI wrapper for the Catapult position synchronization workflow.
"""

from __future__ import annotations

from ingestion.catapult.position_sync import main_run_position_sync


if __name__ == "__main__":
    raise SystemExit(main_run_position_sync())
