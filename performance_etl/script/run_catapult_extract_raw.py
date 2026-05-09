"""
CLI wrapper for the Catapult raw extraction stage.
"""

from __future__ import annotations

from ingestion.catapult.pipeline import main_run_extract_raw


if __name__ == "__main__":
    raise SystemExit(main_run_extract_raw())
