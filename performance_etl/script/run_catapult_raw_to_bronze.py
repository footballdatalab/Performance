"""
CLI wrapper for the Catapult raw->bronze replay stage.
"""

from __future__ import annotations

from ingestion.catapult.pipeline import main_run_raw_to_bronze


if __name__ == "__main__":
    raise SystemExit(main_run_raw_to_bronze())
