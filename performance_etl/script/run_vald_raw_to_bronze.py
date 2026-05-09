"""
CLI wrapper for the VALD raw->bronze replay stage.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_run_raw_to_bronze


if __name__ == "__main__":
    raise SystemExit(main_run_raw_to_bronze())
