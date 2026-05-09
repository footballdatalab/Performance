"""
CLI wrapper for the VALD bronze->silver stage.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_run_bronze_to_silver


if __name__ == "__main__":
    raise SystemExit(main_run_bronze_to_silver())
