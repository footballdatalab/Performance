"""
CLI wrapper for the VALD silver->gold stage.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_run_silver_to_gold


if __name__ == "__main__":
    raise SystemExit(main_run_silver_to_gold())
