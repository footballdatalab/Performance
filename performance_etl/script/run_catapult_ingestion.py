"""
CLI wrapper for the end-to-end Catapult raw and bronze pipeline.
"""

from __future__ import annotations

from ingestion.catapult.pipeline import main_run_ingestion


if __name__ == "__main__":
    raise SystemExit(main_run_ingestion())
