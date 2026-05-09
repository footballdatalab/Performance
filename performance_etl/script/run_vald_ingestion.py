"""
CLI wrapper for the end-to-end VALD pipeline.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_run_ingestion


if __name__ == "__main__":
    raise SystemExit(main_run_ingestion())
