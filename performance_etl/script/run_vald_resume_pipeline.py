"""
CLI wrapper for resuming the VALD pipeline from a specific stage.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_run_resume_pipeline


if __name__ == "__main__":
    raise SystemExit(main_run_resume_pipeline())
