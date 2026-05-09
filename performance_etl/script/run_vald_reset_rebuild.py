"""
CLI wrapper for a destructive VALD reset and rebuild.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_run_reset_rebuild


if __name__ == "__main__":
    raise SystemExit(main_run_reset_rebuild())
