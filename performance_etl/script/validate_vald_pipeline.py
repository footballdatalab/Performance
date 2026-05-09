"""
CLI wrapper for VALD pipeline validation.
"""

from __future__ import annotations

from ingestion.vald.pipeline import main_validate_pipeline


if __name__ == "__main__":
    raise SystemExit(main_validate_pipeline())
