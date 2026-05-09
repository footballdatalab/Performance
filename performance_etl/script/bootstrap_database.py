"""
CLI wrapper for database bootstrap.
"""

from __future__ import annotations

from ingestion.bootstrap import main_bootstrap_database

if __name__ == "__main__":
    raise SystemExit(main_bootstrap_database())
