"""
ingestion.common -- Shared infrastructure for the performance data ingestion pipeline.

Modules:
    config       - Environment and provider YAML configuration loading
    logging      - Structured logging setup
    http_client  - Base HTTP client with retry and rate limiting
    db           - PostgreSQL connection pool and query helpers
    watermark    - Sync watermark read/write against raw.sync_watermark
    batch        - Ingestion batch lifecycle (raw.ingestion_batch_log)
    utils        - Data parsing, validation, and hashing utilities
"""
