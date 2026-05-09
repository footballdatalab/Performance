-- ============================================================================
-- 12_raw_ingestion_metadata.sql
-- Ingestion tracking: sync watermarks and batch log
-- ============================================================================

-- Sync watermark: tracks incremental sync state per provider/endpoint/account
CREATE TABLE IF NOT EXISTS raw.sync_watermark (
    watermark_id        SERIAL PRIMARY KEY,
    provider            VARCHAR(20)     NOT NULL,
    source_account      VARCHAR(50)     NOT NULL,
    api_name            VARCHAR(100)    NOT NULL,
    tenant_id           UUID,
    last_watermark      VARCHAR(100),
    last_sync_started   TIMESTAMPTZ,
    last_sync_completed TIMESTAMPTZ,
    last_sync_status    VARCHAR(20),
    records_synced      INTEGER         DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Functional unique index to handle nullable tenant_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_sync_watermark
    ON raw.sync_watermark (provider, source_account, api_name, COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::UUID));

CREATE INDEX IF NOT EXISTS idx_watermark_provider_account ON raw.sync_watermark (provider, source_account);

-- Ingestion batch log: one row per ingestion execution
CREATE TABLE IF NOT EXISTS raw.ingestion_batch_log (
    batch_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider            VARCHAR(20)     NOT NULL,
    source_account      VARCHAR(50)     NOT NULL,
    api_name            VARCHAR(100)    NOT NULL,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running',
    records_extracted   INTEGER         DEFAULT 0,
    records_loaded      INTEGER         DEFAULT 0,
    error_message       TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_batch_log_provider_status ON raw.ingestion_batch_log (provider, source_account, status);
CREATE INDEX IF NOT EXISTS idx_batch_log_started ON raw.ingestion_batch_log (started_at DESC);
