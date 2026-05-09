-- ============================================================================
-- 46_silver_data_quality.sql
-- Silver schema: VALD quality flag log retained for overlap review workflows
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.data_quality_flag (
    flag_id                 BIGSERIAL PRIMARY KEY,
    source_table            VARCHAR(100)    NOT NULL,
    record_id               VARCHAR(255)    NOT NULL,
    profile_id              UUID,
    tenant_id               UUID,
    test_date               TIMESTAMPTZ,
    metric_name             VARCHAR(100)    NOT NULL,
    metric_value            NUMERIC,
    flag_type               VARCHAR(50)     NOT NULL,
    severity                VARCHAR(20)     NOT NULL DEFAULT 'warning',
    details                 JSONB,
    resolution_status       VARCHAR(20)     NOT NULL DEFAULT 'open',
    reviewed_by             VARCHAR(255),
    reviewed_at             TIMESTAMPTZ,
    review_notes            TEXT,
    batch_id                UUID,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT chk_flag_type CHECK (flag_type IN (
        'outlier_zscore',
        'outlier_iqr',
        'outlier_modified_zscore',
        'range_violation',
        'asymmetry_extreme',
        'null_required_field',
        'negative_value',
        'duplicate_suspect',
        'stale_data'
    )),
    CONSTRAINT chk_flag_severity CHECK (severity IN ('info', 'warning', 'critical')),
    CONSTRAINT chk_flag_resolution CHECK (resolution_status IN ('open', 'reviewed', 'valid', 'invalid', 'suppressed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_quality_flag_record_metric
    ON silver.data_quality_flag (source_table, record_id, metric_name, flag_type);

CREATE INDEX IF NOT EXISTS idx_quality_flag_source ON silver.data_quality_flag (source_table, record_id);
CREATE INDEX IF NOT EXISTS idx_quality_flag_profile ON silver.data_quality_flag (profile_id);
CREATE INDEX IF NOT EXISTS idx_quality_flag_status ON silver.data_quality_flag (resolution_status) WHERE resolution_status = 'open';
CREATE INDEX IF NOT EXISTS idx_quality_flag_severity ON silver.data_quality_flag (severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_quality_flag_batch ON silver.data_quality_flag (batch_id);
CREATE INDEX IF NOT EXISTS idx_quality_flag_metric ON silver.data_quality_flag (source_table, metric_name);
