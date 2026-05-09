-- ============================================================================
-- 43_silver_vald_reference_metric_coverage.sql
-- Silver schema: VALD reference metric coverage audit for gold promotion
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.vald_reference_metric_coverage (
    coverage_id               BIGSERIAL PRIMARY KEY,
    source_table              VARCHAR(100)    NOT NULL,
    source_module             VARCHAR(50)     NOT NULL,
    assessment_family         VARCHAR(50)     NOT NULL,
    test_name                 VARCHAR(255),
    reference_metric_name     VARCHAR(255),
    coverage_status           VARCHAR(20)     NOT NULL,
    source_test_count         BIGINT          NOT NULL DEFAULT 0,
    latest_test_date          TIMESTAMPTZ,
    created_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT chk_vald_reference_metric_coverage_status
        CHECK (coverage_status IN ('covered', 'unmapped'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_vald_reference_metric_coverage_lookup
    ON silver.vald_reference_metric_coverage (
        source_module,
        assessment_family,
        COALESCE(test_name, '__NULL__')
    );

CREATE INDEX IF NOT EXISTS idx_vald_reference_metric_coverage_status
    ON silver.vald_reference_metric_coverage (coverage_status, source_module, assessment_family);

CREATE INDEX IF NOT EXISTS idx_vald_reference_metric_coverage_latest_test_date
    ON silver.vald_reference_metric_coverage (latest_test_date DESC);
