-- ============================================================================
-- 53_silver_data_quality_team_group.sql
-- Phase 1 (2026-05-09): VALD IQR outlier detection.
--
-- This migration recreates ``silver.data_quality_baseline`` (which was
-- dropped in 47_silver_vald_quality_cleanup.sql) and adds the
-- ``team_group_id`` dimension to both the baseline and the flag table.
--
-- Why team_group_id?
--   The user's audit requirement is "outliers per team per test", not
--   "outliers across the whole organization". A 70cm jump-height that
--   would be normal for Equipa A's senior squad is an outlier for the
--   U17 group, and vice versa. Grouping baselines by
--   (tenant_id, team_group_id, source_table, test_type, metric_name)
--   makes that comparison correct by construction.
--
-- IQR-baseline window = ALL-TIME (locked decision #5). The baseline
-- query has no date filter; ``MIN_SAMPLE_SIZE`` (in QualityEngine)
-- gates against thin samples.
--
-- Idempotent: every CREATE / ALTER uses IF NOT EXISTS / IF EXISTS so
-- re-applying via bootstrap_database is a no-op.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- silver.data_quality_baseline
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.data_quality_baseline (
    baseline_id        BIGSERIAL PRIMARY KEY,
    source_table       VARCHAR(100)  NOT NULL,
    metric_name        VARCHAR(100)  NOT NULL,
    test_type          VARCHAR(100),
    tenant_id          UUID,
    team_group_id      UUID,
    sample_count       BIGINT        NOT NULL,
    mean_value         NUMERIC,
    std_value          NUMERIC,
    median_value       NUMERIC,
    mad_value          NUMERIC,
    p25_value          NUMERIC,
    p75_value          NUMERIC,
    min_value          NUMERIC,
    max_value          NUMERIC,
    last_computed_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ON CONFLICT key for the upsert path. ``COALESCE(...)`` fenceposts
-- bridge the optional dimensions: NULLs become a fixed sentinel so
-- (NULL, NULL) keys are still unique.
CREATE UNIQUE INDEX IF NOT EXISTS uq_data_quality_baseline
    ON silver.data_quality_baseline (
        source_table,
        metric_name,
        (COALESCE(test_type, '__all__')),
        (COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::UUID)),
        (COALESCE(team_group_id, '00000000-0000-0000-0000-000000000000'::UUID))
    );

CREATE INDEX IF NOT EXISTS idx_data_quality_baseline_team_group
    ON silver.data_quality_baseline (team_group_id, source_table, metric_name)
    WHERE team_group_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_data_quality_baseline_recent
    ON silver.data_quality_baseline (last_computed_at DESC);


-- ----------------------------------------------------------------------------
-- silver.data_quality_flag — add team_group_id + provider lookup index
-- ----------------------------------------------------------------------------

ALTER TABLE silver.data_quality_flag
    ADD COLUMN IF NOT EXISTS team_group_id UUID;

-- Lookup index for the new ``GET /v1/quality/flags`` endpoint:
--   ?team_group_id=&severity=&open_only=
CREATE INDEX IF NOT EXISTS idx_data_quality_flag_team_group_lookup
    ON silver.data_quality_flag (team_group_id, severity, resolution_status)
    WHERE team_group_id IS NOT NULL;


-- ----------------------------------------------------------------------------
-- Phase 1 audit-run tracking — supports incremental audits
-- (``run_vald_quality_audit(incremental=True)``)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.data_quality_audit_run (
    run_id              UUID          PRIMARY KEY,
    pipeline            VARCHAR(50)   NOT NULL,
    family              VARCHAR(50),
    incremental         BOOLEAN       NOT NULL,
    started_at          TIMESTAMPTZ   NOT NULL,
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(20)   NOT NULL DEFAULT 'running',
    records_audited     BIGINT,
    flags_written       BIGINT,
    error_message       TEXT,
    extra               JSONB,
    CONSTRAINT chk_audit_run_status
        CHECK (status IN ('running', 'success', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_audit_run_pipeline_started
    ON silver.data_quality_audit_run (pipeline, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_run_family_started
    ON silver.data_quality_audit_run (family, started_at DESC)
    WHERE family IS NOT NULL;
