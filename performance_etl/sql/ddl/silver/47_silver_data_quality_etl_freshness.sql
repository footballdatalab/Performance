-- ============================================================================
-- 47_silver_data_quality_etl_freshness.sql
-- Phase 8.5: extend silver.data_quality_flag.flag_type CHECK constraint to
-- accept the 'etl_freshness' value emitted by the freshness_monitor DAG.
--
-- This migration is idempotent — it drops the constraint if it exists and
-- recreates it with the extended value list.
-- ============================================================================

ALTER TABLE silver.data_quality_flag
    DROP CONSTRAINT IF EXISTS chk_flag_type;

ALTER TABLE silver.data_quality_flag
    ADD CONSTRAINT chk_flag_type CHECK (flag_type IN (
        'outlier_zscore',
        'outlier_iqr',
        'outlier_modified_zscore',
        'range_violation',
        'asymmetry_extreme',
        'null_required_field',
        'negative_value',
        'duplicate_suspect',
        'stale_data',
        'etl_freshness'
    ));