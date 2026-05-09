-- ============================================================================
-- 47_silver_vald_quality_cleanup.sql
-- Silver schema: remove obsolete VALD metric-quality artifacts
-- ============================================================================

DROP INDEX IF EXISTS silver.idx_quality_flag_vald_metric_blocking;
DROP TABLE IF EXISTS silver.vald_metric_quality_baseline;
DROP TABLE IF EXISTS silver.data_quality_baseline;
DROP TABLE IF EXISTS silver.data_quality_threshold;

DELETE FROM silver.data_quality_flag
WHERE source_table = 'silver.vald_assessment_metric'
   OR source_table LIKE 'bronze.vald_%';
