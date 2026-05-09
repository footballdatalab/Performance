-- ============================================================================
-- 51_silver_data_quality_superseded.sql
-- Phase 8.7.D (2026-05-09): extend silver.data_quality_flag.resolution_status
-- CHECK constraint to accept the 'superseded' value.
--
-- Rationale: _sync_overlap_quality_flags previously DELETEd open
-- 'duplicate_suspect' flags before re-asserting them, which destroyed the
-- audit trail (flag_id was reset every silver run, reviewers' notes were
-- lost). Locked decision #7 (no DELETE in ETL) also forbids this.
--
-- The new pattern (Phase 8.7.D refactor):
--   1. UPDATE existing open flags to resolution_status='superseded' so the
--      audit trail is preserved AND the row no longer appears in the
--      "open" reader path (idx_quality_flag_status partial index already
--      filters on resolution_status='open').
--   2. INSERT new flags via QualityEngine.persist_flags(...). The unique
--      index ``uq_quality_flag_record_metric`` on
--      (source_table, record_id, metric_name, flag_type) is what
--      previously made the DELETE necessary — but persist_flags uses
--      ON CONFLICT DO UPDATE so an existing 'superseded' row gets
--      reactivated to 'open' if the same (record, metric, type) tuple is
--      re-asserted in this run.
--
-- Audit trail: the 'superseded' rows stay queryable. Reviewers can now
-- ``SELECT … WHERE resolution_status='superseded'`` to see all the
-- historical states of a given flag. The "open" partial index keeps the
-- live-flag read path fast.
--
-- Idempotency: the migration drops + recreates the constraint, so
-- re-running it on a DB that already has 'superseded' is a no-op.
-- ============================================================================

ALTER TABLE silver.data_quality_flag
    DROP CONSTRAINT IF EXISTS chk_flag_resolution;

ALTER TABLE silver.data_quality_flag
    ADD CONSTRAINT chk_flag_resolution CHECK (resolution_status IN (
        'open',
        'reviewed',
        'valid',
        'invalid',
        'suppressed',
        'superseded'
    ));
