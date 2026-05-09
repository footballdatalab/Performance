-- ============================================================================
-- 58_gold_vald_natural_key_unique.sql
-- Phase 8.7.B.1 (2026-05-09): add natural-key UNIQUE constraints to the five
-- gold VALD assessment marts. Once in place, Phase 8.7.B can replace
-- DELETE-then-INSERT in `_publish_gold_family` (scoped paths) and
-- `_clear_gold_table_rows_cursor` with `INSERT … ON CONFLICT DO UPDATE`,
-- satisfying locked decision #7 for those sites.
--
-- Audit on 2026-05-09 (production warehouse):
--   gold.vald_nordics      — 0 dup groups, clean
--   gold.vald_forceframe   — 0 dup groups, clean
--   gold.vald_forcedecks   — 20 dup groups, 380 extra rows ⚠ DEDUP REQUIRED FIRST
--   gold.vald_dynamo       — 0 dup groups, clean
--   gold.vald_speed        — 0 dup groups, clean
--
-- Strategy for `gold.vald_forcedecks`: keep the row with the **largest
-- metric_id** per natural-key group (i.e. the most recently inserted),
-- delete the rest. This matches the loader's intent — the most recent
-- INSERT for any given (test_id, metric_name, side, rep_number) is the
-- canonical value. The 380 dropped rows are 0.1% of the 380,715 total
-- and exist because earlier non-atomic rebuilds accumulated duplicates
-- (the very bug 8.7.A + 8.7.B is fixing).
--
-- The DEDUP step is a one-off DDL-stage operation (not part of the live ETL
-- pipeline, which is read-only / upsert-only per locked decision #7). It is
-- intentionally located in the DDL tree, not in `ingestion/`.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- gold.vald_forcedecks — DEDUP first, then UNIQUE
-- ----------------------------------------------------------------------------
DO $$
DECLARE
    deleted_count INTEGER;
BEGIN
    WITH dups AS (
        SELECT test_id, metric_name, side, rep_number, MAX(metric_id) AS keep_id
        FROM gold.vald_forcedecks
        GROUP BY test_id, metric_name, side, rep_number
        HAVING COUNT(*) > 1
    )
    DELETE FROM gold.vald_forcedecks g
    USING dups d
    WHERE g.test_id      = d.test_id
      AND g.metric_name  = d.metric_name
      AND g.side         IS NOT DISTINCT FROM d.side
      AND g.rep_number   IS NOT DISTINCT FROM d.rep_number
      AND g.metric_id    < d.keep_id;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'gold.vald_forcedecks dedup: removed % duplicate rows', deleted_count;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_vald_forcedecks_nk
    ON gold.vald_forcedecks
    (test_id, metric_name, side, rep_number)
    NULLS NOT DISTINCT;

-- ----------------------------------------------------------------------------
-- gold.vald_nordics — clean; UNIQUE only.
-- Natural key: (test_id, metric_name, side). No `rep_number` on this mart.
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_vald_nordics_nk
    ON gold.vald_nordics
    (test_id, metric_name, side)
    NULLS NOT DISTINCT;

-- ----------------------------------------------------------------------------
-- gold.vald_forceframe — clean; UNIQUE only.
-- Natural key: (test_id, metric_name, side). No `rep_number` on this mart.
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_vald_forceframe_nk
    ON gold.vald_forceframe
    (test_id, metric_name, side)
    NULLS NOT DISTINCT;

-- ----------------------------------------------------------------------------
-- gold.vald_dynamo — clean; UNIQUE only.
-- Natural key: (test_id, metric_name, side, rep_number).
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_vald_dynamo_nk
    ON gold.vald_dynamo
    (test_id, metric_name, side, rep_number)
    NULLS NOT DISTINCT;

-- ----------------------------------------------------------------------------
-- gold.vald_speed — clean; UNIQUE only.
-- Natural key: (test_id, metric_name, rep_number). No `side` on this mart.
-- ----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_vald_speed_nk
    ON gold.vald_speed
    (test_id, metric_name, rep_number)
    NULLS NOT DISTINCT;
