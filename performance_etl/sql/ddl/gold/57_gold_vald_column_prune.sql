-- ============================================================================
-- 57_gold_vald_column_prune.sql
-- Gold schema: drop family-specific zero-value columns confirmed live
-- ============================================================================

DO $$
DECLARE
    candidate RECORD;
    non_null_count BIGINT;
BEGIN
    FOR candidate IN
        SELECT *
        FROM (
            VALUES
                ('gold', 'vald_forceframe', 'rep_number'),
                ('gold', 'vald_nordics', 'rep_number'),
                ('gold', 'vald_speed', 'side')
        ) AS prune_targets(schema_name, table_name, column_name)
    LOOP
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = candidate.schema_name
              AND table_name = candidate.table_name
              AND column_name = candidate.column_name
        ) THEN
            EXECUTE format(
                'SELECT COUNT(*) FROM %I.%I WHERE %I IS NOT NULL',
                candidate.schema_name,
                candidate.table_name,
                candidate.column_name
            )
            INTO non_null_count;

            IF non_null_count <> 0 THEN
                RAISE EXCEPTION
                    'Cannot drop %.%.%: found % non-null rows',
                    candidate.schema_name,
                    candidate.table_name,
                    candidate.column_name,
                    non_null_count;
            END IF;
        END IF;
    END LOOP;
END $$;

ALTER TABLE gold.vald_forceframe DROP COLUMN IF EXISTS rep_number;
ALTER TABLE gold.vald_nordics DROP COLUMN IF EXISTS rep_number;
ALTER TABLE gold.vald_speed DROP COLUMN IF EXISTS side;
