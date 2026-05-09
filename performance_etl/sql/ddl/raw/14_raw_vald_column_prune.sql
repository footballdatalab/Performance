-- ============================================================================
-- 14_raw_vald_column_prune.sql
-- Raw schema: drop zero-value VALD columns confirmed against the live warehouse
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
                ('raw', 'vald_profiles', 'page_number'),
                ('raw', 'vald_forcedecks_tests', 'page_number'),
                ('raw', 'vald_forcedecks_trials', 'page_number'),
                ('raw', 'vald_forcedecks_result_definitions', 'page_number'),
                ('raw', 'vald_forceframe_tests', 'page_number'),
                ('raw', 'vald_forceframe_test_metrics', 'page_number'),
                ('raw', 'vald_forceframe_force_traces', 'page_number'),
                ('raw', 'vald_nordbord_tests', 'page_number'),
                ('raw', 'vald_nordbord_test_metrics', 'page_number'),
                ('raw', 'vald_nordbord_ecc_exercises', 'page_number'),
                ('raw', 'vald_nordbord_ecc_repetitions', 'page_number'),
                ('raw', 'vald_smartspeed_test_details', 'page_number'),
                ('raw', 'vald_dynamo_test_details', 'page_number'),
                ('raw', 'vald_dynamo_traces', 'page_number')
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

ALTER TABLE raw.vald_profiles DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_forcedecks_tests DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_forcedecks_trials DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_forcedecks_result_definitions DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_forceframe_tests DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_forceframe_test_metrics DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_forceframe_force_traces DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_nordbord_tests DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_nordbord_test_metrics DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_nordbord_ecc_exercises DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_nordbord_ecc_repetitions DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_smartspeed_test_details DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_dynamo_test_details DROP COLUMN IF EXISTS page_number;
ALTER TABLE raw.vald_dynamo_traces DROP COLUMN IF EXISTS page_number;
