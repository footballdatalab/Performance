-- =============================================================================
-- File: 37_bronze_vald_column_prune.sql
-- Description: Drop zero-value bronze VALD columns confirmed against live data.
-- =============================================================================

DO $$
DECLARE
    candidate RECORD;
    non_null_count BIGINT;
BEGIN
    FOR candidate IN
        SELECT *
        FROM (
            VALUES
                ('bronze', 'vald_dynamo_repetitions', 'force_newtons'),
                ('bronze', 'vald_dynamo_tests', 'modified_date_utc'),
                ('bronze', 'vald_profiles', 'being_merged_with'),
                ('bronze', 'vald_profiles', 'date_of_birth'),
                ('bronze', 'vald_profiles', 'email'),
                ('bronze', 'vald_profiles', 'merge_expiry'),
                ('bronze', 'vald_profiles', 'sex'),
                ('bronze', 'vald_profiles', 'sync_id'),
                ('bronze', 'vald_smartspeed_test_summaries', 'additional_options'),
                ('bronze', 'vald_smartspeed_test_summaries', 'running_summary'),
                ('bronze', 'vald_smartspeed_test_summaries', 'jumping_summary')
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

DROP INDEX IF EXISTS bronze.ix_vald_dynamo_tests_tenant_profile_modified;

ALTER TABLE bronze.vald_dynamo_repetitions DROP COLUMN IF EXISTS force_newtons;
ALTER TABLE bronze.vald_dynamo_tests DROP COLUMN IF EXISTS modified_date_utc;
ALTER TABLE bronze.vald_profiles DROP COLUMN IF EXISTS being_merged_with;
ALTER TABLE bronze.vald_profiles DROP COLUMN IF EXISTS date_of_birth;
ALTER TABLE bronze.vald_profiles DROP COLUMN IF EXISTS email;
ALTER TABLE bronze.vald_profiles DROP COLUMN IF EXISTS merge_expiry;
ALTER TABLE bronze.vald_profiles DROP COLUMN IF EXISTS sex;
ALTER TABLE bronze.vald_profiles DROP COLUMN IF EXISTS sync_id;
ALTER TABLE bronze.vald_smartspeed_test_summaries DROP COLUMN IF EXISTS additional_options;
ALTER TABLE bronze.vald_smartspeed_test_summaries DROP COLUMN IF EXISTS running_summary;
ALTER TABLE bronze.vald_smartspeed_test_summaries DROP COLUMN IF EXISTS jumping_summary;

CREATE INDEX IF NOT EXISTS ix_vald_dynamo_tests_tenant_profile_test_date
    ON bronze.vald_dynamo_tests (
        tenant_id,
        profile_id,
        (COALESCE(start_time_utc, analysed_date_utc))
    );
