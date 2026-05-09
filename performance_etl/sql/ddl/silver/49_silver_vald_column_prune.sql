-- ============================================================================
-- 49_silver_vald_column_prune.sql
-- Silver schema: drop zero-value VALD profile columns confirmed live
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
                ('silver', 'vald_athlete_profile', 'provider_birth_date'),
                ('silver', 'vald_athlete_profile', 'provider_email'),
                ('silver', 'vald_athlete_profile', 'provider_external_id'),
                ('silver', 'vald_athlete_profile', 'provider_sex'),
                ('silver', 'vald_athlete_profile', 'provider_sync_id'),
                ('silver', 'vald_athlete_profile', 'raw_payload_hash')
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

ALTER TABLE silver.vald_athlete_profile DROP COLUMN IF EXISTS provider_birth_date;
ALTER TABLE silver.vald_athlete_profile DROP COLUMN IF EXISTS provider_email;
ALTER TABLE silver.vald_athlete_profile DROP COLUMN IF EXISTS provider_external_id;
ALTER TABLE silver.vald_athlete_profile DROP COLUMN IF EXISTS provider_sex;
ALTER TABLE silver.vald_athlete_profile DROP COLUMN IF EXISTS provider_sync_id;
ALTER TABLE silver.vald_athlete_profile DROP COLUMN IF EXISTS raw_payload_hash;
