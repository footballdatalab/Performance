-- ============================================================================
-- verify_schema.sql
-- Validation queries to run after DDL deployment
-- ============================================================================

-- 1. Table count per schema
SELECT schemaname, COUNT(*) AS table_count
FROM pg_tables
WHERE schemaname IN ('raw', 'bronze', 'silver', 'gold')
GROUP BY schemaname
ORDER BY schemaname;

-- 1b. Pipeline metadata objects
SELECT
    to_regclass('raw.sync_watermark') AS sync_watermark,
    to_regclass('raw.ingestion_batch_log') AS ingestion_batch_log;

-- 2. Extension verification
SELECT extname, extversion
FROM pg_extension
WHERE extname IN ('uuid-ossp', 'btree_gist', 'pg_trgm');

-- 3. Active Catapult raw/bronze tables that must exist
SELECT table_name, EXISTS (
    SELECT 1
    FROM information_schema.tables t
    WHERE t.table_schema || '.' || t.table_name = required.table_name
) AS exists_flag
FROM (
    VALUES
        ('raw.catapult_teams'),
        ('raw.catapult_athletes'),
        ('raw.catapult_positions'),
        ('raw.catapult_parameters'),
        ('raw.catapult_venues'),
        ('raw.catapult_tag_types'),
        ('raw.catapult_tags'),
        ('raw.catapult_entity_tags'),
        ('raw.catapult_activities'),
        ('raw.catapult_periods'),
        ('raw.catapult_annotations'),
        ('raw.catapult_stats'),
        ('raw.catapult_efforts'),
        ('raw.catapult_events'),
        ('raw.catapult_sensor_data'),
        ('bronze.catapult_teams'),
        ('bronze.catapult_positions'),
        ('bronze.catapult_parameters'),
        ('bronze.catapult_venues'),
        ('bronze.catapult_tag_types'),
        ('bronze.catapult_tags'),
        ('bronze.catapult_athletes'),
        ('bronze.catapult_activities'),
        ('bronze.catapult_periods'),
        ('bronze.catapult_annotations'),
        ('bronze.catapult_entity_tags'),
        ('bronze.catapult_stats'),
        ('bronze.catapult_efforts'),
        ('bronze.catapult_events'),
        ('bronze.catapult_sensor_data')
) AS required(table_name)
ORDER BY table_name;

-- 3b. Catapult partition coverage
WITH required(parent_table, current_partition) AS (
    VALUES
        (
            'bronze.catapult_stats',
            'bronze.catapult_stats_' || to_char(date_trunc('month', current_date), 'YYYY_MM')
        ),
        (
            'bronze.catapult_sensor_data',
            'bronze.catapult_sensor_data_' || to_char(date_trunc('month', current_date), 'YYYY_MM')
        )
)
SELECT
    parent_table,
    to_regclass(parent_table) AS parent_exists,
    (
        SELECT COUNT(*)
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        WHERE parent_ns.nspname || '.' || parent.relname = required.parent_table
    ) AS partition_count,
    to_regclass(current_partition) AS current_partition_exists
FROM required
ORDER BY parent_table;

-- 3c. Catapult athlete profile columns that must exist
SELECT
    required.column_name,
    EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = 'bronze'
          AND c.table_name = 'catapult_athletes'
          AND c.column_name = required.column_name
    ) AS exists_flag
FROM (
    VALUES
        ('current_team_id'),
        ('gender'),
        ('nickname'),
        ('height'),
        ('weight'),
        ('velocity_max'),
        ('acceleration_max'),
        ('heart_rate_max'),
        ('player_load_max'),
        ('max_player_load_per_minute'),
        ('image'),
        ('icon'),
        ('stroke_colour'),
        ('fill_colour'),
        ('trail_colour_start'),
        ('trail_colour_end'),
        ('is_synced'),
        ('is_deleted'),
        ('is_demo'),
        ('provider_created_at'),
        ('provider_modified_at')
) AS required(column_name)
ORDER BY column_name;

-- 3d. Catapult athlete lookup-duplicate columns that must not exist
SELECT
    forbidden.column_name,
    EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = 'bronze'
          AND c.table_name = 'catapult_athletes'
          AND c.column_name = forbidden.column_name
    ) AS exists_flag
FROM (
    VALUES
        ('position'),
        ('position_name'),
        ('tag_list'),
        ('tags'),
        ('date_of_birth_date')
) AS forbidden(column_name)
ORDER BY column_name;

-- 4. Foreign key constraints in silver/gold
SELECT
    conname AS constraint_name,
    conrelid::regclass AS table_name,
    confrelid::regclass AS references_table
FROM pg_constraint
WHERE contype = 'f'
  AND connamespace IN (
      (SELECT oid FROM pg_namespace WHERE nspname = 'silver'),
      (SELECT oid FROM pg_namespace WHERE nspname = 'gold')
  )
ORDER BY conrelid::regclass::text, conname;

-- 5. Check constraints
SELECT
    conname AS constraint_name,
    conrelid::regclass AS table_name,
    pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE contype = 'c'
  AND connamespace IN (
      (SELECT oid FROM pg_namespace WHERE nspname = 'silver')
  )
ORDER BY conrelid::regclass::text, conname;

-- 6. Exclusion constraints (microcycle overlap)
SELECT
    conname AS constraint_name,
    conrelid::regclass AS table_name,
    pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE contype = 'x'
ORDER BY conrelid::regclass::text;

-- 7. Index count per schema
SELECT schemaname, COUNT(*) AS index_count
FROM pg_indexes
WHERE schemaname IN ('raw', 'bronze', 'silver', 'gold')
GROUP BY schemaname
ORDER BY schemaname;

-- 8. Unique indexes (partial)
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname IN ('silver', 'gold')
  AND indexdef LIKE '%WHERE%'
ORDER BY schemaname, tablename;

-- 9. Active VALD tables that must exist
SELECT table_name, EXISTS (
    SELECT 1
    FROM information_schema.tables t
    WHERE t.table_schema || '.' || t.table_name = required.table_name
) AS exists_flag
FROM (
    VALUES
        ('raw.vald_profiles'),
        ('raw.vald_forcedecks_tests'),
        ('raw.vald_forcedecks_result_definitions'),
        ('raw.vald_forcedecks_trials'),
        ('raw.vald_forceframe_tests'),
        ('raw.vald_forceframe_test_metrics'),
        ('raw.vald_forceframe_force_traces'),
        ('raw.vald_nordbord_tests'),
        ('raw.vald_nordbord_ecc_exercises'),
        ('raw.vald_nordbord_ecc_repetitions'),
        ('raw.vald_nordbord_test_metrics'),
        ('raw.vald_smartspeed_test_summaries'),
        ('raw.vald_smartspeed_test_details'),
        ('raw.vald_dynamo_tests'),
        ('raw.vald_dynamo_test_details'),
        ('raw.vald_dynamo_traces'),
        ('bronze.vald_profiles'),
        ('bronze.vald_profile_categories'),
        ('bronze.vald_forcedecks_tests'),
        ('bronze.vald_forcedecks_result_definitions'),
        ('bronze.vald_forcedecks_trials'),
        ('bronze.vald_forcedecks_trial_results'),
        ('bronze.vald_forceframe_tests'),
        ('bronze.vald_forceframe_test_metrics'),
        ('bronze.vald_forceframe_force_traces'),
        ('bronze.vald_nordbord_tests'),
        ('bronze.vald_nordbord_ecc_exercises'),
        ('bronze.vald_nordbord_ecc_repetitions'),
        ('bronze.vald_nordbord_test_metrics'),
        ('bronze.vald_smartspeed_test_summaries'),
        ('bronze.vald_smartspeed_test_details'),
        ('bronze.vald_smartspeed_rep_results'),
        ('bronze.vald_dynamo_tests'),
        ('bronze.vald_dynamo_rep_summaries'),
        ('bronze.vald_dynamo_repetitions'),
        ('bronze.vald_dynamo_traces'),
        ('silver.vald_target_group_membership'),
        ('silver.vald_athlete_profile'),
        ('silver.vald_assessment_metric'),
        ('silver.data_quality_flag'),
        ('gold.vald_nordics'),
        ('gold.vald_forceframe'),
        ('gold.vald_forcedecks'),
        ('gold.vald_dynamo'),
        ('gold.vald_speed')
) AS required(table_name)
ORDER BY table_name;

-- 9b. VALD columns that must still exist
SELECT
    required.table_name,
    required.column_name,
    EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema || '.' || c.table_name = required.table_name
          AND c.column_name = required.column_name
    ) AS exists_flag
FROM (
    VALUES
        ('raw.vald_dynamo_tests', 'page_number'),
        ('raw.vald_smartspeed_test_summaries', 'page_number'),
        ('bronze.vald_forcedecks_tests', 'notes'),
        ('bronze.vald_forcedecks_tests', 'parameter'),
        ('bronze.vald_profiles', 'external_id'),
        ('gold.vald_forceframe', 'side'),
        ('gold.vald_nordics', 'side'),
        ('gold.vald_speed', 'rep_number')
) AS required(table_name, column_name)
ORDER BY table_name, column_name;

-- 9c. VALD columns that must have been removed
SELECT
    forbidden.table_name,
    forbidden.column_name,
    EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema || '.' || c.table_name = forbidden.table_name
          AND c.column_name = forbidden.column_name
    ) AS exists_flag
FROM (
    VALUES
        ('raw.vald_profiles', 'page_number'),
        ('raw.vald_forcedecks_tests', 'page_number'),
        ('raw.vald_forcedecks_trials', 'page_number'),
        ('raw.vald_forcedecks_result_definitions', 'page_number'),
        ('raw.vald_forceframe_tests', 'page_number'),
        ('raw.vald_forceframe_test_metrics', 'page_number'),
        ('raw.vald_forceframe_force_traces', 'page_number'),
        ('raw.vald_nordbord_tests', 'page_number'),
        ('raw.vald_nordbord_test_metrics', 'page_number'),
        ('raw.vald_nordbord_ecc_exercises', 'page_number'),
        ('raw.vald_nordbord_ecc_repetitions', 'page_number'),
        ('raw.vald_smartspeed_test_details', 'page_number'),
        ('raw.vald_dynamo_test_details', 'page_number'),
        ('raw.vald_dynamo_traces', 'page_number'),
        ('bronze.vald_dynamo_repetitions', 'force_newtons'),
        ('bronze.vald_dynamo_tests', 'modified_date_utc'),
        ('bronze.vald_profiles', 'being_merged_with'),
        ('bronze.vald_profiles', 'date_of_birth'),
        ('bronze.vald_profiles', 'email'),
        ('bronze.vald_profiles', 'merge_expiry'),
        ('bronze.vald_profiles', 'sex'),
        ('bronze.vald_profiles', 'sync_id'),
        ('bronze.vald_smartspeed_test_summaries', 'additional_options'),
        ('bronze.vald_smartspeed_test_summaries', 'running_summary'),
        ('bronze.vald_smartspeed_test_summaries', 'jumping_summary'),
        ('silver.vald_athlete_profile', 'provider_birth_date'),
        ('silver.vald_athlete_profile', 'provider_email'),
        ('silver.vald_athlete_profile', 'provider_external_id'),
        ('silver.vald_athlete_profile', 'provider_sex'),
        ('silver.vald_athlete_profile', 'provider_sync_id'),
        ('silver.vald_athlete_profile', 'raw_payload_hash'),
        ('gold.vald_forceframe', 'rep_number'),
        ('gold.vald_nordics', 'rep_number'),
        ('gold.vald_speed', 'side')
) AS forbidden(table_name, column_name)
ORDER BY table_name, column_name;

-- 10. Unexpected silver/gold tables that must not exist
SELECT table_schema, table_name
FROM information_schema.tables
WHERE (
        table_schema = 'silver'
        AND table_name NOT IN (
            'vald_target_group_membership',
            'vald_athlete_profile',
            'vald_assessment_metric',
            'data_quality_flag'
        )
      )
   OR (
        table_schema = 'gold'
        AND table_name NOT IN (
            'vald_nordics',
            'vald_forceframe',
            'vald_forcedecks',
            'vald_dynamo',
            'vald_speed',
            'focus_upload_sessions',
            'focus_uploaded_data'
        )
      )
ORDER BY table_schema, table_name;

-- 11. Obsolete VALD tables that must not exist
SELECT table_name, to_regclass(table_name) AS existing_object
FROM (
    VALUES
        ('raw.pipeline_stage_cursor'),
        ('raw.vald_tenants'),
        ('raw.vald_categories'),
        ('raw.vald_groups'),
        ('raw.vald_forceframe_training_exercises'),
        ('raw.vald_forceframe_training_repetitions'),
        ('raw.vald_nordbord_force_traces'),
        ('raw.vald_nordbord_iso_sessions'),
        ('raw.vald_nordbord_iso_exercises'),
        ('raw.vald_nordbord_iso_repetitions'),
        ('raw.vald_humantrak_tests'),
        ('raw.vald_humantrak_repetitions'),
        ('bronze.vald_tenants'),
        ('bronze.vald_categories'),
        ('bronze.vald_groups'),
        ('bronze.vald_forceframe_training_exercises'),
        ('bronze.vald_forceframe_training_repetitions'),
        ('bronze.vald_nordbord_force_traces'),
        ('bronze.vald_nordbord_iso_sessions'),
        ('bronze.vald_nordbord_iso_exercises'),
        ('bronze.vald_nordbord_iso_repetitions'),
        ('bronze.vald_humantrak_tests'),
        ('bronze.vald_humantrak_repetitions'),
        ('bronze.vald_humantrak_metric_groups'),
        ('bronze.vald_humantrak_metric_summaries'),
        ('bronze.vald_humantrak_metric_asymmetries'),
        ('silver.vald_metric_quality_baseline'),
        ('gold.vald_jumps'),
        ('gold.vald_forcedecks_other')
) AS obsolete(table_name)
ORDER BY table_name;
