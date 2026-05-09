-- ============================================================================
-- 46_bronze_catapult_replay_indexes.sql
-- Bronze Catapult indexes used to derive per-account raw replay watermarks.
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_catapult_teams_source_raw_id
    ON bronze.catapult_teams (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_athletes_source_raw_id
    ON bronze.catapult_athletes (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_positions_source_raw_id
    ON bronze.catapult_positions (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_parameters_source_raw_id
    ON bronze.catapult_parameters (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_venues_source_raw_id
    ON bronze.catapult_venues (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_tag_types_source_raw_id
    ON bronze.catapult_tag_types (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_tags_source_raw_id
    ON bronze.catapult_tags (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_entity_tags_source_raw_id
    ON bronze.catapult_entity_tags (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_activities_source_raw_id
    ON bronze.catapult_activities (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_periods_source_raw_id
    ON bronze.catapult_periods (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_annotations_source_raw_id
    ON bronze.catapult_annotations (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_stats_source_raw_id
    ON bronze.catapult_stats (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_efforts_source_raw_id
    ON bronze.catapult_efforts (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_events_source_raw_id
    ON bronze.catapult_events (source_account, raw_id DESC);
CREATE INDEX IF NOT EXISTS ix_catapult_sensor_data_source_raw_id
    ON bronze.catapult_sensor_data (source_account, raw_id DESC);
