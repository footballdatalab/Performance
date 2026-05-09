-- ============================================================================
-- 16_raw_catapult_replay_indexes.sql
-- Raw Catapult indexes used by bounded raw->bronze replay.
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_raw_catapult_teams_source_raw_id
    ON raw.catapult_teams (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_teams_batch_source_raw_id
    ON raw.catapult_teams (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_athletes_source_raw_id
    ON raw.catapult_athletes (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_athletes_batch_source_raw_id
    ON raw.catapult_athletes (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_positions_source_raw_id
    ON raw.catapult_positions (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_positions_batch_source_raw_id
    ON raw.catapult_positions (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_parameters_source_raw_id
    ON raw.catapult_parameters (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_parameters_batch_source_raw_id
    ON raw.catapult_parameters (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_venues_source_raw_id
    ON raw.catapult_venues (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_venues_batch_source_raw_id
    ON raw.catapult_venues (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_tag_types_source_raw_id
    ON raw.catapult_tag_types (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_tag_types_batch_source_raw_id
    ON raw.catapult_tag_types (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_tags_source_raw_id
    ON raw.catapult_tags (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_tags_batch_source_raw_id
    ON raw.catapult_tags (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_entity_tags_source_raw_id
    ON raw.catapult_entity_tags (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_entity_tags_batch_source_raw_id
    ON raw.catapult_entity_tags (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_activities_source_raw_id
    ON raw.catapult_activities (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_activities_batch_source_raw_id
    ON raw.catapult_activities (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_periods_source_raw_id
    ON raw.catapult_periods (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_periods_batch_source_raw_id
    ON raw.catapult_periods (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_annotations_source_raw_id
    ON raw.catapult_annotations (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_annotations_batch_source_raw_id
    ON raw.catapult_annotations (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_stats_source_raw_id
    ON raw.catapult_stats (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_stats_batch_source_raw_id
    ON raw.catapult_stats (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_efforts_source_raw_id
    ON raw.catapult_efforts (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_efforts_batch_source_raw_id
    ON raw.catapult_efforts (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_events_source_raw_id
    ON raw.catapult_events (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_events_batch_source_raw_id
    ON raw.catapult_events (batch_id, source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_sensor_data_source_raw_id
    ON raw.catapult_sensor_data (source_account, raw_id);
CREATE INDEX IF NOT EXISTS idx_raw_catapult_sensor_data_batch_source_raw_id
    ON raw.catapult_sensor_data (batch_id, source_account, raw_id);
