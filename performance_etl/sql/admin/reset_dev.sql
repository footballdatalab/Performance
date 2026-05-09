-- ============================================================================
-- reset_dev.sql
-- WARNING: Drops and recreates ALL schemas. DEV/TEST ONLY.
-- DO NOT run in production.
-- ============================================================================

-- Drop all schemas and their contents
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS raw CASCADE;

-- Recreate schemas
CREATE SCHEMA raw;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
