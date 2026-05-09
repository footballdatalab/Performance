-- ============================================================================
-- 01_extensions.sql
-- Required PostgreSQL extensions
-- ============================================================================

-- UUID generation for surrogate keys
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enables GiST indexes on scalar types (needed for microcycle overlap exclusion constraint)
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- Trigram similarity for fuzzy name matching in identity resolution
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
