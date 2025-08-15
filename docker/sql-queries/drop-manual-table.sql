-- Drop the manually created table to let the connector auto-create with proper schema
USE ROLE SENTINEL_DB_ADMIN;
USE DATABASE SENTINEL_DB;
USE SCHEMA EXPLOSION_TEST;

-- Drop the manually created table
DROP TABLE IF EXISTS ADR_FLATTENED_CLEAN;
DROP VIEW IF EXISTS FLATTENED_MESSAGES_CLEAN_ANALYSIS;

-- The connector will auto-create the table with proper Protobuf schema
-- when it processes the first message 