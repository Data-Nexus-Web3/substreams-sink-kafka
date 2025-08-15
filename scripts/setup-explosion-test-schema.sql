-- Setup Snowflake schema for explosion feature testing
-- This will test both JSON and Schema Registry exploded messages

USE ROLE SENTINEL_DB_ADMIN;
USE DATABASE SENTINEL_DB;

-- Create dedicated schema for explosion testing
CREATE SCHEMA IF NOT EXISTS EXPLOSION_TEST;
USE SCHEMA EXPLOSION_TEST;

-- Drop existing tables if they exist (for clean testing)
DROP TABLE IF EXISTS ADR_EXPLODED_JSON;
DROP TABLE IF EXISTS ADR_EXPLODED_SCHEMA_REGISTRY;
DROP VIEW IF EXISTS EXPLODED_JSON_ANALYSIS;
DROP VIEW IF EXISTS EXPLODED_SCHEMA_ANALYSIS;

-- Create table for JSON exploded messages
-- This will receive individual transaction messages in JSON format
CREATE TABLE ADR_EXPLODED_JSON (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Create table for Schema Registry exploded messages  
-- This will receive individual transaction messages in protobuf format
CREATE TABLE ADR_EXPLODED_SCHEMA_REGISTRY (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Create analysis view for JSON exploded messages
CREATE VIEW EXPLODED_JSON_ANALYSIS AS
SELECT 
    RECORD_CONTENT:hash::STRING as transaction_hash,
    RECORD_CONTENT:from::STRING as from_address,
    RECORD_CONTENT:to::STRING as to_address,
    RECORD_CONTENT:value::STRING as value,
    RECORD_CONTENT:gas::INTEGER as gas,
    RECORD_CONTENT:gas_price::STRING as gas_price,
    RECORD_CONTENT:input::STRING as input_data,
    RECORD_CONTENT:block_number::INTEGER as block_number,
    RECORD_CONTENT:transaction_index::INTEGER as transaction_index,
    RECORD_METADATA,
    INGESTED_AT,
    CURRENT_TIMESTAMP() as analysis_timestamp
FROM EXPLOSION_TEST.ADR_EXPLODED_JSON;

-- Create analysis view for Schema Registry exploded messages
CREATE VIEW EXPLODED_SCHEMA_ANALYSIS AS
SELECT 
    RECORD_CONTENT:hash::STRING as transaction_hash,
    RECORD_CONTENT:from::STRING as from_address,
    RECORD_CONTENT:to::STRING as to_address,
    RECORD_CONTENT:value::STRING as value,
    RECORD_CONTENT:gas::INTEGER as gas,
    RECORD_CONTENT:gas_price::STRING as gas_price,
    RECORD_CONTENT:input::STRING as input_data,
    RECORD_CONTENT:block_number::INTEGER as block_number,
    RECORD_CONTENT:transaction_index::INTEGER as transaction_index,
    RECORD_METADATA,
    INGESTED_AT,
    CURRENT_TIMESTAMP() as analysis_timestamp
FROM EXPLOSION_TEST.ADR_EXPLODED_SCHEMA_REGISTRY;

-- Grant permissions for testing
GRANT USAGE ON SCHEMA EXPLOSION_TEST TO ROLE SENTINEL_LOADER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA EXPLOSION_TEST TO ROLE SENTINEL_LOADER;
GRANT SELECT ON ALL VIEWS IN SCHEMA EXPLOSION_TEST TO ROLE SENTINEL_LOADER;

-- Show the schema structure
DESCRIBE SCHEMA EXPLOSION_TEST;

-- Show table structures
DESCRIBE TABLE ADR_EXPLODED_JSON;
DESCRIBE TABLE ADR_EXPLODED_SCHEMA_REGISTRY;

SELECT 'Schema setup complete for explosion feature testing!' as status;