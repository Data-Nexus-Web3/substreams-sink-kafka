-- Create a new schema for explosion testing
USE DATABASE SENTINEL_DB;
CREATE SCHEMA IF NOT EXISTS EXPLOSION_TEST;

-- Create table for exploded transaction records (Schema Registry format)
CREATE OR REPLACE TABLE EXPLOSION_TEST.ADR_TRANSACTIONS_SCHEMA_REGISTRY (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT,
    INGESTED_AT TIMESTAMP_NTZ
) ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Create a view for easier querying of exploded transactions
CREATE OR REPLACE VIEW EXPLOSION_TEST.EXPLODED_TRANSACTIONS_ANALYSIS AS
SELECT 
    RECORD_CONTENT:source_field::STRING as source_field,
    RECORD_CONTENT:item_index::INTEGER as item_index,
    RECORD_CONTENT:cursor::STRING as cursor,
    RECORD_CONTENT:data as transaction_data,
    RECORD_CONTENT:parent_context as parent_context,
    RECORD_CONTENT:metadata as metadata,
    INGESTED_AT,
    CURRENT_TIMESTAMP() as analysis_timestamp
FROM EXPLOSION_TEST.ADR_TRANSACTIONS_SCHEMA_REGISTRY;

-- Show the schema structure
DESCRIBE SCHEMA EXPLOSION_TEST;
