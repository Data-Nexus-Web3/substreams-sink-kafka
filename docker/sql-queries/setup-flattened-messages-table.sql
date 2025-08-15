-- Setup Snowflake table for flattened ADR messages
-- Each row will be an individual FlattenedMessage (activity or operation)

USE ROLE SENTINEL_DB_ADMIN;
USE DATABASE SENTINEL_DB;
USE SCHEMA EXPLOSION_TEST;

-- Create table for individual flattened messages (Schema Registry format)
CREATE TABLE IF NOT EXISTS ADR_FLATTENED_MESSAGES (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT,
    INGESTED_AT TIMESTAMP_NTZ
) ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Create a view for easier querying of flattened messages
CREATE OR REPLACE VIEW FLATTENED_MESSAGES_ANALYSIS AS
SELECT 
    -- Block and transaction context
    RECORD_CONTENT:data:block::BIGINT as block_number,
    RECORD_CONTENT:data:txn_id::STRING as transaction_id,
    RECORD_CONTENT:data:txn_cursor::STRING as transaction_cursor,
    TO_TIMESTAMP(RECORD_CONTENT:data:txn_timestamp::BIGINT / 1000) as transaction_timestamp,
    RECORD_CONTENT:data:txn_successful::BOOLEAN as transaction_successful,
    
    -- Network context  
    RECORD_CONTENT:data:version::STRING as version,
    RECORD_CONTENT:data:network::STRING as network,
    
    -- Wallet/Token
    RECORD_CONTENT:data:wallet_token::STRING as wallet_token,
    
    -- Activity data (when representing an activity)
    RECORD_CONTENT:data:act_type::STRING as activity_type,
    RECORD_CONTENT:data:act_address::STRING as activity_address,
    RECORD_CONTENT:data:act_operations as activity_operations,
    
    -- Operation data (when representing an operation)
    RECORD_CONTENT:data:ops_id::STRING as operation_id,
    RECORD_CONTENT:data:ops_token::STRING as operation_token,
    RECORD_CONTENT:data:ops_address::STRING as operation_address,
    RECORD_CONTENT:data:ops_amount::STRING as operation_amount,
    RECORD_CONTENT:data:ops_type::STRING as operation_type,
    
    -- Metadata
    RECORD_CONTENT:data:has_all_operations::BOOLEAN as has_all_operations,
    TO_TIMESTAMP(RECORD_CONTENT:data:received_at::BIGINT / 1000) as received_at,
    TO_TIMESTAMP(RECORD_CONTENT:data:sent_at::BIGINT / 1000) as sent_at,
    RECORD_CONTENT:data:provider::STRING as provider,
    
    -- Explosion context
    RECORD_CONTENT:source_field::STRING as source_field,
    RECORD_CONTENT:item_index::INTEGER as item_index,
    RECORD_CONTENT:cursor::STRING as cursor,
    
    -- Ingestion
    INGESTED_AT,
    CURRENT_TIMESTAMP() as analysis_timestamp
FROM EXPLOSION_TEST.ADR_FLATTENED_MESSAGES;

-- Show the schema structure
DESCRIBE TABLE ADR_FLATTENED_MESSAGES;