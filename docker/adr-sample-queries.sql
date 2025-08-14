-- Sample Snowflake Queries for ADR (Active Data Registry) Data
-- These queries show how to extract and analyze your ADR protobuf data

USE DATABASE SENTINEL_DB;
USE SCHEMA STREAMING_SCHEMA;

-- 1. Basic data inspection
SELECT COUNT(*) as total_messages FROM ADR_LISTENER_MESSAGES;

-- 2. View recent ADR messages with metadata
SELECT 
  RECORD_METADATA:topic::STRING as topic,
  RECORD_METADATA:partition::INT as partition,
  RECORD_METADATA:offset::INT as offset,
  RECORD_METADATA:CreateTime::TIMESTAMP as kafka_timestamp,
  RECORD_CONTENT:version::STRING as adr_version,
  RECORD_CONTENT:network::STRING as network,
  RECORD_CONTENT:prev_cursor::STRING as previous_cursor
FROM ADR_LISTENER_MESSAGES 
ORDER BY RECORD_METADATA:offset DESC 
LIMIT 10;

-- 3. Extract transaction data from ListenerMessage
SELECT 
  RECORD_CONTENT:version::STRING as adr_version,
  RECORD_CONTENT:network::STRING as network,
  t.value:id::STRING as transaction_id,
  t.value:cursor::STRING as transaction_cursor,
  t.value:timestamp::BIGINT as transaction_timestamp,
  t.value:successful::BOOLEAN as transaction_successful,
  t.value:memo::STRING as transaction_memo
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t
ORDER BY t.value:timestamp DESC
LIMIT 20;

-- 4. Extract operation data from transactions
SELECT 
  RECORD_CONTENT:network::STRING as network,
  t.value:id::STRING as transaction_id,
  t.value:successful::BOOLEAN as transaction_successful,
  op.value:id::STRING as operation_id,
  op.value:type::STRING as operation_type,
  op.value:token::STRING as token,
  op.value:address::STRING as address,
  op.value:amount::STRING as amount
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t,
LATERAL FLATTEN(input => t.value:operations) as op
WHERE t.value:successful = true
ORDER BY t.value:timestamp DESC
LIMIT 50;

-- 5. Extract activity data
SELECT 
  t.value:id::STRING as transaction_id,
  act.value:type::STRING as activity_type,
  act.value:perspectives:wallets as wallet_perspectives,
  act.value:perspectives:tokens as token_perspectives
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t,
LATERAL FLATTEN(input => t.value:activities) as act
ORDER BY t.value:timestamp DESC
LIMIT 30;

-- 6. Token metadata analysis
SELECT 
  RECORD_CONTENT:network::STRING as network,
  tm.value:address::STRING as token_address,
  tm.value:symbol::STRING as token_symbol,
  tm.value:name::STRING as token_name,
  tm.value:decimals::STRING as token_decimals,
  COUNT(*) as message_count
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:metadata:tokens) as tm
GROUP BY 1,2,3,4,5
ORDER BY message_count DESC;

-- 7. Network activity summary
SELECT 
  RECORD_CONTENT:network::STRING as network,
  COUNT(*) as total_messages,
  COUNT(DISTINCT t.value:id) as unique_transactions,
  SUM(CASE WHEN t.value:successful = true THEN 1 ELSE 0 END) as successful_transactions,
  MIN(t.value:timestamp) as earliest_timestamp,
  MAX(t.value:timestamp) as latest_timestamp
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t
GROUP BY RECORD_CONTENT:network
ORDER BY total_messages DESC;

-- 8. Operation type analysis
SELECT 
  op.value:type::STRING as operation_type,
  COUNT(*) as operation_count,
  COUNT(DISTINCT op.value:address) as unique_addresses,
  COUNT(DISTINCT op.value:token) as unique_tokens
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t,
LATERAL FLATTEN(input => t.value:operations) as op
WHERE t.value:successful = true
GROUP BY op.value:type
ORDER BY operation_count DESC;

-- 9. Recent failed transactions
SELECT 
  RECORD_CONTENT:network::STRING as network,
  t.value:id::STRING as transaction_id,
  t.value:timestamp::BIGINT as timestamp,
  t.value:memo::STRING as memo,
  ARRAY_SIZE(t.value:operations) as operation_count
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t
WHERE t.value:successful = false
ORDER BY t.value:timestamp DESC
LIMIT 10;

-- 10. Address activity tracking
SELECT 
  op.value:address::STRING as address,
  op.value:type::STRING as operation_type,
  COUNT(*) as operation_count,
  COUNT(DISTINCT t.value:id) as transaction_count,
  COUNT(DISTINCT RECORD_CONTENT:network) as networks_active
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t,
LATERAL FLATTEN(input => t.value:operations) as op
WHERE t.value:successful = true
  AND op.value:address IS NOT NULL
GROUP BY op.value:address, op.value:type
HAVING operation_count > 1
ORDER BY operation_count DESC
LIMIT 20;