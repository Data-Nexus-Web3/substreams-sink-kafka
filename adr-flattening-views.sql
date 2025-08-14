-- Create flattened transaction view
CREATE OR REPLACE VIEW adr_transactions_flattened AS
SELECT 
    NETWORK,
    VERSION,
    PREV_CURSOR,
    METADATA:block_number::BIGINT as BLOCK_NUMBER,
    METADATA:block_hash::STRING as BLOCK_HASH,
    METADATA:block_timestamp::BIGINT as BLOCK_TIMESTAMP,
    tx.value:hash::STRING as TX_HASH,
    tx.value:from::STRING as TX_FROM,
    tx.value:to::STRING as TX_TO,
    tx.value:value::STRING as TX_VALUE,
    tx.value:gas::BIGINT as TX_GAS,
    tx.value:gas_price::STRING as TX_GAS_PRICE,
    RECORD_METADATA:CreateTime::BIGINT as KAFKA_TIMESTAMP
FROM adr_test_schema_registry_messages,
LATERAL FLATTEN(input => TRANSACTIONS) tx;

-- Create materialized table for ultra-fast queries
CREATE OR REPLACE TABLE adr_transactions_materialized AS
SELECT * FROM adr_transactions_flattened;

-- Auto-refresh every minute for near real-time
CREATE OR REPLACE TASK refresh_adr_transactions
WAREHOUSE = 'YOUR_WAREHOUSE'
SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every minute
AS
INSERT INTO adr_transactions_materialized
SELECT * FROM adr_transactions_flattened
WHERE KAFKA_TIMESTAMP > (
    SELECT COALESCE(MAX(KAFKA_TIMESTAMP), 0) 
    FROM adr_transactions_materialized
);