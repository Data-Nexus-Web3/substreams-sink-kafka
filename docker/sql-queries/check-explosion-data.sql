-- Check if exploded transaction data has landed in Snowflake
USE ROLE SENTINEL_DB_ADMIN;
USE DATABASE SENTINEL_DB;
USE SCHEMA EXPLOSION_TEST;

-- Check raw table
SELECT COUNT(*) as total_records FROM ADR_TRANSACTIONS_EXPLODED;

-- Check the analysis view
SELECT * FROM EXPLODED_TRANSACTIONS_ANALYSIS LIMIT 5;

-- Show the structure of the first record
SELECT 
    source_field,
    item_index,
    cursor,
    transaction_data,
    INGESTED_AT
FROM EXPLODED_TRANSACTIONS_ANALYSIS 
LIMIT 2;