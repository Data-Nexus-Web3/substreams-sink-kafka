-- Setup Snowflake tables for chain head latency testing
-- These tables will store block info data with timestamps for latency analysis

USE ROLE SENTINEL_DB_ADMIN;
USE DATABASE SENTINEL_DB;

-- Create dedicated schema for latency benchmarking
CREATE SCHEMA IF NOT EXISTS LATENCY_BENCHMARKING;
USE SCHEMA LATENCY_BENCHMARKING;

-- Create table for JSON format block info
CREATE TABLE IF NOT EXISTS BLOCK_INFO_LATENCY_TEST_JSON (
  RECORD_METADATA VARIANT,
  RECORD_CONTENT VARIANT,
  INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create table for Schema Registry format block info  
CREATE TABLE IF NOT EXISTS BLOCK_INFO_LATENCY_TEST_SCHEMA_REGISTRY (
  RECORD_METADATA VARIANT,
  RECORD_CONTENT VARIANT,
  INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Enable schema evolution for automatic field detection
ALTER TABLE BLOCK_INFO_LATENCY_TEST_JSON SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE BLOCK_INFO_LATENCY_TEST_SCHEMA_REGISTRY SET ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Create views for easy latency analysis
CREATE OR REPLACE VIEW BLOCK_INFO_LATENCY_ANALYSIS_JSON AS
SELECT 
  -- Extract block info from JSON format
  RECORD_CONTENT:block_number::BIGINT as block_number,
  RECORD_CONTENT:block_timestamp::BIGINT as block_timestamp_unix,
  TO_TIMESTAMP(RECORD_CONTENT:block_timestamp::BIGINT) as block_timestamp,
  
  -- Kafka metadata timestamps  
  RECORD_METADATA:CreateTime::BIGINT as kafka_timestamp_unix,
  TO_TIMESTAMP(RECORD_METADATA:CreateTime::BIGINT / 1000) as kafka_timestamp,
  
  -- Snowflake ingestion timestamp
  INGESTED_AT as snowflake_timestamp,
  
  -- Calculate latencies (in seconds)
  (RECORD_METADATA:CreateTime::BIGINT / 1000 - RECORD_CONTENT:block_timestamp::BIGINT) as kafka_latency_seconds,
  (EXTRACT(EPOCH FROM INGESTED_AT) - RECORD_METADATA:CreateTime::BIGINT / 1000) as snowflake_latency_seconds,
  (EXTRACT(EPOCH FROM INGESTED_AT) - RECORD_CONTENT:block_timestamp::BIGINT) as total_latency_seconds
  
FROM BLOCK_INFO_LATENCY_TEST_JSON
ORDER BY block_number DESC;

CREATE OR REPLACE VIEW BLOCK_INFO_LATENCY_ANALYSIS_SCHEMA_REGISTRY AS
SELECT 
  -- Extract block info from Schema Registry format
  RECORD_CONTENT:block_number::BIGINT as block_number,
  RECORD_CONTENT:block_timestamp::BIGINT as block_timestamp_unix,
  TO_TIMESTAMP(RECORD_CONTENT:block_timestamp::BIGINT) as block_timestamp,
  
  -- Kafka metadata timestamps
  RECORD_METADATA:CreateTime::BIGINT as kafka_timestamp_unix,
  TO_TIMESTAMP(RECORD_METADATA:CreateTime::BIGINT / 1000) as kafka_timestamp,
  
  -- Snowflake ingestion timestamp
  INGESTED_AT as snowflake_timestamp,
  
  -- Calculate latencies (in seconds)
  (RECORD_METADATA:CreateTime::BIGINT / 1000 - RECORD_CONTENT:block_timestamp::BIGINT) as kafka_latency_seconds,
  (EXTRACT(EPOCH FROM INGESTED_AT) - RECORD_METADATA:CreateTime::BIGINT / 1000) as snowflake_latency_seconds,
  (EXTRACT(EPOCH FROM INGESTED_AT) - RECORD_CONTENT:block_timestamp::BIGINT) as total_latency_seconds
  
FROM BLOCK_INFO_LATENCY_TEST_SCHEMA_REGISTRY
ORDER BY block_number DESC;

-- Create summary view for quick latency stats
CREATE OR REPLACE VIEW LATENCY_TEST_SUMMARY AS
SELECT 
  'JSON' as format,
  COUNT(*) as total_blocks,
  AVG(kafka_latency_seconds) as avg_kafka_latency_sec,
  AVG(snowflake_latency_seconds) as avg_snowflake_latency_sec,
  AVG(total_latency_seconds) as avg_total_latency_sec,
  MAX(total_latency_seconds) as max_total_latency_sec,
  MIN(total_latency_seconds) as min_total_latency_sec
FROM BLOCK_INFO_LATENCY_ANALYSIS_JSON

UNION ALL

SELECT 
  'SCHEMA_REGISTRY' as format,
  COUNT(*) as total_blocks,
  AVG(kafka_latency_seconds) as avg_kafka_latency_sec,
  AVG(snowflake_latency_seconds) as avg_snowflake_latency_sec,
  AVG(total_latency_seconds) as avg_total_latency_sec,
  MAX(total_latency_seconds) as max_total_latency_sec,
  MIN(total_latency_seconds) as min_total_latency_sec
FROM BLOCK_INFO_LATENCY_ANALYSIS_SCHEMA_REGISTRY;

-- Verify setup
SELECT 'Latency Test Tables Setup Complete!' as STATUS;
SHOW TABLES LIKE '%LATENCY%';
SHOW VIEWS LIKE '%LATENCY%';