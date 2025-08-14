# Snowflake Kafka Connector with Snowpipe Streaming Setup

This guide shows you how to set up the Snowflake Kafka Connector with **Snowpipe Streaming** for high-performance, low-latency data ingestion from your Kafka topics to Snowflake.

## 🏔️ What's New

Your stack now includes:

- **Confluent Schema Registry** (port 8081) - Manages protobuf schemas
- **Kafka Connect** (port 8083) - Runs the Snowflake connector
- **Snowflake Kafka Connector v2.0.0+** - With Snowpipe Streaming enabled
- **Enhanced Kafka UI** - Now shows Schema Registry and Connect status

## ⚡ Key Features

- **High-Performance Streaming**: No batch files, direct streaming to Snowflake
- **Schema Detection & Evolution**: Automatic table schema management
- **Protobuf Support**: Native protobuf message deserialization
- **Exactly-Once Semantics**: No data loss or duplication
- **Dead Letter Queue**: Error handling for problematic messages

## 🚀 Quick Start

### 1. Start the Enhanced Stack

```bash
cd docker
docker-compose up -d
```

This starts:
- Zookeeper + Kafka
- Schema Registry
- Kafka Connect (with Snowflake connector)
- Kafka UI (enhanced with Schema Registry integration)

### 2. Install SnowSQL CLI (if not already installed)

```bash
# Option 1: Download from Snowflake
# https://developers.snowflake.com/snowsql/

# Option 2: Using Homebrew (macOS)
brew install snowflake/snowflake/snowsql

# Option 3: Using pip
pip install snowflake-cli-labs
```

### 3. Test Your Snowflake Connection

```bash
cd docker
./scripts/test-snowflake-connection.sh
```

This verifies your credentials work before proceeding.

### 4. Register Your Protobuf Schema

Your ADR protobuf schema is already configured in `docker/protobuf-example/adr.proto`. Register it with:

```bash
cd docker
./scripts/register-protobuf-schema.sh
```

**Note**: The script automatically handles the ADR schema with its complex nested structure including `ListenerMessage`, `Transaction`, `Activity`, and `Operation` types.

### 5. Run Complete Setup

```bash
cd docker
./scripts/setup-snowflake.sh
```

This automated script will:
1. **Set up Snowflake database** - Creates tables with SnowSQL CLI
2. **Install Kafka connector** - Deploys with your configuration
3. **Verify everything** - Tests the complete pipeline

## 🔧 Configuration Deep Dive

### Snowpipe Streaming Settings

```json
{
  "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
  "snowflake.enable.schematization": "true",
  "enable.streaming.client.optimization": "true",
  "snowflake.streaming.enable.single.buffer": "true",
  "snowflake.streaming.max.client.lag": "10"
}
```

### High-Performance Settings

Based on the documentation for high-throughput scenarios:

```json
{
  "buffer.flush.time": "5",          // 5 seconds (vs default 10)
  "buffer.count.records": "1000",    // Lower for faster processing
  "buffer.size.bytes": "1000000"     // 1MB buffer
}
```

### Schema Registry Integration

```json
{
  "schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

## 📊 Monitoring

### Kafka UI (http://localhost:8080)
- View topics and messages
- Monitor connector status
- Browse schemas in Schema Registry

### Kafka Connect API (http://localhost:8083)
```bash
# Check connector status
curl http://localhost:8083/connectors/snowflake-sink-connector/status

# View connector config
curl http://localhost:8083/connectors/snowflake-sink-connector/config

# Check tasks
curl http://localhost:8083/connectors/snowflake-sink-connector/tasks
```

### Schema Registry API (http://localhost:8081)
```bash
# List all subjects
curl http://localhost:8081/subjects

# Get schema for topic
curl http://localhost:8081/subjects/blockchain-data-value/versions/latest
```

## 🎯 Testing

### 1. Verify Schema Registration
```bash
curl http://localhost:8081/subjects/blockchain-data-value/versions/latest | jq
```

### 2. Check Connector Status
```bash
curl http://localhost:8083/connectors/snowflake-sink-connector/status | jq
```

### 3. Query Snowflake
```sql
USE DATABASE SENTINEL_DB;
USE SCHEMA STREAMING_SCHEMA;

-- Check if ADR data is flowing
SELECT COUNT(*) FROM ADR_LISTENER_MESSAGES;

-- View recent ADR messages
SELECT 
  RECORD_METADATA:topic::STRING as topic,
  RECORD_METADATA:partition::INT as partition,
  RECORD_METADATA:offset::INT as offset,
  RECORD_CONTENT:version::STRING as adr_version,
  RECORD_CONTENT:network::STRING as network,
  ARRAY_SIZE(RECORD_CONTENT:transactions) as transaction_count
FROM ADR_LISTENER_MESSAGES 
ORDER BY RECORD_METADATA:offset DESC 
LIMIT 10;

-- Extract transaction data
SELECT 
  t.value:id::STRING as transaction_id,
  t.value:successful::BOOLEAN as successful,
  t.value:timestamp::BIGINT as timestamp,
  ARRAY_SIZE(t.value:operations) as operation_count
FROM ADR_LISTENER_MESSAGES,
LATERAL FLATTEN(input => RECORD_CONTENT:transactions) as t
ORDER BY t.value:timestamp DESC 
LIMIT 10;
```

See `docker/adr-sample-queries.sql` for more comprehensive query examples.

## 🔍 Troubleshooting

### Common Issues

1. **Connector fails to start**
   ```bash
   docker logs kafka-connect
   ```

2. **Schema registration fails**
   - Check if Schema Registry is running: `curl http://localhost:8081/subjects`
   - Verify protobuf syntax in your `.proto` file

3. **Snowflake connection issues**
   - Verify credentials in connector config
   - Check if RSA public key is set correctly in Snowflake
   - Ensure network connectivity to Snowflake

4. **No data flowing to Snowflake**
   - Check connector status: `curl http://localhost:8083/connectors/snowflake-sink-connector/status`
   - Verify topic has messages: Kafka UI → Topics → blockchain-data
   - Check Snowflake table exists and has proper permissions

### Performance Tuning

For high-throughput scenarios (>50 MB/s):
```json
{
  "enable.streaming.client.optimization": "false",
  "tasks.max": "8",
  "buffer.flush.time": "2",
  "snowflake.streaming.max.client.lag": "5"
}
```

## 🚨 Important Notes

1. **Schema Evolution**: Tables automatically evolve to match new protobuf schema fields
2. **Exactly-Once**: Guaranteed no duplicates or data loss
3. **Dead Letter Queue**: Failed records go to `snowflake-dlq` topic
4. **High-Performance Mode**: Uses single buffer for lowest latency
5. **Cost**: Snowpipe Streaming billing applies - monitor usage

## 🔗 Resources

- [Snowflake Kafka Connector Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-kafka)
- [Schema Detection and Evolution](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-kafka-schema-detection)
- [High-Performance Architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming-high-performance-overview)
- [Protobuf with Kafka Connect](https://docs.snowflake.com/en/user-guide/kafka-connector-protobuf)