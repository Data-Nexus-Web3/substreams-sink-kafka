#!/bin/bash
# JSON Explosion Test - Isolated and Clean
# Tests JSON explosion with Snowflake connector end-to-end

set -e

echo "🧨💥 JSON EXPLOSION TEST - ISOLATED & CLEAN 💥🧨"
echo "=================================================="

# Configuration
KAFKA_BROKERS="localhost:9092"
MANIFEST_PATH="/Users/thomasmonte/Repos/sentinel/substreams/adr/substreams.yaml"
ENDPOINT="10.0.11.152:9000"
OUTPUT_MODULE="map_flattened_messages"
EXPLODE_FIELD="messages"
BLOCK_RANGE="20833796:20833799"

# Use unique topic names with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
JSON_TOPIC="json-explosion-test-${TIMESTAMP}"
JSON_CURSOR="./json-explosion-cursor-${TIMESTAMP}.txt"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

# Step 1: Fresh Kafka Environment
echo ""
log_info "Step 1: Setting up fresh Kafka environment..."
echo "🔄 Stopping existing Kafka containers..."
docker-compose -f docker/docker-compose.yml down --remove-orphans || true

echo "🚀 Starting fresh Kafka environment..."
docker-compose -f docker/docker-compose.yml up -d

echo "⏰ Waiting 30 seconds for Kafka ecosystem to be ready..."
sleep 30

# Verify Kafka is ready
echo "🔍 Verifying Kafka is ready..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "   Waiting for Kafka..."
    sleep 5
done

until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo "   Waiting for Kafka Connect..."
    sleep 5
done

log_success "Fresh Kafka environment ready!"

# Step 2: Run JSON Explosion
echo ""
log_info "Step 2: Running JSON explosion test..."

echo "🔄 Command: ./substreams-sink-kafka run \"$KAFKA_BROKERS\" \"$MANIFEST_PATH\" \"$BLOCK_RANGE\" \\"
echo "  --endpoint=\"$ENDPOINT\" --plaintext \\"
echo "  --output-module=\"$OUTPUT_MODULE\" \\"
echo "  --kafka-topic=\"$JSON_TOPIC\" \\"
echo "  --output-format=json \\"
echo "  --explode-field=\"$EXPLODE_FIELD\" \\"
echo "  --cursor-file=\"$JSON_CURSOR\""
echo ""

# Run the explosion
timeout 60s ./substreams-sink-kafka run "$KAFKA_BROKERS" "$MANIFEST_PATH" "$BLOCK_RANGE" \
    --endpoint="$ENDPOINT" --plaintext \
    --output-module="$OUTPUT_MODULE" \
    --kafka-topic="$JSON_TOPIC" \
    --output-format=json \
    --explode-field="$EXPLODE_FIELD" \
    --cursor-file="$JSON_CURSOR" &

SINK_PID=$!
sleep 30
kill $SINK_PID 2>/dev/null || true
wait $SINK_PID 2>/dev/null || true

# Check message count
MESSAGE_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$JSON_TOPIC" --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")

if [ "$MESSAGE_COUNT" -gt 0 ]; then
    log_success "JSON explosion successful! Produced $MESSAGE_COUNT exploded messages"
    
    # Show sample message
    echo ""
    echo "📋 Sample JSON message:"
    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$JSON_TOPIC" --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | jq . || echo "Could not parse JSON"
else
    log_error "JSON explosion failed - no messages produced"
    exit 1
fi

# Step 3: Pre-create DLQ topic and Create Snowflake Connector
echo ""
log_info "Step 3: Creating DLQ topic and Snowflake connector for JSON data..."

# Create DLQ topic with replication factor 1 (single broker)
echo "📝 Creating dead letter queue topic..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic snowflake-dlq --partitions 3 --replication-factor 1 --if-not-exists

# Create connector config
cat > /tmp/json-explosion-connector.json << EOF
{
  "name": "json-explosion-connector-${TIMESTAMP}",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "2",
    "topics": "$JSON_TOPIC",
    
    "snowflake.url.name": "DTCC-DEVX1_US_WEST_2.snowflakecomputing.com:443",
    "snowflake.user.name": "TMONTE",
    "snowflake.private.key": "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC2tRd01CurbjBaDzZIsAqc/9Yx7J7koQvqHHwcJc88mGSls4/CG3FbdSe23iJuozqxv1SWLm0ZlXSoIhrX47WO9/6A5nLs3g7a7pnUMTRr0WoD9R9QdHOH0k+rnDHflxV+61T+OyXzp4axOTuowlaiD0Z7UwKcwwxFIdc8p+YnXOhMjzJ2SuJaO37Nn7jSXX5aAXekdyK5dQ3FG40NJGw3qQpmXk1aEEHR9zvt4HDmT+iJld/1aoLcDFuHYXPZZzkEB6iSrpR8dW/H9g+M6niGw1SGNZETIFToUNtqnei5eEl6qsMhYSoPCFLoiJ6xEh3XM3N1ixEc0gSXHACqCpz3AgMBAAECggEACJpyZk/vVn0zzanVtoqDlWz+mw83VDC5LOb2eSTWo1XRt3PDGzLLnbDgk93V5TvOcbw+sWuyO3gsLBafCPdx/y9yToOLedfi/zApEjLW8xTDVzhdpx40qnqewcfzGUI2Aun18Jq8aMLEtuepBYNRLQRnHzuDbfpJQ6AaUILfdY74Cj5Puq0+tjmN3Uk1asRWNEe/zWyTF6DouoaTpEAMGUoYa6bYeq/ldWVxRprHQTFSYJ236Vv5G3OWC98gxYx9/Hyxz7lxR+4wrJUWAS85bEuicE71BWZXqRlJZe2da0a/QnbU14Bd6dP4kCFXBE1ka7Y2qlA2hAXQZYp38xRQIQKBgQDcYd7B3ScAjyOmxk395Dwgx1fmGRU443Fx6bMMNy8TvT5+OOoFnCGHtlpaMvVutUt9+Tp2G11g+qALEn+DPxfQIOAUvK1U0sJ4o5VBB0ciAbVk0KZx5zyhYKsOG7dL2Y1/TZWCYYBnqLBwT/wFxaM2lOMhac94athqoy0MziiiPwKBgQDUPHQ3cWgfOWbPVOBa3riYa4LnjgF5/tP2fbnzk8kvOLMTFXVDzIBiZl0dyS6Lu9YpsrFPtuNIwFlxL5m3JD4xjtDIECKt4Sg2yaUc+axpo6kAeI3ZoBRkGuXNX5Lh0c0MG6ekxBdkem2WRMW9yA9x3mIKqRvr9nf9jX6sY5NnSQKBgQCMTlL/oci+9mKAPyhQSApv4/n2KGn2efczytGPKWLzcjxjJ+D2CVzfhh0n5+GRxiJ15UJvByUAJQ/XfMNpz7VdcFC5YxYHNtdQ7vSLHhMPd38A3EXbpphXNbAxnzunMq5/KenRxDl3xVHvbzzIf/dVPJ0OHMtVotB9s71utaHxCQKBgQCOVbzbMgNSbXy1r4aHarcCAZhZErOKzYv503fSE1rpgG0Yb1uljJDNbTklsLa2n9KRRHYFr/Hd6KITdojwei37rnv2sFuRoV8G31UMiaVHc6rz1eXL/b+vIxrbES3ApKOPazyTE69cg37bOGQlUvaIt0upOqCvLyxpr2s00dMZgQKBgQCtyTHocPCjgIuHPdrQHOLNh5upWxB3biIVxZ+beVdmwlNm8O7vWe8MbITfaTxoedcF+OEOf04jmzfEYUh9TPKdd+tMOtsQElSJJuef3WuSw9ic+Omhx6AatoH1loXQ975HOB7WYXx4vLdZ/wOD2NDuG7SCN2mCZVFlmS5GNIYRGA==",
    "snowflake.database.name": "SENTINEL_DB",
    "snowflake.schema.name": "EXPLOSION_TEST",
    "snowflake.role.name": "SENTINEL_DB_ADMIN",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "false",
    
    "enable.streaming.client.optimization": "true",
    "snowflake.streaming.enable.single.buffer": "true",
    "snowflake.streaming.max.client.lag": "10",
    
    "buffer.flush.time": "5",
    "buffer.count.records": "100",
    "buffer.size.bytes": "100000",
    
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "snowflake-dlq",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
EOF

# Create the connector
CONNECTOR_RESPONSE=$(curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @/tmp/json-explosion-connector.json)

if echo "$CONNECTOR_RESPONSE" | grep -q '"name"'; then
    log_success "Snowflake connector created: json-explosion-connector-${TIMESTAMP}"
else
    log_error "Failed to create Snowflake connector"
    echo "Response: $CONNECTOR_RESPONSE"
    exit 1
fi

# Step 4: Monitor connector status
echo ""
log_info "Step 4: Monitoring connector status..."
sleep 10

CONNECTOR_STATUS=$(curl -s "http://localhost:8083/connectors/json-explosion-connector-${TIMESTAMP}/status")
CONNECTOR_STATE=$(echo "$CONNECTOR_STATUS" | jq -r '.connector.state // "unknown"')
TASK_STATES=$(echo "$CONNECTOR_STATUS" | jq -r '.tasks[]?.state // "unknown"' | tr '\n' ',' | sed 's/,$//')

echo "📊 Connector Status:"
echo "   Connector: $CONNECTOR_STATE"
echo "   Tasks: $TASK_STATES"

# Check consumer group
echo ""
echo "👥 Consumer Group Status:"
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "connect-json-explosion-connector-${TIMESTAMP}" 2>/dev/null || echo "   Consumer group not found yet"

# Final Results
echo ""
echo "🎉 JSON EXPLOSION TEST COMPLETE!"
echo "=================================="
echo ""
echo "📊 Results Summary:"
echo "   ✅ Fresh Kafka environment: Ready"
echo "   ✅ JSON explosion: $MESSAGE_COUNT messages produced"
echo "   ✅ Topic: $JSON_TOPIC"
echo "   ✅ Snowflake connector: json-explosion-connector-${TIMESTAMP} ($CONNECTOR_STATE)"
echo ""
echo "🔍 Next Steps:"
echo "   1. Wait 2-3 minutes for Snowflake ingestion"
echo "   2. Check EXPLOSION_TEST schema for table: $(echo $JSON_TOPIC | tr '[:lower:]' '[:upper:]' | tr '-' '_')"
echo "   3. Query your exploded JSON data!"
echo ""
echo "🛠️  Useful Commands:"
echo "   • Check connector: curl http://localhost:8083/connectors/json-explosion-connector-${TIMESTAMP}/status | jq '.connector.state'"
echo "   • View messages: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $JSON_TOPIC --from-beginning | head -5"
echo "   • Consumer group: docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-json-explosion-connector-${TIMESTAMP}"
echo ""
echo "🧹 Cleanup:"
echo "   • Delete connector: curl -X DELETE http://localhost:8083/connectors/json-explosion-connector-${TIMESTAMP}"
echo "   • Stop environment: docker-compose -f docker/docker-compose.yml down"
echo "   • Clean cursor: rm -f $JSON_CURSOR"
echo ""
log_success "🎊 JSON explosion pipeline ready! Your data should be flowing to Snowflake! 🎊"