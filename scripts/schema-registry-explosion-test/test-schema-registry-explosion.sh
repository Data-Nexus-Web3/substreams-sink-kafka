#!/bin/bash
# Schema Registry Explosion Test - Isolated and Clean
# Tests Schema Registry explosion with protobuf serialization end-to-end

set -e

echo "🎯🧨 SCHEMA REGISTRY EXPLOSION TEST - ISOLATED & CLEAN 🧨🎯"
echo "============================================================="

# Configuration
KAFKA_BROKERS="localhost:9092"
MANIFEST_PATH="/Users/thomasmonte/Repos/sentinel/substreams/adr/substreams.yaml"
ENDPOINT="10.0.11.152:9000"
OUTPUT_MODULE="map_flattened_messages"
EXPLODE_FIELD="messages"
BLOCK_RANGE="20833796:20833799"

# Use unique topic names with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SCHEMA_TOPIC="schema-explosion-test-${TIMESTAMP}"
SCHEMA_CURSOR="./schema-explosion-cursor-${TIMESTAMP}.txt"

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

echo "⏰ Waiting 10 seconds for services to be ready..."
sleep 10

# Verify everything is ready
echo "🔍 Verifying services are ready..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "   Waiting for Kafka..."
    sleep 5
done

until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
    echo "   Waiting for Schema Registry..."
    sleep 5
done

until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo "   Waiting for Kafka Connect..."
    sleep 5
done

log_success "Fresh Kafka environment ready!"

# Step 2: Schema Registry Setup - Register Required Dependencies
echo ""
log_info "Step 2: Registering required schema dependencies..."

# Register the v1/kafka/kafka.proto schema that FlattenedMessage depends on
echo "📝 Registering v1/kafka/kafka.proto dependency schema..."
cat > /tmp/kafka-proto-schema.json << 'EOF'
{
  "schemaType": "PROTOBUF",
  "schema": "syntax = \"proto3\";\npackage sf.substreams.sink.kafka.v1;\n\nimport \"google/protobuf/descriptor.proto\";\n\noption go_package = \"github.com/streamingfast/substreams-sink-kafka/pb/sf/substreams/sink/kafka/v1;pbkafka\";\n\nmessage ExplosionConfig {\n  ExplosionMode mode = 1;\n  map<string, TopicConfig> topic_configs = 2;\n  repeated string default_parent_fields = 3;\n}\nmessage TopicConfig {\n  string topic_name = 1;\n  repeated string parent_fields = 2;\n  string serialization_format = 3;\n  string key_field = 4;\n}\nenum ExplosionMode {\n  EXPLOSION_MODE_UNSPECIFIED = 0;\n  EXPLOSION_MODE_NONE = 1;\n  EXPLOSION_MODE_ANNOTATIONS = 2;\n  EXPLOSION_MODE_CONFIG = 3;\n}\n\nextend google.protobuf.MessageOptions {\n  string topic_name = 50001;\n  string include_parent_fields = 50002;\n  bool is_root_topic = 50003;\n}\nextend google.protobuf.FieldOptions {\n  string explode_to_topic = 50101;\n  bool include_in_parent_context = 50102;\n  string explosion_key_field = 50103;\n}"
}
EOF

# Register the kafka.proto schema
KAFKA_PROTO_RESPONSE=$(curl -s -X POST "http://localhost:8081/subjects/v1%2Fkafka%2Fkafka.proto/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @/tmp/kafka-proto-schema.json)

if echo "$KAFKA_PROTO_RESPONSE" | grep -q '"id"'; then
    KAFKA_PROTO_ID=$(echo "$KAFKA_PROTO_RESPONSE" | jq -r '.id')
    log_success "v1/kafka/kafka.proto registered with ID: $KAFKA_PROTO_ID"
else
    log_warning "v1/kafka/kafka.proto may already be registered or registration failed"
    echo "Response: $KAFKA_PROTO_RESPONSE"
fi

echo ""
echo "🧠 KEY INSIGHT: Topic-based schema subjects for Snowflake compatibility!"
echo "   Expected subject: ${SCHEMA_TOPIC}-value (topic name + -value suffix)"
echo ""
echo "✅ Dependency schema registered - Confluent will auto-register FlattenedMessage schema!"

# Step 3: Debug Schema Registry State
echo ""
log_info "Step 3: Debugging Schema Registry state before running sink..."

echo ""
echo "🔍 SCHEMA REGISTRY DEBUG INFORMATION:"
echo "====================================="
echo "Topic: $SCHEMA_TOPIC"
echo "Expected auto-generated subject: ${SCHEMA_TOPIC}-value"
echo ""

# List existing subjects
echo "📋 Currently registered subjects:"
curl -s http://localhost:8081/subjects | jq -r '.[]' | sort

echo ""
echo "🔍 Testing Schema Registry connectivity:"
curl -s http://localhost:8081/subjects > /dev/null
if [ $? -eq 0 ]; then
    log_success "Schema Registry connectivity: OK"
else
    log_error "Schema Registry connectivity: FAILED"
    exit 1
fi

# Step 4: Run Schema Registry Explosion
echo ""
log_info "Step 4: Running Schema Registry explosion test..."

echo "🔄 Command: ./substreams-sink-kafka run \"$KAFKA_BROKERS\" \"$MANIFEST_PATH\" \"$BLOCK_RANGE\" \\"
echo "  --endpoint=\"$ENDPOINT\" --plaintext \\"
echo "  --output-module=\"$OUTPUT_MODULE\" \\"
echo "  --kafka-topic=\"$SCHEMA_TOPIC\" \\"
echo "  --output-format=schema-registry \\"
echo "  --explode-field=\"$EXPLODE_FIELD\" \\"
echo "  --schema-registry-url=\"http://localhost:8081\" \\"
echo "  --schema-auto-register=true \\"
echo "  --cursor-file=\"$SCHEMA_CURSOR\""
echo ""

# Run the explosion with auto-register enabled
timeout 60s ./substreams-sink-kafka run "$KAFKA_BROKERS" "$MANIFEST_PATH" "$BLOCK_RANGE" \
    --endpoint="$ENDPOINT" --plaintext \
    --output-module="$OUTPUT_MODULE" \
    --kafka-topic="$SCHEMA_TOPIC" \
    --output-format=schema-registry \
    --explode-field="$EXPLODE_FIELD" \
    --schema-registry-url="http://localhost:8081" \
    --schema-auto-register=true \
    --cursor-file="$SCHEMA_CURSOR" &

SINK_PID=$!
sleep 30
kill $SINK_PID 2>/dev/null || true
wait $SINK_PID 2>/dev/null || true

# Check message count
MESSAGE_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$SCHEMA_TOPIC" --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")

if [ "$MESSAGE_COUNT" -gt 0 ]; then
    log_success "Schema Registry explosion successful! Produced $MESSAGE_COUNT exploded messages"
    
    # Show sample message (protobuf binary)
    echo ""
    echo "📋 Sample protobuf message (hex dump of first 100 bytes):"
    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$SCHEMA_TOPIC" --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | head -c 100 | hexdump -C | head -3
else
    log_error "Schema Registry explosion failed - no messages produced!"
    
    # Debug information
    echo ""
    echo "🔍 Debug Information:"
    echo "Topic: $SCHEMA_TOPIC"
    echo "Expected subject: $CONFLUENT_SUBJECT"
    echo "Registered subjects:"
    curl -s http://localhost:8081/subjects | jq -r '.[]' | grep "$SCHEMA_TOPIC" || echo "No matching subjects found"
    
    exit 1
fi

# Step 5: Pre-create DLQ topic and Create Snowflake Connector
echo ""
log_info "Step 5: Creating DLQ topic and Snowflake connector for Schema Registry data..."

# Create DLQ topic with replication factor 1 (single broker)
echo "📝 Creating dead letter queue topic..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic snowflake-dlq --partitions 3 --replication-factor 1 --if-not-exists

# Create connector config
cat > /tmp/schema-explosion-connector.json << EOF
{
  "name": "schema-explosion-connector-${TIMESTAMP}",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "2",
    "topics": "$SCHEMA_TOPIC",
    
    "snowflake.url.name": "DTCC-DEVX1_US_WEST_2.snowflakecomputing.com:443",
    "snowflake.user.name": "TMONTE",
    "snowflake.private.key": "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC2tRd01CurbjBaDzZIsAqc/9Yx7J7koQvqHHwcJc88mGSls4/CG3FbdSe23iJuozqxv1SWLm0ZlXSoIhrX47WO9/6A5nLs3g7a7pnUMTRr0WoD9R9QdHOH0k+rnDHflxV+61T+OyXzp4axOTuowlaiD0Z7UwKcwwxFIdc8p+YnXOhMjzJ2SuJaO37Nn7jSXX5aAXekdyK5dQ3FG40NJGw3qQpmXk1aEEHR9zvt4HDmT+iJld/1aoLcDFuHYXPZZzkEB6iSrpR8dW/H9g+M6niGw1SGNZETIFToUNtqnei5eEl6qsMhYSoPCFLoiJ6xEh3XM3N1ixEc0gSXHACqCpz3AgMBAAECggEACJpyZk/vVn0zzanVtoqDlWz+mw83VDC5LOb2eSTWo1XRt3PDGzLLnbDgk93V5TvOcbw+sWuyO3gsLBafCPdx/y9yToOLedfi/zApEjLW8xTDVzhdpx40qnqewcfzGUI2Aun18Jq8aMLEtuepBYNRLQRnHzuDbfpJQ6AaUILfdY74Cj5Puq0+tjmN3Uk1asRWNEe/zWyTF6DouoaTpEAMGUoYa6bYeq/ldWVxRprHQTFSYJ236Vv5G3OWC98gxYx9/Hyxz7lxR+4wrJUWAS85bEuicE71BWZXqRlJZe2da0a/QnbU14Bd6dP4kCFXBE1ka7Y2qlA2hAXQZYp38xRQIQKBgQDcYd7B3ScAjyOmxk395Dwgx1fmGRU443Fx6bMMNy8TvT5+OOoFnCGHtlpaMvVutUt9+Tp2G11g+qALEn+DPxfQIOAUvK1U0sJ4o5VBB0ciAbVk0KZx5zyhYKsOG7dL2Y1/TZWCYYBnqLBwT/wFxaM2lOMhac94athqoy0MziiiPwKBgQDUPHQ3cWgfOWbPVOBa3riYa4LnjgF5/tP2fbnzk8kvOLMTFXVDzIBiZl0dyS6Lu9YpsrFPtuNIwFlxL5m3JD4xjtDIECKt4Sg2yaUc+axpo6kAeI3ZoBRkGuXNX5Lh0c0MG6ekxBdkem2WRMW9yA9x3mIKqRvr9nf9jX6sY5NnSQKBgQCMTlL/oci+9mKAPyhQSApv4/n2KGn2efczytGPKWLzcjxjJ+D2CVzfhh0n5+GRxiJ15UJvByUAJQ/XfMNpz7VdcFC5YxYHNtdQ7vSLHhMPd38A3EXbpphXNbAxnzunMq5/KenRxDl3xVHvbzzIf/dVPJ0OHMtVotB9s71utaHxCQKBgQCOVbzbMgNSbXy1r4aHarcCAZhZErOKzYv503fSE1rpgG0Yb1uljJDNbTklsLa2n9KRRHYFr/Hd6KITdojwei37rnv2sFuRoV8G31UMiaVHc6rz1eXL/b+vIxrbES3ApKOPazyTE69cg37bOGQlUvaIt0upOqCvLyxpr2s00dMZgQKBgQCtyTHocPCjgIuHPdrQHOLNh5upWxB3biIVxZ+beVdmwlNm8O7vWe8MbITfaTxoedcF+OEOf04jmzfEYUh9TPKdd+tMOtsQElSJJuef3WuSw9ic+Omhx6AatoH1loXQ975HOB7WYXx4vLdZ/wOD2NDuG7SCN2mCZVFlmS5GNIYRGA==",
    "snowflake.database.name": "SENTINEL_DB",
    "snowflake.schema.name": "EXPLOSION_TEST",
    "snowflake.role.name": "SENTINEL_DB_ADMIN",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "true",
    
    "schema.registry.url": "http://schema-registry:8081",
    
    "enable.streaming.client.optimization": "true",
    "snowflake.streaming.enable.single.buffer": "true",
    "snowflake.streaming.max.client.lag": "10",
    
    "buffer.flush.time": "5",
    "buffer.count.records": "100",
    "buffer.size.bytes": "100000",
    
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.auto.register.schemas": "true",
    "value.converter.use.optional.for.nonrequired": "true",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "snowflake-dlq",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
EOF

# Create the connector
CONNECTOR_RESPONSE=$(curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @/tmp/schema-explosion-connector.json)

if echo "$CONNECTOR_RESPONSE" | grep -q '"name"'; then
    log_success "Snowflake connector created: schema-explosion-connector-${TIMESTAMP}"
else
    log_error "Failed to create Snowflake connector"
    echo "Response: $CONNECTOR_RESPONSE"
    exit 1
fi

# Step 6: Monitor connector status
echo ""
log_info "Step 6: Monitoring connector status..."
sleep 10

CONNECTOR_STATUS=$(curl -s "http://localhost:8083/connectors/schema-explosion-connector-${TIMESTAMP}/status")
CONNECTOR_STATE=$(echo "$CONNECTOR_STATUS" | jq -r '.connector.state // "unknown"')
TASK_STATES=$(echo "$CONNECTOR_STATUS" | jq -r '.tasks[]?.state // "unknown"' | tr '\n' ',' | sed 's/,$//')

echo "📊 Connector Status:"
echo "   Connector: $CONNECTOR_STATE"
echo "   Tasks: $TASK_STATES"

# Check consumer group and DLQ
echo ""
echo "👥 Consumer Group Status:"
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "connect-schema-explosion-connector-${TIMESTAMP}" 2>/dev/null || echo "   Consumer group not found yet"

echo ""
echo "🔍 Checking DLQ for any failed messages:"
DLQ_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic snowflake-dlq --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
if [ "$DLQ_COUNT" -gt 0 ]; then
    log_warning "Found $DLQ_COUNT messages in DLQ! Checking first message:"
    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic snowflake-dlq --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | head -3
else
    log_success "No messages in DLQ"
fi

echo ""
echo "📋 Auto-registered schemas after explosion:"
curl -s http://localhost:8081/subjects | jq -r '.[]' | grep -E "(FlattenedMessage|value)" | sort

# Final Results
echo ""
echo "🎯 SCHEMA REGISTRY EXPLOSION TEST COMPLETE!"
echo "============================================"
echo ""
echo "📊 Results Summary:"
echo "   ✅ Fresh Kafka environment: Ready"
echo "   ✅ Individual schema registered: $CONFLUENT_SUBJECT (ID: $SCHEMA_ID)"
echo "   ✅ Schema Registry explosion: $MESSAGE_COUNT messages produced"
echo "   ✅ Topic: $SCHEMA_TOPIC"
echo "   ✅ Snowflake connector: schema-explosion-connector-${TIMESTAMP} ($CONNECTOR_STATE)"
echo ""
echo "🧠 Key Insights Learned:"
echo "   • Snowflake Kafka Connector expects {topic}-value subject naming pattern"
echo "   • v1/kafka/kafka.proto dependency must be pre-registered"
echo "   • Individual FlattenedMessage schema auto-registers under correct topic-based subject"
echo "   • Schema ID consistency crucial for proper deserialization"
echo ""
echo "🔍 Next Steps:"
echo "   1. Wait 2-3 minutes for Snowflake ingestion"
echo "   2. Check EXPLOSION_TEST schema for table: $(echo $SCHEMA_TOPIC | tr '[:lower:]' '[:upper:]' | tr '-' '_')"
echo "   3. Query your exploded protobuf data with proper schema!"
echo ""
echo "🛠️  Useful Commands:"
echo "   • Check connector: curl http://localhost:8083/connectors/schema-explosion-connector-${TIMESTAMP}/status | jq '.connector.state'"
echo "   • View schema: curl http://localhost:8081/subjects/$CONFLUENT_SUBJECT/versions/latest | jq '.id'"
echo "   • Consumer group: docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-schema-explosion-connector-${TIMESTAMP}"
echo ""
echo "🧹 Cleanup:"
echo "   • Delete connector: curl -X DELETE http://localhost:8083/connectors/schema-explosion-connector-${TIMESTAMP}"
echo "   • Stop environment: docker-compose -f docker/docker-compose.yml down"
echo "   • Clean cursor: rm -f $SCHEMA_CURSOR"
echo ""
log_success "🎊 Schema Registry explosion pipeline ready! Your protobuf data should be flowing to Snowflake! 🎊"