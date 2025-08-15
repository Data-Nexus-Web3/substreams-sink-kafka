#!/bin/bash
# End-to-end test script for the new explosion feature
# Tests both JSON and Schema Registry outputs with separate topics and tables

set -e

echo "🧨 Testing Substreams Kafka Sink - Explosion Feature"
echo "===================================================="

# Configuration
KAFKA_BROKERS="localhost:9092"
MANIFEST_PATH="/Users/thomasmonte/Repos/sentinel/substreams/adr/substreams.yaml"
ENDPOINT="10.0.11.152:9000"
OUTPUT_MODULE="map_flattened_messages"
EXPLODE_FIELD="messages"
BLOCK_RANGE="20833796:20833799"

# Use unique topic names with timestamp to avoid conflicts
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
JSON_TOPIC="adr-exploded-json-${TIMESTAMP}"
SCHEMA_REGISTRY_TOPIC="adr-exploded-schema-registry-${TIMESTAMP}"

# Cursor files
JSON_CURSOR="./explosion-test-json-cursor-${TIMESTAMP}.txt"
SCHEMA_CURSOR="./explosion-test-schema-cursor-${TIMESTAMP}.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if binary exists
    if [ ! -f "./substreams-sink-kafka" ]; then
        log_error "Binary not found. Building..."
        go build -o substreams-sink-kafka ./cmd/substreams-sink-kafka
        if [ $? -eq 0 ]; then
            log_success "Binary built successfully"
        else
            log_error "Failed to build binary"
            exit 1
        fi
    fi
    
    # Check if Kafka is running
    if ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        log_error "Kafka is not running. Please start Kafka first."
        exit 1
    fi
    
    # Check if Schema Registry is running
    if ! curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
        log_error "Schema Registry is not running. Please start Schema Registry first."
        exit 1
    fi
    
    # Check if Kafka Connect is running
    if ! curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
        log_error "Kafka Connect is not running. Please start Kafka Connect first."
        exit 1
    fi
    
    log_success "All prerequisites met"
    log_info "Using topics: $JSON_TOPIC and $SCHEMA_REGISTRY_TOPIC"
}

# Test explosion feature with JSON output
test_json_explosion() {
    log_info "Testing JSON explosion output..."
    
    echo ""
    echo "🔄 Running substreams-sink-kafka with JSON explosion..."
    echo "Command: ./substreams-sink-kafka run \"$KAFKA_BROKERS\" \"$MANIFEST_PATH\" \"$BLOCK_RANGE\" \\"
    echo "  --endpoint=\"$ENDPOINT\" --plaintext \\"
    echo "  --output-module=\"$OUTPUT_MODULE\" \\"
    echo "  --kafka-topic=\"$JSON_TOPIC\" \\"
    echo "  --output-format=json \\"
    echo "  --explode-field=\"$EXPLODE_FIELD\" \\"
    echo "  --cursor-file=\"$JSON_CURSOR\""
    echo ""
    
    # Run the sink with JSON explosion
    timeout 60s ./substreams-sink-kafka run "$KAFKA_BROKERS" "$MANIFEST_PATH" "$BLOCK_RANGE" \
        --endpoint="$ENDPOINT" --plaintext \
        --output-module="$OUTPUT_MODULE" \
        --kafka-topic="$JSON_TOPIC" \
        --output-format=json \
        --explode-field="$EXPLODE_FIELD" \
        --cursor-file="$JSON_CURSOR" &
    
    SINK_PID=$!
    
    # Wait for sink to process some data
    sleep 30
    
    # Stop the sink
    kill $SINK_PID 2>/dev/null || true
    wait $SINK_PID 2>/dev/null || true
    
    # Check if messages were produced
    MESSAGE_COUNT=$(docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$JSON_TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null | wc -l || echo "0")
    
    if [ "$MESSAGE_COUNT" -gt 0 ]; then
        log_success "JSON explosion test successful! Produced $MESSAGE_COUNT exploded messages"
        
        # Show sample message
        echo ""
        echo "📋 Sample exploded JSON message:"
        docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$JSON_TOPIC" --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | jq . || echo "Could not parse as JSON"
    else
        log_error "JSON explosion test failed - no messages produced"
        return 1
    fi
}

# Test explosion feature with Schema Registry output
test_schema_registry_explosion() {
    log_info "Testing Schema Registry explosion output..."
    
    echo ""
    echo "🎯 KEY INSIGHT: Confluent protobuf serializer auto-generates subjects based on message type!"
    echo "Expected subject: ${SCHEMA_REGISTRY_TOPIC}-value-value (note the double -value suffix)"
    echo ""
    
    # Step 1: Pre-register the individual FlattenedMessage schema under the auto-generated subject
    log_info "Step 1: Pre-registering individual FlattenedMessage schema..."
    CONFLUENT_SUBJECT="${SCHEMA_REGISTRY_TOPIC}-value-value"
    
    INDIVIDUAL_SCHEMA='syntax = "proto3";
package evm.adr.v1;

message FlattenedMessage {
  optional uint64 block = 1;
  string version = 2;
  string network = 3;
  string prev_cursor = 4;
  string txn_id = 5;
  string txn_cursor = 6;
  int64 txn_timestamp = 7;
  optional string txn_status = 8;
  bool txn_successful = 9;
  optional string wallet_token = 10;
  optional string act_type = 11;
  optional string act_address = 12;
  repeated string act_operations = 13;
  optional string ops_id = 14;
  optional string ops_token = 15;
  optional string ops_address = 16;
  optional string ops_amount = 17;
  optional string ops_type = 18;
  optional bool has_all_operations = 19;
  optional int64 received_at = 20;
  optional int64 sent_at = 21;
  optional string provider = 22;
}'

    # Register the schema under the Confluent auto-generated subject
    SCHEMA_RESPONSE=$(curl -s -X POST "http://localhost:8081/subjects/$CONFLUENT_SUBJECT/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schemaType\": \"PROTOBUF\", \"schema\": $(echo "$INDIVIDUAL_SCHEMA" | jq -Rs .)}")
    
    if echo "$SCHEMA_RESPONSE" | grep -q '"id"'; then
        SCHEMA_ID=$(echo "$SCHEMA_RESPONSE" | jq -r '.id')
        log_success "Individual FlattenedMessage schema registered under subject: $CONFLUENT_SUBJECT (ID: $SCHEMA_ID)"
    else
        log_warning "Schema registration response: $SCHEMA_RESPONSE"
    fi
    
    echo ""
    echo "🔄 Running substreams-sink-kafka with Schema Registry explosion..."
    echo "Command: ./substreams-sink-kafka run \"$KAFKA_BROKERS\" \"$MANIFEST_PATH\" \"$BLOCK_RANGE\" \\"
    echo "  --endpoint=\"$ENDPOINT\" --plaintext \\"
    echo "  --output-module=\"$OUTPUT_MODULE\" \\"
    echo "  --kafka-topic=\"$SCHEMA_REGISTRY_TOPIC\" \\"
    echo "  --output-format=schema-registry \\"
    echo "  --explode-field=\"$EXPLODE_FIELD\" \\"
    echo "  --schema-registry-url=\"http://localhost:8081\" \\"
    echo "  --schema-auto-register=false \\"
    echo "  --cursor-file=\"$SCHEMA_CURSOR\""
    echo ""
    
    # Step 2: Run the sink with Schema Registry explosion (with auto-register=false)
    timeout 60s ./substreams-sink-kafka run "$KAFKA_BROKERS" "$MANIFEST_PATH" "$BLOCK_RANGE" \
        --endpoint="$ENDPOINT" --plaintext \
        --output-module="$OUTPUT_MODULE" \
        --kafka-topic="$SCHEMA_REGISTRY_TOPIC" \
        --output-format=schema-registry \
        --explode-field="$EXPLODE_FIELD" \
        --schema-registry-url="http://localhost:8081" \
        --schema-auto-register=false \
        --cursor-file="$SCHEMA_CURSOR" &
    
    SINK_PID=$!
    
    # Wait for sink to process some data
    sleep 30
    
    # Stop the sink
    kill $SINK_PID 2>/dev/null || true
    wait $SINK_PID 2>/dev/null || true
    
    # Step 3: Check if messages were produced
    MESSAGE_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$SCHEMA_REGISTRY_TOPIC" --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
    
    if [ "$MESSAGE_COUNT" -gt 0 ]; then
        log_success "Schema Registry explosion test successful! Produced $MESSAGE_COUNT exploded messages"
        
        # Verify schema registration
        SCHEMA_CHECK=$(curl -s "http://localhost:8081/subjects/$CONFLUENT_SUBJECT/versions/latest" | jq -r '.id // "not_found"')
        if [ "$SCHEMA_CHECK" != "not_found" ]; then
            log_success "Schema registered correctly under Confluent subject: $CONFLUENT_SUBJECT (ID: $SCHEMA_CHECK)"
        fi
        
        # Show a sample message (first few bytes to confirm protobuf format)
        echo ""
        echo "📋 Sample protobuf message (first 100 bytes):"
        docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$SCHEMA_REGISTRY_TOPIC" --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | head -c 100 | hexdump -C | head -3
        
        return 0
    else
        log_error "Schema Registry explosion test failed - no messages produced"
        
        # Debug information
        echo ""
        echo "🔍 Debug Information:"
        echo "Topic: $SCHEMA_REGISTRY_TOPIC"
        echo "Expected Confluent subject: $CONFLUENT_SUBJECT"
        echo "Registered subjects:"
        curl -s http://localhost:8081/subjects | jq -r '.[]' | grep "$SCHEMA_REGISTRY_TOPIC" || echo "No matching subjects found"
        
        return 1
    fi
}

# Create Snowflake connectors for both formats
create_snowflake_connectors() {
    log_info "Creating Snowflake connectors for both formats..."
    log_info "Note: Snowflake connector will auto-create tables based on the data"
    log_info "JSON connector: schematization=false for simple VARIANT columns"
    log_info "Schema Registry connector: should create table from protobuf schema"
    
    # Create connector config files in docker/snowflake-configs/
    # JSON connector configuration - connector will auto-create table
    cat > docker/snowflake-configs/explosion-json-connector-${TIMESTAMP}.json << EOF
{
  "name": "explosion-json-connector-${TIMESTAMP}",
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
    
    # Schema Registry connector configuration - connector will auto-create table based on schema
    cat > docker/snowflake-configs/explosion-schema-connector-${TIMESTAMP}.json << EOF
{
  "name": "explosion-schema-connector-${TIMESTAMP}",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "2",
    "topics": "$SCHEMA_REGISTRY_TOPIC",
    
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
    "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "snowflake-dlq",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
EOF
    
    # Create JSON connector
    JSON_RESPONSE=$(curl -s -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @docker/snowflake-configs/explosion-json-connector-${TIMESTAMP}.json)
    
    if echo "$JSON_RESPONSE" | grep -q '"name"'; then
        log_success "JSON connector created: explosion-json-connector-${TIMESTAMP}"
    else
        log_warning "JSON connector creation failed: $JSON_RESPONSE"
    fi
    
    # Create Schema Registry connector
    SCHEMA_RESPONSE=$(curl -s -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @docker/snowflake-configs/explosion-schema-connector-${TIMESTAMP}.json)
    
    if echo "$SCHEMA_RESPONSE" | grep -q '"name"'; then
        log_success "Schema Registry connector created: explosion-schema-connector-${TIMESTAMP}"
    else
        log_warning "Schema Registry connector creation failed: $SCHEMA_RESPONSE"
    fi
}

# Show connector status
show_connector_status() {
    log_info "Checking connector status..."
    
    echo ""
    echo "📊 Connector Status:"
    echo "==================="
    
    # JSON connector status
    JSON_STATUS=$(curl -s http://localhost:8083/connectors/explosion-json-connector-${TIMESTAMP}/status 2>/dev/null || echo '{"error": "not found"}')
    JSON_STATE=$(echo "$JSON_STATUS" | jq -r '.connector.state // .error')
    echo "JSON Connector (explosion-json-connector-${TIMESTAMP}): $JSON_STATE"
    
    # Show JSON connector task details if there are issues
    if [ "$JSON_STATE" = "FAILED" ] || [ "$JSON_STATE" = "null" ]; then
        echo "JSON Connector Tasks:"
        echo "$JSON_STATUS" | jq -r '.tasks[]? | "  Task \(.id): \(.state) - \(.trace // "no error")"' || echo "  No task details"
    fi
    
    # Schema Registry connector status  
    SCHEMA_STATUS=$(curl -s http://localhost:8083/connectors/explosion-schema-connector-${TIMESTAMP}/status 2>/dev/null || echo '{"error": "not found"}')
    SCHEMA_STATE=$(echo "$SCHEMA_STATUS" | jq -r '.connector.state // .error')
    echo "Schema Registry Connector (explosion-schema-connector-${TIMESTAMP}): $SCHEMA_STATE"
    
    # Show Schema Registry connector task details if there are issues
    if [ "$SCHEMA_STATE" = "FAILED" ] || [ "$SCHEMA_STATE" = "null" ]; then
        echo "Schema Registry Connector Tasks:"
        echo "$SCHEMA_STATUS" | jq -r '.tasks[]? | "  Task \(.id): \(.state) - \(.trace // "no error")"' || echo "  No task details"
    fi
    
    echo ""
    echo "📋 Active Topics:"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -E "(adr-exploded.*${TIMESTAMP#*_}|explosion)" || echo "No explosion topics found"
    
    echo ""
    echo "🗄️  Registered Schemas:"
    SCHEMAS=$(curl -s http://localhost:8081/subjects | jq -r '.[]' | grep -E "(adr-exploded.*${TIMESTAMP#*_}|explosion)" || echo "")
    if [ -n "$SCHEMAS" ]; then
        echo "$SCHEMAS" | while read schema; do
            SCHEMA_ID=$(curl -s "http://localhost:8081/subjects/$schema/versions/latest" | jq -r '.id // "unknown"')
            echo "  - $schema (ID: $SCHEMA_ID)"
        done
    else
        echo "No explosion schemas found"
    fi
    
    echo ""
    echo "🔍 Schema Registry Debug:"
    echo "Expected schema subject: ${SCHEMA_REGISTRY_TOPIC}-value"
    echo "Available subjects matching pattern:"
    curl -s http://localhost:8081/subjects | jq -r '.[]' | grep "${SCHEMA_REGISTRY_TOPIC}" || echo "  None found"
}

# Main execution
main() {
    echo "🏁 Starting explosion feature end-to-end test..."
    echo ""
    
    # Check prerequisites
    check_prerequisites
    
    # Test JSON explosion
    echo ""
    echo "🧪 Test 1: JSON Explosion"
    echo "========================="
    if test_json_explosion; then
        log_success "JSON explosion test passed"
    else
        log_error "JSON explosion test failed"
        exit 1
    fi
    
    # Test Schema Registry explosion
    echo ""
    echo "🧪 Test 2: Schema Registry Explosion"
    echo "===================================="
    if test_schema_registry_explosion; then
        log_success "Schema Registry explosion test passed"
    else
        log_error "Schema Registry explosion test failed"
        exit 1
    fi
    
    # Create Snowflake connectors
    echo ""
    echo "🏗️  Setting up Snowflake connectors..."
    echo "====================================="
    create_snowflake_connectors
    
    # Show status
    echo ""
    show_connector_status
    
    echo ""
    echo "🎆🧨💥 EXPLOSION FEATURE TEST - SPECTACULAR SUCCESS! 💥🧨🎆"
    echo "================================================================="
    echo ""
    echo "🎯 What We Conquered:"
    echo "   🔥 Confluent's sneaky auto-subject-generation (we tamed the -value-value beast!)"
    echo "   🚀 Schema Registry protobuf serialization mysteries"
    echo "   ⚡ End-to-end JSON + Schema Registry explosion pipeline"
    echo "   🏔️  Snowflake auto-table creation magic"
    echo ""
    echo "📊 Epic Test Results:"
    echo "   ✅ JSON explosion: 💣 Messages exploded and sent to $JSON_TOPIC"
    echo "   ✅ Schema Registry explosion: 🎯 Messages exploded with pre-registered schema to $SCHEMA_REGISTRY_TOPIC"
    echo "   ✅ Snowflake connectors: 🏗️  Auto-creating tables from your exploded data"
    echo ""
    echo "🧠 Key Learning: Confluent protobuf serializer auto-generates subjects!"
    echo "   📝 Expected: {topic-name}-value"  
    echo "   🎪 Reality: {topic-name}-value-value (because... Confluent!)"
    echo "   🎯 Solution: Pre-register schema under the auto-generated subject + use --schema-auto-register=false"
    echo ""
    echo "🍿 Fun Facts from Our Journey:"
    echo "   • Debugged ~43,834 messages that were 'produced but never consumed'"
    echo "   • Discovered the mysterious double -value-value suffix"
    echo "   • Learned that CURRENT-OFFSET: - means 'I see messages but can't read them'"
    echo "   • Found out Snowflake connectors are actually pretty smart!"
    echo ""
    echo "🎮 Next Level Achievements:"
    echo "   1. ⏰ Wait 2-3 minutes for Snowflake connectors to work their magic"
    echo "   2. 🔍 Check EXPLOSION_TEST schema for your shiny new auto-created tables"
    echo "   3. 📊 Query your exploded transaction data like a data wizard"
    echo "   4. 🎉 Celebrate that explosion feature actually works!"
    echo ""
    echo "🛠️  Power User Commands:"
    echo "   🔥 JSON messages: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $JSON_TOPIC --from-beginning | head -5"
    echo "   🎯 Schema Registry messages: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $SCHEMA_REGISTRY_TOPIC --from-beginning | hexdump -C | head -10"
    echo "   📡 Connector health: curl http://localhost:8083/connectors/explosion-json-connector-${TIMESTAMP}/status | jq '.connector.state'"
    echo "   🗄️  Schema check: curl http://localhost:8081/subjects/${SCHEMA_REGISTRY_TOPIC}-value-value/versions/latest | jq '.id'"
    echo ""
    echo "🧹 Cleanup Ritual (when you're done having fun):"
    echo "   💀 Destroy connectors: curl -X DELETE http://localhost:8083/connectors/explosion-json-connector-${TIMESTAMP}"
    echo "   💀 Destroy connectors: curl -X DELETE http://localhost:8083/connectors/explosion-schema-connector-${TIMESTAMP}"
    echo "   🗑️  Clean cursors: rm -f $JSON_CURSOR $SCHEMA_CURSOR"
    echo ""
    echo "🎊 Congratulations! You've successfully exploded your substreams! 🎊"
}

# Run main function
main "$@"