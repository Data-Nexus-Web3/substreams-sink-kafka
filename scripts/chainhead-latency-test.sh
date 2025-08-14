#!/bin/bash

# Real-time Chain Head Latency Testing Script
# Measures end-to-end latency: Firehose -> Substream -> Kafka -> Snowflake
# Runs continuously at chain head until stopped

set -e

echo "🔥 CHAIN HEAD LATENCY TESTING"
echo "============================="
echo "📊 Measuring: Firehose → Substream → Kafka → Snowflake"
echo "🎯 Module: map_block_info (block + timestamp data)"
echo "⏱️  Running continuously at chain head until stopped..."
echo ""

# Configuration
KAFKA_BROKERS="localhost:9092"
MANIFEST_PATH="/Users/thomasmonte/Repos/sentinel/substreams/adr/substreams.yaml"
ENDPOINT="eth.substreams.pinax.network:443"
TOPIC="block-info-latency"
OUTPUT_MODULE="map_block_info"
CURSOR_FILE="chainhead-latency-cursor.txt"

# Snowflake connector config for block info
CONNECTOR_NAME="snowflake-sink-block-info-latency"
TABLE_NAME="block_info_latency_test"

# Function to create Snowflake connector config for block info
create_block_info_connector_config() {
    local format=$1
    local config_file="block-info-connector-${format}.json"
    
    echo "📝 Creating connector config for $format format..."
    
    case $format in
        "json")
            cat > "$config_file" << EOF
{
  "name": "${CONNECTOR_NAME}-${format}",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "4",
    "topics": "$TOPIC",
    "snowflake.topic2table.map": "$TOPIC:${TABLE_NAME}_${format}",
    
    "snowflake.url.name": "DTCC-DEVX1_US_WEST_2.snowflakecomputing.com:443",
    "snowflake.user.name": "TMONTE",
    "snowflake.private.key": "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC2tRd01CurbjBaDzZIsAqc/9Yx7J7koQvqHHwcJc88mGSls4/CG3FbdSe23iJuozqxv1SWLm0ZlXSoIhrX47WO9/6A5nLs3g7a7pnUMTRr0WoD9R9QdHOH0k+rnDHflxV+61T+OyXzp4axOTuowlaiD0Z7UwKcwwxFIdc8p+YnXOhMjzJ2SuJaO37Nn7jSXX5aAXekdyK5dQ3FG40NJGw3qQpmXk1aEEHR9zvt4HDmT+iJld/1aoLcDFuHYXPZZzkEB6iSrpR8dW/H9g+M6niGw1SGNZETIFToUNtqnei5eEl6qsMhYSoPCFLoiJ6xEh3XM3N1ixEc0gSXHACqCpz3AgMBAAECggEACJpyZk/vVn0zzanVtoqDlWz+mw83VDC5LOb2eSTWo1XRt3PDGzLLnbDgk93V5TvOcbw+sWuyO3gsLBafCPdx/y9yToOLedfi/zApEjLW8xTDVzhdpx40qnqewcfzGUI2Aun18Jq8aMLEtuepBYNRLQRnHzuDbfpJQ6AaUILfdY74Cj5Puq0+tjmN3Uk1asRWNEe/zWyTF6DouoaTpEAMGUoYa6bYeq/ldWVxRprHQTFSYJ236Vv5G3OWC98gxYx9/Hyxz7lxR+4wrJUWAS85bEuicE71BWZXqRlJZe2da0a/QnbU14Bd6dP4kCFXBE1ka7Y2qlA2hAXQZYp38xRQIQKBgQDcYd7B3ScAjyOmxk395Dwgx1fmGRU443Fx6bMMNy8TvT5+OOoFnCGHtlpaMvVutUt9+Tp2G11g+qALEn+DPxfQIOAUvK1U0sJ4o5VBB0ciAbVk0KZx5zyhYKsOG7dL2Y1/TZWCYYBnqLBwT/wFxaM2lOMhac94athqoy0MziiiPwKBgQDUPHQ3cWgfOWbPVOBa3riYa4LnjgF5/tP2fbnzk8kvOLMTFXVDzIBiZl0dyS6Lu9YpsrFPtuNIwFlxL5m3JD4xjtDIECKt4Sg2yaUc+axpo6kAeI3ZoBRkGuXNX5Lh0c0MG6ekxBdkem2WRMW9yA9x3mIKqRvr9nf9jX6sY5NnSQKBgQCMTlL/oci+9mKAPyhQSApv4/n2KGn2efczytGPKWLzcjxjJ+D2CVzfhh0n5+GRxiJ15UJvByUAJQ/XfMNpz7VdcFC5YxYHNtdQ7vSLHhMPd38A3EXbpphXNbAxnzunMq5/KenRxDl3xVHvbzzIf/dVPJ0OHMtVotB9s71utaHxCQKBgQCOVbzbMgNSbXy1r4aHarcCAZhZErOKzYv503fSE1rpgG0Yb1uljJDNbTklsLa2n9KRRHYFr/Hd6KITdojwei37rnv2sFuRoV8G31UMiaVHc6rz1eXL/b+vIxrbES3ApKOPazyTE69cg37bOGQlUvaIt0upOqCvLyxpr2s00dMZgQKBgQCtyTHocPCjgIuHPdrQHOLNh5upWxB3biIVxZ+beVdmwlNm8O7vWe8MbITfaTxoedcF+OEOf04jmzfEYUh9TPKdd+tMOtsQElSJJuef3WuSw9ic+Omhx6AatoH1loXQ975HOB7WYXx4vLdZ/wOD2NDuG7SCN2mCZVFlmS5GNIYRGA==",
    "snowflake.database.name": "SENTINEL_DB",
    "snowflake.schema.name": "LATENCY_BENCHMARKING",
    "snowflake.role.name": "SENTINEL_DB_ADMIN",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "false",
    
    "enable.streaming.client.optimization": "true",
    "snowflake.streaming.enable.single.buffer": "true",
    "snowflake.streaming.max.client.lag": "1",
    
    "buffer.flush.time": "1",
    "buffer.count.records": "100",
    "buffer.size.bytes": "100000",
    
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "snowflake-dlq",
    "consumer.override.auto.offset.reset": "latest"
  }
}
EOF
            ;;
        "schema-registry")
            cat > "$config_file" << EOF
{
  "name": "${CONNECTOR_NAME}-${format}",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "4",
    "topics": "$TOPIC",
    "snowflake.topic2table.map": "$TOPIC:${TABLE_NAME}_SCHEMA_REGISTRY",
    
    "snowflake.url.name": "DTCC-DEVX1_US_WEST_2.snowflakecomputing.com:443",
    "snowflake.user.name": "TMONTE",
    "snowflake.private.key": "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC2tRd01CurbjBaDzZIsAqc/9Yx7J7koQvqHHwcJc88mGSls4/CG3FbdSe23iJuozqxv1SWLm0ZlXSoIhrX47WO9/6A5nLs3g7a7pnUMTRr0WoD9R9QdHOH0k+rnDHflxV+61T+OyXzp4axOTuowlaiD0Z7UwKcwwxFIdc8p+YnXOhMjzJ2SuJaO37Nn7jSXX5aAXekdyK5dQ3FG40NJGw3qQpmXk1aEEHR9zvt4HDmT+iJld/1aoLcDFuHYXPZZzkEB6iSrpR8dW/H9g+M6niGw1SGNZETIFToUNtqnei5eEl6qsMhYSoPCFLoiJ6xEh3XM3N1ixEc0gSXHACqCpz3AgMBAAECggEACJpyZk/vVn0zzanVtoqDlWz+mw83VDC5LOb2eSTWo1XRt3PDGzLLnbDgk93V5TvOcbw+sWuyO3gsLBafCPdx/y9yToOLedfi/zApEjLW8xTDVzhdpx40qnqewcfzGUI2Aun18Jq8aMLEtuepBYNRLQRnHzuDbfpJQ6AaUILfdY74Cj5Puq0+tjmN3Uk1asRWNEe/zWyTF6DouoaTpEAMGUoYa6bYeq/ldWVxRprHQTFSYJ236Vv5G3OWC98gxYx9/Hyxz7lxR+4wrJUWAS85bEuicE71BWZXqRlJZe2da0a/QnbU14Bd6dP4kCFXBE1ka7Y2qlA2hAXQZYp38xRQIQKBgQDcYd7B3ScAjyOmxk395Dwgx1fmGRU443Fx6bMMNy8TvT5+OOoFnCGHtlpaMvVutUt9+Tp2G11g+qALEn+DPxfQIOAUvK1U0sJ4o5VBB0ciAbVk0KZx5zyhYKsOG7dL2Y1/TZWCYYBnqLBwT/wFxaM2lOMhac94athqoy0MziiiPwKBgQDUPHQ3cWgfOWbPVOBa3riYa4LnjgF5/tP2fbnzk8kvOLMTFXVDzIBiZl0dyS6Lu9YpsrFPtuNIwFlxL5m3JD4xjtDIECKt4Sg2yaUc+axpo6kAeI3ZoBRkGuXNX5Lh0c0MG6ekxBdkem2WRMW9yA9x3mIKqRvr9nf9jX6sY5NnSQKBgQCMTlL/oci+9mKAPyhQSApv4/n2KGn2efczytGPKWLzcjxjJ+D2CVzfhh0n5+GRxiJ15UJvByUAJQ/XfMNpz7VdcFC5YxYHNtdQ7vSLHhMPd38A3EXbpphXNbAxnzunMq5/KenRxDl3xVHvbzzIf/dVPJ0OHMtVotB9s71utaHxCQKBgQCOVbzbMgNSbXy1r4aHarcCAZhZErOKzYv503fSE1rpgG0Yb1uljJDNbTklsLa2n9KRRHYFr/Hd6KITdojwei37rnv2sFuRoV8G31UMiaVHc6rz1eXL/b+vIxrbES3ApKOPazyTE69cg37bOGQlUvaIt0upOqCvLyxpr2s00dMZgQKBgQCtyTHocPCjgIuHPdrQHOLNh5upWxB3biIVxZ+beVdmwlNm8O7vWe8MbITfaTxoedcF+OEOf04jmzfEYUh9TPKdd+tMOtsQElSJJuef3WuSw9ic+Omhx6AatoH1loXQ975HOB7WYXx4vLdZ/wOD2NDuG7SCN2mCZVFlmS5GNIYRGA==",
    "snowflake.database.name": "SENTINEL_DB",
    "snowflake.schema.name": "LATENCY_BENCHMARKING",
    "snowflake.role.name": "SENTINEL_DB_ADMIN",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "true",
    "schema.registry.url": "http://schema-registry:8081",
    
    "enable.streaming.client.optimization": "true",
    "snowflake.streaming.enable.single.buffer": "true",
    "snowflake.streaming.max.client.lag": "1",
    
    "buffer.flush.time": "1",
    "buffer.count.records": "100",
    "buffer.size.bytes": "100000",
    
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "snowflake-dlq",
    "consumer.override.auto.offset.reset": "latest"
  }
}
EOF
            ;;
    esac
    
    echo "✅ Created $config_file"
}

# Function to setup environment
setup_environment() {
    local format=$1
    
    echo "🔧 Setting up environment for $format format..."
    
    # Clean up any existing connectors
    curl -s -X DELETE "http://localhost:8083/connectors/${CONNECTOR_NAME}-json" 2>/dev/null || true
    curl -s -X DELETE "http://localhost:8083/connectors/${CONNECTOR_NAME}-schema-registry" 2>/dev/null || true
    sleep 2
    
    # Create/recreate topic for fresh start
    docker exec kafka kafka-topics --delete --topic "$TOPIC" --bootstrap-server localhost:9092 2>/dev/null || true
    sleep 3
    docker exec kafka kafka-topics --create --topic "$TOPIC" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || true
    
    # Create connector config and deploy
    create_block_info_connector_config "$format"
    
    echo "📡 Deploying connector..."
    curl -s -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @"block-info-connector-${format}.json" > /dev/null
    
    # Wait for connector to be ready
    echo "⏳ Waiting for connector to be ready..."
    sleep 10
    
    echo "✅ Environment setup complete!"
}

# Function to run latency test
run_latency_test() {
    local format=$1
    
    echo ""
    echo "🚀 STARTING CHAIN HEAD LATENCY TEST - $format FORMAT"
    echo "=================================================="
    echo "📊 Module: $OUTPUT_MODULE"
    echo "🎯 Topic: $TOPIC"
    echo "🏔️  Table: ${TABLE_NAME}_$(echo ${format} | tr '-' '_' | tr '[:lower:]' '[:upper:]')"
    echo "⏱️  Mode: CONTINUOUS (Ctrl+C to stop)"
    echo ""
    
    # Remove old cursor to start fresh
    rm -f "$CURSOR_FILE"
    
    # Build the command - start near chain head for latency testing
    local start_block="23133458"  # Current Ethereum chain head
    local cmd="./bin/substreams-sink-kafka run \"$KAFKA_BROKERS\" \"$MANIFEST_PATH\" \"$start_block:\" \
               --endpoint=\"$ENDPOINT\" --kafka-topic=\"$TOPIC\" \
               --cursor-file=\"./$CURSOR_FILE\" --kafka-batch-size=1000 --kafka-batch-timeout-ms=100 \
               --output-module=\"$OUTPUT_MODULE\" --output-format=\"$format\""
    
    # Add schema registry parameters if needed
    if [ "$format" = "schema-registry" ]; then
        cmd="$cmd --schema-registry-url=\"http://localhost:8081\" --schema-subject=\"${TOPIC}\" --schema-auto-register"
    fi
    
    echo "🎯 Command: $cmd"
    echo ""
    echo "🔥 STARTING CONTINUOUS CHAIN HEAD PROCESSING..."
    echo "   Press Ctrl+C to stop and see latency analysis"
    echo ""
    
    # Run the command (this will run until interrupted)
    eval $cmd
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [format]"
    echo ""
    echo "Formats:"
    echo "  json             - JSON format for easy debugging"
    echo "  schema-registry  - Schema Registry format for production"
    echo ""
    echo "Example:"
    echo "  $0 json                    # Test with JSON format"
    echo "  $0 schema-registry         # Test with Schema Registry format"
    echo ""
    echo "The script will:"
    echo "  1. Setup a fresh Kafka topic: $TOPIC"
    echo "  2. Deploy Snowflake connector for block info data"
    echo "  3. Run substreams continuously at chain head"
    echo "  4. Measure end-to-end latency from firehose to Snowflake"
}

# Main execution
main() {
    if [ $# -eq 0 ]; then
        show_usage
        exit 1
    fi
    
    local format=$1
    
    # Validate format
    case $format in
        "json"|"schema-registry")
            echo "✅ Valid format: $format"
            ;;
        *)
            echo "❌ Invalid format: $format"
            show_usage
            exit 1
            ;;
    esac
    
    # Ensure binary is built
    echo "🔨 Building latest binary..."
    go build -o ./bin/substreams-sink-kafka ./cmd/substreams-sink-kafka
    
    # Setup environment
    setup_environment "$format"
    
    # Run the test
    run_latency_test "$format"
}

# Handle Ctrl+C gracefully
trap 'echo -e "\n\n🛑 Test stopped by user. Check Snowflake for latency data!"' INT

# Run main function
main "$@"