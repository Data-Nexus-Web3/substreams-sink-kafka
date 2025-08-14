#!/bin/bash

# Comprehensive benchmark script for all output formats
# Tests JSON, Protobuf, and Schema Registry formats over 100,000 blocks

set -e

# Configuration
KAFKA_BROKERS="localhost:9092"
SUBSTREAMS_MANIFEST="/Users/thomasmonte/Repos/sentinel/substreams/adr/substreams.yaml"
ENDPOINT="10.0.11.152:9000"
TOPIC="adr-data"
SCHEMA_REGISTRY_URL="http://localhost:8081"
SCHEMA_SUBJECT="adr-data"
OUTPUT_MODULE="map_adr_messages"

# Benchmark parameters - 100,000 blocks
START_BLOCK=300000
END_BLOCK=400000
TOTAL_BLOCKS=$((END_BLOCK - START_BLOCK))

echo "🚀 Starting comprehensive benchmark of all formats"
echo "📊 Block range: $START_BLOCK to $END_BLOCK ($TOTAL_BLOCKS blocks)"
echo "=================================="

# Function to clean topic
clean_topic() {
    echo "🧹 Cleaning Kafka topic..."
    
    # Try to delete the topic, ignore errors
    docker exec kafka kafka-topics --delete --topic $TOPIC --bootstrap-server localhost:9092 2>/dev/null || echo "Topic delete failed or topic didn't exist"
    sleep 5
    
    # Try to create the topic, if it exists just continue
    if docker exec kafka kafka-topics --create --topic $TOPIC --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null; then
        echo "✅ Topic created successfully"
    else
        echo "⚠️ Topic already exists, continuing..."
        # Consume any existing messages to clear the topic
        timeout 5 docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning --timeout-ms 2000 > /dev/null 2>&1 || true
    fi
}

# Function to remove all connectors
remove_connectors() {
    echo "🗑️ Removing existing connectors..."
    curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-json 2>/dev/null || true
    curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-protobuf 2>/dev/null || true
    curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-schema-registry 2>/dev/null || true
    sleep 2
    echo "✅ Connectors removed"
}

# Function to benchmark a format
benchmark_format() {
    local format=$1
    local connector_config=$2
    local cursor_file="benchmark-${format}-cursor.txt"
    
    echo ""
    echo "🔥 BENCHMARKING $format FORMAT"
    echo "=================================="
    
    # Clean environment
    clean_topic
    remove_connectors
    
    # Start appropriate connector
    echo "📡 Starting $format connector..."
    curl -s -X POST http://localhost:8083/connectors \
         -H "Content-Type: application/json" \
         -d @docker/snowflake-configs/$connector_config > /dev/null
    
    # Wait for connector to be ready
    sleep 5
    
    # Remove old cursor file
    rm -f $cursor_file
    
    # Build command based on format
    local cmd="substreams-sink-kafka run \"$KAFKA_BROKERS\" \"$SUBSTREAMS_MANIFEST\" \"$START_BLOCK:$END_BLOCK\" \
               --endpoint=\"$ENDPOINT\" --plaintext --kafka-topic=\"$TOPIC\" \
               --cursor-file=\"./$cursor_file\" --kafka-batch-size=10000 --kafka-batch-timeout-ms=25 \
               --output-module=\"$OUTPUT_MODULE\" --output-format=\"$format\""
    
    # Add schema registry specific parameters
    if [ "$format" = "schema-registry" ]; then
        cmd="$cmd --schema-registry-url=\"$SCHEMA_REGISTRY_URL\" --schema-subject=\"$SCHEMA_SUBJECT\""
    fi
    
    echo "⚡ Starting benchmark run..."
    echo "Command: $cmd"
    
    # Run the benchmark and capture timing
    start_time=$(date +%s)
    eval $cmd
    end_time=$(date +%s)
    
    # Calculate metrics
    duration=$((end_time - start_time))
    blocks_per_second=$((TOTAL_BLOCKS / duration))
    
    echo ""
    echo "📈 $format FORMAT RESULTS:"
    echo "  Duration: ${duration}s"
    echo "  Blocks processed: $TOTAL_BLOCKS"
    echo "  Blocks/second: $blocks_per_second"
    echo "  Total time: $(date -u -d @${duration} +%H:%M:%S)"
    
    # Wait for Snowflake ingestion to complete
    echo "⏳ Waiting for Snowflake ingestion to complete..."
    sleep 30
    
    # Check connector status
    echo "📊 Final connector status:"
    docker logs kafka-connect --tail 5 | grep -E "(Successfully|safe to commit)" | tail -2
    
    echo "✅ $format benchmark completed!"
}

# Main benchmark execution
main() {
    echo "🏁 Starting comprehensive format benchmark"
    
    # Using system binary from PATH (~/go/bin)
    echo "🔍 Using system substreams-sink-kafka binary from PATH..."
    which substreams-sink-kafka
    
    # Benchmark each format
    benchmark_format "json" "snowflake-connector-json.json"
    benchmark_format "protobuf" "snowflake-connector-protobuf.json" 
    benchmark_format "schema-registry" "snowflake-connector-schema-registry.json"
    
    echo ""
    echo "🎉 ALL BENCHMARKS COMPLETED!"
    echo "=================================="
    echo "Check Snowflake tables for data comparison:"
    echo "  - adr_listener_messages_json"
    echo "  - adr_listener_messages_protobuf" 
    echo "  - adr_listener_messages_schema_registry"
    echo ""
    echo "🚀 Your beast mode system has been fully benchmarked!"
}

# Run the benchmark
main "$@"