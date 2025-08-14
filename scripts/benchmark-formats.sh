#!/bin/bash
# Script to benchmark different output formats

set -e

echo "🚀 Substreams Kafka Sink - Format Benchmarking Tool"
echo "=================================================="

# Configuration
KAFKA_BROKERS="localhost:9092"
MANIFEST_PATH="/Users/thomasmonte/Repos/sentinel/substreams/adr/substreams.yaml"
ENDPOINT="10.0.11.152:9000"
TOPIC="adr-data"
BLOCK_RANGE="300100:300110"  # Small range for quick testing
OUTPUT_MODULE="map_adr_messages"

# Clean up function
cleanup() {
    echo "🧹 Cleaning up..."
    # Delete any existing connectors
    curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-json 2>/dev/null || true
    curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-protobuf 2>/dev/null || true
    curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-schema-registry 2>/dev/null || true
    
    # Clean up topics
    docker exec kafka kafka-topics --delete --topic adr-data --bootstrap-server localhost:9092 2>/dev/null || true
    sleep 2
    docker exec kafka kafka-topics --create --topic adr-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
}

# Test function
test_format() {
    local format=$1
    local description=$2
    local connector_config=$3
    
    echo ""
    echo "📊 Testing Format: $format ($description)"
    echo "----------------------------------------"
    
    # Clean up first
    cleanup
    
    # Start timing
    start_time=$(date +%s.%N)
    
    # Run the sink
    echo "🔄 Running substreams-sink-kafka with --output-format=$format..."
    substreams-sink-kafka run "$KAFKA_BROKERS" "$MANIFEST_PATH" "$BLOCK_RANGE" \
        --endpoint="$ENDPOINT" \
        --plaintext \
        --kafka-topic="$TOPIC" \
        --cursor-file="./benchmark-${format}-cursor.txt" \
        --kafka-batch-size=10000 \
        --kafka-batch-timeout-ms=25 \
        --output-module="$OUTPUT_MODULE" \
        --output-format="$format" \
        --schema-registry-url="http://localhost:8081" \
        --schema-subject="adr-data" \
        --schema-auto-register=true
    
    # Calculate sink timing
    end_time=$(date +%s.%N)
    sink_duration=$(echo "$end_time - $start_time" | bc)
    
    # Get message count and size
    echo "📏 Checking message statistics..."
    sleep 2
    
    # Get topic statistics
    offsets=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic adr-data --time -1)
    total_messages=$(echo "$offsets" | awk -F: '{sum+=$3} END {print sum}')
    
    # Sample a message to check size
    sample_message=$(docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic adr-data --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null | wc -c)
    
    echo "✅ Results for $format:"
    echo "   📊 Messages: $total_messages"
    echo "   📏 Sample message size: ~$sample_message bytes"
    echo "   ⏱️  Sink duration: ${sink_duration}s"
    echo "   🚀 Throughput: $(echo "scale=2; $total_messages / $sink_duration" | bc) msg/s"
    
    # Test Snowflake connector if config provided
    if [ -n "$connector_config" ]; then
        echo "🔄 Testing Snowflake connector..."
        connector_start=$(date +%s.%N)
        
        curl -X POST http://localhost:8083/connectors \
            -H "Content-Type: application/json" \
            -d @"docker/snowflake-configs/$connector_config" > /dev/null 2>&1
        
        # Wait for processing
        echo "⏳ Waiting for connector to process messages..."
        sleep 10
        
        # Check connector status
        status=$(curl -s http://localhost:8083/connectors/snowflake-sink-connector-${format}/status | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
        
        connector_end=$(date +%s.%N)
        connector_duration=$(echo "$connector_end - $connector_start" | bc)
        
        echo "   🔌 Connector status: $status"
        echo "   ⏱️  Connector duration: ${connector_duration}s"
        
        # Clean up connector
        curl -s -X DELETE http://localhost:8083/connectors/snowflake-sink-connector-${format} > /dev/null 2>&1 || true
    fi
    
    echo "✅ $format test completed!"
}

# Main execution
echo "🏁 Starting benchmark tests..."

# Test 1: JSON Format
test_format "json" "Direct JSON serialization" "snowflake-connector-json.json"

# Test 2: Raw Protobuf Format  
test_format "protobuf" "Raw protobuf bytes" "snowflake-connector-protobuf.json"

# Test 3: Schema Registry Format
test_format "schema-registry" "Confluent Schema Registry" "snowflake-connector-schema-registry.json"

echo ""
echo "🎉 Benchmark completed!"
echo "📊 Summary:"
echo "   • JSON: Human-readable, largest size, simple debugging"
echo "   • Protobuf: Compact binary, fastest processing, minimal overhead"  
echo "   • Schema Registry: Structured versioning, most complex, best for evolution"
echo ""
echo "💡 Recommendation: Use protobuf for production, JSON for debugging"