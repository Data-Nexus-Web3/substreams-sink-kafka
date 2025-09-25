#!/bin/bash

# Script to compare keys between two Kafka topics
# Usage: ./compare-kafka-topic-keys.sh <topic1> <topic2>

set -e

KAFKA_BROKER="localhost:9092"
TOPIC1=${1:-"adr-stream-output-new"}
TOPIC2=${2:-"flat-adr"}

echo "Comparing keys between topics: $TOPIC1 and $TOPIC2"
echo "Kafka broker: $KAFKA_BROKER"
echo ""

# Check if kafka-console-consumer is available
if ! command -v kafka-console-consumer &> /dev/null; then
    echo "kafka-console-consumer not found. Trying to use Docker..."
    KAFKA_CMD="docker exec kafka kafka-console-consumer"
else
    KAFKA_CMD="kafka-console-consumer"
fi

# Function to extract keys from a topic
extract_keys() {
    local topic=$1
    local output_file=$2
    
    echo "Extracting keys from topic: $topic"
    
    # Use timeout to prevent hanging and extract keys only
    timeout 30s $KAFKA_CMD \
        --bootstrap-server $KAFKA_BROKER \
        --topic $topic \
        --from-beginning \
        --property print.key=true \
        --property print.value=false \
        --property key.separator="|" \
        --timeout-ms 10000 2>/dev/null | \
        cut -d'|' -f1 | \
        grep -v "^$" | \
        sort > $output_file || true
    
    local count=$(wc -l < $output_file)
    echo "Found $count keys in $topic"
}

# Create temporary files
TEMP_DIR=$(mktemp -d)
KEYS1="$TEMP_DIR/keys1.txt"
KEYS2="$TEMP_DIR/keys2.txt"

# Extract keys from both topics
extract_keys $TOPIC1 $KEYS1
extract_keys $TOPIC2 $KEYS2

echo ""
echo "=== COMPARISON RESULTS ==="

# Count keys
COUNT1=$(wc -l < $KEYS1)
COUNT2=$(wc -l < $KEYS2)

echo "Topic $TOPIC1: $COUNT1 keys"
echo "Topic $TOPIC2: $COUNT2 keys"
echo ""

# Find keys in topic1 but not in topic2
ONLY_IN_TOPIC1="$TEMP_DIR/only_in_topic1.txt"
comm -23 $KEYS1 $KEYS2 > $ONLY_IN_TOPIC1

# Find keys in topic2 but not in topic1
ONLY_IN_TOPIC2="$TEMP_DIR/only_in_topic2.txt"
comm -13 $KEYS1 $KEYS2 > $ONLY_IN_TOPIC2

# Report differences
DIFF1_COUNT=$(wc -l < $ONLY_IN_TOPIC1)
DIFF2_COUNT=$(wc -l < $ONLY_IN_TOPIC2)

if [ $DIFF1_COUNT -gt 0 ]; then
    echo "Keys present in $TOPIC1 but NOT in $TOPIC2 ($DIFF1_COUNT keys):"
    cat $ONLY_IN_TOPIC1
    echo ""
fi

if [ $DIFF2_COUNT -gt 0 ]; then
    echo "Keys present in $TOPIC2 but NOT in $TOPIC1 ($DIFF2_COUNT keys):"
    cat $ONLY_IN_TOPIC2
    echo ""
fi

if [ $DIFF1_COUNT -eq 0 ] && [ $DIFF2_COUNT -eq 0 ]; then
    echo "✅ All keys match between both topics!"
else
    echo "❌ Found differences between topics"
fi

# Save results for further analysis
echo "Detailed results saved in: $TEMP_DIR"
echo "- $KEYS1 (all keys from $TOPIC1)"
echo "- $KEYS2 (all keys from $TOPIC2)"
echo "- $ONLY_IN_TOPIC1 (keys only in $TOPIC1)"
echo "- $ONLY_IN_TOPIC2 (keys only in $TOPIC2)"

# Don't cleanup temp dir so user can inspect files
echo ""
echo "Temp directory: $TEMP_DIR"
echo "Run 'rm -rf $TEMP_DIR' when done analyzing"