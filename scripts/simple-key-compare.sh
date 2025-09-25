#!/bin/bash

# Simple script to compare keys between two Kafka topics using Docker
set -e

TOPIC1=${1:-"adr-stream-output-new"}
TOPIC2=${2:-"flat-adr"}

echo "Comparing keys between topics: $TOPIC1 and $TOPIC2"
echo ""

# Function to extract keys using Docker
extract_keys_docker() {
    local topic=$1
    local output_file=$2
    
    echo "Extracting keys from topic: $topic"
    
    # Use Docker to run kafka-console-consumer
    docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --from-beginning \
        --max-messages 10000 \
        --property print.key=true \
        --property print.value=false \
        --property print.timestamp=false \
        --timeout-ms 5000 2>/dev/null | \
        grep -v "^null$" | \
        sort | uniq > $output_file || true
    
    local count=$(wc -l < $output_file)
    echo "Found $count unique keys in $topic"
}

# Create temporary files
TEMP_DIR=$(mktemp -d)
KEYS1="$TEMP_DIR/keys1.txt"
KEYS2="$TEMP_DIR/keys2.txt"

# Extract keys from both topics
extract_keys_docker $TOPIC1 $KEYS1
extract_keys_docker $TOPIC2 $KEYS2

echo ""
echo "=== COMPARISON RESULTS ==="

# Count keys
COUNT1=$(wc -l < $KEYS1)
COUNT2=$(wc -l < $KEYS2)

echo "Topic $TOPIC1: $COUNT1 unique keys"
echo "Topic $TOPIC2: $COUNT2 unique keys"
echo ""

# Find differences
comm -3 $KEYS1 $KEYS2 > $TEMP_DIR/differences.txt

if [ -s $TEMP_DIR/differences.txt ]; then
    echo "❌ Found differences:"
    echo ""
    
    # Keys only in topic1
    comm -23 $KEYS1 $KEYS2 > $TEMP_DIR/only_topic1.txt
    if [ -s $TEMP_DIR/only_topic1.txt ]; then
        echo "Keys only in $TOPIC1:"
        cat $TEMP_DIR/only_topic1.txt
        echo ""
    fi
    
    # Keys only in topic2
    comm -13 $KEYS1 $KEYS2 > $TEMP_DIR/only_topic2.txt
    if [ -s $TEMP_DIR/only_topic2.txt ]; then
        echo "Keys only in $TOPIC2:"
        cat $TEMP_DIR/only_topic2.txt
        echo ""
    fi
else
    echo "✅ All keys match between both topics!"
fi

echo "Results saved in: $TEMP_DIR"
echo "- $KEYS1 (keys from $TOPIC1)"
echo "- $KEYS2 (keys from $TOPIC2)"