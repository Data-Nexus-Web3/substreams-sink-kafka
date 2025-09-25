#!/bin/bash

# Script to count messages by txn_id field in Kafka topics
set -e

TOPIC1=${1:-"adr-stream-output-new"}
TOPIC2=${2:-"flat-adr"}

echo "Analyzing messages by txn_id in topics: $TOPIC1 and $TOPIC2"
echo ""

# Function to extract and count txn_ids from a topic
count_by_txn_id() {
    local topic=$1
    local output_file=$2
    
    echo "Extracting txn_ids from topic: $topic"
    
    # Extract messages and parse JSON to get txn_id field
    docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --from-beginning \
        --max-messages 5000 \
        --timeout-ms 10000 2>/dev/null | \
        jq -r '.txn_id // .transaction_id // .id // empty' 2>/dev/null | \
        grep -v "^$" | \
        sort | uniq -c | sort -nr > $output_file || true
    
    local total_txns=$(wc -l < $output_file)
    local total_messages=$(awk '{sum += $1} END {print sum}' $output_file)
    
    echo "Found $total_txns unique txn_ids with $total_messages total messages in $topic"
    
    # Show top 10 most frequent txn_ids
    echo "Top 10 most frequent txn_ids in $topic:"
    head -10 $output_file
    echo ""
}

# Create temporary files
TEMP_DIR=$(mktemp -d)
TXN_COUNTS1="$TEMP_DIR/txn_counts1.txt"
TXN_COUNTS2="$TEMP_DIR/txn_counts2.txt"

# Count txn_ids in both topics
count_by_txn_id $TOPIC1 $TXN_COUNTS1
count_by_txn_id $TOPIC2 $TXN_COUNTS2

echo "=== COMPARISON ==="

# Find txn_ids that appear more than once in topic1
echo "Duplicate txn_ids in $TOPIC1 (count > 1):"
awk '$1 > 1 {print $1, $2}' $TXN_COUNTS1 | head -10

echo ""
echo "Duplicate txn_ids in $TOPIC2 (count > 1):"
awk '$1 > 1 {print $1, $2}' $TXN_COUNTS2 | head -10

echo ""
echo "Results saved in: $TEMP_DIR"
echo "- $TXN_COUNTS1 (txn_id counts from $TOPIC1)"
echo "- $TXN_COUNTS2 (txn_id counts from $TOPIC2)"