#!/bin/bash

# Script to count total messages in Kafka topics
set -e

TOPIC1=${1:-"adr-stream-output-new"}
TOPIC2=${2:-"flat-adr"}

echo "Counting messages in topics..."
echo ""

# Function to count messages in a topic
count_messages() {
    local topic=$1
    
    echo "Counting messages in topic: $topic"
    
    # Count all messages (including duplicates)
    local count=$(docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --from-beginning \
        --max-messages 10000 \
        --property print.key=true \
        --property print.value=false \
        --timeout-ms 10000 2>/dev/null | wc -l)
    
    echo "Total messages in $topic: $count"
    
    # Count unique keys
    local unique_keys=$(docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --from-beginning \
        --max-messages 10000 \
        --property print.key=true \
        --property print.value=false \
        --timeout-ms 10000 2>/dev/null | sort | uniq | wc -l)
    
    echo "Unique keys in $topic: $unique_keys"
    echo ""
}

count_messages $TOPIC1
count_messages $TOPIC2

echo "=== ANALYSIS ==="
echo "If adr-stream-output-new has 1767 messages with 1 unique key,"
echo "and flat-adr has 1766 messages with 1766 unique keys,"
echo "then adr-stream-output-new has 1 extra message with the same key."