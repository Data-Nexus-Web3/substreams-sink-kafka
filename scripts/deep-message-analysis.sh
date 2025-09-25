#!/bin/bash

# Script to do a deeper analysis of the missing messages
set -e

echo "=== DEEP MESSAGE ANALYSIS ==="
echo ""

TEMP_DIR=$(mktemp -d)

echo "1. Getting raw message counts (no JSON parsing)..."

# Count raw messages without any JSON processing
adr_raw_count=$(docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 20000 2>/dev/null | wc -l)

flat_raw_count=$(docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flat-adr \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 20000 2>/dev/null | wc -l)

echo "Raw message counts:"
echo "adr-stream-output-new: $adr_raw_count"
echo "flat-adr: $flat_raw_count"
echo "Difference: $((adr_raw_count - flat_raw_count))"
echo ""

echo "2. Checking for malformed JSON messages..."

# Check for messages that fail JSON parsing
echo "Checking adr-stream-output-new for JSON parsing errors:"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 20000 2>/dev/null | \
    while IFS= read -r line; do
        if ! echo "$line" | jq . >/dev/null 2>&1; then
            echo "MALFORMED JSON: $line"
        fi
    done | head -5

echo ""
echo "3. Looking at the last few messages in each topic..."

echo "Last 3 messages from adr-stream-output-new:"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 20000 2>/dev/null | \
    tail -3 | \
    jq -r '.txn_id // "NO_TXN_ID"'

echo ""
echo "Last 3 messages from flat-adr:"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flat-adr \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 20000 2>/dev/null | \
    tail -3 | \
    jq -r '.txn_id // "NO_TXN_ID"'

echo ""
echo "4. Checking message timestamps to see if there are recent additions..."

echo "Getting timestamps of last few messages in adr-stream-output-new:"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --from-beginning \
    --max-messages 2000 \
    --property print.timestamp=true \
    --timeout-ms 20000 2>/dev/null | \
    tail -5

echo ""
echo "Results saved in: $TEMP_DIR"