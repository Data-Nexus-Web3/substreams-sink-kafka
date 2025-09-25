#!/bin/bash

# Script to try to inspect the missing messages
set -e

echo "=== TRYING TO ACCESS THE MISSING MESSAGES ==="
echo ""

echo "1. Trying to consume from specific offsets near the end..."
echo ""

# Try to consume from the last few offsets in each partition
echo "=== PARTITION 0 ==="
echo "Expected messages: 598 (offsets 0-597)"
echo "Consumable messages: 597"
echo "Trying to get message at offset 597 (the missing one):"

docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --partition 0 \
    --offset 597 \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null || echo "Could not consume offset 597"

echo ""
echo "=== PARTITION 1 ==="
echo "Expected messages: 612 (offsets 0-611)" 
echo "Consumable messages: 611"
echo "Trying to get message at offset 611 (the missing one):"

docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --partition 1 \
    --offset 611 \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null || echo "Could not consume offset 611"

echo ""
echo "=== PARTITION 2 ==="
echo "Expected messages: 559 (offsets 14136-14694)"
echo "Consumable messages: 558" 
echo "Trying to get message at offset 14694 (the missing one):"

docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --partition 2 \
    --offset 14694 \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null || echo "Could not consume offset 14694"

echo ""
echo "2. Trying to use kafka-dump-log to inspect the log files directly..."
echo ""

# Try to dump the log segments to see if we can find the missing messages
echo "Attempting to dump log segments (this might not work in Docker):"
docker exec kafka find /var/lib/kafka/data -name "*.log" -path "*adr-stream-output-new*" | head -3

echo ""
echo "3. Checking for any transaction markers or control records..."
echo ""

# Try to see if there are any transaction markers
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --from-beginning \
    --property print.key=true \
    --property print.headers=true \
    --property print.timestamp=true \
    --max-messages 5 \
    --timeout-ms 5000 2>/dev/null | tail -5

echo ""
echo "4. Trying to consume with different consumer settings..."
echo ""

# Try with isolation level read_uncommitted to see uncommitted messages
echo "Trying with read_uncommitted isolation level:"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --partition 0 \
    --from-beginning \
    --isolation-level read_uncommitted \
    --max-messages 600 \
    --timeout-ms 10000 2>/dev/null | wc -l