#!/bin/bash

# Script to find the 3 missing messages between topics
set -e

echo "=== FINDING THE 3 MISSING MESSAGES ==="
echo ""

TEMP_DIR=$(mktemp -d)
ADR_MESSAGES="$TEMP_DIR/adr_messages.txt"
FLAT_MESSAGES="$TEMP_DIR/flat_messages.txt"

echo "Extracting all messages from both topics..."

# Extract all messages from adr-stream-output-new with their txn_id
echo "Getting messages from adr-stream-output-new..."
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic adr-stream-output-new \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 15000 2>/dev/null | \
    jq -r '.txn_id // .transaction_id // .id // "NO_TXN_ID"' | \
    sort > $ADR_MESSAGES

# Extract all messages from flat-adr with their txn_id  
echo "Getting messages from flat-adr..."
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flat-adr \
    --from-beginning \
    --max-messages 2000 \
    --timeout-ms 15000 2>/dev/null | \
    jq -r '.txn_id // .transaction_id // .id // "NO_TXN_ID"' | \
    sort > $FLAT_MESSAGES

echo ""
echo "Message counts:"
echo "adr-stream-output-new: $(wc -l < $ADR_MESSAGES)"
echo "flat-adr: $(wc -l < $FLAT_MESSAGES)"
echo ""

# Find messages in adr but not in flat
echo "=== MESSAGES IN adr-stream-output-new BUT NOT IN flat-adr ==="
comm -23 $ADR_MESSAGES $FLAT_MESSAGES > $TEMP_DIR/only_in_adr.txt
echo "Count: $(wc -l < $TEMP_DIR/only_in_adr.txt)"
if [ -s $TEMP_DIR/only_in_adr.txt ]; then
    echo "Transaction IDs:"
    cat $TEMP_DIR/only_in_adr.txt
fi
echo ""

# Find messages in flat but not in adr
echo "=== MESSAGES IN flat-adr BUT NOT IN adr-stream-output-new ==="
comm -13 $ADR_MESSAGES $FLAT_MESSAGES > $TEMP_DIR/only_in_flat.txt
echo "Count: $(wc -l < $TEMP_DIR/only_in_flat.txt)"
if [ -s $TEMP_DIR/only_in_flat.txt ]; then
    echo "Transaction IDs:"
    cat $TEMP_DIR/only_in_flat.txt
fi
echo ""

# Check for duplicates within each topic
echo "=== DUPLICATE ANALYSIS ==="
echo "Duplicates in adr-stream-output-new:"
sort $ADR_MESSAGES | uniq -d | head -5

echo ""
echo "Duplicates in flat-adr:"
sort $FLAT_MESSAGES | uniq -d | head -5

echo ""
echo "Results saved in: $TEMP_DIR"
echo "- $ADR_MESSAGES (all txn_ids from adr-stream-output-new)"
echo "- $FLAT_MESSAGES (all txn_ids from flat-adr)"
echo "- $TEMP_DIR/only_in_adr.txt (txn_ids only in adr-stream-output-new)"
echo "- $TEMP_DIR/only_in_flat.txt (txn_ids only in flat-adr)"