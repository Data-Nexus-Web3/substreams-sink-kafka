#!/bin/bash

# Script to analyze the discrepancy between topics
set -e

echo "=== KAFKA TOPIC ANALYSIS ==="
echo ""

echo "Getting exact message counts from offsets:"
echo ""

# Get adr-stream-output-new offsets
echo "adr-stream-output-new partitions:"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic adr-stream-output-new --time -1
adr_total=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic adr-stream-output-new --time -1 | awk -F: '{sum += $3} END {print sum}')
echo "Total messages in adr-stream-output-new: $adr_total"
echo ""

# Get flat-adr offsets  
echo "flat-adr partitions:"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic flat-adr --time -1
flat_total=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic flat-adr --time -1 | awk -F: '{sum += $3} END {print sum}')
echo "Total messages in flat-adr: $flat_total"
echo ""

echo "=== ANALYSIS ==="
echo "Difference: $((adr_total - flat_total)) messages"
echo ""

if [ $adr_total -gt $flat_total ]; then
    echo "adr-stream-output-new has MORE messages than flat-adr"
    echo "This suggests that some messages in adr-stream-output-new are not being"
    echo "properly processed/exploded into flat-adr, OR there are duplicate messages"
    echo "in adr-stream-output-new that shouldn't be there."
elif [ $flat_total -gt $adr_total ]; then
    echo "flat-adr has MORE messages than adr-stream-output-new"
    echo "This suggests that the explosion process is creating more messages"
    echo "than expected from the source."
else
    echo "Both topics have the same number of messages - no discrepancy!"
fi

echo ""
echo "=== RECOMMENDATIONS ==="
echo "1. Check for duplicate messages in adr-stream-output-new"
echo "2. Verify the explosion logic is working correctly"
echo "3. Check if there are any failed/retried messages"
echo "4. Look at the timestamps to see when the extra messages were created"