#!/bin/bash

# Debug DLQ Issue Script
# This script investigates why messages are going to DLQ

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

echo "🔍 KAFKA CONNECT DLQ DEBUG INVESTIGATION"
echo "========================================"

# Get the latest connector name
CONNECTOR_NAME=$(curl -s http://localhost:8083/connectors | jq -r '.[]' | grep schema-explosion-connector | head -1)

if [ -z "$CONNECTOR_NAME" ]; then
    log_error "No schema-explosion-connector found!"
    exit 1
fi

log_info "Investigating connector: $CONNECTOR_NAME"

echo ""
echo "📊 CONNECTOR STATUS & CONFIGURATION"
echo "===================================="

# Get connector status
echo "🔍 Connector Status:"
CONNECTOR_STATUS=$(curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/status")
echo "$CONNECTOR_STATUS" | jq '.'

echo ""
echo "🔍 Connector Configuration:"
curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/config" | jq '.'

echo ""
echo "📋 TASK DETAILS"
echo "==============="

# Get task details
TASK_COUNT=$(echo "$CONNECTOR_STATUS" | jq '.tasks | length')
echo "Number of tasks: $TASK_COUNT"

for i in $(seq 0 $((TASK_COUNT-1))); do
    echo ""
    echo "🔍 Task $i Status:"
    curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/tasks/$i/status" | jq '.'
done

echo ""
echo "🔍 SCHEMA REGISTRY INVESTIGATION"
echo "================================"

# Check what schemas are registered
echo "📋 All registered subjects:"
curl -s http://localhost:8081/subjects | jq -r '.[]' | sort

echo ""
echo "🔍 FlattenedMessage schema details:"
FLATTENED_SUBJECT="FlattenedMessage-value"
if curl -s "http://localhost:8081/subjects/$FLATTENED_SUBJECT/versions/latest" | jq -e '.id' > /dev/null 2>&1; then
    echo "Schema ID: $(curl -s "http://localhost:8081/subjects/$FLATTENED_SUBJECT/versions/latest" | jq -r '.id')"
    echo "Schema version: $(curl -s "http://localhost:8081/subjects/$FLATTENED_SUBJECT/versions/latest" | jq -r '.version')"
    echo "Schema:"
    curl -s "http://localhost:8081/subjects/$FLATTENED_SUBJECT/versions/latest" | jq -r '.schema' | head -10
else
    log_warning "FlattenedMessage-value schema not found!"
fi

echo ""
echo "🔍 DLQ ANALYSIS"
echo "==============="

# Count DLQ messages
DLQ_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic snowflake-dlq --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
echo "Total DLQ messages: $DLQ_COUNT"

if [ "$DLQ_COUNT" -gt 0 ]; then
    echo ""
    echo "🔍 Sample DLQ messages (first 3):"
    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic snowflake-dlq --from-beginning --max-messages 3 --timeout-ms 10000 2>/dev/null | while IFS= read -r line; do
        echo "---"
        echo "$line"
        echo "Hex dump:"
        echo "$line" | xxd | head -3
    done
fi

echo ""
echo "🔍 KAFKA CONNECT LOGS"
echo "====================="

echo "🔍 Recent Connect logs (last 50 lines):"
docker logs connect --tail 50 | grep -E "(ERROR|WARN|Exception|Failed)" || echo "No errors/warnings in recent logs"

echo ""
echo "🔍 TOPIC INVESTIGATION"
echo "====================="

# Get topic from connector config
TOPIC=$(curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/config" | jq -r '.topics')
echo "Target topic: $TOPIC"

# Check topic details
echo ""
echo "🔍 Topic details:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic "$TOPIC"

echo ""
echo "🔍 Sample messages from topic (first 3):"
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000 2>/dev/null | while IFS= read -r line; do
    echo "---"
    echo "Raw message length: ${#line}"
    echo "First 200 chars: ${line:0:200}"
    echo "Hex dump (first 100 bytes):"
    echo "$line" | xxd | head -5
done

echo ""
echo "🔍 CONVERTER COMPATIBILITY CHECK"
echo "================================"

# Check if the connector's value converter matches what we're producing
CONNECTOR_VALUE_CONVERTER=$(curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/config" | jq -r '."value.converter"')
CONNECTOR_SCHEMA_REGISTRY_URL=$(curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/config" | jq -r '."value.converter.schema.registry.url"')

echo "Connector value converter: $CONNECTOR_VALUE_CONVERTER"
echo "Connector schema registry URL: $CONNECTOR_SCHEMA_REGISTRY_URL"

# Test schema registry connectivity from connector's perspective
echo ""
echo "🔍 Testing Schema Registry connectivity:"
if curl -s "$CONNECTOR_SCHEMA_REGISTRY_URL/subjects" > /dev/null; then
    log_success "Schema Registry accessible from connector"
else
    log_error "Schema Registry NOT accessible from connector!"
fi

echo ""
echo "🎯 SUMMARY & RECOMMENDATIONS"
echo "============================"

if [ "$DLQ_COUNT" -gt 0 ]; then
    log_warning "Messages are going to DLQ - check the following:"
    echo "   1. Schema compatibility between producer and consumer"
    echo "   2. Message format vs connector expectations"
    echo "   3. Schema Registry subject naming"
    echo "   4. Connector configuration"
else
    log_success "No messages in DLQ - connector is processing successfully"
fi

echo ""
echo "🛠️  USEFUL DEBUG COMMANDS:"
echo "=========================="
echo "• Restart connector: curl -X POST http://localhost:8083/connectors/$CONNECTOR_NAME/restart"
echo "• Delete connector: curl -X DELETE http://localhost:8083/connectors/$CONNECTOR_NAME"
echo "• View connector logs: docker logs connect | grep $CONNECTOR_NAME"
echo "• Monitor DLQ: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic snowflake-dlq --from-beginning"