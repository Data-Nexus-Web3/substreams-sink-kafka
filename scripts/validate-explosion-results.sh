#!/bin/bash
# Script to validate explosion test results in Snowflake

set -e

echo "🔍 Validating Explosion Test Results"
echo "===================================="

# Configuration
JSON_TOPIC="adr-exploded-json"
SCHEMA_REGISTRY_TOPIC="adr-exploded-schema-registry"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
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

# Check Kafka topics
check_kafka_topics() {
    log_info "Checking Kafka topics..."
    
    echo ""
    echo "📊 Topic Status:"
    echo "==============="
    
    # Check JSON topic
    JSON_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$JSON_TOPIC" --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
    echo "JSON Topic ($JSON_TOPIC): $JSON_COUNT messages"
    
    # Check Schema Registry topic
    SCHEMA_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$SCHEMA_REGISTRY_TOPIC" --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
    echo "Schema Registry Topic ($SCHEMA_REGISTRY_TOPIC): $SCHEMA_COUNT messages"
    
    if [ "$JSON_COUNT" -gt 0 ] && [ "$SCHEMA_COUNT" -gt 0 ]; then
        log_success "Both topics have messages"
    else
        log_warning "Some topics may be empty"
    fi
}

# Check Schema Registry
check_schema_registry() {
    log_info "Checking Schema Registry..."
    
    echo ""
    echo "🗄️  Schema Registry Status:"
    echo "=========================="
    
    # List all subjects
    SUBJECTS=$(curl -s http://localhost:8081/subjects | jq -r '.[]' | grep -E "(adr-exploded|explosion)" || echo "")
    
    if [ -n "$SUBJECTS" ]; then
        echo "Registered schemas:"
        echo "$SUBJECTS" | while read subject; do
            SCHEMA_ID=$(curl -s "http://localhost:8081/subjects/$subject/versions/latest" | jq -r '.id // "unknown"')
            echo "  - $subject (ID: $SCHEMA_ID)"
        done
        log_success "Schemas are registered"
    else
        log_warning "No explosion-related schemas found"
    fi
}

# Check Kafka Connect connectors
check_connectors() {
    log_info "Checking Kafka Connect connectors..."
    
    echo ""
    echo "🔗 Connector Status:"
    echo "==================="
    
    # Check JSON connector
    JSON_STATUS=$(curl -s http://localhost:8083/connectors/explosion-json-connector/status 2>/dev/null || echo '{"error": "not found"}')
    JSON_STATE=$(echo "$JSON_STATUS" | jq -r '.connector.state // .error')
    echo "JSON Connector: $JSON_STATE"
    
    # Check Schema Registry connector
    SCHEMA_STATUS=$(curl -s http://localhost:8083/connectors/explosion-schema-connector/status 2>/dev/null || echo '{"error": "not found"}')
    SCHEMA_STATE=$(echo "$SCHEMA_STATUS" | jq -r '.connector.state // .error')
    echo "Schema Registry Connector: $SCHEMA_STATE"
    
    if [ "$JSON_STATE" = "RUNNING" ] && [ "$SCHEMA_STATE" = "RUNNING" ]; then
        log_success "All connectors are running"
    else
        log_warning "Some connectors may not be running properly"
    fi
}

# Show sample messages
show_sample_messages() {
    log_info "Showing sample messages..."
    
    echo ""
    echo "📋 Sample JSON Message:"
    echo "======================"
    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$JSON_TOPIC" --from-beginning --max-messages 1 --timeout-ms 3000 2>/dev/null | jq . || echo "Could not retrieve JSON message"
    
    echo ""
    echo "📋 Sample Schema Registry Message (raw):"
    echo "========================================"
    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$SCHEMA_REGISTRY_TOPIC" --from-beginning --max-messages 1 --timeout-ms 3000 2>/dev/null | head -c 200 || echo "Could not retrieve Schema Registry message"
    echo ""
}

# Create Snowflake validation queries
create_snowflake_queries() {
    log_info "Creating Snowflake validation queries..."
    log_info "Note: Table names will be based on the topic names used in your test"
    
    cat > /tmp/validate-explosion-snowflake.sql << 'EOF'
-- Validation queries for explosion test results
-- Note: Replace table names with actual topic-based table names
USE ROLE SENTINEL_DB_ADMIN;
USE DATABASE SENTINEL_DB;
USE SCHEMA EXPLOSION_TEST;

-- List all tables in the schema to find the auto-created ones
SHOW TABLES IN SCHEMA EXPLOSION_TEST;

-- Example queries (update table names based on your topic names):
-- SELECT COUNT(*) FROM "ADR_EXPLODED_JSON_YYYYMMDD_HHMMSS";
-- SELECT COUNT(*) FROM "ADR_EXPLODED_SCHEMA_REGISTRY_YYYYMMDD_HHMMSS";

-- Show recent records from auto-created JSON table (update table name):
-- SELECT 
--     RECORD_CONTENT:hash::STRING as tx_hash,
--     RECORD_CONTENT:from::STRING as from_addr,
--     RECORD_CONTENT:block_number::INTEGER as block_num,
--     RECORD_METADATA,
--     INGESTED_AT
-- FROM "ADR_EXPLODED_JSON_YYYYMMDD_HHMMSS"
-- ORDER BY INGESTED_AT DESC 
-- LIMIT 5;
EOF
    
    echo ""
    echo "📄 Snowflake validation queries template created at: /tmp/validate-explosion-snowflake.sql"
    echo ""
    echo "🔍 To validate Snowflake data:"
    echo "   1. Run: snowsql -f /tmp/validate-explosion-snowflake.sql"
    echo "   2. Update table names in the queries based on your topic names"
    echo "   3. Tables are auto-created by the Snowflake connector based on topic names"
    echo ""
    echo "📋 Quick check commands:"
    echo "   • List tables: SHOW TABLES IN SCHEMA EXPLOSION_TEST;"
    echo "   • Check specific table: SELECT COUNT(*) FROM \"your_topic_name\";"
}

# Main validation
main() {
    echo "🏁 Starting explosion test validation..."
    echo ""
    
    # Check Kafka topics
    check_kafka_topics
    
    # Check Schema Registry
    check_schema_registry
    
    # Check connectors
    check_connectors
    
    # Show sample messages
    show_sample_messages
    
    # Create Snowflake queries
    create_snowflake_queries
    
    echo ""
    log_success "🎉 Validation complete!"
    echo ""
    echo "📊 Summary:"
    echo "   • Kafka topics checked"
    echo "   • Schema Registry status verified"
    echo "   • Connector status reviewed"
    echo "   • Sample messages displayed"
    echo "   • Snowflake validation queries generated"
    echo ""
    echo "💡 Next steps:"
    echo "   1. Run the Snowflake validation queries"
    echo "   2. Check that data is flowing to both tables"
    echo "   3. Verify the explosion logic is working correctly"
    echo "   4. Compare message counts between formats"
}

# Run main function
main "$@"