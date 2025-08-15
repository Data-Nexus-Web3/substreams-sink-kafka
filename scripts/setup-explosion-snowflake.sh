#!/bin/bash
# Setup Snowflake schema for explosion testing

echo "🏗️  Setting up Snowflake schema for explosion testing"
echo "=================================================="

# Check if snowsql is available
if ! command -v snowsql &> /dev/null; then
    echo "❌ snowsql not found. Please install SnowSQL CLI first."
    echo "   Visit: https://docs.snowflake.com/en/user-guide/snowsql-install-config.html"
    exit 1
fi

echo "✅ SnowSQL found"
echo ""
echo "🔄 Running schema setup script..."

# Run the SQL setup script
snowsql -f scripts/setup-explosion-test-schema.sql

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Snowflake schema setup completed successfully!"
    echo ""
    echo "📊 Created tables:"
    echo "   • EXPLOSION_TEST.ADR_EXPLODED_JSON"
    echo "   • EXPLOSION_TEST.ADR_EXPLODED_SCHEMA_REGISTRY"
    echo ""
    echo "📋 Created views:"
    echo "   • EXPLOSION_TEST.EXPLODED_JSON_ANALYSIS"
    echo "   • EXPLOSION_TEST.EXPLODED_SCHEMA_ANALYSIS"
    echo ""
    echo "🚀 Ready to run explosion tests!"
else
    echo "❌ Schema setup failed. Please check your Snowflake connection and permissions."
    exit 1
fi