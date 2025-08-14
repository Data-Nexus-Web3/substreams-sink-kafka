#!/bin/bash

# Automated Snowflake Database Setup for ADR Kafka Connector
# This script uses SnowSQL CLI to run the setup automatically

echo "🏔️  Snowflake Database Setup for ADR Kafka Connector"
echo "=================================================="

# Check if SnowSQL is installed
if ! command -v snowsql &> /dev/null; then
    echo "❌ SnowSQL CLI not found!"
    echo ""
    echo "Please install SnowSQL first:"
    echo "  - Download from: https://developers.snowflake.com/snowsql/"
    echo "  - Or use pip: pip install snowflake-cli-labs"
    echo "  - Or use brew: brew install snowflake/snowflake/snowsql"
    echo ""
    exit 1
fi

echo "✅ SnowSQL CLI found"

# Extract connection details from our Kafka connector config
SNOWFLAKE_ACCOUNT="DTCC-DEVX1_US_WEST_2"
SNOWFLAKE_USER="TMONTE"
SNOWFLAKE_ROLE="SENTINEL_DB_ADMIN"

echo ""
echo "📋 Connection Details:"
echo "   Account: $SNOWFLAKE_ACCOUNT"
echo "   User: $SNOWFLAKE_USER"
echo "   Role: $SNOWFLAKE_ROLE"
echo ""

# Check if we have the private key file
PRIVATE_KEY_FILE="$HOME/.ssh/snowflake_rsa_key.p8"
if [ ! -f "$PRIVATE_KEY_FILE" ]; then
    echo "❌ Private key file not found: $PRIVATE_KEY_FILE"
    echo "Please ensure your Snowflake private key is at the expected location."
    exit 1
fi

echo "🔑 Private key found: $PRIVATE_KEY_FILE"
echo ""

# Run the Snowflake setup
echo "🚀 Running Snowflake database setup..."
echo "   (You may be prompted for your private key passphrase if it's encrypted)"
echo ""

snowsql \
    --accountname "$SNOWFLAKE_ACCOUNT" \
    --username "$SNOWFLAKE_USER" \
    --private-key-path "$PRIVATE_KEY_FILE" \
    --rolename "$SNOWFLAKE_ROLE" \
    --filename "docker/snowflake-setup.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Snowflake database setup completed successfully!"
    echo ""
    echo "📊 Created:"
    echo "   - Database: SENTINEL_DB"
    echo "   - Schema: STREAMING_SCHEMA"
    echo "   - Table: ADR_LISTENER_MESSAGES (with schema evolution enabled)"
    echo "   - Table: SNOWFLAKE_DLQ (for error handling)"
    echo ""
    echo "🎯 Ready for Kafka connector deployment!"
else
    echo ""
    echo "❌ Snowflake setup failed!"
    echo ""
    echo "Common issues:"
    echo "   - Check your private key file and passphrase"
    echo "   - Verify your account name and user permissions"
    echo "   - Ensure SENTINEL_DB_ADMIN role has necessary privileges"
    echo ""
    echo "Manual setup option:"
    echo "   Copy and paste the contents of snowflake-setup.sql into Snowflake UI"
    exit 1
fi