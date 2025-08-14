#!/bin/bash

# Quick Snowflake Connection Test
# Verifies your credentials work before running the full setup

echo "🔍 Testing Snowflake Connection"
echo "=============================="

# Check if SnowSQL is available
if ! command -v snowsql &> /dev/null; then
    echo "❌ SnowSQL CLI not found. Install it first:"
    echo "   https://developers.snowflake.com/snowsql/"
    exit 1
fi

# Connection details
SNOWFLAKE_ACCOUNT="DTCC-DEVX1_US_WEST_2"
SNOWFLAKE_USER="TMONTE"
SNOWFLAKE_ROLE="SENTINEL_DB_ADMIN"
PRIVATE_KEY_FILE="$HOME/.ssh/snowflake_rsa_key.p8"

echo "Testing connection with:"
echo "   Account: $SNOWFLAKE_ACCOUNT"
echo "   User: $SNOWFLAKE_USER"
echo "   Role: $SNOWFLAKE_ROLE"
echo ""

# Simple connection test
snowsql \
    --accountname "$SNOWFLAKE_ACCOUNT" \
    --username "$SNOWFLAKE_USER" \
    --private-key-path "$PRIVATE_KEY_FILE" \
    --rolename "$SNOWFLAKE_ROLE" \
    --query "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_ACCOUNT();"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Connection successful! You're ready to run the setup."
else
    echo ""
    echo "❌ Connection failed. Check your credentials and try again."
fi