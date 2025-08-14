#!/bin/bash

# Snowflake Kafka Connector Setup Script
# This script helps you configure the Snowflake Kafka Connector with Snowpipe Streaming

echo "🏔️  Snowflake Kafka Connector Setup"
echo "=================================="
echo ""

# Check if Kafka Connect is running
echo "⏳ Waiting for Kafka Connect to be ready..."
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
  echo "   Kafka Connect not ready yet, waiting..."
  sleep 5
done
echo "✅ Kafka Connect is ready!"

# Check Schema Registry
echo "⏳ Checking Schema Registry..."
until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
  echo "   Schema Registry not ready yet, waiting..."
  sleep 5
done
echo "✅ Schema Registry is ready!"

echo ""
echo "📋 Setup Steps:"
echo "   1. ✅ Snowflake credentials configured in snowflake-connector-config.json"
echo "   2. 🔄 Setting up Snowflake database and tables..."
echo ""

# Run Snowflake database setup first
echo "🏔️  Step 1: Setting up Snowflake database..."
if ./setup-snowflake-db.sh; then
  echo "✅ Snowflake database setup completed!"
else
  echo "❌ Snowflake database setup failed!"
  echo "Please run './scripts/setup-snowflake-db.sh' manually or check the error messages."
  exit 1
fi

echo ""
echo "🔄 Step 2: Installing Kafka connector..."

echo ""
echo "🚀 Installing Snowflake Connector..."

# Install the connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @docker/snowflake-configs/snowflake-connector-config.json

if [ $? -eq 0 ]; then
  echo "✅ Snowflake connector installed successfully!"
  echo ""
  echo "📊 You can monitor the connector at:"
  echo "   - Kafka UI: http://localhost:8080"
  echo "   - Kafka Connect API: http://localhost:8083/connectors/snowflake-sink-connector"
  echo ""
  echo "🔍 To check connector status:"
  echo "   curl http://localhost:8083/connectors/snowflake-sink-connector/status"
else
  echo "❌ Failed to install Snowflake connector!"
  echo "Check the logs: docker logs kafka-connect"
fi