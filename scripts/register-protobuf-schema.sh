#!/bin/bash

# Script to register protobuf schema with Confluent Schema Registry
# This enables the Snowflake connector to deserialize protobuf messages

echo "📋 Registering Protobuf Schema with Schema Registry"
echo "=================================================="

# Wait for Schema Registry to be ready
echo "⏳ Waiting for Schema Registry..."
until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
  echo "   Schema Registry not ready yet, waiting..."
  sleep 5
done
echo "✅ Schema Registry is ready!"

# Function to register a schema for a topic
register_schema() {
  local PROTO_FILE=$1
  local TOPIC_SUBJECT=$2
  
  if [ ! -f "$PROTO_FILE" ]; then
    echo "❌ Protobuf file not found: $PROTO_FILE"
    return 1
  fi

  echo "📝 Reading protobuf schema from: $PROTO_FILE"
  
  # Read the protobuf schema content
  PROTO_CONTENT=$(cat "$PROTO_FILE" | jq -Rs .)

  # Create the schema registry payload
  SCHEMA_PAYLOAD=$(cat <<EOF
{
  "schemaType": "PROTOBUF",
  "schema": $PROTO_CONTENT
}
EOF
)

  echo "🚀 Registering schema for subject: $TOPIC_SUBJECT"

  # Register the schema
  RESPONSE=$(curl -s -X POST http://localhost:8081/subjects/$TOPIC_SUBJECT/versions \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$SCHEMA_PAYLOAD")

  if echo "$RESPONSE" | grep -q '"id"'; then
    SCHEMA_ID=$(echo "$RESPONSE" | jq -r '.id')
    echo "✅ Schema registered successfully with ID: $SCHEMA_ID"
    echo "   - Subject: $TOPIC_SUBJECT"
    echo "   - Schema ID: $SCHEMA_ID"
    echo "   - Schema Type: PROTOBUF"
    return 0
  else
    echo "❌ Failed to register schema for $TOPIC_SUBJECT!"
    echo "Response: $RESPONSE"
    return 1
  fi
}

echo ""
echo "📋 Registering ADR Protobuf Schema..."

# Register ADR schema for different topics/subjects
register_schema "docker/protobuf-example/adr.proto" "adr-data-value"

# If you have other topics, register them too
# register_schema "protobuf-example/adr.proto" "blockchain-data-value"

echo ""
echo "🔗 View registered schemas:"
echo "   curl http://localhost:8081/subjects"
echo "   curl http://localhost:8081/subjects/adr-data-value/versions/latest"

echo ""
echo "✅ Protobuf schema registration complete!"
echo "The Snowflake connector can now deserialize protobuf messages from your topic."