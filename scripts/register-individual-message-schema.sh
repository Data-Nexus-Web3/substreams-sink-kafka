#!/bin/bash

# Script to register individual FlattenedMessage schema for explosion feature
# This fixes the Schema Registry subject mismatch issue

TOPIC_NAME=$1

if [ -z "$TOPIC_NAME" ]; then
    echo "Usage: $0 <topic-name>"
    echo "Example: $0 adr-exploded-schema-registry-20250814_123829"
    exit 1
fi

SUBJECT_NAME="${TOPIC_NAME}-value"

echo "🔧 Registering individual FlattenedMessage schema for explosion feature"
echo "Topic: $TOPIC_NAME"
echo "Subject: $SUBJECT_NAME"

# Create the individual FlattenedMessage schema (without the repeated field wrapper)
INDIVIDUAL_SCHEMA='syntax = "proto3";
package evm.adr.v1;

message FlattenedMessage {
  optional uint64 block = 1;
  string version = 2;
  string network = 3;
  string prev_cursor = 4;
  string txn_id = 5;
  string txn_cursor = 6;
  int64 txn_timestamp = 7;
  optional string txn_status = 8;
  bool txn_successful = 9;
  optional string wallet_token = 10;
  optional string act_type = 11;
  optional string act_address = 12;
  repeated string act_operations = 13;
  optional string ops_id = 14;
  optional string ops_token = 15;
  optional string ops_address = 16;
  optional string ops_amount = 17;
  optional string ops_type = 18;
  optional bool has_all_operations = 19;
  optional int64 received_at = 20;
  optional int64 sent_at = 21;
  optional string provider = 22;
}'

# Create the schema registry payload
SCHEMA_PAYLOAD=$(cat <<EOF
{
  "schemaType": "PROTOBUF",
  "schema": $(echo "$INDIVIDUAL_SCHEMA" | jq -Rs .)
}
EOF
)

echo "🚀 Registering schema..."

# Register the schema
RESPONSE=$(curl -s -X POST "http://localhost:8081/subjects/$SUBJECT_NAME/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "$SCHEMA_PAYLOAD")

if echo "$RESPONSE" | grep -q '"id"'; then
  SCHEMA_ID=$(echo "$RESPONSE" | jq -r '.id')
  echo "✅ Schema registered successfully!"
  echo "   - Subject: $SUBJECT_NAME"
  echo "   - Schema ID: $SCHEMA_ID"
  echo "   - Schema Type: PROTOBUF"
else
  echo "❌ Failed to register schema!"
  echo "Response: $RESPONSE"
  exit 1
fi

echo ""
echo "🔍 Verifying registration..."
curl -s "http://localhost:8081/subjects/$SUBJECT_NAME/versions/latest" | jq '.id, .subject, .version'

echo ""
echo "✅ Individual FlattenedMessage schema registered for topic: $TOPIC_NAME"
echo "   The Snowflake connector should now be able to process exploded messages!"