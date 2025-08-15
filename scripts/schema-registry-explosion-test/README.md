# Schema Registry Explosion Test - Isolated

This directory contains an isolated test for **Schema Registry explosion** functionality.

## What it tests:
- ✅ Schema Registry explosion of array fields into individual protobuf messages
- ✅ Confluent protobuf serializer behavior with auto-generated subjects
- ✅ Snowflake connector with protobuf data (schematization=true)
- ✅ End-to-end pipeline with proper schema registration

## Key Insights Tested:
- 🎯 **Confluent auto-generates subjects** with `-value-value` suffix
- 🔧 **Pre-registration required** for individual message schemas
- ⚙️  **`--schema-auto-register=false`** prevents conflicts
- 📋 **Individual message schema** needed (not container schema)

## Features:
- 🔄 **Fresh Kafka environment** (docker-compose down/up)
- 📝 **Schema pre-registration** (individual FlattenedMessage schema)
- 📊 **Complete monitoring** (message counts, connector status, schema verification)
- 🧹 **Clean isolation** (unique topics, timestamped)
- 🎯 **Focused testing** (Schema Registry only, no JSON complexity)

## Usage:
```bash
./test-schema-registry-explosion.sh
```

## Expected Results:
- Individual FlattenedMessage schema registered under auto-generated subject
- Protobuf messages exploded into individual Kafka messages
- Snowflake table auto-created with proper schema-based columns
- Clean protobuf pipeline with Schema Registry integration

## Debugging:
The script includes comprehensive debugging information for Schema Registry issues.

## Cleanup:
The script provides cleanup commands at the end.