# JSON Explosion Test - Isolated

This directory contains an isolated test for **JSON explosion** functionality.

## What it tests:
- ✅ JSON explosion of array fields into individual messages
- ✅ Snowflake connector with JSON data (schematization=false)
- ✅ End-to-end pipeline from substreams to Snowflake

## Features:
- 🔄 **Fresh Kafka environment** (docker-compose down/up)
- 📊 **Complete monitoring** (message counts, connector status)
- 🧹 **Clean isolation** (unique topics, timestamped)
- 🎯 **Focused testing** (JSON only, no Schema Registry complexity)

## Usage:
```bash
./test-json-explosion.sh
```

## Expected Results:
- JSON messages exploded into individual Kafka messages
- Snowflake table auto-created with VARIANT columns
- Clean, predictable pipeline for JSON data

## Cleanup:
The script provides cleanup commands at the end.