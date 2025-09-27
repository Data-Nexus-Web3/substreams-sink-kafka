# Substreams Kafka Sink

A high-performance Substreams sink for Kafka that streams blockchain data with native protobuf message support for maximum throughput and type safety.

## Features

- **High Performance**: Optimized for extreme throughput with batching and async processing
- **Native Protobuf**: Preserves original protobuf format for maximum efficiency
- **Universal Compatibility**: Works with ANY Substreams map module output - no sink config required!
- **Reliable Checkpointing**: Cursor-based recovery system for fault tolerance
- **Production Ready**: Built on StreamingFast's proven sink architecture
- **Flexible Configuration**: Support for various Kafka authentication methods
- **Chain Reorganization Handling**: Proper handling of blockchain forks

## Quick Start

### 1. Install

```bash
go install github.com/Data-Nexus-Web3/substreams-sink-kafka@latest
```

### 2. Start Local Kafka (for testing)

```bash
cd docker
docker-compose up -d
```

This starts Kafka with Kafka UI available at http://localhost:8080

### 3. Setup and Validate Kafka Infrastructure

```bash
substreams-sink-kafka setup localhost:9092 \
  --kafka-topic=blockchain-data \
  --create-topic=true \
  --topic-partitions=6 \
  --topic-replication-factor=1
```

**What the setup command validates:**
- ✅ **Connection Test**: Verifies Kafka cluster connectivity
- ✅ **Authentication Test**: Validates SASL credentials (if configured)
- ✅ **Topic Management**: Creates topic with optimized settings
- ✅ **Permissions Test**: Verifies producer permissions
- ✅ **Performance Baseline**: Quick throughput test (100 messages)
- ✅ **Cursor File Setup**: Ensures cursor directory is writable

**Always run setup before production deployment!**

### 4. Run the Sink

```bash
# Set your Substreams API token
export SUBSTREAMS_API_TOKEN="your-token-here"

# Run the sink with explicit module selection
substreams-sink-kafka run localhost:9092 path/to/your/substreams.yaml \
  --output-module=map_pools \
  --kafka-topic=blockchain-data

# Run with Schema Registry (fast-path; primes at startup in same run)
substreams-sink-kafka run localhost:9092 path/to/your/substreams.yaml \
  --output-module=map_pools \
  --kafka-topic=blockchain-data \
  --output-format=schema-registry \
  --schema-registry-url=http://localhost:8081 \
  --schema-fast-path=true \
  --schema-auto-register=false

# Or let it infer from manifest (requires sink config in substreams.yaml)
substreams-sink-kafka run localhost:9092 path/to/your/substreams.yaml
```

## Configuration

### Basic Usage

```bash
# Specify any map module from your substreams
substreams-sink-kafka run <kafka_brokers> <manifest> --output-module=<module_name>

# Or use traditional sink config approach (requires sink section in manifest)
substreams-sink-kafka run <kafka_brokers> <manifest>
```

### Advanced Configuration

```bash
substreams-sink-kafka run localhost:9092 ./substreams.yaml \
  --output-module=map_transfers \
  --kafka-topic=blockchain-data \
  --kafka-batch-size=1000 \
  --kafka-batch-timeout-ms=100 \
  --cursor-file=./cursor.txt
```

### Authentication

```bash
# SASL/PLAIN authentication
substreams-sink-kafka run broker1:9092,broker2:9092 ./substreams.yaml \
  --kafka-username=user \
  --kafka-password=pass \
  --kafka-security-protocol=SASL_PLAINTEXT \
  --kafka-sasl-mechanism=PLAIN

# SASL/SCRAM authentication
substreams-sink-kafka run broker1:9092,broker2:9092 ./substreams.yaml \
  --kafka-username=user \
  --kafka-password=pass \
  --kafka-security-protocol=SASL_SSL \
  --kafka-sasl-mechanism=SCRAM-SHA-256
```

## Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `--output-module` | Substreams module to consume (if not specified, infers from manifest) | |
| `--output-format` | Output format: `json`, `protobuf`, or `schema-registry` | `protobuf` |
| `--kafka-topic` | Kafka topic to produce messages to | `substreams` |
| `--kafka-username` | SASL username (optional) | |
| `--kafka-password` | SASL password (optional) | |
| `--kafka-security-protocol` | Security protocol | `PLAINTEXT` |
| `--kafka-sasl-mechanism` | SASL mechanism | `PLAIN` |
| `--kafka-batch-size` | Max messages per batch | `1000` |
| `--kafka-batch-timeout-ms` | Batch timeout in milliseconds | `100` |
| `--cursor-file` | File to store cursor for recovery | `cursor.txt` |
| `--explode-field` | Repeated field name to explode into individual messages | |
| `--explode-workers` | Max parallel workers for explosion | `5` |
| `--explode-always-parallel` | Always use workers even for small batches | `true` |
| `--processing-buffer-size` | Post-undo processing queue length (0 disables) | `0` |
| `--processing-buffer-bytes` | Post-undo processing byte budget (0 disables) | `0` |
| `--processing-buffer-max-items` | Fuse to cap processing queue size | `4096` |
| `--undo-buffer-size` | Reorg protection buffer (provided by base sink) | `0` or as configured |
| **Schema Registry Options** | | |
| `--schema-registry-url` | Schema Registry URL (enables SR integration) | |
| `--schema-subject` | Schema subject name (serializer appends `-value`) | `{topic}` |
| `--schema-auto-register` | Auto-register schemas if missing | `false` |
| `--schema-fast-path` | Precompute SR header at startup, then header+payload on hot path | `false` |

## Module Selection

The Kafka sink supports two approaches for selecting which Substreams module to consume:

### 🚀 Direct Module Selection (Recommended)
Specify any map module directly with `--output-module`:

```bash
substreams-sink-kafka run localhost:9092 ./substreams.yaml \
  --output-module=map_transfers \
  --kafka-topic=transfers
```

**Benefits:**
- ✅ Works with ANY Substreams map module
- ✅ No manifest modification required
- ✅ Simple and flexible
- ✅ Perfect for existing Substreams

### 📝 Sink Configuration (Traditional)
Use a sink configuration in your `substreams.yaml`:

```yaml
sink:
  module: map_transfers
  type: sf.substreams.sink.kafka.v1.Service
```

**When to use:**
- When you want the module selection embedded in the manifest
- For compatibility with other StreamingFast sink patterns

## 🏢 Schema Registry Integration

The Kafka sink supports **Confluent Schema Registry** for enterprise-grade schema management and evolution.

### ✅ Benefits

- **🔧 Protobuf Format**: Messages include magic bytes + schema ID + protobuf data
- **📋 Schema Evolution**: Automatic schema registration and versioning
- **🛡️ Data Governance**: Centralized schema management and validation
- **🚀 Low Overhead**: With `--schema-fast-path`, SR adds minimal overhead (header cached at startup)

### 🚀 Quick Start with Schema Registry (Fast Path)

```bash
# 1. Start Schema Registry (if using Docker)
docker run -d --name schema-registry \
  -p 8081:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=localhost:9092 \
  -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
  confluentinc/cp-schema-registry:latest

# 2. Run sink with Schema Registry integration
substreams-sink-kafka run localhost:9092 substreams.yaml \
  --output-module=map_adr_messages \
  --kafka-topic=adr-data \
  --output-format=schema-registry \
  --schema-registry-url=http://localhost:8081 \
  --schema-subject=adr-data \
  --schema-fast-path=true \
  --schema-auto-register=false
```

### 🔄 Message Format (Protobuf + SR Framing)

**Without Schema Registry:**
```
[protobuf_bytes]
```

**With Schema Registry (Confluent Protobuf framing):**
```
[magic_byte][schema_id][message_index_path(varint...)][protobuf_bytes]
```

### ⚙️ Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `--schema-registry-url` | Schema Registry URL (e.g., `http://localhost:8081`) | *disabled* |
| `--schema-subject` | Subject name for schema registration | `{topic}-value` |
| `--schema-auto-register` | Auto-register schemas if they don't exist | `true` |

### 🏗️ Schema Subject Naming

- Default subject naming uses Confluent's TopicNameStrategy for Protobuf values: `{topic}-value`.
- The `--schema-subject` flag sets the base subject we pass to the serializer; the serializer appends `-value`.
- If `--schema-subject` is omitted, we pass the Kafka topic name, resulting in `{topic}-value`.
- Example: topic `adr-data` → subject `adr-data-value`.

### 🔧 Troubleshooting Schema Registry

```bash
# Check if schema was registered
curl http://localhost:8081/subjects

# View specific schema
curl http://localhost:8081/subjects/adr-data-value/versions/latest

# Test Schema Registry connectivity
curl http://localhost:8081/config
```

## Message Format

Messages are sent to Kafka in the following format:

- **Key**: `block_{block_number}_{block_id}` or `undo_{block_number}_{block_id}` for undo signals
- **Value**: Protobuf-serialized `BlockScopedData` or `BlockUndoSignal` message
- **Headers**: Additional metadata (optional)

## Performance Tuning

The sink is optimized for high throughput with the following features:

### Producer Optimizations
- Batching with configurable size and timeout
- Snappy compression
- Async delivery with callback handling
- Connection pooling

### Recommended Settings for High Throughput
```bash
--kafka-batch-size=5000 \
--kafka-batch-timeout-ms=50
```

### Kafka Broker Tuning
For maximum performance, tune your Kafka brokers:

```properties
# Increase network threads
num.network.threads=8
num.io.threads=8

# Increase buffer sizes  
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

# Increase batch size
batch.size=65536
linger.ms=10
```

## Monitoring

The sink provides detailed logging and metrics:

```bash
# View progress logs
substreams-sink-kafka run localhost:9092 ./substreams.yaml --log-level=info

# Kafka UI for monitoring topics
# http://localhost:8080 (when using docker-compose)
```

## Error Handling

### Chain Reorganizations
When blockchain reorganizations occur, the sink:
1. Receives a `BlockUndoSignal` from Substreams
2. Sends the undo signal to Kafka for downstream consumers
3. Updates the cursor to the last valid block
4. Continues processing from the correct chain state

### Recovery from Failures
The sink automatically recovers from failures using cursor checkpoints:
1. Cursor is saved after each processed block
2. On restart, processing resumes from the last saved cursor
3. No data loss or duplication occurs

## Development

### Building from Source

```bash
git clone https://github.com/Data-Nexus-Web3/substreams-sink-kafka
cd substreams-sink-kafka
go build -o bin/substreams-sink-kafka ./cmd/substreams-sink-kafka
```

### Running Tests

```bash
# Start test Kafka
cd docker && docker-compose up -d

# Run tests
go test ./...
```

## Architecture

The sink follows StreamingFast's proven architecture:

```
Substreams → BlockScopedDataHandler → Kafka Producer → Kafka Topic
           ↓
    BlockUndoSignalHandler → Kafka Producer → Kafka Topic
           ↓
    Cursor Persistence → Local File
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0

## Support

- [StreamingFast Documentation](https://substreams.streamingfast.io/)
- [GitHub Issues](https://github.com/streamingfast/substreams-sink-kafka/issues)
- [StreamingFast Discord](https://discord.gg/streamingfast)