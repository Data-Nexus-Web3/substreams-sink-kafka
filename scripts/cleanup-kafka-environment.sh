#!/bin/bash

echo "🧹 Cleaning up Kafka environment for fresh start..."

# Stop all Kafka Connect connectors
echo "Stopping all Kafka Connect connectors..."
curl -s http://localhost:8083/connectors | jq -r '.[]' | while read connector; do
    echo "Stopping connector: $connector"
    curl -X DELETE "http://localhost:8083/connectors/$connector" 2>/dev/null || echo "Failed to stop $connector"
done

# Wait for connectors to stop
sleep 5

# Delete all topics except essential Kafka internals
echo "Deleting all topics except essential Kafka internals..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -v -E "^(_consumer_offsets|_schemas)$" | while read topic; do
    echo "Deleting topic: $topic"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || echo "Failed to delete $topic"
done

# Clear Schema Registry (keep system schemas)
echo "Clearing test schemas from Schema Registry..."
curl -s http://localhost:8081/subjects | jq -r '.[]' | grep -E "(adr-|adr_|block-|test-|explosion-|flattened-)" | while read subject; do
    echo "Deleting schema: $subject"
    curl -X DELETE "http://localhost:8081/subjects/$subject" 2>/dev/null || echo "Failed to delete $subject"
done

# Clear consumer groups
echo "Clearing test consumer groups..."
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list | grep -E "(connect-|test-|flattened-|explosion-)" | while read group; do
    echo "Clearing consumer group: $group"
    docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group "$group" --reset-offsets --to-earliest --all-topics --execute 2>/dev/null || echo "Failed to clear $group"
done

# Wait for cleanup to complete
sleep 5

echo "✅ Kafka environment cleaned up!"
echo ""
echo "Current state:"
echo "Connectors: $(curl -s http://localhost:8083/connectors | jq length) running"
echo "Topics: $(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | wc -l) total"
echo "Schemas: $(curl -s http://localhost:8081/subjects | jq length) registered"
echo ""
echo "Ready for fresh testing! 🚀" 