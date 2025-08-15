package sinker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sink "github.com/streamingfast/substreams-sink"
	pbkafka "github.com/streamingfast/substreams-sink-kafka/proto/sf/substreams/sink/kafka/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ExplodedArrayItem represents a single array item ready for batch processing
type ExplodedArrayItem struct {
	Value     protoreflect.Value
	Field     protoreflect.FieldDescriptor
	Index     int
	BlockData *pbsubstreamsrpc.BlockScopedData
	Cursor    *sink.Cursor
}

// BatchedKafkaMessage represents a serialized message ready for Kafka production
type BatchedKafkaMessage struct {
	Key          string
	MessageBytes []byte
	Cursor       *sink.Cursor
}

// handleWithFieldExplosion processes messages by exploding a single specified array field using batch processing
func (s *KafkaSinker) handleWithFieldExplosion(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	s.logger.Debug("Processing with field explosion",
		zap.String("explode_field", s.explodeFieldName),
		zap.Uint64("block_number", data.Clock.Number),
	)

	// Deserialize the main message using dynamic protobuf
	dynamicMessage := dynamicpb.NewMessage(s.v2MessageDescriptor)
	err := proto.Unmarshal(data.Output.MapOutput.Value, dynamicMessage)
	if err != nil {
		return fmt.Errorf("failed to unmarshal dynamic message: %w", err)
	}

	// Find the specified field to explode
	msgDesc := dynamicMessage.Descriptor()
	field := msgDesc.Fields().ByName(protoreflect.Name(s.explodeFieldName))
	if field == nil {
		return fmt.Errorf("field '%s' not found in message", s.explodeFieldName)
	}

	if !field.IsList() {
		return fmt.Errorf("field '%s' is not an array field, cannot explode", s.explodeFieldName)
	}

	// Get the array value
	listValue := dynamicMessage.Get(field).List()
	arraySize := listValue.Len()

	s.logger.Info("Exploding array field",
		zap.String("field_name", s.explodeFieldName),
		zap.Int("array_size", arraySize),
		zap.Uint64("block_number", data.Clock.Number),
	)

	// Early return for empty arrays
	if arraySize == 0 {
		return nil
	}

	// 🚀 BATCH PROCESSING: Pre-allocate batch slice for optimal performance
	batch := make([]*ExplodedArrayItem, 0, arraySize)

	// Build batch of items to process
	for i := 0; i < arraySize; i++ {
		item := &ExplodedArrayItem{
			Value:     listValue.Get(i),
			Field:     field,
			Index:     i,
			BlockData: data,
			Cursor:    cursor,
		}
		batch = append(batch, item)
	}

	// Process the entire batch efficiently
	return s.processBatchedArrayItems(batch)
}

// processBatchedArrayItems efficiently processes a batch of array items using optimized patterns
func (s *KafkaSinker) processBatchedArrayItems(batch []*ExplodedArrayItem) error {
	if len(batch) == 0 {
		return nil
	}

	startTime := time.Now()

	// 🚀 PERFORMANCE: Pre-allocate slice for batch messages to avoid repeated allocations
	batchMessages := make([]*BatchedKafkaMessage, 0, len(batch))

	// Pre-allocate string builder for efficient key generation (avoids fmt.Sprintf allocations)
	var keyBuilder strings.Builder
	keyBuilder.Grow(64) // Pre-allocate reasonable buffer size

	// Pre-allocate dynamic message for schema-registry to avoid repeated allocations
	var reusableMsg *dynamicpb.Message
	var individualMsgDesc protoreflect.MessageDescriptor
	if s.outputFormat == "schema-registry" && len(batch) > 0 {
		individualMsgDesc = batch[0].Field.Message()
		if individualMsgDesc != nil {
			reusableMsg = dynamicpb.NewMessage(individualMsgDesc)
		}
	}

	s.logger.Debug("Processing batch of exploded array items",
		zap.Int("batch_size", len(batch)),
		zap.String("output_format", s.outputFormat),
	)

	// Serialize all items in the batch
	for _, item := range batch {
		// 🚀 OPTIMIZED KEY GENERATION: Use string builder instead of fmt.Sprintf
		keyBuilder.Reset()
		keyBuilder.WriteString("block_")
		keyBuilder.WriteString(strconv.FormatUint(item.BlockData.Clock.Number, 10))
		keyBuilder.WriteString("_")
		keyBuilder.WriteString(item.BlockData.Clock.Id)
		keyBuilder.WriteString("_")
		keyBuilder.WriteString(strconv.Itoa(item.Index))
		key := keyBuilder.String()

		// Serialize the array item based on output format
		messageBytes, err := s.serializeBatchedArrayItem(item, reusableMsg)
		if err != nil {
			return fmt.Errorf("failed to serialize batch item %d: %w", item.Index, err)
		}

		// Add to batch messages
		batchMessages = append(batchMessages, &BatchedKafkaMessage{
			Key:          key,
			MessageBytes: messageBytes,
			Cursor:       item.Cursor,
		})
	}

	serializationTime := time.Since(startTime)

	// 🚀 BATCH KAFKA PRODUCTION: Produce all messages efficiently
	productionStart := time.Now()
	err := s.produceBatchMessages(batchMessages)
	if err != nil {
		return fmt.Errorf("failed to produce batch messages: %w", err)
	}
	productionTime := time.Since(productionStart)

	s.logger.Debug("Batch processing completed",
		zap.Int("batch_size", len(batch)),
		zap.Duration("serialization_time", serializationTime),
		zap.Duration("production_time", productionTime),
		zap.Duration("total_time", time.Since(startTime)),
	)

	return nil
}

// serializeBatchedArrayItem serializes a single array item using optimized patterns
func (s *KafkaSinker) serializeBatchedArrayItem(item *ExplodedArrayItem, reusableMsg *dynamicpb.Message) ([]byte, error) {
	switch s.outputFormat {
	case "json":
		// Convert the array item to a JSON-serializable value and serialize
		itemValue := s.convertProtoreflectValue(item.Value)
		return json.Marshal(itemValue)

	case "protobuf":
		// For protobuf, serialize the message directly if it's a message type
		if item.Value.Message().IsValid() {
			return proto.Marshal(item.Value.Message().Interface())
		} else {
			// For primitive types, convert to JSON as fallback
			itemValue := s.convertProtoreflectValue(item.Value)
			return json.Marshal(itemValue)
		}

	case "schema-registry":
		// For schema registry explosion, serialize individual messages using reusable message
		if item.Value.Message().IsValid() {
			// 🚀 PERFORMANCE OPTIMIZATION: Reuse dynamic message instead of creating new ones
			if reusableMsg != nil {
				// Clear the reusable message
				reusableMsg.Reset()
				// Copy the data from the array item to the reusable message
				proto.Merge(reusableMsg, item.Value.Message().Interface())

				// Use the topic name as subject - Confluent ProtobufConverter expects {topic}-value pattern
				return s.protobufSerializer.Serialize(s.topic, reusableMsg)
			} else {
				// Fallback to creating new message if reusable message is not available
				individualMsgDesc := item.Field.Message()
				individualMsg := dynamicpb.NewMessage(individualMsgDesc)
				proto.Merge(individualMsg, item.Value.Message().Interface())
				return s.protobufSerializer.Serialize(s.topic, individualMsg)
			}
		} else {
			// For primitive types, convert to JSON as fallback
			itemValue := s.convertProtoreflectValue(item.Value)
			return json.Marshal(itemValue)
		}

	default:
		return nil, fmt.Errorf("unsupported output format: %s", s.outputFormat)
	}
}

// produceBatchMessages efficiently produces a batch of messages to Kafka
func (s *KafkaSinker) produceBatchMessages(batchMessages []*BatchedKafkaMessage) error {
	if len(batchMessages) == 0 {
		return nil
	}

	// Track pending messages for delivery confirmation
	for _, msg := range batchMessages {
		s.addPendingMessage(msg.Key, msg.Cursor)
	}

	// 🚀 BATCH PRODUCTION: Produce all messages in sequence for optimal throughput
	// Note: Kafka's librdkafka automatically batches these internally for network efficiency
	var failedKeys []string
	for _, msg := range batchMessages {
		err := s.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &s.topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(msg.Key),
			Value: msg.MessageBytes,
		}, s.deliveryChan)

		if err != nil {
			// Track failed message for cleanup
			failedKeys = append(failedKeys, msg.Key)
			s.logger.Error("Failed to produce batch message",
				zap.String("key", msg.Key),
				zap.Error(err))
		}
	}

	// Clean up failed messages from pending tracking
	for _, key := range failedKeys {
		s.removePendingMessage(key)
	}

	// Update metrics atomically
	successCount := len(batchMessages) - len(failedKeys)
	atomic.AddInt64(&s.messagesProduced, int64(successCount))
	atomic.AddInt64(&s.messagesPending, int64(successCount))

	s.logger.Debug("Batch messages produced",
		zap.Int("total_messages", len(batchMessages)),
		zap.Int("successful_messages", successCount),
		zap.Int("failed_messages", len(failedKeys)),
	)

	// Return error if any messages failed
	if len(failedKeys) > 0 {
		return fmt.Errorf("failed to produce %d out of %d batch messages", len(failedKeys), len(batchMessages))
	}

	return nil
}

// produceExplodedArrayItem creates and produces a single message for an array item
func (s *KafkaSinker) produceExplodedArrayItem(item protoreflect.Value, field protoreflect.FieldDescriptor, index int, data *pbsubstreamsrpc.BlockScopedData, cursor *sink.Cursor) error {
	// Create message key using the same pattern as current implementation
	key := fmt.Sprintf("block_%d_%s_%d", data.Clock.Number, data.Clock.Id, index)

	// Serialize the array item directly based on output format
	var messageBytes []byte
	var err error

	switch s.outputFormat {
	case "json":
		// Convert the array item to a JSON-serializable value and serialize
		itemValue := s.convertProtoreflectValue(item)
		messageBytes, err = json.Marshal(itemValue)
		if err != nil {
			return fmt.Errorf("failed to serialize array item to JSON: %w", err)
		}

	case "protobuf":
		// For protobuf, serialize the message directly if it's a message type
		if item.Message().IsValid() {
			messageBytes, err = proto.Marshal(item.Message().Interface())
			if err != nil {
				return fmt.Errorf("failed to marshal array item message: %w", err)
			}
		} else {
			// For primitive types, convert to JSON as fallback
			itemValue := s.convertProtoreflectValue(item)
			messageBytes, err = json.Marshal(itemValue)
			if err != nil {
				return fmt.Errorf("failed to marshal primitive array item: %w", err)
			}
		}

	case "schema-registry":
		// For schema registry explosion, serialize individual messages using auto-generated subjects
		if item.Message().IsValid() {
			// Create a proper dynamic message for the individual FlattenedMessage
			individualMsgDesc := field.Message()
			individualMsg := dynamicpb.NewMessage(individualMsgDesc)

			// Copy the data from the array item to the new message
			proto.Merge(individualMsg, item.Message().Interface())

			// Use the topic name as subject - Confluent ProtobufConverter expects {topic}-value pattern
			// The Confluent serializer will automatically append "-value" suffix
			messageBytes, err = s.protobufSerializer.Serialize(s.topic, individualMsg)
			if err != nil {
				zap.L().Error("Schema Registry serialization failed",
					zap.String("topic", s.topic),
					zap.String("schema_subject", s.schemaSubject),
					zap.String("message_type", string(individualMsg.ProtoReflect().Descriptor().FullName())),
					zap.Error(err))
				return fmt.Errorf("failed to serialize array item with Schema Registry: %w", err)
			}
		} else {
			// For primitive types, convert to JSON as fallback
			itemValue := s.convertProtoreflectValue(item)
			messageBytes, err = json.Marshal(itemValue)
			if err != nil {
				return fmt.Errorf("failed to marshal primitive array item: %w", err)
			}
		}

	default:
		return fmt.Errorf("unsupported output format: %s", s.outputFormat)
	}

	// Store pending message for confirmation tracking
	s.addPendingMessage(key, cursor)

	// Produce to the same topic as configured (per requirements)
	err = s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: messageBytes,
	}, s.deliveryChan)

	if err != nil {
		s.removePendingMessage(key)
		return fmt.Errorf("failed to produce exploded message to Kafka: %w", err)
	}

	// Update metrics
	atomic.AddInt64(&s.messagesProduced, 1)
	atomic.AddInt64(&s.messagesPending, 1)

	s.logger.Debug("Produced exploded array item",
		zap.String("topic", s.topic),
		zap.String("key", key),
		zap.Int("array_index", index),
		zap.Int("message_size", len(messageBytes)),
	)

	return nil
}

func (s *KafkaSinker) extractExplosions(message protoreflect.Message, data *pbsubstreamsrpc.BlockScopedData, cursor *sink.Cursor) ([]*ExplodedMessage, error) {
	var explosions []*ExplodedMessage

	// Get the message descriptor to read annotations
	msgDesc := message.Descriptor()

	// Extract parent context fields based on message annotations
	parentContext := s.extractParentContext(message, msgDesc)

	// Process each field to find explosion annotations
	fields := msgDesc.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)

		// Check if this field has an explosion annotation
		explodeToTopic := s.getFieldAnnotation(field, pbkafka.E_ExplodeToTopic)
		if explodeToTopic == "" {
			continue
		}

		// Generate the final topic name using smart naming
		finalTopicName := s.generateExplodedTopicName(explodeToTopic, string(field.Name()))

		s.logger.Debug("Found explosion annotation",
			zap.String("field_name", string(field.Name())),
			zap.String("explode_to_topic_raw", explodeToTopic),
			zap.String("final_topic_name", finalTopicName),
		)

		// Extract and explode this field
		fieldExplosions, err := s.explodeField(message, field, finalTopicName, parentContext, data, cursor)
		if err != nil {
			return nil, fmt.Errorf("failed to explode field %s: %w", field.Name(), err)
		}

		explosions = append(explosions, fieldExplosions...)
	}

	return explosions, nil
}

func (s *KafkaSinker) extractParentContext(message protoreflect.Message, msgDesc protoreflect.MessageDescriptor) map[string]interface{} {
	parentContext := make(map[string]interface{})

	// Get the include_parent_fields annotation from message options
	includeFields := s.getMessageAnnotation(msgDesc, pbkafka.E_IncludeParentFields)
	if includeFields == "" {
		return parentContext
	}

	// Parse the comma-separated field list
	fieldPaths := strings.Split(includeFields, ",")
	for _, fieldPath := range fieldPaths {
		fieldPath = strings.TrimSpace(fieldPath)
		value := s.extractFieldValue(message, fieldPath)
		if value != nil {
			parentContext[fieldPath] = value
		}
	}

	return parentContext
}

func (s *KafkaSinker) explodeField(message protoreflect.Message, field protoreflect.FieldDescriptor, targetTopic string, parentContext map[string]interface{}, data *pbsubstreamsrpc.BlockScopedData, cursor *sink.Cursor) ([]*ExplodedMessage, error) {
	var explosions []*ExplodedMessage

	if !field.IsList() {
		return nil, fmt.Errorf("field %s is not a list, cannot explode", field.Name())
	}

	// Get the list value
	listValue := message.Get(field).List()

	s.logger.Debug("Exploding list field",
		zap.String("field_name", string(field.Name())),
		zap.String("target_topic", targetTopic),
		zap.Int("list_size", listValue.Len()),
	)

	// Create an exploded message for each list item
	for i := 0; i < listValue.Len(); i++ {
		item := listValue.Get(i)

		// Create exploded message with parent context
		explodedMsg := s.createExplodedMessage(item, field, targetTopic, parentContext, i, data, cursor)
		if explodedMsg != nil {
			explosions = append(explosions, explodedMsg)
		}
	}

	return explosions, nil
}

func (s *KafkaSinker) createExplodedMessage(item protoreflect.Value, field protoreflect.FieldDescriptor, targetTopic string, parentContext map[string]interface{}, index int, data *pbsubstreamsrpc.BlockScopedData, cursor *sink.Cursor) *ExplodedMessage {
	// Create the exploded record combining parent context and item data
	explodedRecord := make(map[string]interface{})

	// Add parent context fields
	for key, value := range parentContext {
		explodedRecord[key] = value
	}

	// Add item-specific fields
	explodedRecord[string(field.Name())+"_index"] = index

	if item.Message().IsValid() {
		// If it's a message, extract its fields
		itemMsg := item.Message()
		itemFields := itemMsg.Descriptor().Fields()
		for j := 0; j < itemFields.Len(); j++ {
			itemField := itemFields.Get(j)
			fieldValue := s.convertProtoreflectValue(itemMsg.Get(itemField))
			explodedRecord[string(itemField.Name())] = fieldValue
		}
	} else {
		// If it's a primitive value, use the field name
		explodedRecord[string(field.Name())] = s.convertProtoreflectValue(item)
	}

	// Serialize the exploded record based on output format
	messageBytes, err := s.serializeExplodedRecord(explodedRecord)
	if err != nil {
		s.logger.Error("Failed to serialize exploded record",
			zap.Error(err),
			zap.String("target_topic", targetTopic),
		)
		return nil
	}

	// Create message key
	key := fmt.Sprintf("exploded_%d_%s_%d", data.Clock.Number, data.Clock.Id, index)

	return &ExplodedMessage{
		Topic:          targetTopic,
		Key:            key,
		MessageBytes:   messageBytes,
		ParentContext:  parentContext,
		OriginalCursor: cursor,
	}
}

func (s *KafkaSinker) produceExplodedMessage(explosion *ExplodedMessage) error {
	// Store pending message for confirmation tracking
	s.addPendingMessage(explosion.Key, explosion.OriginalCursor)

	// Produce to the exploded topic
	err := s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &explosion.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(explosion.Key),
		Value: explosion.MessageBytes,
	}, s.deliveryChan)

	if err != nil {
		s.removePendingMessage(explosion.Key)
		return fmt.Errorf("failed to produce message to topic %s: %w", explosion.Topic, err)
	}

	// Update metrics
	atomic.AddInt64(&s.messagesProduced, 1)
	atomic.AddInt64(&s.messagesPending, 1)

	s.logger.Debug("Produced exploded message",
		zap.String("topic", explosion.Topic),
		zap.String("key", explosion.Key),
		zap.Int("message_size", len(explosion.MessageBytes)),
	)

	return nil
}

func (s *KafkaSinker) shouldProduceOriginal() bool {
	// For now, don't produce original when exploding
	// TODO: Make this configurable
	return false
}

// generateExplodedTopicName creates a smart topic name for exploded fields
func (s *KafkaSinker) generateExplodedTopicName(annotationValue, fieldName string) string {
	// If annotation is empty or just "*", auto-generate from field name
	if annotationValue == "" || annotationValue == "*" {
		return s.sanitizeTopicName(s.baseTopic + "_" + fieldName)
	}

	// Support template variables
	topicName := annotationValue
	topicName = strings.ReplaceAll(topicName, "{base_topic}", s.baseTopic)
	topicName = strings.ReplaceAll(topicName, "{field_name}", fieldName)
	topicName = strings.ReplaceAll(topicName, "{field}", fieldName) // shorter alias

	return s.sanitizeTopicName(topicName)
}

// sanitizeTopicName ensures the topic name is Kafka-compliant
func (s *KafkaSinker) sanitizeTopicName(name string) string {
	// Kafka topic naming rules:
	// - Only alphanumeric, '.', '_', '-'
	// - Cannot be empty or just '.' or '..'
	// - Max 249 characters

	// Convert to lowercase and replace invalid chars with underscores
	result := strings.ToLower(name)
	result = strings.ReplaceAll(result, " ", "_")

	// Remove or replace other invalid characters
	var sanitized strings.Builder
	for _, r := range result {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			sanitized.WriteRune(r)
		} else {
			sanitized.WriteRune('_')
		}
	}

	result = sanitized.String()

	// Ensure it's not empty and not just dots
	if result == "" || result == "." || result == ".." {
		result = "exploded_topic"
	}

	return result
}

// Helper methods for annotation processing and explosion logic

func (s *KafkaSinker) getMessageAnnotation(msgDesc protoreflect.MessageDescriptor, extensionType protoreflect.ExtensionType) string {
	opts := msgDesc.Options()
	if !proto.HasExtension(opts, extensionType) {
		return ""
	}
	value := proto.GetExtension(opts, extensionType)
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

func (s *KafkaSinker) getFieldAnnotation(field protoreflect.FieldDescriptor, extensionType protoreflect.ExtensionType) string {
	opts := field.Options()
	if !proto.HasExtension(opts, extensionType) {
		return ""
	}
	value := proto.GetExtension(opts, extensionType)
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

func (s *KafkaSinker) extractFieldValue(message protoreflect.Message, fieldPath string) interface{} {
	// Support nested field paths like "metadata.provider"
	parts := strings.Split(fieldPath, ".")
	currentMsg := message

	for i, part := range parts {
		field := currentMsg.Descriptor().Fields().ByName(protoreflect.Name(part))
		if field == nil {
			s.logger.Debug("Field not found in message",
				zap.String("field_path", fieldPath),
				zap.String("missing_part", part),
			)
			return nil
		}

		value := currentMsg.Get(field)

		// If this is the last part, return the value
		if i == len(parts)-1 {
			return s.convertProtoreflectValue(value)
		}

		// Otherwise, continue traversing
		if value.Message().IsValid() {
			currentMsg = value.Message()
		} else {
			s.logger.Debug("Cannot traverse non-message field",
				zap.String("field_path", fieldPath),
				zap.String("field_part", part),
			)
			return nil
		}
	}

	return nil
}

func (s *KafkaSinker) convertProtoreflectValue(value protoreflect.Value) interface{} {
	// Check the actual type of the value using the interface
	switch v := value.Interface().(type) {
	case bool:
		return v
	case int32:
		return v
	case int64:
		return v
	case uint32:
		return v
	case uint64:
		return v
	case float32:
		return v
	case float64:
		return v
	case string:
		return v
	case []byte:
		return v
	case protoreflect.Message:
		// For messages, convert to map
		return s.convertMessageToMap(v)
	case protoreflect.List:
		// For lists, convert to slice
		list := v
		result := make([]interface{}, list.Len())
		for i := 0; i < list.Len(); i++ {
			result[i] = s.convertProtoreflectValue(list.Get(i))
		}
		return result
	default:
		// Try to handle other protoreflect types safely
		if value.Message().IsValid() {
			return s.convertMessageToMap(value.Message())
		}
		if value.List().IsValid() {
			list := value.List()
			result := make([]interface{}, list.Len())
			for i := 0; i < list.Len(); i++ {
				result[i] = s.convertProtoreflectValue(list.Get(i))
			}
			return result
		}
		return v
	}
}

func (s *KafkaSinker) convertMessageToMap(message protoreflect.Message) map[string]interface{} {
	result := make(map[string]interface{})
	fields := message.Descriptor().Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		value := message.Get(field)
		result[string(field.Name())] = s.convertProtoreflectValue(value)
	}

	return result
}
