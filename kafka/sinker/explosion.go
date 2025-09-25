package sinker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	pbkafka "github.com/streamingfast/substreams-sink-kafka/proto/sf/substreams/sink/kafka/v1"
	tokenmetadatav1 "github.com/streamingfast/substreams-sink-kafka/proto/token_metadata/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// 🚀 PHASE 3: OBJECT POOLS FOR MEMORY OPTIMIZATION
var (
	// Pool for ExplodedArrayItem slices to avoid repeated allocations
	explodedItemSlicePool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate with reasonable capacity
			return make([]*ExplodedArrayItem, 0, 1000)
		},
	}

	// Pool for BatchedKafkaMessage slices to avoid repeated allocations
	batchedMessageSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]*BatchedKafkaMessage, 0, 1000)
		},
	}

	// Pool for string builders to avoid repeated allocations
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			var sb strings.Builder
			sb.Grow(64) // Pre-allocate reasonable buffer size
			return &sb
		},
	}

	// Pool for SerializationJob slices for worker communication
	jobSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]SerializationJob, 0, 1000)
		},
	}

	// Pool for error slices to avoid allocations during error handling
	errorSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]error, 0, 10)
		},
	}
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

// SerializationJob represents work to be done by worker pool
type SerializationJob struct {
	Item                    *ExplodedArrayItem
	Index                   int
	Result                  chan SerializationResult
	PreFetchedTokenMetadata map[string]*tokenmetadatav1.TokenMetadata // Pre-fetched token metadata for batch
}

// SerializationResult represents the result of a serialization job
type SerializationResult struct {
	Message *BatchedKafkaMessage
	Error   error
	Index   int
}

// handleWithFieldExplosion processes messages by exploding a single specified array field using batch processing
func (s *KafkaSinker) handleWithFieldExplosion(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	s.logger.Debug("Processing with field explosion",
		zap.String("explode_field", s.explodeFieldName),
		zap.Uint64("block_number", data.Clock.Number),
	)

	// Note: Mutations are now applied to individual exploded items in serializeBatchedArrayItem()

	// Deserialize the main message using dynamic protobuf (now with mutations applied)
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

	// Early return for empty arrays
	if arraySize == 0 {
		return nil
	}

	// 🚀 PHASE 3: OBJECT POOL OPTIMIZATION - Get slice from pool
	batch := explodedItemSlicePool.Get().([]*ExplodedArrayItem)
	batch = batch[:0] // Reset length while keeping capacity

	// Ensure we have enough capacity, grow if needed
	if cap(batch) < arraySize {
		batch = make([]*ExplodedArrayItem, 0, arraySize)
	}

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

	// 🚀 PERFORMANCE OPTIMIZATION: Pre-fetch all token metadata for the entire batch
	var batchTokenMetadata map[string]*tokenmetadatav1.TokenMetadata
	if s.tokenNormalizer != nil && len(s.tokenValueFields) > 0 {
		var metadataErr error
		batchTokenMetadata, metadataErr = s.preFetchBatchTokenMetadata(batch)
		if metadataErr != nil {
			s.logger.Warn("Failed to pre-fetch batch token metadata", zap.Error(metadataErr))
			// Continue without token normalization rather than failing
		}
	}

	// Process the entire batch efficiently
	err = s.processBatchedArrayItemsWithMetadata(batch, batchTokenMetadata)

	// 🚀 PHASE 3: Return slice to pool for reuse
	// Clear references to prevent memory leaks
	for i := range batch {
		batch[i] = nil
	}
	batch = batch[:0] // Reset length
	explodedItemSlicePool.Put(batch)

	return err
}

// processBatchedArrayItems efficiently processes a batch of array items using parallel worker pool
func (s *KafkaSinker) processBatchedArrayItems(batch []*ExplodedArrayItem) error {
	return s.processBatchedArrayItemsWithMetadata(batch, nil)
}

// processBatchedArrayItemsWithMetadata efficiently processes a batch with pre-fetched token metadata
func (s *KafkaSinker) processBatchedArrayItemsWithMetadata(batch []*ExplodedArrayItem, batchTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) error {
	if len(batch) == 0 {
		return nil
	}

	startTime := time.Now()
	batchSize := len(batch)

	// 🚀 PHASE 2: PARALLEL WORKER POOL PROCESSING
	// Determine optimal worker count based on batch size and available CPU
	workerCount := s.calculateOptimalWorkerCount(batchSize)

	s.logger.Debug("Starting parallel batch processing",
		zap.Int("batch_size", batchSize),
		zap.Int("worker_count", workerCount),
		zap.String("output_format", s.outputFormat),
	)

	// Use parallel processing for larger batches, sequential for smaller ones
	if batchSize >= 100 && workerCount > 1 {
		return s.processParallelBatchWithMetadata(batch, workerCount, startTime, batchTokenMetadata)
	} else {
		return s.processSequentialBatchWithMetadata(batch, startTime, batchTokenMetadata)
	}
}

// calculateOptimalWorkerCount determines the best number of workers based on batch size
func (s *KafkaSinker) calculateOptimalWorkerCount(batchSize int) int {
	// Follow StreamingFast pattern: use 5 workers for parallel processing like files sink
	const maxWorkers = 5
	const minWorkers = 1
	const itemsPerWorker = 500 // Optimal items per worker based on testing

	if batchSize < 100 {
		return minWorkers // Use sequential processing for small batches
	}

	workerCount := (batchSize + itemsPerWorker - 1) / itemsPerWorker // Ceiling division
	if workerCount > maxWorkers {
		workerCount = maxWorkers
	}
	if workerCount < minWorkers {
		workerCount = minWorkers
	}

	return workerCount
}

// processParallelBatch processes a large batch using worker pool pattern with memory optimization
func (s *KafkaSinker) processParallelBatch(batch []*ExplodedArrayItem, workerCount int, startTime time.Time) error {
	return s.processParallelBatchWithMetadata(batch, workerCount, startTime, nil)
}

// processParallelBatchWithMetadata processes a large batch with pre-fetched token metadata
func (s *KafkaSinker) processParallelBatchWithMetadata(batch []*ExplodedArrayItem, workerCount int, startTime time.Time, batchTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) error {
	batchSize := len(batch)

	// 🚀 WORKER POOL PATTERN: Following StreamingFast files sink approach
	jobs := make(chan SerializationJob, batchSize)
	results := make(chan SerializationResult, batchSize)

	// 🚀 PHASE 3: OBJECT POOL OPTIMIZATION - Get slice from pool
	batchMessages := batchedMessageSlicePool.Get().([]*BatchedKafkaMessage)
	batchMessages = batchMessages[:0] // Reset length while keeping capacity

	// Ensure we have enough capacity, grow if needed
	if cap(batchMessages) < batchSize {
		batchMessages = make([]*BatchedKafkaMessage, batchSize)
	} else {
		// Extend slice to required length
		for len(batchMessages) < batchSize {
			batchMessages = append(batchMessages, nil)
		}
	}

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go s.serializationWorker(&wg, jobs, results)
	}

	// Send jobs to workers
	go func() {
		defer close(jobs)
		for i, item := range batch {
			job := SerializationJob{
				Item:                    item,
				Index:                   i,
				Result:                  results,
				PreFetchedTokenMetadata: batchTokenMetadata,
			}
			jobs <- job
		}
	}()

	// Close workers when done
	go func() {
		wg.Wait()
		close(results)
	}()

	// 🚀 PHASE 3: OBJECT POOL OPTIMIZATION - Get error slice from pool
	processingErrors := errorSlicePool.Get().([]error)
	processingErrors = processingErrors[:0] // Reset length while keeping capacity
	defer func() {
		// Return error slice to pool
		processingErrors = processingErrors[:0]
		errorSlicePool.Put(processingErrors)
	}()

	// Collect results in order
	for i := 0; i < batchSize; i++ {
		result := <-results
		if result.Error != nil {
			processingErrors = append(processingErrors, fmt.Errorf("worker error at index %d: %w", result.Index, result.Error))
		} else {
			batchMessages[result.Index] = result.Message
		}
	}

	// Check for worker errors
	if len(processingErrors) > 0 {
		return fmt.Errorf("parallel processing failed with %d errors: %v", len(processingErrors), processingErrors[0])
	}

	serializationTime := time.Since(startTime)

	// 🚀 BATCH KAFKA PRODUCTION: Produce all messages efficiently
	productionStart := time.Now()
	err := s.produceBatchMessages(batchMessages)
	if err != nil {
		// Clean up and return slice to pool before error return
		for i := range batchMessages {
			batchMessages[i] = nil
		}
		batchMessages = batchMessages[:0]
		batchedMessageSlicePool.Put(batchMessages)
		return fmt.Errorf("failed to produce batch messages: %w", err)
	}
	productionTime := time.Since(productionStart)

	// 🚀 PHASE 3: Clean up and return slice to pool
	for i := range batchMessages {
		batchMessages[i] = nil
	}
	batchMessages = batchMessages[:0]
	batchedMessageSlicePool.Put(batchMessages)

	s.logger.Debug("Parallel batch processing completed",
		zap.Int("batch_size", batchSize),
		zap.Int("worker_count", workerCount),
		zap.Duration("serialization_time", serializationTime),
		zap.Duration("production_time", productionTime),
		zap.Duration("total_time", time.Since(startTime)),
	)

	return nil
}

// processSequentialBatch processes smaller batches sequentially for efficiency with memory optimization
func (s *KafkaSinker) processSequentialBatch(batch []*ExplodedArrayItem, startTime time.Time) error {
	return s.processSequentialBatchWithMetadata(batch, startTime, nil)
}

// processSequentialBatchWithMetadata processes smaller batches with pre-fetched token metadata
func (s *KafkaSinker) processSequentialBatchWithMetadata(batch []*ExplodedArrayItem, startTime time.Time, batchTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) error {
	batchSize := len(batch)

	// 🚀 PHASE 3: OBJECT POOL OPTIMIZATION - Get slice from pool
	batchMessages := batchedMessageSlicePool.Get().([]*BatchedKafkaMessage)
	batchMessages = batchMessages[:0] // Reset length while keeping capacity

	// Ensure we have enough capacity, grow if needed
	if cap(batchMessages) < batchSize {
		batchMessages = make([]*BatchedKafkaMessage, 0, batchSize)
	}

	// 🚀 PHASE 3: OBJECT POOL OPTIMIZATION - Get string builder from pool
	keyBuilder := stringBuilderPool.Get().(*strings.Builder)
	keyBuilder.Reset() // Reset the builder

	// Pre-allocate dynamic message for schema-registry to avoid repeated allocations
	var reusableMsg *dynamicpb.Message
	var individualMsgDesc protoreflect.MessageDescriptor
	if s.outputFormat == "schema-registry" && batchSize > 0 {
		individualMsgDesc = batch[0].Field.Message()
		if individualMsgDesc != nil {
			reusableMsg = dynamicpb.NewMessage(individualMsgDesc)
		}
	}

	s.logger.Debug("Processing sequential batch of exploded array items",
		zap.Int("batch_size", batchSize),
		zap.String("output_format", s.outputFormat),
	)

	// Serialize all items in the batch
	for _, item := range batch {
		// 🚀 OPTIMIZED KEY GENERATION: Use pooled string builder instead of fmt.Sprintf
		keyBuilder.Reset()
		keyBuilder.WriteString("block_")
		keyBuilder.WriteString(strconv.FormatUint(item.BlockData.Clock.Number, 10))
		keyBuilder.WriteString("_")
		keyBuilder.WriteString(item.BlockData.Clock.Id)
		keyBuilder.WriteString("_")
		keyBuilder.WriteString(strconv.Itoa(item.Index))
		key := keyBuilder.String()

		// Serialize the array item based on output format
		messageBytes, err := s.serializeBatchedArrayItemWithMetadata(item, reusableMsg, batchTokenMetadata)
		if err != nil {
			// Clean up pools before error return
			stringBuilderPool.Put(keyBuilder)
			for i := range batchMessages {
				batchMessages[i] = nil
			}
			batchMessages = batchMessages[:0]
			batchedMessageSlicePool.Put(batchMessages)
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
		// Clean up pools before error return
		stringBuilderPool.Put(keyBuilder)
		for i := range batchMessages {
			batchMessages[i] = nil
		}
		batchMessages = batchMessages[:0]
		batchedMessageSlicePool.Put(batchMessages)
		return fmt.Errorf("failed to produce batch messages: %w", err)
	}
	productionTime := time.Since(productionStart)

	// 🚀 PHASE 3: Clean up and return objects to pools
	stringBuilderPool.Put(keyBuilder)
	for i := range batchMessages {
		batchMessages[i] = nil
	}
	batchMessages = batchMessages[:0]
	batchedMessageSlicePool.Put(batchMessages)

	s.logger.Debug("Sequential batch processing completed",
		zap.Int("batch_size", batchSize),
		zap.Duration("serialization_time", serializationTime),
		zap.Duration("production_time", productionTime),
		zap.Duration("total_time", time.Since(startTime)),
	)

	return nil
}

// serializationWorker processes serialization jobs in parallel with object pool optimization
func (s *KafkaSinker) serializationWorker(wg *sync.WaitGroup, jobs <-chan SerializationJob, results chan<- SerializationResult) {
	defer wg.Done()

	// 🚀 PHASE 3: OBJECT POOL OPTIMIZATION - Get string builder from pool per worker
	keyBuilder := stringBuilderPool.Get().(*strings.Builder)
	defer stringBuilderPool.Put(keyBuilder) // Return to pool when worker finishes

	// Pre-allocate dynamic message per worker for schema-registry to avoid contention
	var reusableMsg *dynamicpb.Message
	var individualMsgDesc protoreflect.MessageDescriptor

	for job := range jobs {
		// 🚀 OPTIMIZED KEY GENERATION: Use pooled string builder instead of fmt.Sprintf
		keyBuilder.Reset()
		keyBuilder.WriteString("block_")
		keyBuilder.WriteString(strconv.FormatUint(job.Item.BlockData.Clock.Number, 10))
		keyBuilder.WriteString("_")
		keyBuilder.WriteString(job.Item.BlockData.Clock.Id)
		keyBuilder.WriteString("_")
		keyBuilder.WriteString(strconv.Itoa(job.Item.Index))
		key := keyBuilder.String()

		// Initialize reusable message for schema-registry if needed
		if s.outputFormat == "schema-registry" && reusableMsg == nil && job.Item.Field.Message() != nil {
			individualMsgDesc = job.Item.Field.Message()
			reusableMsg = dynamicpb.NewMessage(individualMsgDesc)
		}

		// Serialize the array item
		messageBytes, err := s.serializeBatchedArrayItemWithMetadata(job.Item, reusableMsg, job.PreFetchedTokenMetadata)

		result := SerializationResult{
			Index: job.Index,
			Error: err,
		}

		if err == nil {
			result.Message = &BatchedKafkaMessage{
				Key:          key,
				MessageBytes: messageBytes,
				Cursor:       job.Item.Cursor,
			}
		}

		// Send result back
		results <- result
	}
}

// serializeBatchedArrayItem serializes a single array item using optimized patterns
func (s *KafkaSinker) serializeBatchedArrayItem(item *ExplodedArrayItem, reusableMsg *dynamicpb.Message) ([]byte, error) {
	return s.serializeBatchedArrayItemWithMetadata(item, reusableMsg, nil)
}

// serializeBatchedArrayItemWithMetadata serializes a single array item with pre-fetched token metadata
func (s *KafkaSinker) serializeBatchedArrayItemWithMetadata(item *ExplodedArrayItem, reusableMsg *dynamicpb.Message, preFetchedTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) ([]byte, error) {
	switch s.outputFormat {
	case "json":
		// 🎯 APPLY MUTATIONS TO INDIVIDUAL EXPLODED ITEM
		// This is where the actual fields like ops_token, ops_amount exist
		mutatedValue, err := s.applyMutationsToExplodedItemWithMetadata(item, preFetchedTokenMetadata)
		if err != nil {
			s.logger.Warn("Failed to apply mutations to exploded item", zap.Error(err))
			// Fall back to original value
			mutatedValue = s.convertProtoreflectValue(item.Value)
		}

		jsonBytes, err := json.Marshal(mutatedValue)
		if err != nil {
			return nil, err
		}

		// Token normalization already applied upstream in applyUpstreamMutations()
		// Exploded items inherit the mutations from the parent message

		return jsonBytes, nil

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

	// 🚀 OPTIMIZED KEY GENERATION: Use string builder from pool instead of fmt.Sprintf
	keyBuilder := stringBuilderPool.Get().(*strings.Builder)
	defer stringBuilderPool.Put(keyBuilder)

	keyBuilder.Reset()
	keyBuilder.WriteString("exploded_")
	keyBuilder.WriteString(strconv.FormatUint(data.Clock.Number, 10))
	keyBuilder.WriteString("_")
	keyBuilder.WriteString(data.Clock.Id)
	keyBuilder.WriteString("_")
	keyBuilder.WriteString(strconv.Itoa(index))
	key := keyBuilder.String()

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

// convertProtoreflectValueWithField converts a protoreflect.Value to a Go interface{} with field context for enum handling
func (s *KafkaSinker) convertProtoreflectValueWithField(value protoreflect.Value, field protoreflect.FieldDescriptor) interface{} {
	// Debug logging to see what type we're dealing with
	if s.debugMode {
		s.logger.Debug("Converting protoreflect value with field context",
			zap.String("field_name", string(field.Name())),
			zap.String("field_kind", field.Kind().String()),
			zap.String("type", fmt.Sprintf("%T", value.Interface())),
			zap.String("value", value.String()))
	}

	// Check if this field is an enum
	if field.Kind() == protoreflect.EnumKind {
		// For enum fields, convert the numeric value to the enum name
		enumValue := value.Enum()
		enumDescriptor := field.Enum()
		enumValueDescriptor := enumDescriptor.Values().ByNumber(enumValue)
		if enumValueDescriptor != nil {
			return string(enumValueDescriptor.Name())
		}
		// Fallback to numeric value if name not found
		return int32(enumValue)
	}

	// For non-enum fields, use the regular conversion
	return s.convertProtoreflectValue(value)
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
		// Convert bytes based on configuration flag
		if s.hexEncodeBytes {
			// Convert bytes to hex string with 0x prefix (more readable than base64)
			return "0x" + hex.EncodeToString(v)
		}
		// Return raw bytes (default behavior)
		return v
	case protoreflect.EnumNumber:
		// Handle enums - convert to their string name (much more readable than numeric value)
		return value.String()
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
		// Handle unknown types safely - avoid calling .Message() on non-message types
		// This prevents panics when encountering enums or other types
		return value.String()
	}
}

func (s *KafkaSinker) convertMessageToMap(message protoreflect.Message) map[string]interface{} {
	result := make(map[string]interface{})
	fields := message.Descriptor().Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		value := message.Get(field)
		result[string(field.Name())] = s.convertProtoreflectValueWithField(value, field)
	}

	return result
}

// applyMutationsToExplodedItem applies mutations to an individual exploded array item
func (s *KafkaSinker) applyMutationsToExplodedItem(item *ExplodedArrayItem) (interface{}, error) {
	return s.applyMutationsToExplodedItemWithMetadata(item, nil)
}

// applyMutationsToExplodedItemWithMetadata applies mutations with pre-fetched token metadata
func (s *KafkaSinker) applyMutationsToExplodedItemWithMetadata(item *ExplodedArrayItem, preFetchedTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) (interface{}, error) {
	// Convert the protoreflect.Value to a map for easier manipulation
	itemMap := s.convertProtoreflectValue(item.Value)
	itemMapTyped, ok := itemMap.(map[string]interface{})
	if !ok {
		s.logger.Warn("Exploded item is not a map, cannot apply mutations")
		return itemMap, nil
	}

	if s.debugMode {
		s.logger.Debug("Processing exploded item",
			zap.Int("item_index", item.Index),
			zap.Uint64("block_number", item.BlockData.Clock.Number),
		)
	}

	// Apply token amount formatting
	if s.tokenNormalizer != nil && len(s.tokenValueFields) > 0 {
		if preFetchedTokenMetadata != nil {
			// Use pre-fetched metadata for better performance
			s.normalizeTokenFieldsInJSON(itemMapTyped, preFetchedTokenMetadata)
		} else {
			// Fallback to individual lookup (slower)
			err := s.applyTokenFormattingToMap(itemMapTyped, item.BlockData.Clock.Number)
			if err != nil {
				s.logger.Warn("Token formatting failed for exploded item", zap.Error(err))
			}
		}
	}

	// Apply token metadata injection
	if len(s.tokenMetadataRules) > 0 {
		if preFetchedTokenMetadata != nil {
			// Use pre-fetched metadata for better performance
			s.applyTokenMetadataToMapWithMetadata(itemMapTyped, preFetchedTokenMetadata)
		} else {
			// Fallback to individual lookup (slower)
			err := s.applyTokenMetadataToMap(itemMapTyped, item.BlockData.Clock.Number)
			if err != nil {
				s.logger.Warn("Token metadata injection failed for exploded item", zap.Error(err))
			}
		}
	}

	// Apply field filtering (omit empty strings)
	if s.fieldProcessingConfig != nil && s.fieldProcessingConfig.OmitEmptyStrings {
		s.filterEmptyFieldsFromMap(itemMapTyped)
	}

	return itemMapTyped, nil
}

// applyTokenFormattingToMap applies token amount formatting to a map representation
func (s *KafkaSinker) applyTokenFormattingToMap(itemMap map[string]interface{}, blockNumber uint64) error {
	// Use optimized normalizer if available
	if s.optimizedNormalizer != nil {
		return s.optimizedNormalizer.OptimizedNormalizeJSON(itemMap, blockNumber)
	}

	// Fallback to original implementation
	// Extract unique token addresses using the same logic as JSON serialization
	uniqueTokens := s.extractUniqueTokensFromJSON(itemMap, s.tokenAddressFields)
	if len(uniqueTokens) == 0 {
		return nil
	}

	// Get token metadata for all found tokens
	ctx := context.Background()
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, uniqueTokens)
	if err != nil {
		return err
	}

	// Apply normalization using the same logic as JSON serialization
	s.normalizeTokenFieldsInJSON(itemMap, tokenMetadata)
	return nil
}

// applyTokenMetadataToMap applies token metadata injection to a map representation
func (s *KafkaSinker) applyTokenMetadataToMap(itemMap map[string]interface{}, blockNumber uint64) error {
	// Find token address using priority fields
	var tokenAddress string
	for _, fieldName := range s.fieldProcessingConfig.DefaultTokenSourceFields {
		if addr, exists := itemMap[fieldName]; exists {
			if addrStr, ok := addr.(string); ok && addrStr != "" {
				tokenAddress = addrStr
				break
			}
		}
	}

	if tokenAddress == "" {
		return nil
	}

	// Get token metadata
	ctx := context.Background()
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, []string{tokenAddress})
	if err != nil {
		return err
	}

	metadata, exists := tokenMetadata[tokenAddress]
	if !exists {
		return nil
	}

	// Inject metadata into configured fields
	for _, rule := range s.tokenMetadataRules {
		itemMap[rule.TargetField] = map[string]interface{}{
			"address":        metadata.Address,
			"decimals":       metadata.Decimals,
			"symbol":         metadata.Symbol,
			"name":           metadata.Name,
			"validFromBlock": metadata.ValidFromBlock,
		}

		s.mutationStats.RecordTokenMetadataInjected()

		if s.debugMode {
			s.logger.Debug("Token metadata injected",
				zap.String("target_field", rule.TargetField),
				zap.String("token_address", tokenAddress),
				zap.String("symbol", metadata.Symbol),
			)
		}
	}

	return nil
}

// filterEmptyFieldsFromMap removes empty string fields from a map
func (s *KafkaSinker) filterEmptyFieldsFromMap(itemMap map[string]interface{}) {
	for key, value := range itemMap {
		if strValue, ok := value.(string); ok && strValue == "" {
			delete(itemMap, key)
			s.mutationStats.RecordEmptyFieldFiltered()
		}
	}
}

// preFetchBatchTokenMetadata pre-fetches all token metadata needed for a batch of items
func (s *KafkaSinker) preFetchBatchTokenMetadata(batch []*ExplodedArrayItem) (map[string]*tokenmetadatav1.TokenMetadata, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	// Collect all unique tokens from the entire batch
	uniqueTokens := make(map[string]bool)
	blockNumber := batch[0].BlockData.Clock.Number // All items in batch have same block number

	for _, item := range batch {
		// Convert item to map for token extraction
		itemMap := s.convertProtoreflectValue(item.Value)
		if itemMapTyped, ok := itemMap.(map[string]interface{}); ok {
			// Extract tokens for NORMALIZATION (using tokenAddressFields)
			normalizationTokens := s.extractUniqueTokensFromJSON(itemMapTyped, s.tokenAddressFields)
			for _, token := range normalizationTokens {
				uniqueTokens[token] = true
			}

			// Extract tokens for METADATA INJECTION (using DefaultTokenSourceFields)
			if s.fieldProcessingConfig != nil && len(s.fieldProcessingConfig.DefaultTokenSourceFields) > 0 {
				metadataTokens := s.extractUniqueTokensFromJSON(itemMapTyped, s.fieldProcessingConfig.DefaultTokenSourceFields)
				for _, token := range metadataTokens {
					uniqueTokens[token] = true
				}
			}
		}
	}

	// Convert to slice
	tokenList := make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		tokenList = append(tokenList, token)
	}

	if len(tokenList) == 0 {
		return nil, nil
	}

	// Single batch lookup for all tokens
	ctx := context.Background()
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, tokenList)
	if err != nil {
		return nil, err
	}

	if s.debugMode {
		s.logger.Debug("Pre-fetched batch token metadata",
			zap.Int("batch_size", len(batch)),
			zap.Int("unique_tokens", len(tokenList)),
			zap.Strings("tokens", tokenList),
			zap.Strings("normalization_fields", s.tokenAddressFields),
			zap.Strings("metadata_injection_fields", s.fieldProcessingConfig.DefaultTokenSourceFields),
			zap.Uint64("block_number", blockNumber),
		)
	}

	return tokenMetadata, nil
}

// applyTokenMetadataToMapWithMetadata applies token metadata injection using pre-fetched metadata
func (s *KafkaSinker) applyTokenMetadataToMapWithMetadata(itemMap map[string]interface{}, preFetchedTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) {
	// Find token addresses using priority fields (now supports nested paths)
	var tokenAddresses []string
	var sourcePathInfo NestedPathInfo

	for _, fieldName := range s.fieldProcessingConfig.DefaultTokenSourceFields {
		pathInfo := ParseNestedFieldPath(fieldName)
		addresses := ExtractTokenAddressesFromNestedPath(itemMap, pathInfo)

		if len(addresses) > 0 {
			tokenAddresses = addresses
			sourcePathInfo = pathInfo
			break // Use first matching field in priority order
		}
	}

	if len(tokenAddresses) == 0 {
		return
	}

	// Inject metadata into configured fields
	for _, rule := range s.tokenMetadataRules {
		s.injectTokenMetadataAtNestedPath(itemMap, rule.TargetField, tokenAddresses, sourcePathInfo, preFetchedTokenMetadata)
	}
}

// injectTokenMetadataAtNestedPath injects token metadata at a nested path with array inference
func (s *KafkaSinker) injectTokenMetadataAtNestedPath(itemMap map[string]interface{}, targetField string, tokenAddresses []string, sourcePathInfo NestedPathInfo, preFetchedTokenMetadata map[string]*tokenmetadatav1.TokenMetadata) {
	targetPathInfo := ParseNestedFieldPath(targetField)

	// Determine if we should inject as array or single object based on source structure
	isArraySource := len(tokenAddresses) > 1
	if !isArraySource && sourcePathInfo.IsNested {
		// Check if the source container was an array
		container := GetNestedValueFromPath(itemMap, sourcePathInfo.ContainerPath)
		if _, isArray := container.([]interface{}); isArray {
			isArraySource = true
		}
	}

	if targetPathInfo.IsNested {
		// Inject at nested path
		s.setNestedValue(itemMap, targetPathInfo.ContainerPath, s.createTokenMetadataObjects(tokenAddresses, preFetchedTokenMetadata, isArraySource))
	} else {
		// Inject at root level
		itemMap[targetField] = s.createTokenMetadataObjects(tokenAddresses, preFetchedTokenMetadata, isArraySource)
	}

	if s.debugMode {
		s.logger.Debug("Token metadata injected at nested path",
			zap.String("target_field", targetField),
			zap.Strings("token_addresses", tokenAddresses),
			zap.Bool("is_array_source", isArraySource),
			zap.Bool("is_nested", targetPathInfo.IsNested),
		)
	}
}

// createTokenMetadataObjects creates token metadata objects (single or array based on source)
func (s *KafkaSinker) createTokenMetadataObjects(tokenAddresses []string, preFetchedTokenMetadata map[string]*tokenmetadatav1.TokenMetadata, isArraySource bool) interface{} {
	var metadataObjects []map[string]interface{}
	seenAddresses := make(map[string]bool) // Deduplicate addresses

	for _, address := range tokenAddresses {
		// Skip if we've already processed this address
		if seenAddresses[address] {
			continue
		}
		seenAddresses[address] = true

		if metadata, exists := preFetchedTokenMetadata[address]; exists {
			metadataObj := map[string]interface{}{
				"address":  metadata.Address,
				"decimals": metadata.Decimals,
				"symbol":   metadata.Symbol,
				"name":     metadata.Name,
				// Exclude valid_from_block and valid_to_block from explosion logic too
			}
			metadataObjects = append(metadataObjects, metadataObj)
		}
	}

	if isArraySource || len(metadataObjects) > 1 {
		return metadataObjects
	} else if len(metadataObjects) == 1 {
		return metadataObjects[0]
	}

	return nil
}

// setNestedValue sets a value at a nested path in a map
func (s *KafkaSinker) setNestedValue(data map[string]interface{}, path string, value interface{}) {
	if path == "" {
		return
	}

	parts := strings.Split(path, ".")
	current := data

	// Navigate to the parent of the target field
	for _, part := range parts[:len(parts)-1] {
		if next, exists := current[part]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// Path exists but is not a map, create new map
				newMap := make(map[string]interface{})
				current[part] = newMap
				current = newMap
			}
		} else {
			// Create new nested map
			newMap := make(map[string]interface{})
			current[part] = newMap
			current = newMap
		}
	}

	// Set the final value
	finalPart := parts[len(parts)-1]
	current[finalPart] = value
}
