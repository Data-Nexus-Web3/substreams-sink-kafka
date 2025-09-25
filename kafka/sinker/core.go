package sinker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func (s *KafkaSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// Increment block counter atomically for thread safety
	atomic.AddInt64(&s.blocksProcessed, 1)

	// Update received cursor (for undo signal handling)
	s.lastReceivedCursor = cursor

	// Check if undo buffer is enabled
	if s.undoBufferSize > 0 && s.undoBuffer != nil {
		// Undo buffer mode: buffer the block instead of processing immediately
		return s.undoBuffer.AddBlock(data, isLive, cursor)
	}

	// Immediate processing mode: process block right away
	if s.explodeFieldName != "" {
		return s.handleWithFieldExplosion(ctx, data, isLive, cursor)
	}

	// Default behavior (no explosion)
	return s.handleWithoutExplosion(ctx, data, isLive, cursor)
}

func (s *KafkaSinker) handleWithoutExplosion(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// 🚀 OPTIMIZED KEY GENERATION: Use string builder from pool instead of fmt.Sprintf
	keyBuilder := stringBuilderPool.Get().(*strings.Builder)
	defer stringBuilderPool.Put(keyBuilder)

	keyBuilder.Reset()
	keyBuilder.WriteString("block_")
	keyBuilder.WriteString(strconv.FormatUint(data.Clock.Number, 10))
	keyBuilder.WriteString("_")
	keyBuilder.WriteString(data.Clock.Id)
	key := keyBuilder.String()

	// Serialize based on output format
	var messageBytes []byte
	var err error

	// Start timing for performance metrics
	startTime := time.Now()

	// Apply all field mutations to the protobuf message BEFORE serialization
	// This ensures ALL output formats (json, protobuf, schema-registry) benefit from the mutations
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		mutatedBytes, err := s.applyUpstreamMutations(ctx, data.Output.MapOutput.Value, data.Clock.Number)
		if err != nil {
			s.logger.Warn("Upstream mutations failed, using original message", zap.Error(err))
		} else {
			// Replace the original message with the mutated version
			data.Output.MapOutput.Value = mutatedBytes
		}
	}

	switch s.outputFormat {
	case "json":
		messageBytes, err = s.serializeToJSON(data)
		if err != nil {
			return fmt.Errorf("failed to serialize to JSON: %w", err)
		}
		s.logger.Debug("Serialized to JSON",
			zap.String("format", "json"),
			zap.Int("message_bytes", len(messageBytes)),
			zap.Duration("serialization_time", time.Since(startTime)),
		)

	case "protobuf":
		messageBytes, err = s.serializeToRawProtobuf(data)
		if err != nil {
			return fmt.Errorf("failed to serialize to raw protobuf: %w", err)
		}
		s.logger.Debug("Serialized to raw protobuf",
			zap.String("format", "protobuf"),
			zap.Int("message_bytes", len(messageBytes)),
			zap.Duration("serialization_time", time.Since(startTime)),
		)

	case "schema-registry":
		messageBytes, err = s.serializeWithSchemaRegistry(data)
		if err != nil {
			return fmt.Errorf("failed to serialize with Schema Registry: %w", err)
		}
		s.logger.Debug("Serialized with Schema Registry",
			zap.String("format", "schema-registry"),
			zap.Int("message_bytes", len(messageBytes)),
			zap.Duration("serialization_time", time.Since(startTime)),
		)

	default:
		return fmt.Errorf("unsupported output format: %s", s.outputFormat)
	}

	// Store pending message for confirmation tracking
	s.addPendingMessage(key, cursor)

	// 🚀 ASYNC FIRE-AND-FORGET PRODUCTION FOR MAXIMUM SPEED
	err = s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &s.topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: messageBytes,
	}, s.deliveryChan)
	if err != nil {
		// Remove from pending on failure
		s.removePendingMessage(key)
		return fmt.Errorf("failed to produce message to Kafka: %w", err)
	}

	// Increment produced counter atomically
	atomic.AddInt64(&s.messagesProduced, 1)

	// Report mutation statistics periodically
	if s.mutationStats != nil && s.mutationStats.ShouldReportStats() {
		s.mutationStats.ReportStats(s.logger)
	}
	atomic.AddInt64(&s.messagesPending, 1)

	// Check for backpressure - if too many pending messages, apply gentle throttling
	if atomic.LoadInt64(&s.messagesPending) > int64(s.batchSize*10) {
		// Brief pause to let confirmations catch up
		time.Sleep(1 * time.Millisecond)
	}

	// Update performance metrics and log progress
	s.updatePerformanceMetrics()

	return nil
}

func (s *KafkaSinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	s.undoSignalsHandled++

	s.logger.Warn("chain reorganization detected",
		zap.String("last_valid_block_id", undoSignal.LastValidBlock.Id),
		zap.Uint64("last_valid_block_number", undoSignal.LastValidBlock.Number),
		zap.Int64("undo_signals_handled", s.undoSignalsHandled),
		zap.Bool("undo_buffer_enabled", s.undoBufferSize > 0),
	)

	// If undo buffer is enabled, handle reorg in buffer instead of sending to Kafka
	if s.undoBufferSize > 0 && s.undoBuffer != nil {
		s.logger.Info("undo buffer enabled - handling reorg in buffer, not sending undo signal to Kafka",
			zap.Int("buffer_size", s.undoBufferSize),
			zap.Uint64("last_valid_block", undoSignal.LastValidBlock.Number),
		)

		// Handle the undo in the buffer (discard invalidated blocks)
		s.undoBuffer.HandleUndo(undoSignal)

		// Save cursor but don't send undo message to Kafka
		if err := s.saveCursor(cursor); err != nil {
			s.logger.Warn("failed to save cursor after undo", zap.Error(err))
		}

		return nil
	}

	// Original immediate mode behavior: send undo signal to Kafka
	s.logger.Info("immediate processing mode - sending undo signal to Kafka for downstream consumers")

	// For Kafka, we can't "undo" messages that have already been sent,
	// but we can send an undo signal message to inform downstream consumers
	key := fmt.Sprintf("undo_%d_%s", undoSignal.LastValidBlock.Number, undoSignal.LastValidBlock.Id)

	// Serialize the undo signal
	var messageBytes []byte
	var err error

	// Use a separate topic for undo signals to avoid mixing message types
	undoTopic := s.topic + "_undo"

	if s.useSchemaRegistry {
		// Use Schema Registry serialization for undo signals too
		messageBytes, err = s.protobufSerializer.Serialize(undoTopic, undoSignal)
		if err != nil {
			return fmt.Errorf("failed to serialize BlockUndoSignal with Schema Registry: %w", err)
		}
	} else {
		// Use standard protobuf serialization
		messageBytes, err = proto.Marshal(undoSignal)
		if err != nil {
			return fmt.Errorf("failed to marshal BlockUndoSignal: %w", err)
		}
	}

	// Send undo signal to Kafka
	err = s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &undoTopic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: messageBytes,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to produce undo signal to Kafka: %w", err)
	}

	// Save the cursor pointing to the last valid block
	if err := s.saveCursor(cursor); err != nil {
		s.logger.Warn("failed to save cursor after undo", zap.Error(err))
	}

	return nil
}

func (s *KafkaSinker) Run(ctx context.Context) error {
	cursor, err := s.loadCursor()
	if err != nil {
		s.logger.Warn("failed to load cursor, starting from beginning", zap.Error(err))
		cursor = sink.NewBlankCursor()
	}

	s.logger.Info("starting Kafka sinker", zap.String("cursor", cursor.String()))

	// Run the base sinker with our handlers
	s.Sinker.Run(ctx, cursor, s)
	return s.Sinker.Err()
}

func (s *KafkaSinker) Close() error {
	if s.IsTerminated() {
		return nil
	}

	blocksProcessed := atomic.LoadInt64(&s.blocksProcessed)
	messagesProduced := atomic.LoadInt64(&s.messagesProduced)
	messagesConfirmed := atomic.LoadInt64(&s.messagesConfirmed)
	messagesPending := atomic.LoadInt64(&s.messagesPending)

	s.logger.Info("🏁 SHUTTING DOWN HIGH-PERFORMANCE KAFKA SINKER",
		zap.Int64("total_blocks_processed", blocksProcessed),
		zap.Int64("total_messages_produced", messagesProduced),
		zap.Int64("total_messages_confirmed", messagesConfirmed),
		zap.Int64("messages_pending", messagesPending),
		zap.Int64("total_undo_signals", s.undoSignalsHandled),
	)

	if s.producer != nil {
		// Flush any remaining messages with extended timeout for high volume
		s.logger.Info("flushing remaining messages to Kafka...")
		remaining := s.producer.Flush(60 * 1000) // 60 seconds timeout for high throughput
		if remaining > 0 {
			s.logger.Warn("failed to flush all messages", zap.Int("remaining", remaining))
		} else {
			s.logger.Info("✅ all messages successfully flushed to Kafka")
		}

		s.producer.Close()
	}

	// Close token normalizer if enabled
	if s.tokenNormalizer != nil {
		if err := s.tokenNormalizer.Close(); err != nil {
			s.logger.Warn("Failed to close token normalizer", zap.Error(err))
		}
	}

	// Close undo buffer if enabled
	if s.undoBuffer != nil {
		if err := s.undoBuffer.Close(); err != nil {
			s.logger.Warn("Failed to close undo buffer", zap.Error(err))
		}
	}

	// Stop background timers
	if s.cursorSaveTicker != nil {
		s.cursorSaveTicker.Stop()
	}

	s.Shutdown(nil)
	return nil
}

// applyTokenNormalization applies token normalization to the block data
func (s *KafkaSinker) applyTokenNormalization(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData) error {
	if data.Output == nil || data.Output.MapOutput == nil || data.Output.MapOutput.Value == nil {
		return nil // No data to normalize
	}

	// Deserialize the message to extract token information
	dynamicMsg := dynamicpb.NewMessage(s.v2MessageDescriptor)
	if err := proto.Unmarshal(data.Output.MapOutput.Value, dynamicMsg); err != nil {
		return fmt.Errorf("failed to unmarshal message for token normalization: %w", err)
	}

	// Extract unique token addresses from the message
	uniqueTokens := s.tokenNormalizer.ExtractUniqueTokens(dynamicMsg, s.tokenAddressFields)
	if len(uniqueTokens) == 0 {
		return nil // No tokens to normalize
	}

	// Batch lookup token metadata
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, data.Clock.Number, uniqueTokens)
	if err != nil {
		return fmt.Errorf("failed to get token metadata: %w", err)
	}

	// Apply normalization to the message
	s.normalizeTokenFields(dynamicMsg, tokenMetadata)

	// Apply field processing (token metadata injection, etc.) if enabled
	if err := s.processMessageWithFieldAnnotations(ctx, dynamicMsg, data.Clock.Number); err != nil {
		s.logger.Warn("Field processing failed during token normalization", zap.Error(err))
	}

	// Re-serialize the normalized message back to the data
	normalizedBytes, err := proto.Marshal(dynamicMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal normalized message: %w", err)
	}

	// Update the original data with normalized message
	data.Output.MapOutput.Value = normalizedBytes

	return nil
}

// normalizeTokenFields recursively normalizes token amount fields in the message
func (s *KafkaSinker) normalizeTokenFields(message protoreflect.Message, tokenMetadata map[string]*TokenMetadata) {
	msgDesc := message.Descriptor()
	fields := msgDesc.Fields()

	// Iterate through all fields in the message
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := string(field.Name())

		if !message.Has(field) {
			continue
		}

		value := message.Get(field)

		// Check if this is a token value field that needs normalization
		for _, tokenValueField := range s.tokenValueFields {
			if fieldName == tokenValueField {
				s.normalizeTokenValueField(message, field, value, tokenMetadata)
				break
			}
		}

		// Recursively process nested messages
		if field.Kind() == protoreflect.MessageKind {
			if field.IsList() {
				// List of messages
				list := value.List()
				for j := 0; j < list.Len(); j++ {
					nestedMsg := list.Get(j).Message()
					s.normalizeTokenFields(nestedMsg, tokenMetadata)
				}
			} else {
				// Single nested message
				nestedMsg := value.Message()
				s.normalizeTokenFields(nestedMsg, tokenMetadata)
			}
		}
	}
}

// normalizeTokenValueField normalizes a specific token value field
func (s *KafkaSinker) normalizeTokenValueField(message protoreflect.Message, field protoreflect.FieldDescriptor, value protoreflect.Value, tokenMetadata map[string]*TokenMetadata) {
	// Find the corresponding token address field
	tokenAddress := s.findTokenAddress(message, field)
	if tokenAddress == "" {
		return
	}

	// Get token metadata
	metadata, exists := tokenMetadata[tokenAddress]
	if !exists {
		return
	}

	// Get the raw token amount
	rawAmount := ""
	switch field.Kind() {
	case protoreflect.StringKind:
		rawAmount = value.String()
	case protoreflect.Uint64Kind:
		rawAmount = fmt.Sprintf("%d", value.Uint())
	case protoreflect.Int64Kind:
		rawAmount = fmt.Sprintf("%d", value.Int())
	default:
		return // Unsupported field type
	}

	if rawAmount == "" || rawAmount == "0" {
		return
	}

	// Normalize the amount
	formattedAmount, err := s.tokenNormalizer.NormalizeTokenAmount(rawAmount, metadata.Decimals)
	if err != nil {
		s.logger.Debug("Failed to normalize token amount",
			zap.String("raw_amount", rawAmount),
			zap.String("token_address", tokenAddress),
			zap.Uint32("decimals", metadata.Decimals),
			zap.Error(err),
		)
		return
	}

	// Create a new field name for the formatted amount
	formattedFieldName := string(field.Name()) + "_formatted"

	// Try to find or create the formatted field
	msgDesc := message.Descriptor()
	formattedField := msgDesc.Fields().ByName(protoreflect.Name(formattedFieldName))

	if formattedField != nil && formattedField.Kind() == protoreflect.StringKind {
		// Set the formatted amount
		message.Set(formattedField, protoreflect.ValueOfString(formattedAmount))

		s.logger.Debug("Token amount normalized",
			zap.String("field", string(field.Name())),
			zap.String("raw_amount", rawAmount),
			zap.String("formatted_amount", formattedAmount),
			zap.String("token_address", tokenAddress),
			zap.String("symbol", metadata.Symbol),
			zap.Uint32("decimals", metadata.Decimals),
		)
	}
}

// findTokenAddress finds the token address corresponding to a token value field
func (s *KafkaSinker) findTokenAddress(message protoreflect.Message, valueField protoreflect.FieldDescriptor) string {
	msgDesc := message.Descriptor()

	// Try each configured token address field
	for _, addressFieldName := range s.tokenAddressFields {
		addressField := msgDesc.Fields().ByName(protoreflect.Name(addressFieldName))
		if addressField != nil && addressField.Kind() == protoreflect.StringKind && message.Has(addressField) {
			return message.Get(addressField).String()
		}
	}

	return ""
}
