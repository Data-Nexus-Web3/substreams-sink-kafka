package sinker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pbkafka "github.com/Data-Nexus-Web3/substreams-sink-kafka/proto/sf/substreams/sink/kafka/v1"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// bufferedProcessItem represents a block ready for processing after undo buffering
type bufferedProcessItem struct {
	data   *pbsubstreamsrpc.BlockScopedData
	isLive *bool
	cursor *sink.Cursor
}

// sizeEstimate estimates the bytes of a block payload for buffering
func sizeEstimate(data *pbsubstreamsrpc.BlockScopedData) int64 {
	if data != nil && data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		return int64(len(data.Output.MapOutput.Value)) + 512 // small overhead
	}
	return 1024 // minimal default
}

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

	// If processing buffer is enabled, enqueue and return immediately
	if (s.processingBufferSize > 0 || s.processingBufferBytes > 0) && s.processingQueue != nil {
		// Byte-budgeted admission control with optional item cap
		itemSize := sizeEstimate(data)
		// Block until there is capacity (uses a mutex to guard bytes counter)
		var once sync.Once
		for {
			// Check item limit
			if s.processingBufferMaxItems > 0 && len(s.processingQueue) >= s.processingBufferMaxItems {
				// brief sleep and retry to avoid busy spin
				time.Sleep(1 * time.Millisecond)
				continue
			}
			// Check bytes budget
			if s.processingBufferBytes > 0 {
				// Try atomic admission: if adding would exceed, wait
				cur := atomic.LoadInt64(&s.processingCurrentBytes)
				if cur+itemSize > s.processingBufferBytes {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				// Reserve
				if atomic.CompareAndSwapInt64(&s.processingCurrentBytes, cur, cur+itemSize) {
					once.Do(func() {})
					break
				}
				continue
			}
			// No bytes budget, proceed
			break
		}
		// Enqueue (may block if only size cap is set via channel length)
		s.processingQueue <- bufferedProcessItem{data: data, isLive: isLive, cursor: cursor}
		return nil
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

	// Apply field filtering only (token features removed)
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Skip token-related mutations; retain original bytes
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
		// If there is no module output for this block, skip producing anything in SR mode.
		// This avoids falling back to serializing BlockScopedData which would require SR deps.
		if data.Output == nil || data.Output.MapOutput == nil || data.Output.MapOutput.Value == nil {
			return nil
		}
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

	// (Mutation statistics removed)
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

	// Serialize a compact reorg message
	var messageBytes []byte
	var err error

	// Use a separate topic for undo signals to avoid mixing message types
	undoTopic := s.topic + "-undo"

	// Build compact BlockReorg payload
	reorg := &pbkafka.BlockReorg{
		LastValidCursor:                cursor.String(),
		LastValidBlockId:               undoSignal.LastValidBlock.Id,
		LastValidBlockNumber:           undoSignal.LastValidBlock.Number,
		SubstreamNotificationTimestamp: uint64(time.Now().Unix()),
	}

	if s.useSchemaRegistry {
		// Use Schema Registry serialization for reorg messages too
		messageBytes, err = s.protobufSerializer.Serialize(undoTopic, reorg)
		if err != nil {
			return fmt.Errorf("failed to serialize BlockReorg with Schema Registry: %w", err)
		}
	} else {
		// Use standard protobuf serialization
		messageBytes, err = proto.Marshal(reorg)
		if err != nil {
			return fmt.Errorf("failed to marshal BlockReorg: %w", err)
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

	// Gracefully drain processing queue if enabled
	if s.processingQueue != nil {
		s.logger.Info("closing processing queue and waiting for drain")
		close(s.processingQueue)
		s.processingWG.Wait()
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
	return nil
}

// normalizeTokenFields recursively normalizes token amount fields in the message
func (s *KafkaSinker) normalizeTokenFields(message protoreflect.Message, tokenMetadata map[string]*TokenMetadata) {
	msgDesc := message.Descriptor()
	fields := msgDesc.Fields()

	// Iterate through all fields in the message
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		_ = string(field.Name())

		if !message.Has(field) {
			continue
		}

		value := message.Get(field)

		// removed

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
}

// findTokenAddress finds the token address corresponding to a token value field
func (s *KafkaSinker) findTokenAddress(message protoreflect.Message, valueField protoreflect.FieldDescriptor) string {
	return ""
}
