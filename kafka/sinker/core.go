package sinker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func (s *KafkaSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// Increment block counter atomically for thread safety
	atomic.AddInt64(&s.blocksProcessed, 1)

	// Check if field explosion is enabled and route accordingly
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
	)

	// For Kafka, we can't "undo" messages that have already been sent,
	// but we can send an undo signal message to inform downstream consumers
	key := fmt.Sprintf("undo_%d_%s", undoSignal.LastValidBlock.Number, undoSignal.LastValidBlock.Id)

	// Serialize the undo signal
	var messageBytes []byte
	var err error

	if s.useSchemaRegistry {
		// Use Schema Registry serialization for undo signals too
		messageBytes, err = s.protobufSerializer.Serialize(s.schemaSubject, undoSignal)
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
			Topic:     &s.topic,
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

	// Stop background timers
	if s.cursorSaveTicker != nil {
		s.cursorSaveTicker.Stop()
	}

	s.Shutdown(nil)
	return nil
}
