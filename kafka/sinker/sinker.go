package sinker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type KafkaSinker struct {
	*shutter.Shutter
	*sink.Sinker

	producer     *kafka.Producer
	topic        string
	batchSize    int
	batchTimeout time.Duration
	cursorFile   string
	logger       *zap.Logger
	tracer       logging.Tracer

	// Output format configuration
	outputFormat string // "json", "protobuf", or "schema-registry"

	// Dynamic protobuf support for universal module handling
	outputModuleDescriptor *desc.MessageDescriptor // jhump descriptor (for backward compatibility)
	v2MessageDescriptor    protoreflect.MessageDescriptor // official V2 descriptor (for Schema Registry)

	// Schema Registry integration (only used with outputFormat="schema-registry")
	schemaRegistryClient schemaregistry.Client
	protobufSerializer   *protobuf.Serializer
	schemaSubject        string
	useSchemaRegistry    bool

	// High-performance async delivery tracking
	deliveryChan        chan kafka.Event
	pendingMessages     map[string]*sink.Cursor // message key -> cursor
	confirmedMessages   chan string             // confirmed message keys
	lastConfirmedCursor *sink.Cursor
	cursorSaveTicker    *time.Ticker

	// Metrics
	blocksProcessed    int64
	messagesProduced   int64
	messagesConfirmed  int64
	messagesPending    int64
	undoSignalsHandled int64

	// Performance tracking
	lastStatsTime       time.Time
	lastBlocksProcessed int64
	currentThroughput   float64
}

type SinkerFactoryOptions struct {
	KafkaBrokers     string
	KafkaTopic       string
	KafkaUsername    string
	KafkaPassword    string
	SecurityProtocol string
	SASLMechanism    string
	BatchSize        int
	BatchTimeoutMs   int
	CursorFile       string

	// Output format selection
	OutputFormat string // "json", "protobuf", or "schema-registry"

	// Schema Registry options (only used with OutputFormat="schema-registry")
	SchemaRegistryURL  string
	SchemaSubject      string
	SchemaAutoRegister bool
}

// createOutputModuleMessageDescriptor creates a message descriptor for the output module
// This enables dynamic protobuf deserialization for any substreams module type
func createOutputModuleMessageDescriptor(sinker *sink.Sinker) (*desc.MessageDescriptor, error) {
	// Create file descriptors from the substreams package proto files
	fileDescs, err := desc.CreateFileDescriptors(sinker.Package().ProtoFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptors: %w", err)
	}

	// Get the output module type (without "proto:" prefix)
	outputModuleType := sinker.OutputModuleTypeUnprefixed()

	// Find the message descriptor for the output module type
	var msgDesc *desc.MessageDescriptor
	for _, file := range fileDescs {
		msgDesc = file.FindMessage(outputModuleType)
		if msgDesc != nil {
			return msgDesc, nil
		}
	}

	return nil, fmt.Errorf("output module descriptor for type %q not found in proto files", outputModuleType)
}

// createV2MessageDescriptor creates an official protobuf-go V2 message descriptor
// This is compatible with Confluent's Schema Registry serializer
func createV2MessageDescriptor(sinker *sink.Sinker) (protoreflect.MessageDescriptor, error) {
	// Get the output module type (without "proto:" prefix)
	outputTypeName := protoreflect.FullName(sinker.OutputModuleTypeUnprefixed())
	
	// Create a FileDescriptorSet from the substreams package proto files
	protoSet := &descriptorpb.FileDescriptorSet{
		File: sinker.Package().ProtoFiles,
	}

	// Create protoreflect files from the descriptor set
	protoFiles, err := protodesc.NewFiles(protoSet)
	if err != nil {
		return nil, fmt.Errorf("failed to create proto files: %w", err)
	}

	// Find the message descriptor by name
	descriptor, err := protoFiles.FindDescriptorByName(outputTypeName)
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return nil, fmt.Errorf("output module descriptor for type %q not found in proto files", outputTypeName)
		}
		return nil, fmt.Errorf("failed to find descriptor by name: %w", err)
	}

	// Ensure it's a message descriptor
	if msgDesc, ok := descriptor.(protoreflect.MessageDescriptor); ok {
		return msgDesc, nil
	}

	return nil, fmt.Errorf("descriptor for type %q is not a message descriptor", outputTypeName)
}

func SinkerFactory(baseSinker *sink.Sinker, options SinkerFactoryOptions) func(ctx context.Context, logger *zap.Logger, tracer logging.Tracer) (*KafkaSinker, error) {
	return func(ctx context.Context, logger *zap.Logger, tracer logging.Tracer) (*KafkaSinker, error) {
		// Create Kafka producer configuration
		kafkaConfig := kafka.ConfigMap{
			"bootstrap.servers": options.KafkaBrokers,

			// Performance optimizations for high throughput
			"acks":                                  "1",                    // Wait for leader acknowledgment only
			"retries":                               10,                     // Retry failed sends
			"batch.size":                            65536,                  // 64KB batch size
			"linger.ms":                             options.BatchTimeoutMs, // Batch timeout
			"compression.type":                      "snappy",               // Use snappy compression
			"max.in.flight.requests.per.connection": 5,                      // Allow up to 5 unacknowledged requests

			// Increase queue sizes to handle high throughput
			"queue.buffering.max.messages": 1000000, // 1M message buffer
			"queue.buffering.max.kbytes":   1048576, // 1GB buffer

			// Buffer and timeout settings
			"request.timeout.ms":  30000,  // 30 second timeout
			"delivery.timeout.ms": 120000, // 2 minute delivery timeout

			// Error handling
			"retry.backoff.ms":         100,  // 100ms between retries
			"reconnect.backoff.ms":     50,   // 50ms reconnect backoff
			"reconnect.backoff.max.ms": 1000, // Max 1s reconnect backoff
		}

		// Add security configuration if needed
		if options.SecurityProtocol != "PLAINTEXT" {
			kafkaConfig["security.protocol"] = options.SecurityProtocol

			if options.SecurityProtocol == "SASL_PLAINTEXT" || options.SecurityProtocol == "SASL_SSL" {
				kafkaConfig["sasl.mechanism"] = options.SASLMechanism
				if options.KafkaUsername != "" {
					kafkaConfig["sasl.username"] = options.KafkaUsername
				}
				if options.KafkaPassword != "" {
					kafkaConfig["sasl.password"] = options.KafkaPassword
				}
			}
		}

		producer, err := kafka.NewProducer(&kafkaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
		}

		// Initialize Schema Registry client only for schema-registry output format
		var schemaRegistryClient schemaregistry.Client
		var protobufSerializer *protobuf.Serializer
		useSchemaRegistry := options.OutputFormat == "schema-registry"

		if useSchemaRegistry {
			schemaRegistryClient, err = schemaregistry.NewClient(schemaregistry.NewConfig(options.SchemaRegistryURL))
			if err != nil {
				return nil, fmt.Errorf("failed to create Schema Registry client: %w", err)
			}

			serializerConfig := protobuf.NewSerializerConfig()
			serializerConfig.AutoRegisterSchemas = options.SchemaAutoRegister

			protobufSerializer, err = protobuf.NewSerializer(schemaRegistryClient, serde.ValueSerde, serializerConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create protobuf serializer: %w", err)
			}

			logger.Info("Schema Registry integration enabled",
				zap.String("schema_registry_url", options.SchemaRegistryURL),
				zap.String("schema_subject", options.SchemaSubject),
				zap.Bool("auto_register", options.SchemaAutoRegister),
			)
		}

		// Create message descriptor for dynamic protobuf deserialization (jhump - for backward compatibility)
		messageDescriptor, err := createOutputModuleMessageDescriptor(baseSinker)
		if err != nil {
			return nil, fmt.Errorf("failed to create output module message descriptor: %w", err)
		}

		// Create V2 message descriptor for Schema Registry compatibility
		v2MessageDescriptor, err := createV2MessageDescriptor(baseSinker)
		if err != nil {
			return nil, fmt.Errorf("failed to create V2 message descriptor: %w", err)
		}

		kafkaSinker := &KafkaSinker{
			Shutter:      shutter.New(),
			Sinker:       baseSinker,
			producer:     producer,
			topic:        options.KafkaTopic,
			batchSize:    options.BatchSize,
			batchTimeout: time.Duration(options.BatchTimeoutMs) * time.Millisecond,
			cursorFile:   options.CursorFile,
			logger:       logger,
			tracer:       tracer,

			// Output format configuration
			outputFormat: options.OutputFormat,

			// Dynamic protobuf support
			outputModuleDescriptor: messageDescriptor,
			v2MessageDescriptor:    v2MessageDescriptor,

			// Schema Registry integration (only used with schema-registry format)
			schemaRegistryClient: schemaRegistryClient,
			protobufSerializer:   protobufSerializer,
			schemaSubject:        options.SchemaSubject,
			useSchemaRegistry:    useSchemaRegistry,

			// Initialize high-performance async system
			deliveryChan:      make(chan kafka.Event, 10000), // Large buffer for delivery events
			pendingMessages:   make(map[string]*sink.Cursor),
			confirmedMessages: make(chan string, 10000),        // Buffer for confirmed message keys
			cursorSaveTicker:  time.NewTicker(1 * time.Second), // Save cursor every second
			lastStatsTime:     time.Now(),
		}

		// Start background delivery confirmation handler
		go kafkaSinker.deliveryConfirmationHandler(ctx)

		// Start background cursor saver
		go kafkaSinker.cursorSaveHandler(ctx)

		logger.Info("Kafka sinker created",
			zap.String("brokers", options.KafkaBrokers),
			zap.String("topic", options.KafkaTopic),
			zap.String("security_protocol", options.SecurityProtocol),
			zap.Int("batch_size", options.BatchSize),
			zap.Int("batch_timeout_ms", options.BatchTimeoutMs),
		)

		return kafkaSinker, nil
	}
}

func (s *KafkaSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// Increment block counter atomically for thread safety
	atomic.AddInt64(&s.blocksProcessed, 1)

	// Create message key from block information
	key := fmt.Sprintf("block_%d_%s", data.Clock.Number, data.Clock.Id)

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

// serializeToJSON properly deserializes the protobuf message and converts it to JSON
func (s *KafkaSinker) serializeToJSON(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Extract the actual module output from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Create a V2 dynamic message using the official protobuf-go runtime
		dynamicMsg := dynamicpb.NewMessage(s.v2MessageDescriptor)
		if err := proto.Unmarshal(data.Output.MapOutput.Value, dynamicMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf message: %w", err)
		}

		// Convert the dynamic message to JSON using official protojson
		// This produces proper JSON with field names and values, not base64 blobs
		return protojson.Marshal(dynamicMsg)
	}

	// Fallback: serialize entire BlockScopedData to JSON using protojson
	return protojson.Marshal(data)
}

// serializeToRawProtobuf extracts the raw ListenerMessage protobuf bytes
func (s *KafkaSinker) serializeToRawProtobuf(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Extract the actual ListenerMessage from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Return the raw ListenerMessage protobuf bytes directly
		return data.Output.MapOutput.Value, nil
	}

	// Fallback: serialize entire BlockScopedData
	return proto.Marshal(data)
}

// serializeWithSchemaRegistry uses Confluent Schema Registry format with the actual module output
func (s *KafkaSinker) serializeWithSchemaRegistry(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Extract the actual module output from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Create a V2 dynamic message using the official protobuf-go runtime (compatible with Confluent)
		dynamicMsg := dynamicpb.NewMessage(s.v2MessageDescriptor)
		if err := proto.Unmarshal(data.Output.MapOutput.Value, dynamicMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf message for Schema Registry serialization: %w", err)
		}

		// Use the Confluent serializer to handle schema registration and wire format
		// This should now work because dynamicMsg implements the official proto.Message interface
		return s.protobufSerializer.Serialize(s.schemaSubject, dynamicMsg)
	}

	// Fallback: serialize entire BlockScopedData (this should not happen in normal operation)
	return s.protobufSerializer.Serialize(s.schemaSubject, data)
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

// 🚀 HIGH-PERFORMANCE ASYNC DELIVERY SYSTEM

var pendingMutex sync.RWMutex

func (s *KafkaSinker) addPendingMessage(key string, cursor *sink.Cursor) {
	pendingMutex.Lock()
	s.pendingMessages[key] = cursor
	pendingMutex.Unlock()
}

func (s *KafkaSinker) removePendingMessage(key string) *sink.Cursor {
	pendingMutex.Lock()
	defer pendingMutex.Unlock()
	cursor := s.pendingMessages[key]
	delete(s.pendingMessages, key)
	return cursor
}

func (s *KafkaSinker) deliveryConfirmationHandler(ctx context.Context) {
	s.logger.Info("🚀 Starting high-performance delivery confirmation handler")

	for {
		select {
		case event := <-s.deliveryChan:
			if msg, ok := event.(*kafka.Message); ok {
				key := string(msg.Key)

				if msg.TopicPartition.Error != nil {
					s.logger.Error("message delivery failed",
						zap.String("key", key),
						zap.Error(msg.TopicPartition.Error))
					// Remove failed message from pending
					s.removePendingMessage(key)
				} else {
					// Message successfully delivered!
					atomic.AddInt64(&s.messagesConfirmed, 1)
					atomic.AddInt64(&s.messagesPending, -1)

					// Update last confirmed cursor
					if cursor := s.removePendingMessage(key); cursor != nil {
						s.lastConfirmedCursor = cursor
					}

					// Notify cursor saver
					select {
					case s.confirmedMessages <- key:
					default:
						// Channel full, skip notification (cursor will be saved by timer)
					}
				}
			}

		case <-ctx.Done():
			s.logger.Info("delivery confirmation handler shutting down")
			return
		}
	}
}

func (s *KafkaSinker) cursorSaveHandler(ctx context.Context) {
	s.logger.Info("💾 Starting background cursor saver")

	for {
		select {
		case <-s.cursorSaveTicker.C:
			// Save cursor periodically
			if s.lastConfirmedCursor != nil {
				if err := s.saveCursor(s.lastConfirmedCursor); err != nil {
					s.logger.Warn("failed to save cursor", zap.Error(err))
				}
			}

		case <-s.confirmedMessages:
			// Message confirmed, save cursor immediately for low latency
			if s.lastConfirmedCursor != nil {
				if err := s.saveCursor(s.lastConfirmedCursor); err != nil {
					s.logger.Warn("failed to save cursor", zap.Error(err))
				}
			}

		case <-ctx.Done():
			s.logger.Info("cursor saver shutting down")
			// Final cursor save
			if s.lastConfirmedCursor != nil {
				s.saveCursor(s.lastConfirmedCursor)
			}
			return
		}
	}
}

func (s *KafkaSinker) updatePerformanceMetrics() {
	blocksProcessed := atomic.LoadInt64(&s.blocksProcessed)

	// Log progress every 1000 blocks with enhanced metrics
	if blocksProcessed%1000 == 0 {
		now := time.Now()
		duration := now.Sub(s.lastStatsTime)

		if duration > 0 {
			blocksDelta := blocksProcessed - s.lastBlocksProcessed
			s.currentThroughput = float64(blocksDelta) / duration.Seconds()
			s.lastStatsTime = now
			s.lastBlocksProcessed = blocksProcessed
		}

		messagesProduced := atomic.LoadInt64(&s.messagesProduced)
		messagesConfirmed := atomic.LoadInt64(&s.messagesConfirmed)
		messagesPending := atomic.LoadInt64(&s.messagesPending)

		s.logger.Info("🚀 HIGH-PERFORMANCE PROCESSING",
			zap.Int64("blocks_processed", blocksProcessed),
			zap.Int64("messages_produced", messagesProduced),
			zap.Int64("messages_confirmed", messagesConfirmed),
			zap.Int64("messages_pending", messagesPending),
			zap.Float64("blocks_per_second", s.currentThroughput),
			zap.Float64("messages_per_second", s.currentThroughput), // 1:1 ratio for this use case
		)
	}
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

func (s *KafkaSinker) loadCursor() (*sink.Cursor, error) {
	if s.cursorFile == "" {
		return sink.NewBlankCursor(), nil
	}

	// Check if file exists
	if _, err := os.Stat(s.cursorFile); os.IsNotExist(err) {
		return sink.NewBlankCursor(), nil
	}

	// Read cursor from file
	data, err := os.ReadFile(s.cursorFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read cursor from file %s: %w", s.cursorFile, err)
	}

	if len(data) == 0 {
		return sink.NewBlankCursor(), nil
	}

	cursor, err := sink.NewCursor(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse cursor from file %s: %w", s.cursorFile, err)
	}

	return cursor, nil
}

func (s *KafkaSinker) saveCursor(cursor *sink.Cursor) error {
	if s.cursorFile == "" {
		return nil // No cursor file specified
	}

	cursorStr := cursor.String()
	if err := os.WriteFile(s.cursorFile, []byte(cursorStr), 0644); err != nil {
		return fmt.Errorf("failed to write cursor to file %s: %w", s.cursorFile, err)
	}

	return nil
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
