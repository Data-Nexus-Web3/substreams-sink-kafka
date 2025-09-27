package sinker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// (jhump-based descriptor creation removed; v2-only)

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
			baseTopic:    options.KafkaTopic, // use main topic as base for exploded topics
			batchSize:    options.BatchSize,
			batchTimeout: time.Duration(options.BatchTimeoutMs) * time.Millisecond,
			cursorFile:   options.CursorFile,
			logger:       logger,
			tracer:       tracer,

			// Output format configuration
			outputFormat: options.OutputFormat,

			// Dynamic protobuf support (v2 only)
			v2MessageDescriptor: v2MessageDescriptor,

			// Schema Registry integration (only used with schema-registry format)
			schemaRegistryClient: schemaRegistryClient,
			protobufSerializer:   protobufSerializer,
			schemaSubject:        options.SchemaSubject,
			useSchemaRegistry:    useSchemaRegistry,
			schemaFastPath:       options.SchemaFastPath,

			// Simple explosion configuration for array field handling
			explodeFieldName: options.ExplodeField,

			// Token normalization removed

			// Undo buffer configuration
			undoBufferSize: options.UndoBufferSize,

			// Processing buffer configuration
			processingBufferSize:     options.ProcessingBufferSize,
			processingBufferBytes:    options.ProcessingBufferBytes,
			processingBufferMaxItems: options.ProcessingBufferMaxItems,

			// Explosion parallelization controls
			explodeWorkers:        options.ExplodeWorkers,
			explodeAlwaysParallel: options.ExplodeAlwaysParallel,

			// Initialize high-performance async system
			deliveryChan:      make(chan kafka.Event, 10000), // Large buffer for delivery events
			pendingMessages:   make(map[string]*sink.Cursor),
			confirmedMessages: make(chan string, 10000),        // Buffer for confirmed message keys
			cursorSaveTicker:  time.NewTicker(1 * time.Second), // Save cursor every second
			lastStatsTime:     time.Now(),
		}

		// Start Prometheus server & 20s metrics reporter aligned with substreams
		startPrometheusServer()
		kafkaSinker.startMetricsReporter()

		// Field processing removed

		// Set debug mode
		kafkaSinker.debugMode = options.DebugMode

		// Output formatting removed

		// Mutation stats removed

		// Token metadata injection and normalization removed

		// Initialize undo buffer if enabled
		if options.UndoBufferSize > 0 {
			kafkaSinker.undoBuffer = NewUndoBuffer(options.UndoBufferSize, kafkaSinker, logger)
			logger.Info("Undo buffer enabled for reorg protection",
				zap.Int("buffer_size", options.UndoBufferSize),
				zap.String("mode", "blocks_behind_head"),
			)
		} else {
			logger.Info("Undo buffer disabled - using immediate processing mode")
		}

		// Start background delivery confirmation handler
		go kafkaSinker.deliveryConfirmationHandler(ctx)

		// Start background cursor saver
		go kafkaSinker.cursorSaveHandler(ctx)

		// Start processing queue consumer if enabled
		if kafkaSinker.processingBufferSize > 0 || kafkaSinker.processingBufferBytes > 0 {
			capItems := kafkaSinker.processingBufferSize
			if capItems <= 0 {
				// default small channel if only bytes budget is used
				capItems = 1024
			}
			kafkaSinker.processingQueue = make(chan bufferedProcessItem, capItems)
			kafkaSinker.processingWG.Add(1)
			go func() {
				defer kafkaSinker.processingWG.Done()
				for item := range kafkaSinker.processingQueue {
					// Adjust byte counter when we start processing this item
					if kafkaSinker.processingBufferBytes > 0 {
						delta := sizeEstimate(item.data)
						atomic.AddInt64(&kafkaSinker.processingCurrentBytes, -delta)
					}
					// Process in-order from the buffer
					if kafkaSinker.explodeFieldName != "" {
						_ = kafkaSinker.handleWithFieldExplosion(ctx, item.data, item.isLive, item.cursor)
					} else {
						_ = kafkaSinker.handleWithoutExplosion(ctx, item.data, item.isLive, item.cursor)
					}
				}
			}()
		}

		// If fast-path is enabled, precompute Schema Registry headers
		if useSchemaRegistry && options.SchemaFastPath {
			if err := kafkaSinker.initializeSchemaRegistryHeaders(); err != nil {
				return nil, fmt.Errorf("failed to initialize SR fast-path headers: %w", err)
			}
		}

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
