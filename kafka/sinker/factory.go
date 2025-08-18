package sinker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

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
			baseTopic:    options.KafkaTopic, // use main topic as base for exploded topics
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

			// Simple explosion configuration for array field handling
			explodeFieldName: options.ExplodeField,

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
