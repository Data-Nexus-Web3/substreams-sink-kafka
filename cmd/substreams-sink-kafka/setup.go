package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"go.uber.org/zap"
)

var sinkSetupCmd = Command(sinkSetupE,
	"setup <kafka_brokers>",
	"Setup and validate Kafka connection",
	ExactArgs(1),
	Flags(func(flags *pflag.FlagSet) {
		addKafkaFlags(flags)
		flags.Bool("create-topic", true, "Create Kafka topic if it doesn't exist")
		flags.Int("topic-partitions", 3, "Number of partitions for topic creation")
		flags.Int("topic-replication-factor", 1, "Replication factor for topic creation")
	}),
	Example("substreams-sink-kafka setup 'localhost:9092'"),
	OnCommandErrorLogAndExit(zlog),
)

type SetupOptions struct {
	KafkaBrokers      string
	KafkaTopic        string
	KafkaUsername     string
	KafkaPassword     string
	SecurityProtocol  string
	SASLMechanism     string
	CursorFile        string
	Partitions        int
	ReplicationFactor int
	CreateTopic       bool
}

func sinkSetupE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	kafkaBrokers := args[0]

	// Get configuration values
	options := SetupOptions{
		KafkaBrokers:      kafkaBrokers,
		KafkaTopic:        sflags.MustGetString(cmd, "kafka-topic"),
		KafkaUsername:     sflags.MustGetString(cmd, "kafka-username"),
		KafkaPassword:     sflags.MustGetString(cmd, "kafka-password"),
		SecurityProtocol:  sflags.MustGetString(cmd, "kafka-security-protocol"),
		SASLMechanism:     sflags.MustGetString(cmd, "kafka-sasl-mechanism"),
		CursorFile:        sflags.MustGetString(cmd, "cursor-file"),
		Partitions:        sflags.MustGetInt(cmd, "topic-partitions"),
		ReplicationFactor: sflags.MustGetInt(cmd, "topic-replication-factor"),
		CreateTopic:       sflags.MustGetBool(cmd, "create-topic"),
	}

	zlog.Info("🚀 Starting Kafka setup validation",
		zap.String("brokers", options.KafkaBrokers),
		zap.String("topic", options.KafkaTopic),
		zap.String("security_protocol", options.SecurityProtocol),
	)

	return setupKafka(ctx, options, zlog)
}

func setupKafka(ctx context.Context, options SetupOptions, logger *zap.Logger) error {
	// Step 1: Validate cursor file path
	if err := validateCursorPath(options.CursorFile, logger); err != nil {
		return fmt.Errorf("cursor file validation failed: %w", err)
	}

	// Step 2: Create Kafka configuration
	config := createKafkaConfig(options)

	// Step 3: Test admin client connection
	if err := testAdminConnection(ctx, config, logger); err != nil {
		return fmt.Errorf("admin connection failed: %w", err)
	}

	// Step 4: Create topic if requested
	if options.CreateTopic {
		if err := createTopicIfNeeded(ctx, config, options, logger); err != nil {
			return fmt.Errorf("topic creation failed: %w", err)
		}
	}

	// Step 5: Test producer connection and permissions
	if err := testProducerConnection(config, options, logger); err != nil {
		return fmt.Errorf("producer test failed: %w", err)
	}

	// Step 6: Performance baseline test
	if err := performanceTest(config, options, logger); err != nil {
		logger.Warn("performance test failed, but setup is functional", zap.Error(err))
	}

	logger.Info("🎉 Kafka setup completed successfully!")
	return nil
}

func validateCursorPath(cursorFile string, logger *zap.Logger) error {
	if cursorFile == "" {
		logger.Info("✅ No cursor file specified - checkpointing disabled")
		return nil
	}

	// Check if directory exists and is writable
	dir := filepath.Dir(cursorFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("cannot create cursor directory %s: %w", dir, err)
	}

	// Test write permissions
	testFile := filepath.Join(dir, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("cannot write to cursor directory %s: %w", dir, err)
	}
	os.Remove(testFile)

	logger.Info("✅ Cursor file path is valid", zap.String("path", cursorFile))
	return nil
}

func createKafkaConfig(options SetupOptions) kafka.ConfigMap {
	config := kafka.ConfigMap{
		"bootstrap.servers": options.KafkaBrokers,
	}

	// Add security configuration if needed
	if options.SecurityProtocol != "PLAINTEXT" {
		config["security.protocol"] = options.SecurityProtocol

		if options.SecurityProtocol == "SASL_PLAINTEXT" || options.SecurityProtocol == "SASL_SSL" {
			config["sasl.mechanism"] = options.SASLMechanism
			if options.KafkaUsername != "" {
				config["sasl.username"] = options.KafkaUsername
			}
			if options.KafkaPassword != "" {
				config["sasl.password"] = options.KafkaPassword
			}
		}
	}

	return config
}

func testAdminConnection(ctx context.Context, config kafka.ConfigMap, logger *zap.Logger) error {
	logger.Info("🔗 Testing admin connection to Kafka cluster...")

	adminClient, err := kafka.NewAdminClient(&config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	// Test connection by getting metadata
	metadata, err := adminClient.GetMetadata(nil, false, 10000) // 10 second timeout
	if err != nil {
		return fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	logger.Info("✅ Successfully connected to Kafka cluster",
		zap.Int("brokers_count", len(metadata.Brokers)),
	)

	return nil
}

func createTopicIfNeeded(ctx context.Context, config kafka.ConfigMap, options SetupOptions, logger *zap.Logger) error {
	logger.Info("📋 Checking if topic exists...", zap.String("topic", options.KafkaTopic))

	adminClient, err := kafka.NewAdminClient(&config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	// Check if topic already exists
	metadata, err := adminClient.GetMetadata(&options.KafkaTopic, false, 10000)
	if err != nil {
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	if topicMeta, exists := metadata.Topics[options.KafkaTopic]; exists && topicMeta.Error.Code() == kafka.ErrNoError {
		logger.Info("✅ Topic already exists", zap.String("topic", options.KafkaTopic))
		return nil
	}

	// Create topic
	logger.Info("🔨 Creating topic...",
		zap.String("topic", options.KafkaTopic),
		zap.Int("partitions", options.Partitions),
		zap.Int("replication_factor", options.ReplicationFactor),
	)

	topicSpec := kafka.TopicSpecification{
		Topic:             options.KafkaTopic,
		NumPartitions:     options.Partitions,
		ReplicationFactor: options.ReplicationFactor,
		Config: map[string]string{
			"compression.type": "snappy",
			"cleanup.policy":   "delete",
			"retention.ms":     "604800000", // 7 days
		},
	}

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("topic creation failed: %s", result.Error.String())
		}
	}

	logger.Info("✅ Topic created successfully", zap.String("topic", options.KafkaTopic))
	return nil
}

func testProducerConnection(config kafka.ConfigMap, options SetupOptions, logger *zap.Logger) error {
	logger.Info("🚀 Testing producer connection...")

	// Add producer-specific configuration
	producerConfig := config
	producerConfig["acks"] = "1"
	producerConfig["retries"] = 3

	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Send a test message
	testMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &options.KafkaTopic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte("setup_test"),
		Value: []byte("test message from setup"),
	}

	deliveryChan := make(chan kafka.Event)
	if err := producer.Produce(testMsg, deliveryChan); err != nil {
		return fmt.Errorf("failed to produce test message: %w", err)
	}

	// Wait for delivery confirmation
	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("test message delivery failed: %v", m.TopicPartition.Error)
		}
		logger.Info("✅ Producer test successful",
			zap.String("topic", options.KafkaTopic),
			zap.Int32("partition", m.TopicPartition.Partition),
			zap.Int64("offset", int64(m.TopicPartition.Offset)),
		)
	case <-time.After(30 * time.Second):
		return fmt.Errorf("test message delivery timeout")
	}

	return nil
}

func performanceTest(config kafka.ConfigMap, options SetupOptions, logger *zap.Logger) error {
	logger.Info("⚡ Running performance baseline test...")

	// Add high-performance producer configuration
	perfConfig := config
	perfConfig["acks"] = "1"
	perfConfig["batch.size"] = 65536
	perfConfig["linger.ms"] = 100
	perfConfig["compression.type"] = "snappy"

	producer, err := kafka.NewProducer(&perfConfig)
	if err != nil {
		return fmt.Errorf("failed to create performance test producer: %w", err)
	}
	defer producer.Close()

	// Send 100 test messages
	messageCount := 100
	deliveryChan := make(chan kafka.Event, messageCount)

	start := time.Now()

	for i := 0; i < messageCount; i++ {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &options.KafkaTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(fmt.Sprintf("perf_test_%d", i)),
			Value: []byte(fmt.Sprintf("performance test message %d", i)),
		}

		if err := producer.Produce(msg, deliveryChan); err != nil {
			return fmt.Errorf("failed to produce performance test message: %w", err)
		}
	}

	// Wait for all deliveries
	delivered := 0
	for delivered < messageCount {
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				logger.Warn("performance test message failed", zap.Error(m.TopicPartition.Error))
			} else {
				delivered++
			}
		case <-time.After(60 * time.Second):
			return fmt.Errorf("performance test timeout after %d/%d messages", delivered, messageCount)
		}
	}

	duration := time.Since(start)
	throughput := float64(messageCount) / duration.Seconds()

	logger.Info("✅ Performance test completed",
		zap.Int("messages", messageCount),
		zap.Duration("duration", duration),
		zap.Float64("messages_per_second", throughput),
	)

	return nil
}
