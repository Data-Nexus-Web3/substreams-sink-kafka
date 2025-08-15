package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	sink "github.com/streamingfast/substreams-sink"
	kafkaSinker "github.com/streamingfast/substreams-sink-kafka/kafka/sinker"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <kafka_brokers> <manifest> [<start>:<stop>]",
	"Runs Kafka sink process",
	RangeArgs(2, 3),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		addKafkaFlags(flags)
	}),
	Example("substreams-sink-kafka run 'localhost:9092' uniswap-v3@v0.2.10 --output-module=map_pools"),
	OnCommandErrorLogAndExit(zlog),
)

func addKafkaFlags(flags *pflag.FlagSet) {
	flags.String("kafka-topic", "substreams", "Kafka topic to produce messages to")
	flags.String("kafka-username", "", "Kafka SASL username (optional)")
	flags.String("kafka-password", "", "Kafka SASL password (optional)")
	flags.String("kafka-security-protocol", "PLAINTEXT", "Security protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL)")
	flags.String("kafka-sasl-mechanism", "PLAIN", "SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	flags.Int("kafka-batch-size", 1000, "Maximum number of messages to batch before sending")
	flags.Int("kafka-batch-timeout-ms", 100, "Maximum time to wait for batch to fill (milliseconds)")
	flags.String("cursor-file", "cursor.txt", "File to store cursor for checkpoint recovery")
	flags.StringP("endpoint", "e", "", "Specify the substreams endpoint, ex: `mainnet.eth.streamingfast.io:443`")
	flags.String("output-module", "", "Name of the substreams module to consume (if not specified, will try to infer from manifest)")

	// Output format selection
	flags.String("output-format", "protobuf", "Output format: json, protobuf, or schema-registry")

	// Schema Registry flags (only used with --output-format=schema-registry)
	flags.String("schema-registry-url", "", "Schema Registry URL (e.g., http://localhost:8081) - required for schema-registry format")
	flags.String("schema-subject", "", "Schema subject name (defaults to {topic}-value)")
	flags.Bool("schema-auto-register", true, "Automatically register schemas if they don't exist")

	// Simple explosion flag for array field handling
	flags.String("explode-field", "", "Field name to explode (must be an array field). Each array item becomes a separate message.")
}

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	sink.RegisterMetrics()

	kafkaBrokers := args[0]
	manifestPath := args[1]
	blockRange := ""
	if len(args) > 2 {
		blockRange = args[2]
	}

	endpoint := sflags.MustGetString(cmd, "endpoint")
	if endpoint == "" {
		network := sflags.MustGetString(cmd, "network")
		if network == "" {
			reader, err := manifest.NewReader(manifestPath)
			if err != nil {
				return fmt.Errorf("setup manifest reader: %w", err)
			}
			pkgBundle, err := reader.Read()
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			network = pkgBundle.Package.Network
		}
		var err error
		endpoint, err = manifest.ExtractNetworkEndpoint(network, sflags.MustGetString(cmd, "endpoint"), zlog)
		if err != nil {
			return err
		}
	}

	// We'll accept any protobuf type since we're streaming raw data to Kafka
	supportedOutputTypes := sink.IgnoreOutputModuleType

	// Handle output module selection
	outputModule := sflags.MustGetString(cmd, "output-module")
	var outputModuleName string

	if outputModule != "" {
		// User specified a module directly
		outputModuleName = outputModule
		zlog.Info("using specified output module", zap.String("module", outputModule))
	} else {
		// Try to infer from package (fallback to old behavior)
		outputModuleName = sink.InferOutputModuleFromPackage
		zlog.Info("inferring output module from package")
	}

	sinker, err := sink.NewFromViper(
		cmd,
		supportedOutputTypes,
		endpoint,
		manifestPath,
		outputModuleName,
		blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("new base sinker: %w", err)
	}

	// Get Kafka configuration from flags
	kafkaTopic := sflags.MustGetString(cmd, "kafka-topic")
	kafkaUsername := sflags.MustGetString(cmd, "kafka-username")
	kafkaPassword := sflags.MustGetString(cmd, "kafka-password")
	securityProtocol := sflags.MustGetString(cmd, "kafka-security-protocol")
	saslMechanism := sflags.MustGetString(cmd, "kafka-sasl-mechanism")
	batchSize := sflags.MustGetInt(cmd, "kafka-batch-size")
	batchTimeoutMs := sflags.MustGetInt(cmd, "kafka-batch-timeout-ms")
	cursorFile := sflags.MustGetString(cmd, "cursor-file")

	// Get output format configuration
	outputFormat := sflags.MustGetString(cmd, "output-format")

	// Validate output format
	switch outputFormat {
	case "json", "protobuf", "schema-registry":
		// Valid formats
	default:
		return fmt.Errorf("invalid output format '%s', must be one of: json, protobuf, schema-registry", outputFormat)
	}

	// Get Schema Registry configuration from flags (only needed for schema-registry format)
	schemaRegistryURL := sflags.MustGetString(cmd, "schema-registry-url")
	schemaSubject := sflags.MustGetString(cmd, "schema-subject")
	schemaAutoRegister := sflags.MustGetBool(cmd, "schema-auto-register")

	// Get explosion configuration from flags
	explodeField := sflags.MustGetString(cmd, "explode-field")

	// Validate Schema Registry configuration for schema-registry format
	if outputFormat == "schema-registry" && schemaRegistryURL == "" {
		return fmt.Errorf("--schema-registry-url is required when using --output-format=schema-registry")
	}

	// Validate explosion configuration
	// No validation needed for explode-field - empty string means no explosion

	// Default schema subject based on output format
	// For schema-registry with protobuf, Confluent auto-generates {topic}-value-value
	// So we pass just the topic name to let Confluent handle the suffix generation
	if schemaSubject == "" && outputFormat == "schema-registry" {
		schemaSubject = kafkaTopic
	}

	logFields := []zap.Field{
		zap.String("kafka_brokers", kafkaBrokers),
		zap.String("kafka_topic", kafkaTopic),
		zap.String("manifest", manifestPath),
		zap.String("endpoint", endpoint),
		zap.Int("batch_size", batchSize),
		zap.String("output_format", outputFormat),
		zap.String("explode_field", explodeField),
	}

	if outputFormat == "schema-registry" {
		logFields = append(logFields,
			zap.String("schema_registry_url", schemaRegistryURL),
			zap.String("schema_subject", schemaSubject),
			zap.Bool("schema_auto_register", schemaAutoRegister),
		)
		zlog.Info("starting Kafka sink with Schema Registry integration", logFields...)
	} else {
		zlog.Info("starting Kafka sink", logFields...)
	}

	// Create Kafka sinker factory
	sinkerFactory := kafkaSinker.SinkerFactory(sinker, kafkaSinker.SinkerFactoryOptions{
		KafkaBrokers:       kafkaBrokers,
		KafkaTopic:         kafkaTopic,
		KafkaUsername:      kafkaUsername,
		KafkaPassword:      kafkaPassword,
		SecurityProtocol:   securityProtocol,
		SASLMechanism:      saslMechanism,
		BatchSize:          batchSize,
		BatchTimeoutMs:     batchTimeoutMs,
		CursorFile:         cursorFile,
		OutputFormat:       outputFormat,
		SchemaRegistryURL:  schemaRegistryURL,
		SchemaSubject:      schemaSubject,
		SchemaAutoRegister: schemaAutoRegister,
		ExplodeField:       explodeField,
	})

	kafkaSink, err := sinkerFactory(app.Context(), zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup Kafka sinker: %w", err)
	}

	app.SuperviseAndStart(kafkaSink)

	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}
