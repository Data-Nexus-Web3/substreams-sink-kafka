package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	kafkaSinker "github.com/streamingfast/substreams-sink-kafka/kafka/sinker"
	"github.com/streamingfast/substreams/manifest"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <kafka_brokers> <manifest>",
	"Runs Kafka sink process",
	ExactArgs(2),
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
	flags.String("output-module", "", "Name of the substreams module to consume (if not specified, will try to infer from manifest)")

	// Output format selection
	flags.String("output-format", "protobuf", "Output format: json, protobuf, or schema-registry")

	// Schema Registry flags (only used with --output-format=schema-registry)
	flags.String("schema-registry-url", "", "Schema Registry URL (e.g., http://localhost:8081) - required for schema-registry format")
	flags.String("schema-subject", "", "Schema subject name (defaults to {topic}-value)")
	flags.Bool("schema-auto-register", true, "Automatically register schemas if they don't exist")

	// Simple explosion flag for array field handling
	flags.String("explode-field", "", "Field name to explode (must be an array field). Each array item becomes a separate message.")

	// Token normalization flags (optional feature)
	flags.String("token-metadata-grpc-endpoint", "", "Token metadata GRPC service endpoint (e.g., localhost:9090) - enables token normalization")
	flags.StringSlice("token-value-fields", []string{}, "Comma-separated list of field names containing raw token amounts to normalize. Supports nested fields with dot notation (e.g., amount,relatedOperations.amount)")
	flags.StringSlice("token-address-fields", []string{}, "Comma-separated list of field names containing token contract addresses. Supports nested fields with dot notation (e.g., token_address,relatedOperations.token)")

	// Dynamic field processing flags
	flags.StringSlice("token-metadata-fields", []string{}, "Comma-separated list of field names to inject token metadata into (e.g., token_metadata)")
	flags.StringSlice("token-source-fields", []string{"ops_token", "act_address", "token_address"}, "Priority-ordered list of fields to check for token addresses")
	flags.Bool("omit-empty-strings", true, "Omit empty string fields from output messages")

	// Debug flags
	flags.Bool("debug", false, "Enable debug logging (shows detailed processing information)")

	// Performance profiling flags
	flags.Bool("enable-profiling", false, "Enable performance profiling and detailed metrics")
	flags.Bool("enable-optimizations", true, "Enable performance optimizations for token normalization")
	flags.String("profiling-output-dir", "./profiles", "Directory for profiling output files")
	flags.Int("profiling-http-port", 6060, "Port for pprof HTTP server (use 0 to disable)")

	// Output formatting flags
	flags.Bool("hex-encode-bytes", false, "Encode byte fields as hex strings with 0x prefix (default: raw bytes)")

	// Note: undo-buffer-size flag is provided by the base sink.AddFlagsToSet() call above
}

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	// Metrics are automatically registered in the new sink package

	kafkaBrokers := args[0]
	manifestPath := args[1]

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
		manifestPath,
		outputModuleName,
		"substreams-sink-kafka",
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

	// Get token normalization configuration from flags
	tokenMetadataEndpoint := sflags.MustGetString(cmd, "token-metadata-grpc-endpoint")
	tokenValueFields := sflags.MustGetStringSlice(cmd, "token-value-fields")
	tokenAddressFields := sflags.MustGetStringSlice(cmd, "token-address-fields")

	// Get dynamic field processing configuration from flags
	tokenMetadataFields := sflags.MustGetStringSlice(cmd, "token-metadata-fields")
	tokenSourceFields := sflags.MustGetStringSlice(cmd, "token-source-fields")
	omitEmptyStrings := sflags.MustGetBool(cmd, "omit-empty-strings")

	// Get debug configuration
	debugMode := sflags.MustGetBool(cmd, "debug")

	// Get profiling configuration
	enableProfiling := sflags.MustGetBool(cmd, "enable-profiling")
	enableOptimizations := sflags.MustGetBool(cmd, "enable-optimizations")
	profilingOutputDir := sflags.MustGetString(cmd, "profiling-output-dir")
	profilingHTTPPort := sflags.MustGetInt(cmd, "profiling-http-port")

	// Get output formatting configuration
	hexEncodeBytes := sflags.MustGetBool(cmd, "hex-encode-bytes")

	// Get undo buffer configuration
	undoBufferSize := sflags.MustGetInt(cmd, "undo-buffer-size")

	// Validate Schema Registry configuration for schema-registry format
	if outputFormat == "schema-registry" && schemaRegistryURL == "" {
		return fmt.Errorf("--schema-registry-url is required when using --output-format=schema-registry")
	}

	// Validate explosion configuration
	// No validation needed for explode-field - empty string means no explosion

	// Validate token GRPC configuration - two independent use cases
	if tokenMetadataEndpoint != "" {
		hasTokenNormalization := len(tokenValueFields) > 0 || len(tokenAddressFields) > 0
		hasTokenMetadataInjection := len(tokenMetadataFields) > 0 || len(tokenSourceFields) > 0

		// Must have at least one use case configured
		if !hasTokenNormalization && !hasTokenMetadataInjection {
			return fmt.Errorf("--token-metadata-grpc-endpoint requires either token normalization fields (--token-value-fields + --token-address-fields) or token metadata injection fields (--token-metadata-fields + --token-source-fields)")
		}

		// Validate token normalization configuration (if used)
		if len(tokenValueFields) > 0 && len(tokenAddressFields) == 0 {
			return fmt.Errorf("--token-address-fields is required when --token-value-fields is specified")
		}
		if len(tokenAddressFields) > 0 && len(tokenValueFields) == 0 {
			return fmt.Errorf("--token-value-fields is required when --token-address-fields is specified")
		}

		// Validate token metadata injection configuration (if used)
		if len(tokenMetadataFields) > 0 && len(tokenSourceFields) == 0 {
			return fmt.Errorf("--token-source-fields is required when --token-metadata-fields is specified")
		}
	}

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

	// Add token normalization logging if enabled
	if tokenMetadataEndpoint != "" {
		logFields = append(logFields,
			zap.String("token_metadata_endpoint", tokenMetadataEndpoint),
			zap.Strings("token_value_fields", tokenValueFields),
			zap.Strings("token_address_fields", tokenAddressFields),
		)
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

		// Token normalization options
		TokenMetadataEndpoint: tokenMetadataEndpoint,
		TokenValueFields:      tokenValueFields,
		TokenAddressFields:    tokenAddressFields,

		// Dynamic field processing options
		TokenMetadataFields: tokenMetadataFields,
		TokenSourceFields:   tokenSourceFields,
		OmitEmptyStrings:    omitEmptyStrings,

		// Debug options
		DebugMode: debugMode,

		// Performance profiling options
		EnableProfiling:     enableProfiling,
		EnableOptimizations: enableOptimizations,
		ProfilingOutputDir:  profilingOutputDir,
		ProfilingHTTPPort:   profilingHTTPPort,

		// Output formatting options
		HexEncodeBytes: hexEncodeBytes,

		// Undo buffer options
		UndoBufferSize: undoBufferSize,
	})

	kafkaSink, err := sinkerFactory(app.Context(), zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup Kafka sinker: %w", err)
	}

	app.SuperviseAndStart(kafkaSink)

	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}
