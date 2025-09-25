package sinker

import (
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type KafkaSinker struct {
	*shutter.Shutter
	*sink.Sinker

	producer     *kafka.Producer
	topic        string
	baseTopic    string // base topic name for generating exploded topic names
	batchSize    int
	batchTimeout time.Duration
	cursorFile   string
	logger       *zap.Logger
	tracer       logging.Tracer

	// Output format configuration
	outputFormat string // "json", "protobuf", or "schema-registry"

	// Dynamic protobuf support for universal module handling
	outputModuleDescriptor *desc.MessageDescriptor        // jhump descriptor (for backward compatibility)
	v2MessageDescriptor    protoreflect.MessageDescriptor // official V2 descriptor (for Schema Registry)

	// Schema Registry integration (only used with outputFormat="schema-registry")
	schemaRegistryClient schemaregistry.Client
	protobufSerializer   *protobuf.Serializer
	schemaSubject        string
	useSchemaRegistry    bool

	// Simple explosion configuration for array field handling
	explodeFieldName string // field name to explode (empty means no explosion)

	// Token normalization configuration (optional feature)
	tokenNormalizer          *TokenNormalizer          // nil if disabled
	optimizedNormalizer      *OptimizedTokenNormalizer // nil if disabled
	tokenValueFields         []string                  // Field names containing raw token amounts
	tokenAddressFields       []string                  // Field names containing token contract addresses
	enableTokenNormalization bool                      // Whether token normalization is enabled

	// Dynamic field processing configuration
	fieldProcessingConfig *FieldProcessingConfig        // Configuration for field annotations and processing
	tokenMetadataRules    []*TokenMetadataInjectionRule // Rules for injecting token metadata

	// Debug configuration
	debugMode bool // Enable debug logging

	// Output formatting configuration
	hexEncodeBytes bool // Encode byte fields as hex strings

	// Statistics tracking
	mutationStats *MutationStats // Mutation performance statistics

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

	// Undo buffer for reorg protection
	undoBufferSize      int
	undoBuffer          *UndoBuffer
	lastReceivedCursor  *sink.Cursor // Last cursor received from substreams
	lastProcessedCursor *sink.Cursor // Last cursor for blocks sent to Kafka
}

// ExplodedMessage represents a message that has been exploded from a nested structure
type ExplodedMessage struct {
	Topic          string
	Key            string
	MessageBytes   []byte
	ParentContext  map[string]interface{}
	OriginalCursor *sink.Cursor
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

	// Simple explosion option for array field handling
	ExplodeField string // field name to explode (empty means no explosion)

	// Token normalization options (optional feature)
	TokenMetadataEndpoint string   // GRPC endpoint for token metadata service (empty = disabled)
	TokenValueFields      []string // Field names containing raw token amounts
	TokenAddressFields    []string // Field names containing token contract addresses

	// Dynamic field processing options
	TokenMetadataFields []string // Field names to inject token metadata into
	TokenSourceFields   []string // Priority-ordered token source fields (e.g., ["ops_token", "act_address"])
	OmitEmptyStrings    bool     // Whether to omit empty string fields from output

	// Debug options
	DebugMode bool // Enable debug logging

	// Performance profiling options
	EnableProfiling     bool   // Enable performance profiling
	EnableOptimizations bool   // Enable performance optimizations
	ProfilingOutputDir  string // Directory for profiling output files
	ProfilingHTTPPort   int    // Port for pprof HTTP server for detailed processing information

	// Output formatting options
	HexEncodeBytes bool // Encode byte fields as hex strings with 0x prefix

	// Undo buffer options
	UndoBufferSize int // Number of blocks to buffer before processing (0 = disabled)
}

// Global variables for async delivery system
var pendingMutex sync.RWMutex
