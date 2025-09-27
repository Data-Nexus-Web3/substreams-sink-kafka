package sinker

import (
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
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

	// Dynamic protobuf support (v2 only)
	v2MessageDescriptor protoreflect.MessageDescriptor // official V2 descriptor (for Schema Registry)

	// Schema Registry integration (only used with outputFormat="schema-registry")
	schemaRegistryClient schemaregistry.Client
	protobufSerializer   *protobuf.Serializer
	schemaSubject        string
	useSchemaRegistry    bool
	schemaFastPath       bool

	// Fast-path cached headers (Confluent framing: magic + schema id + message index)
	srHeaderMain     []byte
	srHeaderExploded []byte

	// Simple explosion configuration for array field handling
	explodeFieldName string // field name to explode (empty means no explosion)

	// Token normalization and dynamic field processing removed

	// Debug configuration
	debugMode bool // Enable debug logging

	// Output formatting configuration removed
	// removed: hexEncodeBytes

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

	// Processing buffer (post-undo) to smooth CPU-bound bursts
	processingBufferSize     int
	processingQueue          chan bufferedProcessItem
	processingBufferBytes    int64
	processingBufferMaxItems int
	processingCurrentBytes   int64

	// Synchronization for processing queue shutdown
	processingWG sync.WaitGroup

	// Explosion parallelization controls
	explodeWorkers        int
	explodeAlwaysParallel bool
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
	SchemaFastPath     bool

	// Simple explosion option for array field handling
	ExplodeField string // field name to explode (empty means no explosion)
	// Explosion parallelization controls
	ExplodeWorkers        int
	ExplodeAlwaysParallel bool

	// (Deprecated dynamic field processing removed)

	// Debug options
	DebugMode bool // Enable debug logging
	// Undo buffer options
	UndoBufferSize int // Number of blocks to buffer before processing (0 = disabled)

	// Processing buffer (post-undo) options
	ProcessingBufferSize     int   // Number of blocks to buffer after undo (0 = disabled)
	ProcessingBufferBytes    int64 // Total byte budget for processing buffer (0 = disabled)
	ProcessingBufferMaxItems int   // Fuse to cap items in buffer
}

// Global variables for async delivery system
var pendingMutex sync.RWMutex

// initializeSchemaRegistryHeaders precomputes Confluent headers for main and exploded types
func (s *KafkaSinker) initializeSchemaRegistryHeaders() error {
	// Prime registry via a temporary serializer with auto-register enabled
	if s.schemaRegistryClient == nil {
		return nil
	}

	serializerConfig := protobuf.NewSerializerConfig()
	serializerConfig.AutoRegisterSchemas = true
	tempSerializer, err := protobuf.NewSerializer(s.schemaRegistryClient, serde.ValueSerde, serializerConfig)
	if err != nil {
		s.logger.Warn("failed to create temp SR serializer for priming", zap.Error(err))
		return nil
	}

	// Main type header: subject is s.schemaSubject. Serialize empty message to capture header
	emptyMain := dynamicpb.NewMessage(s.v2MessageDescriptor)
	framed, err := tempSerializer.Serialize(s.schemaSubject, emptyMain)
	if err != nil {
		s.logger.Warn("failed SR serialize for header (main)", zap.Error(err))
	} else {
		// For empty message, framed bytes are exactly the header
		s.srHeaderMain = make([]byte, len(framed))
		copy(s.srHeaderMain, framed)
	}

	// Exploded type header: use same subject (topic) but message index path points to nested type.
	s.srHeaderExploded = nil
	if s.explodeFieldName != "" {
		if s.v2MessageDescriptor != nil {
			fld := s.v2MessageDescriptor.Fields().ByName(protoreflect.Name(s.explodeFieldName))
			if fld != nil && fld.Message() != nil {
				emptyItem := dynamicpb.NewMessage(fld.Message())
				framedItem, err := tempSerializer.Serialize(s.topic, emptyItem)
				if err != nil {
					s.logger.Warn("failed SR serialize for header (exploded)", zap.Error(err))
				} else {
					s.srHeaderExploded = make([]byte, len(framedItem))
					copy(s.srHeaderExploded, framedItem)
				}
			}
		}
	}
	return nil
}
