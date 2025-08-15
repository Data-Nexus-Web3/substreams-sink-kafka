package sinker

import (
	"encoding/json"
	"fmt"
	"strings"

	pbkafka "github.com/streamingfast/substreams-sink-kafka/proto/sf/substreams/sink/kafka/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

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

func (s *KafkaSinker) serializeExplodedRecord(record map[string]interface{}) ([]byte, error) {
	// Choose serialization format based on main output format
	switch s.outputFormat {
	case "schema-registry":
		return s.serializeExplodedRecordAsProtobuf(record)
	case "protobuf":
		// For raw protobuf, we can't easily create dynamic messages, so use JSON
		return json.Marshal(record)
	default: // "json"
		return json.Marshal(record)
	}
}

func (s *KafkaSinker) serializeExplodedRecordAsProtobuf(record map[string]interface{}) ([]byte, error) {
	// Create a proper ExplodedRecord protobuf message
	explodedRecord := s.createExplodedRecordMessage(record)

	// Serialize using the Schema Registry protobuf serializer
	if s.protobufSerializer == nil {
		return nil, fmt.Errorf("Schema Registry protobuf serializer not initialized")
	}

	// Use a subject that matches the topic name for exploded records
	// Extract topic name from the base topic and create {topic_name}-value subject
	topicName := strings.TrimPrefix(s.baseTopic, "adr-")
	topicName = strings.ReplaceAll(topicName, "-", "_")
	explodedSubject := topicName + "-value"

	s.logger.Debug("Serializing exploded record as protobuf",
		zap.String("subject", explodedSubject),
		zap.Int("record_fields", len(record)),
	)

	// Serialize using Schema Registry - it expects the protobuf message struct
	return s.protobufSerializer.Serialize(explodedSubject, explodedRecord)
}

func (s *KafkaSinker) createExplodedRecordMessage(record map[string]interface{}) *pbkafka.ExplodedRecord {
	// Convert the record data to protobuf Struct
	dataStruct, err := s.mapToProtobufStruct(record)
	if err != nil {
		s.logger.Error("Failed to convert record to protobuf Struct", zap.Error(err))
		dataStruct = &structpb.Struct{Fields: make(map[string]*structpb.Value)}
	}

	// Extract metadata from the record
	sourceField := ""
	itemIndex := int32(0)
	cursor := ""
	parentContext := &structpb.Struct{Fields: make(map[string]*structpb.Value)}

	// Extract known fields from the record
	if val, ok := record["transactions_index"]; ok {
		if idx, ok := val.(int); ok {
			itemIndex = int32(idx)
		}
	}
	if val, ok := record["cursor"]; ok {
		if c, ok := val.(string); ok {
			cursor = c
		}
	}

	// Create the ExplodedRecord message
	explodedRecord := &pbkafka.ExplodedRecord{
		SourceField:   sourceField,
		Data:          dataStruct,
		ParentContext: parentContext,
		ItemIndex:     itemIndex,
		Cursor:        cursor,
		Metadata:      make(map[string]string),
	}

	return explodedRecord
}

func (s *KafkaSinker) mapToProtobufStruct(m map[string]interface{}) (*structpb.Struct, error) {
	// Convert map[string]interface{} to protobuf Struct
	return structpb.NewStruct(m)
}
