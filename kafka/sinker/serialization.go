package sinker

import (
	"encoding/json"
	"fmt"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type TokenMetadata struct{}

// serializeToJSON properly deserializes the protobuf message and converts it to JSON
func (s *KafkaSinker) serializeToJSON(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Extract the actual module output from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {

		// Create a V2 dynamic message using the official protobuf-go runtime
		dynamicMsg := dynamicpb.NewMessage(s.v2MessageDescriptor)
		if err := proto.Unmarshal(data.Output.MapOutput.Value, dynamicMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal protobuf message: %w", err)
		}

		// Serialize directly

		// Convert the dynamic message to JSON using official protojson with field filtering
		jsonBytes, err := s.marshalToJSONWithFieldFiltering(dynamicMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
		}

		// No post-processing

		return jsonBytes, nil
	}

	// Fallback: serialize entire BlockScopedData to JSON using protojson
	return protojson.Marshal(data)
}

// serializeToRawProtobuf extracts the raw ListenerMessage protobuf bytes
func (s *KafkaSinker) serializeToRawProtobuf(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Extract the actual ListenerMessage from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Return the original bytes (no upstream mutations)
		return data.Output.MapOutput.Value, nil
	}

	// Fallback: serialize entire BlockScopedData
	return proto.Marshal(data)
}

// serializeWithSchemaRegistry uses Confluent Schema Registry format with the actual module output
func (s *KafkaSinker) serializeWithSchemaRegistry(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Fast path: if enabled and we have precomputed header for main type, return header + raw bytes
	if s.schemaFastPath && len(s.srHeaderMain) > 0 {
		if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
			raw := data.Output.MapOutput.Value
			framed := make([]byte, len(s.srHeaderMain)+len(raw))
			copy(framed, s.srHeaderMain)
			copy(framed[len(s.srHeaderMain):], raw)
			return framed, nil
		}
	}
	// Extract the actual module output from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Create a V2 dynamic message using the official protobuf-go runtime (compatible with Confluent)
		// No upstream mutation step
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
	// Simplified: always JSON for exploded records (protobuf path removed)
	return json.Marshal(record)
}

// serializeExplodedRecordAsProtobuf removed

// createExplodedRecordMessage removed; exploded records use JSON path

// mapToProtobufStruct removed; not needed for JSON-only exploded records

// applyTokenNormalizationToJSON is deprecated; returns input as-is
func (s *KafkaSinker) applyTokenNormalizationToJSON(jsonBytes []byte, blockNumber uint64) ([]byte, error) {
	return jsonBytes, nil
}

// extractUniqueTokensFromJSON deprecated
func (s *KafkaSinker) extractUniqueTokensFromJSON(data interface{}, tokenAddressFields []string) []string {
	return nil
}

// extractTokensFromJSONRecursive deprecated
func (s *KafkaSinker) extractTokensFromJSONRecursive(data interface{}, tokenAddressFields []string, uniqueTokens map[string]bool) {
}

// normalizeTokenFieldsInJSON deprecated
func (s *KafkaSinker) normalizeTokenFieldsInJSON(data interface{}, tokenMetadata map[string]*TokenMetadata) {
}

// normalizeTokenFieldPairInJSON deprecated
func (s *KafkaSinker) normalizeTokenFieldPairInJSON(data interface{}, valueField, addressField string, tokenMetadata map[string]*TokenMetadata) {
}

// normalizeRemainingFieldsInJSON deprecated
func (s *KafkaSinker) normalizeRemainingFieldsInJSON(data interface{}, minLen int, tokenMetadata map[string]*TokenMetadata) {
}

// marshalToJSONWithFieldFiltering marshals a protobuf message to JSON while filtering out empty fields
func (s *KafkaSinker) marshalToJSONWithFieldFiltering(message protoreflect.Message) ([]byte, error) {
	if dynamicMsg, ok := message.(*dynamicpb.Message); ok {
		return protojson.Marshal(dynamicMsg)
	}
	return nil, fmt.Errorf("unsupported message type for JSON marshaling")
}

// createFilteredMessage creates a copy of the message with empty fields omitted
func (s *KafkaSinker) createFilteredMessage(message protoreflect.Message) protoreflect.Message {
	return message
}
