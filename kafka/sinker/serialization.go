package sinker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pbkafka "github.com/streamingfast/substreams-sink-kafka/proto/sf/substreams/sink/kafka/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

		// Field processing now happens upstream in applyUpstreamMutations()
		// This ensures all output formats benefit from mutations

		// Convert the dynamic message to JSON using official protojson with field filtering
		jsonBytes, err := s.marshalToJSONWithFieldFiltering(dynamicMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to JSON: %w", err)
		}

		// Token normalization now happens upstream in applyUpstreamMutations()
		// No need for separate JSON post-processing

		return jsonBytes, nil
	}

	// Fallback: serialize entire BlockScopedData to JSON using protojson
	return protojson.Marshal(data)
}

// serializeToRawProtobuf extracts the raw ListenerMessage protobuf bytes
func (s *KafkaSinker) serializeToRawProtobuf(data *pbsubstreamsrpc.BlockScopedData) ([]byte, error) {
	// Extract the actual ListenerMessage from BlockScopedData
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		// Return the mutated protobuf bytes (mutations applied upstream in applyUpstreamMutations())
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
		// The message has already been mutated upstream in applyUpstreamMutations()
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
		return nil, fmt.Errorf("protobuf serializer is not initialized")
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

// applyTokenNormalizationToJSON applies token normalization to JSON data
func (s *KafkaSinker) applyTokenNormalizationToJSON(jsonBytes []byte, blockNumber uint64) ([]byte, error) {
	// Parse JSON into a map
	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return jsonBytes, fmt.Errorf("failed to parse JSON for token normalization: %w", err)
	}

	// Extract unique token addresses
	uniqueTokens := s.extractUniqueTokensFromJSON(data, s.tokenAddressFields)
	if s.debugMode {
		s.logger.Debug("Token normalization - extracted tokens",
			zap.Strings("unique_tokens", uniqueTokens),
			zap.Strings("address_fields", s.tokenAddressFields),
			zap.Uint64("block_number", blockNumber),
		)
	}
	if len(uniqueTokens) == 0 {
		if s.debugMode {
			s.logger.Debug("No tokens found for normalization")
		}
		return jsonBytes, nil // No tokens to normalize
	}

	// Batch lookup token metadata
	ctx := context.Background()
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, uniqueTokens)
	if err != nil {
		s.logger.Warn("Token metadata lookup failed, continuing without normalization",
			zap.Error(err),
			zap.Uint64("block_number", blockNumber),
		)
		return jsonBytes, nil
	}

	// Apply normalization to JSON data
	s.normalizeTokenFieldsInJSON(data, tokenMetadata)

	// Marshal back to JSON
	normalizedBytes, err := json.Marshal(data)
	if err != nil {
		return jsonBytes, fmt.Errorf("failed to marshal normalized JSON: %w", err)
	}

	return normalizedBytes, nil
}

// extractUniqueTokensFromJSON extracts unique token addresses from JSON data
func (s *KafkaSinker) extractUniqueTokensFromJSON(data interface{}, tokenAddressFields []string) []string {
	uniqueTokens := make(map[string]bool)

	// Handle both simple fields and nested field paths
	for _, fieldName := range tokenAddressFields {
		if strings.Contains(fieldName, ".") {
			// Use nested field extraction for dot notation paths
			s.tokenNormalizer.extractTokensFromNestedJSON(data, fieldName, uniqueTokens)
		} else {
			// Use existing recursive extraction for simple field names
			s.extractTokensFromJSONRecursive(data, []string{fieldName}, uniqueTokens)
		}
	}

	// Convert map to slice
	result := make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		if token != "" && token != "0x0000000000000000000000000000000000000000" {
			result = append(result, token)
		}
	}

	return result
}

// extractTokensFromJSONRecursive recursively extracts token addresses from JSON data
func (s *KafkaSinker) extractTokensFromJSONRecursive(data interface{}, tokenAddressFields []string, uniqueTokens map[string]bool) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check if any of the token address fields exist in this object
		for _, fieldName := range tokenAddressFields {
			if tokenAddr, exists := v[fieldName]; exists {
				if addr, ok := tokenAddr.(string); ok && addr != "" {
					uniqueTokens[addr] = true
				}
			}
		}

		// Recursively process nested objects and arrays
		for _, value := range v {
			s.extractTokensFromJSONRecursive(value, tokenAddressFields, uniqueTokens)
		}

	case []interface{}:
		// Process array elements
		for _, item := range v {
			s.extractTokensFromJSONRecursive(item, tokenAddressFields, uniqueTokens)
		}
	}
}

// normalizeTokenFieldsInJSON applies token normalization to JSON data
func (s *KafkaSinker) normalizeTokenFieldsInJSON(data interface{}, tokenMetadata map[string]*TokenMetadata) {
	// Handle paired field normalization (match by position in arrays)
	minLen := len(s.tokenValueFields)
	if len(s.tokenAddressFields) < minLen {
		minLen = len(s.tokenAddressFields)
	}

	// Process paired fields by position
	for i := 0; i < minLen; i++ {
		valueField := s.tokenValueFields[i]
		addressField := s.tokenAddressFields[i]

		if strings.Contains(valueField, ".") || strings.Contains(addressField, ".") {
			// Handle nested field pairs
			s.tokenNormalizer.normalizeAmountInNestedJSON(data, valueField, addressField, tokenMetadata, s.logger, s.debugMode)
		} else {
			// Handle traditional flat field pairs
			s.normalizeTokenFieldPairInJSON(data, valueField, addressField, tokenMetadata)
		}
	}

	// Handle traditional flat field normalization for remaining unpaired fields and recursion
	s.normalizeRemainingFieldsInJSON(data, minLen, tokenMetadata)
}

// normalizeTokenFieldPairInJSON normalizes a specific token field pair in flat JSON structure
func (s *KafkaSinker) normalizeTokenFieldPairInJSON(data interface{}, valueField, addressField string, tokenMetadata map[string]*TokenMetadata) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check if both fields exist in this object
		if rawValue, valueExists := v[valueField]; valueExists {
			if tokenAddr, addrExists := v[addressField]; addrExists {
				if rawStr, ok := rawValue.(string); ok {
					if tokenAddrStr, ok := tokenAddr.(string); ok {
						if metadata, exists := tokenMetadata[tokenAddrStr]; exists {
							formattedAmount, err := s.tokenNormalizer.NormalizeTokenAmount(rawStr, metadata.Decimals)
							if err == nil {
								// Add the formatted field
								v[valueField+"_formatted"] = formattedAmount

								if s.debugMode {
									s.logger.Debug("Token amount normalized in JSON (paired fields)",
										zap.String("value_field", valueField),
										zap.String("address_field", addressField),
										zap.String("raw_amount", rawStr),
										zap.String("formatted_amount", formattedAmount),
										zap.String("token_address", tokenAddrStr),
										zap.String("symbol", metadata.Symbol),
										zap.Uint32("decimals", metadata.Decimals),
									)
								}
							}
						}
					}
				}
			}
		}

		// Recursively process nested objects
		for _, value := range v {
			s.normalizeTokenFieldPairInJSON(value, valueField, addressField, tokenMetadata)
		}

	case []interface{}:
		// Process array elements
		for _, item := range v {
			s.normalizeTokenFieldPairInJSON(item, valueField, addressField, tokenMetadata)
		}
	}
}

// normalizeRemainingFieldsInJSON handles remaining unpaired fields and recursion
func (s *KafkaSinker) normalizeRemainingFieldsInJSON(data interface{}, minLen int, tokenMetadata map[string]*TokenMetadata) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Find token address for this object from remaining address fields
		var tokenAddress string
		for i := minLen; i < len(s.tokenAddressFields); i++ {
			fieldName := s.tokenAddressFields[i]
			if !strings.Contains(fieldName, ".") { // Only handle flat fields here
				if addr, exists := v[fieldName]; exists {
					if addrStr, ok := addr.(string); ok {
						tokenAddress = addrStr
						break
					}
				}
			}
		}

		// If we found a token address, normalize the remaining value fields
		if tokenAddress != "" {
			if metadata, exists := tokenMetadata[tokenAddress]; exists {
				for i := minLen; i < len(s.tokenValueFields); i++ {
					valueFieldName := s.tokenValueFields[i]
					if !strings.Contains(valueFieldName, ".") { // Only handle flat fields here
						if rawValue, exists := v[valueFieldName]; exists {
							if rawStr, ok := rawValue.(string); ok {
								formattedAmount, err := s.tokenNormalizer.NormalizeTokenAmount(rawStr, metadata.Decimals)
								if err == nil {
									// Add the formatted field
									v[valueFieldName+"_formatted"] = formattedAmount

									if s.debugMode {
										s.logger.Debug("Token amount normalized in JSON",
											zap.String("field", valueFieldName),
											zap.String("raw_amount", rawStr),
											zap.String("formatted_amount", formattedAmount),
											zap.String("token_address", tokenAddress),
											zap.String("symbol", metadata.Symbol),
											zap.Uint32("decimals", metadata.Decimals),
										)
									}
								}
							}
						}
					}
				}
			}
		}

		// Recursively process nested objects
		for _, value := range v {
			s.normalizeTokenFieldsInJSON(value, tokenMetadata)
		}

	case []interface{}:
		// Process array elements
		for _, item := range v {
			s.normalizeTokenFieldsInJSON(item, tokenMetadata)
		}
	}
}

// marshalToJSONWithFieldFiltering marshals a protobuf message to JSON while filtering out empty fields
func (s *KafkaSinker) marshalToJSONWithFieldFiltering(message protoreflect.Message) ([]byte, error) {
	if s.fieldProcessingConfig == nil || !s.fieldProcessingConfig.OmitEmptyStrings {
		// No filtering needed, use standard marshaling
		if dynamicMsg, ok := message.(*dynamicpb.Message); ok {
			return protojson.Marshal(dynamicMsg)
		}
		return nil, fmt.Errorf("unsupported message type for JSON marshaling")
	}

	// Create a filtered copy of the message
	filteredMsg := s.createFilteredMessage(message)

	if dynamicMsg, ok := filteredMsg.(*dynamicpb.Message); ok {
		return protojson.Marshal(dynamicMsg)
	}
	return nil, fmt.Errorf("unsupported filtered message type for JSON marshaling")
}

// createFilteredMessage creates a copy of the message with empty fields omitted
func (s *KafkaSinker) createFilteredMessage(message protoreflect.Message) protoreflect.Message {
	msgDesc := message.Descriptor()
	filteredMsg := dynamicpb.NewMessage(msgDesc)

	// Copy non-empty fields
	for i := 0; i < msgDesc.Fields().Len(); i++ {
		field := msgDesc.Fields().Get(i)

		if !message.Has(field) {
			continue
		}

		value := message.Get(field)

		// Check if field should be omitted
		if s.shouldOmitFieldFromOutput(field, value) {
			continue
		}

		// For nested messages, recursively filter
		if field.Kind() == protoreflect.MessageKind {
			if field.IsList() {
				// List of messages
				list := value.List()
				filteredList := filteredMsg.Mutable(field).List()
				for j := 0; j < list.Len(); j++ {
					nestedMsg := list.Get(j).Message()
					filteredNestedMsg := s.createFilteredMessage(nestedMsg)
					filteredList.Append(protoreflect.ValueOfMessage(filteredNestedMsg))
				}
			} else {
				// Single nested message
				nestedMsg := value.Message()
				filteredNestedMsg := s.createFilteredMessage(nestedMsg)
				filteredMsg.Set(field, protoreflect.ValueOfMessage(filteredNestedMsg))
			}
		} else {
			// Copy the value as-is
			filteredMsg.Set(field, value)
		}
	}

	return filteredMsg
}
