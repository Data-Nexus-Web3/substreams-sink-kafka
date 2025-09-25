package sinker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	tokenmetadata "github.com/streamingfast/substreams-sink-kafka/proto/token_metadata/v1"
)

// injectTokenMetadata adds token metadata fields to messages based on configured rules
func (s *KafkaSinker) injectTokenMetadata(ctx context.Context, message protoreflect.Message, blockNumber uint64) error {
	if s.debugMode {
		s.logger.Debug("injectTokenMetadata called",
			zap.Bool("has_normalizer", s.tokenNormalizer != nil),
			zap.Bool("has_config", s.fieldProcessingConfig != nil),
			zap.Int("rules_count", len(s.tokenMetadataRules)),
		)
	}

	if s.tokenNormalizer == nil || s.fieldProcessingConfig == nil {
		if s.debugMode {
			s.logger.Debug("Token metadata injection disabled - normalizer or config missing")
		}
		return nil // Token metadata injection is disabled
	}

	if len(s.tokenMetadataRules) == 0 {
		if s.debugMode {
			s.logger.Debug("No token metadata injection rules configured")
		}
		return nil
	}

	s.logger.Debug("Processing token metadata injection",
		zap.Int("rules_count", len(s.tokenMetadataRules)),
		zap.Uint64("block_number", blockNumber),
	)

	// Collect all token addresses that need metadata
	tokenAddresses := make(map[string]bool)

	// Process each token metadata injection rule
	for _, rule := range s.tokenMetadataRules {
		if s.debugMode {
			s.logger.Debug("Processing token metadata rule",
				zap.String("target_field", rule.TargetField),
				zap.Strings("source_fields", rule.TokenSourceFields),
			)
		}

		addresses := s.fieldProcessingConfig.GetTokenSourcesForMessage(message, rule)

		if s.debugMode {
			s.logger.Debug("Token address extraction result",
				zap.Int("found_token_count", len(addresses)),
			)
		}

		for _, addr := range addresses {
			if addr != "" {
				tokenAddresses[addr] = true
			}
		}
	}

	// If no token addresses found, nothing to inject
	if len(tokenAddresses) == 0 {
		if s.debugMode {
			s.logger.Debug("No token addresses found, skipping injection")
		}
		return nil
	}

	if s.debugMode {
		s.logger.Debug("Found token addresses for injection",
			zap.Int("unique_token_count", len(tokenAddresses)),
		)
	}

	// Convert map to slice for batch lookup
	addresses := make([]string, 0, len(tokenAddresses))
	for addr := range tokenAddresses {
		addresses = append(addresses, addr)
	}

	// Batch fetch token metadata
	if s.debugMode {
		s.logger.Debug("About to call BatchGetTokenMetadata",
			zap.Int("address_count", len(addresses)),
			zap.Uint64("block_number", blockNumber),
		)
	}

	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, addresses)
	if err != nil {
		s.logger.Warn("Failed to fetch token metadata for injection",
			zap.Int("address_count", len(addresses)),
			zap.Error(err),
		)
		return nil // Don't fail the entire message processing
	}

	if s.debugMode {
		s.logger.Debug("BatchGetTokenMetadata succeeded",
			zap.Int("metadata_count", len(tokenMetadata)),
		)
	}

	// Inject metadata into each configured field
	for _, rule := range s.tokenMetadataRules {
		addresses := s.fieldProcessingConfig.GetTokenSourcesForMessage(message, rule)
		if len(addresses) == 0 {
			continue
		}

		// Collect metadata for all found addresses
		var metadataList []*TokenMetadata
		for _, addr := range addresses {
			if metadata, exists := tokenMetadata[addr]; exists {
				metadataList = append(metadataList, metadata)
			}
		}

		if len(metadataList) > 0 {
			s.injectTokenMetadataFieldMultiple(message, rule.TargetField, addresses, metadataList)
		}
	}

	return nil
}

// injectTokenMetadataFieldMultiple injects multiple token metadata into a field (supports arrays and nested paths)
func (s *KafkaSinker) injectTokenMetadataFieldMultiple(message protoreflect.Message, fieldName string, addresses []string, metadataList []*TokenMetadata) {
	if s.debugMode {
		s.logger.Debug("injectTokenMetadataFieldMultiple called",
			zap.String("field_name", fieldName),
			zap.Int("address_count", len(addresses)),
			zap.Int("metadata_count", len(metadataList)),
		)
	}

	// Check if this is a nested field path
	if strings.Contains(fieldName, ".") {
		s.injectTokenMetadataAtNestedProtobufPath(message, fieldName, addresses, metadataList)
		return
	}

	// Handle flat field injection (single metadata object)
	if len(metadataList) > 0 {
		s.injectTokenMetadataField(message, fieldName, metadataList[0])
	}
}

// injectTokenMetadataAtNestedProtobufPath injects token metadata at nested protobuf paths
func (s *KafkaSinker) injectTokenMetadataAtNestedProtobufPath(message protoreflect.Message, fieldPath string, addresses []string, metadataList []*TokenMetadata) {
	if s.debugMode {
		s.logger.Debug("injectTokenMetadataAtNestedProtobufPath called",
			zap.String("field_path", fieldPath),
			zap.Int("address_count", len(addresses)),
			zap.Int("metadata_count", len(metadataList)),
		)
	}

	// Parse the nested field path
	pathInfo := ParseNestedFieldPath(fieldPath)

	// For protobuf injection, we need to create the nested structure
	// This is complex because protobuf fields are strongly typed

	// Determine if we should inject as array or single object
	isArraySource := len(addresses) > 1
	if !isArraySource && pathInfo.IsNested {
		// Check if the source container was an array by looking at the original data
		jsonData := s.convertProtobufToMap(message)
		container := GetNestedValueFromPath(jsonData, pathInfo.ContainerPath)
		if _, isArray := container.([]interface{}); isArray {
			isArraySource = true
		}
	}

	// Create metadata objects
	metadataObjects := s.createMetadataObjects(addresses, metadataList, isArraySource)

	// Instead of updating the entire protobuf, just update the specific target field
	s.updateSpecificProtobufField(message, fieldPath, metadataObjects)

	if s.debugMode {
		s.logger.Debug("Nested protobuf injection completed",
			zap.String("field_path", fieldPath),
			zap.Bool("is_array_source", isArraySource),
		)
	}
}

// updateSpecificProtobufField updates only a specific field in the protobuf message
func (s *KafkaSinker) updateSpecificProtobufField(message protoreflect.Message, fieldPath string, value interface{}) {
	if s.debugMode {
		s.logger.Debug("updateSpecificProtobufField called",
			zap.String("field_path", fieldPath),
		)
	}

	// Parse the field path
	pathInfo := ParseNestedFieldPath(fieldPath)

	if pathInfo.IsNested {
		// Navigate to the nested field
		s.updateNestedProtobufField(message, pathInfo.ContainerPath, pathInfo.FieldName, value)
	} else {
		// Update top-level field
		msgDesc := message.Descriptor()
		field := msgDesc.Fields().ByName(protoreflect.Name(fieldPath))
		if field != nil {
			protoValue, err := s.convertInterfaceToProtoreflectValue(value, field, message)
			if err != nil {
				s.logger.Warn("Failed to convert value for target field",
					zap.String("field_path", fieldPath),
					zap.Error(err),
				)
				return
			}
			message.Set(field, protoValue)
			if s.debugMode {
				s.logger.Debug("Updated top-level protobuf field",
					zap.String("field_path", fieldPath),
				)
			}
		}
	}
}

// updateNestedProtobufField updates a nested field in the protobuf message
func (s *KafkaSinker) updateNestedProtobufField(message protoreflect.Message, containerPath string, fieldName string, value interface{}) {
	if s.debugMode {
		s.logger.Debug("updateNestedProtobufField called",
			zap.String("container_path", containerPath),
			zap.String("field_name", fieldName),
		)
	}

	// Navigate through the nested path to find the target container
	current := message
	parts := strings.Split(containerPath, ".")

	for _, part := range parts {
		msgDesc := current.Descriptor()
		field := msgDesc.Fields().ByName(protoreflect.Name(part))
		if field == nil {
			s.logger.Warn("Field not found in nested path",
				zap.String("part", part),
				zap.String("container_path", containerPath),
			)
			return
		}

		if field.Kind() == protoreflect.MessageKind {
			if current.Has(field) {
				current = current.Get(field).Message()
			} else {
				// Create the nested message if it doesn't exist
				newMsg := current.Mutable(field).Message()
				current = newMsg
			}
		} else {
			s.logger.Warn("Expected message field in nested path",
				zap.String("part", part),
				zap.String("field_kind", field.Kind().String()),
			)
			return
		}
	}

	// Now update the target field in the nested message
	msgDesc := current.Descriptor()
	targetField := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
	if targetField != nil {
		if s.debugMode {
			s.logger.Debug("Target field info",
				zap.String("field_name", fieldName),
				zap.String("field_kind", targetField.Kind().String()),
				zap.String("message_full_name", string(targetField.Message().FullName())),
			)
		}

		// Create messages that match the target field's expected type
		protoValue, err := s.createCompatibleProtobufValue(value, targetField, current)
		if err != nil {
			s.logger.Warn("Failed to create compatible protobuf value",
				zap.String("field_name", fieldName),
				zap.Error(err),
			)
			return
		}
		current.Set(targetField, protoValue)
		if s.debugMode {
			s.logger.Debug("Updated nested protobuf field",
				zap.String("container_path", containerPath),
				zap.String("field_name", fieldName),
			)
		}
	} else {
		s.logger.Warn("Target field not found in nested container",
			zap.String("field_name", fieldName),
			zap.String("container_path", containerPath),
		)
	}
}

// injectTokenMetadataField injects token metadata into a specific field
func (s *KafkaSinker) injectTokenMetadataField(message protoreflect.Message, fieldName string, metadata *TokenMetadata) {
	msgDesc := message.Descriptor()

	// Check if this is a nested field path
	if strings.Contains(fieldName, ".") {
		s.injectTokenMetadataAtNestedFieldPath(message, fieldName, metadata)
		return
	}

	// Try to find the target field (flat field handling)
	field := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
	if field == nil {
		s.logger.Debug("Token metadata target field not found",
			zap.String("field", fieldName),
			zap.String("token_address", metadata.Address),
		)
		return
	}

	// Handle different field types
	switch field.Kind() {
	case protoreflect.StringKind:
		// Inject as JSON string
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			s.logger.Debug("Failed to marshal token metadata to JSON",
				zap.String("token_address", metadata.Address),
				zap.Error(err),
			)
			return
		}
		message.Set(field, protoreflect.ValueOfString(string(metadataJSON)))

	case protoreflect.MessageKind:
		// Inject as nested message (if the field is a TokenMetadata message type)
		if field.Message().FullName() == "token_metadata.v1.TokenMetadata" {
			// Create a new TokenMetadata message
			metadataMsg := dynamicpb.NewMessage(field.Message())

			// Set fields from our metadata
			s.setTokenMetadataFields(metadataMsg, metadata)

			message.Set(field, protoreflect.ValueOfMessage(metadataMsg))
		} else {
			s.logger.Debug("Token metadata field is not compatible message type",
				zap.String("field", fieldName),
				zap.String("expected_type", "token_metadata.v1.TokenMetadata"),
				zap.String("actual_type", string(field.Message().FullName())),
			)
		}

	default:
		s.logger.Debug("Unsupported field type for token metadata injection",
			zap.String("field", fieldName),
			zap.String("field_type", field.Kind().String()),
		)
	}

	s.logger.Debug("Token metadata injected",
		zap.String("field", fieldName),
		zap.String("token_address", metadata.Address),
		zap.String("symbol", metadata.Symbol),
		zap.String("name", metadata.Name),
		zap.Uint32("decimals", metadata.Decimals),
	)
}

// setTokenMetadataFields sets the fields of a TokenMetadata protobuf message
func (s *KafkaSinker) setTokenMetadataFields(message protoreflect.Message, metadata *TokenMetadata) {
	msgDesc := message.Descriptor()

	// Set address field
	if field := msgDesc.Fields().ByName("address"); field != nil {
		message.Set(field, protoreflect.ValueOfString(metadata.Address))
	}

	// Set decimals field
	if field := msgDesc.Fields().ByName("decimals"); field != nil {
		message.Set(field, protoreflect.ValueOfUint32(metadata.Decimals))
	}

	// Set symbol field
	if field := msgDesc.Fields().ByName("symbol"); field != nil {
		message.Set(field, protoreflect.ValueOfString(metadata.Symbol))
	}

	// Set name field
	if field := msgDesc.Fields().ByName("name"); field != nil {
		message.Set(field, protoreflect.ValueOfString(metadata.Name))
	}

	// Set valid_from_block field
	if field := msgDesc.Fields().ByName("valid_from_block"); field != nil {
		message.Set(field, protoreflect.ValueOfUint64(metadata.ValidFromBlock))
	}

	// Set valid_to_block field (if present)
	if field := msgDesc.Fields().ByName("valid_to_block"); field != nil {
		message.Set(field, protoreflect.ValueOfUint64(metadata.ValidToBlock))
	}
}

// processMessageWithFieldAnnotations processes a message according to field processing configuration
func (s *KafkaSinker) processMessageWithFieldAnnotations(ctx context.Context, message protoreflect.Message, blockNumber uint64) error {
	if s.fieldProcessingConfig == nil {
		return nil
	}

	// First, inject token metadata
	if err := s.injectTokenMetadata(ctx, message, blockNumber); err != nil {
		return err
	}

	// Then process nested messages recursively
	s.processNestedMessagesWithAnnotations(ctx, message, blockNumber)

	return nil
}

// processNestedMessagesWithAnnotations recursively processes nested messages
func (s *KafkaSinker) processNestedMessagesWithAnnotations(ctx context.Context, message protoreflect.Message, blockNumber uint64) {
	msgDesc := message.Descriptor()

	for i := 0; i < msgDesc.Fields().Len(); i++ {
		field := msgDesc.Fields().Get(i)

		if !message.Has(field) {
			continue
		}

		value := message.Get(field)

		// Process nested messages
		if field.Kind() == protoreflect.MessageKind {
			if field.IsList() {
				// List of messages
				list := value.List()
				for j := 0; j < list.Len(); j++ {
					nestedMsg := list.Get(j).Message()
					s.processMessageWithFieldAnnotations(ctx, nestedMsg, blockNumber)
				}
			} else {
				// Single nested message
				nestedMsg := value.Message()
				s.processMessageWithFieldAnnotations(ctx, nestedMsg, blockNumber)
			}
		}
	}
}

// shouldOmitFieldFromOutput determines if a field should be omitted from the final output
func (s *KafkaSinker) shouldOmitFieldFromOutput(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
	if s.fieldProcessingConfig == nil {
		return false
	}

	return s.fieldProcessingConfig.ShouldOmitField(field, value)
}

// injectTokenMetadataAtNestedFieldPath injects token metadata at a nested field path in protobuf messages
func (s *KafkaSinker) injectTokenMetadataAtNestedFieldPath(message protoreflect.Message, fieldPath string, metadata *TokenMetadata) {
	// For protobuf messages, we need to navigate the nested structure
	// This is more complex than JSON because protobuf fields are strongly typed

	// For now, log that nested protobuf injection is not yet implemented
	// Most use cases will use JSON serialization where nested injection works
	s.logger.Debug("Nested field path injection for protobuf messages not yet implemented",
		zap.String("field_path", fieldPath),
		zap.String("token_address", metadata.Address),
	)

	// TODO: Implement nested protobuf field injection if needed
	// This would require:
	// 1. Parsing the field path
	// 2. Navigating through nested message fields
	// 3. Creating/updating nested message structures
	// 4. Handling arrays of messages
}

// createCompatibleProtobufValue creates protobuf messages that match the target field's expected type
func (s *KafkaSinker) createCompatibleProtobufValue(value interface{}, targetField protoreflect.FieldDescriptor, parentMessage protoreflect.Message) (protoreflect.Value, error) {
	if targetField.Kind() != protoreflect.MessageKind {
		return protoreflect.Value{}, fmt.Errorf("target field is not a message type")
	}

	if targetField.IsList() {
		// Handle array of messages
		list := parentMessage.Mutable(targetField).List()
		list.Truncate(0) // Clear existing list

		if protoMsgs, ok := value.([]*tokenmetadata.TokenMetadata); ok {
			// Convert our TokenMetadata messages to the target schema
			for _, ourMsg := range protoMsgs {
				targetMsg := list.NewElement().Message()
				s.copyTokenMetadataToTargetMessage(ourMsg, targetMsg)
				list.Append(protoreflect.ValueOfMessage(targetMsg))
			}
			return protoreflect.ValueOfList(list), nil
		} else if singleMsg, ok := value.(*tokenmetadata.TokenMetadata); ok {
			// Single message as array
			targetMsg := list.NewElement().Message()
			s.copyTokenMetadataToTargetMessage(singleMsg, targetMsg)
			list.Append(protoreflect.ValueOfMessage(targetMsg))
			return protoreflect.ValueOfList(list), nil
		}
	} else {
		// Handle single message
		if singleMsg, ok := value.(*tokenmetadata.TokenMetadata); ok {
			targetMsg := parentMessage.Mutable(targetField).Message()
			s.copyTokenMetadataToTargetMessage(singleMsg, targetMsg)
			return protoreflect.ValueOfMessage(targetMsg), nil
		}
	}

	return protoreflect.Value{}, fmt.Errorf("unsupported value type for compatible protobuf creation: %T", value)
}

// copyTokenMetadataToTargetMessage copies fields from our TokenMetadata to the target message schema
func (s *KafkaSinker) copyTokenMetadataToTargetMessage(source *tokenmetadata.TokenMetadata, target protoreflect.Message) {
	targetDesc := target.Descriptor()

	// Debug: Log what fields the target schema has
	if s.debugMode {
		s.logger.Debug("Target protobuf schema fields",
			zap.String("message_name", string(targetDesc.FullName())),
		)

		fields := targetDesc.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			s.logger.Debug("Target schema field",
				zap.String("field_name", string(field.Name())),
				zap.String("field_kind", field.Kind().String()),
				zap.Int("field_number", int(field.Number())),
			)
		}
	}

	// Map our fields to target fields by name (excluding valid_from_block and valid_to_block)
	fieldMappings := map[string]interface{}{
		"address":  source.Address,
		"decimals": source.Decimals,
		"symbol":   source.Symbol,
		"name":     source.Name,
	}

	for fieldName, value := range fieldMappings {
		targetField := targetDesc.Fields().ByName(protoreflect.Name(fieldName))
		if targetField == nil {
			s.logger.Debug("Target field not found, skipping",
				zap.String("field_name", fieldName),
			)
			continue
		}

		// Convert value to appropriate protobuf type
		var protoValue protoreflect.Value
		switch targetField.Kind() {
		case protoreflect.StringKind:
			if str, ok := value.(string); ok {
				protoValue = protoreflect.ValueOfString(str)
			} else {
				protoValue = protoreflect.ValueOfString(fmt.Sprintf("%v", value))
			}
		case protoreflect.Uint32Kind:
			if val, ok := value.(uint32); ok {
				protoValue = protoreflect.ValueOfUint32(val)
			} else if val, ok := value.(uint64); ok {
				protoValue = protoreflect.ValueOfUint32(uint32(val))
			}
		case protoreflect.Uint64Kind:
			if val, ok := value.(uint64); ok {
				protoValue = protoreflect.ValueOfUint64(val)
			} else if val, ok := value.(uint32); ok {
				protoValue = protoreflect.ValueOfUint64(uint64(val))
			}
		case protoreflect.Int32Kind:
			if val, ok := value.(uint32); ok {
				protoValue = protoreflect.ValueOfInt32(int32(val))
			} else if val, ok := value.(uint64); ok {
				protoValue = protoreflect.ValueOfInt32(int32(val))
			}
		case protoreflect.Int64Kind:
			if val, ok := value.(uint64); ok {
				protoValue = protoreflect.ValueOfInt64(int64(val))
			} else if val, ok := value.(uint32); ok {
				protoValue = protoreflect.ValueOfInt64(int64(val))
			}
		default:
			s.logger.Debug("Unsupported target field type, skipping",
				zap.String("field_name", fieldName),
				zap.String("field_kind", targetField.Kind().String()),
			)
			continue
		}

		target.Set(targetField, protoValue)
		s.logger.Debug("Copied field to target message",
			zap.String("field_name", fieldName),
			zap.String("target_type", targetField.Kind().String()),
		)
	}
}

// Helper functions for protobuf-JSON conversion and injection

// convertProtobufToMap converts a protobuf message to a map for manipulation
func (s *KafkaSinker) convertProtobufToMap(message protoreflect.Message) map[string]interface{} {
	result := make(map[string]interface{})
	msgDesc := message.Descriptor()
	fields := msgDesc.Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if !message.Has(field) {
			continue
		}

		fieldName := string(field.Name())
		value := message.Get(field)
		result[fieldName] = s.convertProtoreflectValueToInterface(value, field)
	}

	return result
}

// convertProtoreflectValueToInterface converts a protoreflect.Value to interface{}
func (s *KafkaSinker) convertProtoreflectValueToInterface(value protoreflect.Value, field protoreflect.FieldDescriptor) interface{} {
	switch field.Kind() {
	case protoreflect.StringKind:
		return value.String()
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return value.Int()
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return value.Uint()
	case protoreflect.BoolKind:
		return value.Bool()
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return value.Float()
	case protoreflect.MessageKind:
		if field.IsList() {
			list := value.List()
			result := make([]interface{}, list.Len())
			for j := 0; j < list.Len(); j++ {
				itemValue := list.Get(j)
				if itemValue.Message().IsValid() {
					result[j] = s.convertProtobufToMap(itemValue.Message())
				}
			}
			return result
		} else {
			if value.Message().IsValid() {
				return s.convertProtobufToMap(value.Message())
			}
		}
	case protoreflect.EnumKind:
		return value.Enum()
	case protoreflect.BytesKind:
		return value.Bytes()
	}
	return nil
}

// createMetadataObjects creates proper protobuf TokenMetadata messages for injection
func (s *KafkaSinker) createMetadataObjects(addresses []string, metadataList []*TokenMetadata, isArraySource bool) interface{} {
	var protoMessages []*tokenmetadata.TokenMetadata

	for _, metadata := range metadataList {
		if s.debugMode {
			s.logger.Debug("Creating protobuf TokenMetadata object",
				zap.String("address", metadata.Address),
				zap.Uint32("decimals", metadata.Decimals),
				zap.String("symbol", metadata.Symbol),
				zap.String("name", metadata.Name),
			)
		}

		// Create proper protobuf TokenMetadata message with correct types
		protoMsg := &tokenmetadata.TokenMetadata{
			Address:  metadata.Address,
			Decimals: metadata.Decimals, // Keep as uint32 (correct type)
			Symbol:   metadata.Symbol,
			Name:     metadata.Name,
			// Exclude ValidFromBlock and ValidToBlock from injection
		}
		protoMessages = append(protoMessages, protoMsg)
	}

	if isArraySource || len(protoMessages) > 1 {
		return protoMessages
	} else if len(protoMessages) == 1 {
		return protoMessages[0]
	}
	return nil
}

// setNestedValueInMap sets a value at a nested path in a map
func (s *KafkaSinker) setNestedValueInMap(data map[string]interface{}, path string, value interface{}) {
	if path == "" {
		return
	}

	parts := strings.Split(path, ".")
	current := data

	// Navigate to the parent of the target field
	for _, part := range parts[:len(parts)-1] {
		if next, exists := current[part]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// Path exists but is not a map, create new map
				newMap := make(map[string]interface{})
				current[part] = newMap
				current = newMap
			}
		} else {
			// Create new nested map
			newMap := make(map[string]interface{})
			current[part] = newMap
			current = newMap
		}
	}

	// Set the final value
	finalPart := parts[len(parts)-1]
	current[finalPart] = value
}

// updateProtobufFromMap updates a protobuf message from a map
func (s *KafkaSinker) updateProtobufFromMap(message protoreflect.Message, data map[string]interface{}) {
	s.logger.Info("DEBUG: updateProtobufFromMap called",
		zap.Int("data_fields", len(data)),
	)

	msgDesc := message.Descriptor()

	// Update each field in the protobuf message from the map data
	for fieldName, value := range data {
		field := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
		if field == nil {
			s.logger.Debug("Field not found in protobuf descriptor", zap.String("field_name", fieldName))
			continue
		}

		// Convert the interface{} value to the appropriate protobuf value
		protoValue, err := s.convertInterfaceToProtoreflectValue(value, field, message)
		if err != nil {
			s.logger.Warn("Failed to convert value for protobuf field",
				zap.String("field_name", fieldName),
				zap.Error(err),
			)
			continue
		}

		// Set the value in the protobuf message
		message.Set(field, protoValue)

		s.logger.Debug("Updated protobuf field",
			zap.String("field_name", fieldName),
			zap.String("field_type", field.Kind().String()),
		)
	}

	s.logger.Info("DEBUG: Protobuf message updated from map",
		zap.Int("updated_fields", len(data)),
	)
}

// convertInterfaceToProtoreflectValue converts an interface{} to a protoreflect.Value
func (s *KafkaSinker) convertInterfaceToProtoreflectValue(value interface{}, field protoreflect.FieldDescriptor, parentMessage protoreflect.Message) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		if str, ok := value.(string); ok {
			return protoreflect.ValueOfString(str), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected string, got %T", value)

	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		switch v := value.(type) {
		case int:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case int32:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case int64:
			return protoreflect.ValueOfInt64(v), nil
		case float64:
			return protoreflect.ValueOfInt64(int64(v)), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected int, got %T", value)

	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		switch v := value.(type) {
		case uint:
			return protoreflect.ValueOfUint64(uint64(v)), nil
		case uint32:
			return protoreflect.ValueOfUint64(uint64(v)), nil
		case uint64:
			return protoreflect.ValueOfUint64(v), nil
		case float64:
			return protoreflect.ValueOfUint64(uint64(v)), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected uint, got %T", value)

	case protoreflect.BoolKind:
		if b, ok := value.(bool); ok {
			return protoreflect.ValueOfBool(b), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", value)

	case protoreflect.FloatKind, protoreflect.DoubleKind:
		switch v := value.(type) {
		case float32:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		case float64:
			return protoreflect.ValueOfFloat64(v), nil
		case int:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected float, got %T", value)

	case protoreflect.MessageKind:
		if field.IsList() {
			// Handle array of protobuf messages
			list := parentMessage.Mutable(field).List()
			// Clear existing list first
			list.Truncate(0)

			if protoMsgs, ok := value.([]*tokenmetadata.TokenMetadata); ok {
				// Handle array of proper protobuf TokenMetadata messages
				for _, protoMsg := range protoMsgs {
					list.Append(protoreflect.ValueOfMessage(protoMsg.ProtoReflect()))
				}
				return protoreflect.ValueOfList(list), nil
			} else if arr, ok := value.([]interface{}); ok {
				// Handle mixed array - check each item
				for _, item := range arr {
					if protoMsg, ok := item.(*tokenmetadata.TokenMetadata); ok {
						list.Append(protoreflect.ValueOfMessage(protoMsg.ProtoReflect()))
					} else if itemMap, ok := item.(map[string]interface{}); ok {
						// Fallback to map-based approach
						newMsg := list.NewElement().Message()
						s.updateProtobufFromMapRecursive(newMsg, itemMap)
						list.Append(protoreflect.ValueOfMessage(newMsg))
					}
				}
				return protoreflect.ValueOfList(list), nil
			}
		} else {
			// Handle single message
			if protoMsg, ok := value.(*tokenmetadata.TokenMetadata); ok {
				// Handle proper protobuf TokenMetadata message
				return protoreflect.ValueOfMessage(protoMsg.ProtoReflect()), nil
			} else if objMap, ok := value.(map[string]interface{}); ok {
				// Fallback to map-based approach
				newMsg := parentMessage.Mutable(field).Message()
				s.updateProtobufFromMapRecursive(newMsg, objMap)
				return protoreflect.ValueOfMessage(newMsg), nil
			}
		}
		return protoreflect.Value{}, fmt.Errorf("expected message/object, got %T", value)

	case protoreflect.BytesKind:
		if b, ok := value.([]byte); ok {
			return protoreflect.ValueOfBytes(b), nil
		}
		if str, ok := value.(string); ok {
			return protoreflect.ValueOfBytes([]byte(str)), nil
		}
		return protoreflect.Value{}, fmt.Errorf("expected bytes, got %T", value)

	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind: %s", field.Kind())
	}
}

// updateProtobufFromMapRecursive recursively updates nested protobuf messages
func (s *KafkaSinker) updateProtobufFromMapRecursive(message protoreflect.Message, data map[string]interface{}) {
	msgDesc := message.Descriptor()

	for fieldName, value := range data {
		field := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
		if field == nil {
			continue
		}

		s.logger.Info("DEBUG: Setting protobuf field",
			zap.String("field_name", fieldName),
			zap.Any("value", value),
			zap.String("field_type", field.Kind().String()),
		)

		protoValue, err := s.convertInterfaceToProtoreflectValue(value, field, message)
		if err != nil {
			s.logger.Debug("Failed to convert nested field value",
				zap.String("field_name", fieldName),
				zap.Error(err),
			)
			continue
		}

		message.Set(field, protoValue)
	}
}
