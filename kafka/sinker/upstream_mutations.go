package sinker

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// applyUpstreamMutations applies all field mutations to the protobuf message before serialization
// This ensures ALL output formats (json, protobuf, schema-registry) benefit from the mutations
func (s *KafkaSinker) applyUpstreamMutations(ctx context.Context, protoBytes []byte, blockNumber uint64) ([]byte, error) {
	// If no mutations are configured, return original bytes
	if !s.hasMutationsEnabled() {
		return protoBytes, nil
	}

	startTime := time.Now()
	defer func() {
		if s.mutationStats != nil {
			s.mutationStats.RecordMutation(time.Since(startTime))
		}
	}()

	if s.debugMode {
		s.logger.Debug("Applying upstream mutations",
			zap.Bool("has_normalizer", s.tokenNormalizer != nil),
			zap.Bool("has_config", s.fieldProcessingConfig != nil),
			zap.Int("metadata_rules", len(s.tokenMetadataRules)),
			zap.Bool("omit_empty", s.fieldProcessingConfig != nil && s.fieldProcessingConfig.OmitEmptyStrings),
		)
	}

	// Create dynamic message from protobuf bytes
	dynamicMsg := dynamicpb.NewMessage(s.v2MessageDescriptor)
	if err := proto.Unmarshal(protoBytes, dynamicMsg); err != nil {
		return nil, err
	}

	// Apply all mutations to the protobuf message
	if err := s.applyAllMutations(ctx, dynamicMsg, blockNumber); err != nil {
		return nil, err
	}

	// Marshal the mutated message back to protobuf bytes
	return proto.Marshal(dynamicMsg)
}

// applyAllMutations applies all configured mutations to the protobuf message
func (s *KafkaSinker) applyAllMutations(ctx context.Context, message protoreflect.Message, blockNumber uint64) error {
	if s.debugMode {
		s.logger.Debug("Starting mutations", zap.Uint64("block_number", blockNumber))
	}

	// 1. Token Metadata Injection
	if err := s.injectTokenMetadata(ctx, message, blockNumber); err != nil {
		s.logger.Warn("Token metadata injection failed", zap.Error(err))
	}

	// 2. Token Amount Formatting (unified with legacy system)
	if err := s.formatTokenAmounts(ctx, message, blockNumber); err != nil {
		s.logger.Warn("Token amount formatting failed", zap.Error(err))
	}

	// 3. Empty Field Omission (handled during serialization for JSON, but we can prepare here)
	if err := s.prepareFieldFiltering(message); err != nil {
		s.logger.Warn("Field filtering preparation failed", zap.Error(err))
	}

	return nil
}

// hasMutationsEnabled checks if any mutations are configured
func (s *KafkaSinker) hasMutationsEnabled() bool {
	return s.tokenNormalizer != nil ||
		(s.fieldProcessingConfig != nil && s.fieldProcessingConfig.OmitEmptyStrings) ||
		len(s.tokenMetadataRules) > 0
}

// formatTokenAmounts applies token amount formatting (unified version of legacy normalization)
func (s *KafkaSinker) formatTokenAmounts(ctx context.Context, message protoreflect.Message, blockNumber uint64) error {
	if s.tokenNormalizer == nil || len(s.tokenValueFields) == 0 {
		return nil
	}

	// Extract unique token addresses from the message
	uniqueTokens := s.extractUniqueTokensFromMessage(message)
	if len(uniqueTokens) == 0 {
		return nil
	}

	if s.debugMode {
		s.logger.Debug("Formatting token amounts",
			zap.Strings("unique_tokens", uniqueTokens),
			zap.Strings("value_fields", s.tokenValueFields),
		)
	}

	// Batch lookup token metadata
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, uniqueTokens)
	if err != nil {
		return err
	}

	// Apply formatting to the protobuf message
	s.formatTokenFieldsInMessage(message, tokenMetadata)

	return nil
}

// extractUniqueTokensFromMessage extracts unique token addresses from protobuf message
func (s *KafkaSinker) extractUniqueTokensFromMessage(message protoreflect.Message) []string {
	uniqueTokens := make(map[string]bool)

	// Iterate through all configured token address fields
	for _, fieldName := range s.tokenAddressFields {
		value := s.getFieldValue(message, fieldName)
		if value != "" {
			uniqueTokens[value] = true
		}
	}

	// Convert map to slice
	result := make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		result = append(result, token)
	}

	return result
}

// formatTokenFieldsInMessage applies token formatting to protobuf message fields
func (s *KafkaSinker) formatTokenFieldsInMessage(message protoreflect.Message, tokenMetadata map[string]*TokenMetadata) {
	// Iterate through all configured token value fields
	for _, fieldName := range s.tokenValueFields {
		// Find corresponding token address for this value field
		tokenAddr := s.findTokenAddressForValueField(message, fieldName)
		if tokenAddr == "" {
			continue
		}

		metadata, exists := tokenMetadata[tokenAddr]
		if !exists {
			continue
		}

		// Get the raw amount value
		rawAmount := s.getFieldValue(message, fieldName)
		if rawAmount == "" {
			continue
		}

		// Format the amount
		formattedAmount, err := s.tokenNormalizer.NormalizeTokenAmount(rawAmount, metadata.Decimals)
		if err != nil {
			s.logger.Warn("Failed to normalize token amount",
				zap.String("field", fieldName),
				zap.String("raw_amount", rawAmount),
				zap.Error(err),
			)
			continue
		}

		// Set the formatted field (e.g., "ops_amount" -> "ops_amount_formatted")
		formattedFieldName := fieldName + "_formatted"
		s.setFieldValue(message, formattedFieldName, formattedAmount)

		s.logger.Debug("Token amount formatted in protobuf message",
			zap.String("field", fieldName),
			zap.String("raw_amount", rawAmount),
			zap.String("formatted_amount", formattedAmount),
			zap.String("token_address", tokenAddr),
			zap.String("symbol", metadata.Symbol),
			zap.Uint32("decimals", metadata.Decimals),
		)
	}
}

// findTokenAddressForValueField finds the token address corresponding to a value field
func (s *KafkaSinker) findTokenAddressForValueField(message protoreflect.Message, valueField string) string {
	// For now, use simple heuristic: look for token address fields
	// In the future, this could be configured via annotations
	for _, addrField := range s.tokenAddressFields {
		if addr := s.getFieldValue(message, addrField); addr != "" {
			return addr
		}
	}
	return ""
}

// prepareFieldFiltering prepares the message for field filtering (placeholder for now)
func (s *KafkaSinker) prepareFieldFiltering(message protoreflect.Message) error {
	// For now, this is a placeholder
	// Field filtering will still happen during JSON serialization
	// In the future, we could mark fields for omission here
	return nil
}

// Helper functions for protobuf field access
func (s *KafkaSinker) getFieldValue(message protoreflect.Message, fieldName string) string {
	fields := message.Descriptor().Fields()
	field := fields.ByName(protoreflect.Name(fieldName))
	if field == nil {
		return ""
	}

	value := message.Get(field)
	if !value.IsValid() {
		return ""
	}

	return value.String()
}

func (s *KafkaSinker) setFieldValue(message protoreflect.Message, fieldName, value string) {
	fields := message.Descriptor().Fields()
	field := fields.ByName(protoreflect.Name(fieldName))
	if field == nil {
		// Field doesn't exist in the protobuf schema
		// This is expected for dynamically added fields like "_formatted"
		if s.debugMode {
			s.logger.Debug("Cannot set field - not in protobuf schema",
				zap.String("field", fieldName),
				zap.String("value", value),
			)
		}
		return
	}

	message.Set(field, protoreflect.ValueOfString(value))
}
