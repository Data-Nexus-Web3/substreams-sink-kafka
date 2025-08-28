package sinker

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// injectTokenMetadata adds token metadata fields to messages based on configured rules
func (s *KafkaSinker) injectTokenMetadata(ctx context.Context, message protoreflect.Message, blockNumber uint64) error {
	if s.tokenNormalizer == nil || s.fieldProcessingConfig == nil {
		s.logger.Debug("Token metadata injection disabled - normalizer or config missing")
		return nil // Token metadata injection is disabled
	}

	if len(s.tokenMetadataRules) == 0 {
		s.logger.Debug("No token metadata injection rules configured")
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
		tokenAddr := s.fieldProcessingConfig.GetTokenSourceForMessage(message, rule)
		if tokenAddr != "" {
			tokenAddresses[tokenAddr] = true
		}
	}

	// If no token addresses found, nothing to inject
	if len(tokenAddresses) == 0 {
		return nil
	}

	// Convert map to slice for batch lookup
	addresses := make([]string, 0, len(tokenAddresses))
	for addr := range tokenAddresses {
		addresses = append(addresses, addr)
	}

	// Batch fetch token metadata
	tokenMetadata, err := s.tokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, addresses)
	if err != nil {
		s.logger.Debug("Failed to fetch token metadata for injection",
			zap.Strings("addresses", addresses),
			zap.Error(err),
		)
		return nil // Don't fail the entire message processing
	}

	// Inject metadata into each configured field
	for _, rule := range s.tokenMetadataRules {
		tokenAddr := s.fieldProcessingConfig.GetTokenSourceForMessage(message, rule)
		if tokenAddr == "" {
			continue
		}

		metadata, exists := tokenMetadata[tokenAddr]
		if !exists {
			continue
		}

		s.injectTokenMetadataField(message, rule.TargetField, metadata)
	}

	return nil
}

// injectTokenMetadataField injects token metadata into a specific field
func (s *KafkaSinker) injectTokenMetadataField(message protoreflect.Message, fieldName string, metadata *TokenMetadata) {
	msgDesc := message.Descriptor()

	// Try to find the target field
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
