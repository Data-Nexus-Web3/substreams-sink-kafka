package sinker

import (
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// FieldAnnotation represents processing instructions for a field
type FieldAnnotation struct {
	// Token metadata injection
	InjectTokenMetadata bool     `json:"inject_token_metadata,omitempty"`
	TokenSourceFields   []string `json:"token_source_fields,omitempty"` // Priority order: ops_token, act_address, etc.

	// Amount formatting
	FormatAmount      bool   `json:"format_amount,omitempty"`
	TokenAddressField string `json:"token_address_field,omitempty"`

	// Field processing
	OmitIfEmpty bool `json:"omit_if_empty,omitempty"` // Don't include empty strings in output
}

// FieldProcessingConfig holds all field processing configuration
type FieldProcessingConfig struct {
	// Global token source priority (fallback if field-specific not defined)
	DefaultTokenSourceFields []string `json:"default_token_source_fields,omitempty"`

	// Field-specific annotations
	FieldAnnotations map[string]*FieldAnnotation `json:"field_annotations,omitempty"`

	// Global settings
	OmitEmptyStrings bool `json:"omit_empty_strings,omitempty"`
}

// TokenMetadataInjectionRule defines how to inject token metadata for a message
type TokenMetadataInjectionRule struct {
	TargetField       string   // Field name to inject metadata into (e.g., "token_metadata")
	TokenSourceFields []string // Priority-ordered fields to check for token address
}

// GetTokenSourceForMessage determines which token address to use for metadata injection
func (config *FieldProcessingConfig) GetTokenSourceForMessage(message protoreflect.Message, rule *TokenMetadataInjectionRule) string {
	msgDesc := message.Descriptor()

	// Use rule-specific fields first, then fall back to global defaults
	sourceFields := rule.TokenSourceFields
	if len(sourceFields) == 0 {
		sourceFields = config.DefaultTokenSourceFields
	}

	// Check each source field in priority order
	for _, fieldName := range sourceFields {
		field := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
		if field != nil && field.Kind() == protoreflect.StringKind && message.Has(field) {
			tokenAddr := message.Get(field).String()
			// Skip empty strings and zero addresses
			if tokenAddr != "" && tokenAddr != "0x0000000000000000000000000000000000000000" {
				return tokenAddr
			}
		}
	}

	return ""
}

// ShouldOmitField determines if a field should be omitted from output
func (config *FieldProcessingConfig) ShouldOmitField(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
	fieldName := string(field.Name())

	// Check field-specific annotation
	if annotation, exists := config.FieldAnnotations[fieldName]; exists && annotation.OmitIfEmpty {
		return isEmptyValue(field, value)
	}

	// Check global setting
	if config.OmitEmptyStrings && field.Kind() == protoreflect.StringKind {
		return value.String() == ""
	}

	return false
}

// isEmptyValue checks if a protobuf value is considered empty
func isEmptyValue(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
	switch field.Kind() {
	case protoreflect.StringKind:
		return value.String() == ""
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return value.Int() == 0
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return value.Uint() == 0
	case protoreflect.BoolKind:
		return !value.Bool()
	case protoreflect.MessageKind:
		// For messages, check if it's the zero value
		return !value.Message().IsValid()
	default:
		return false
	}
}

// ParseFieldAnnotationsFromComment parses field processing annotations from protobuf comments
// Example comment: @token_metadata(source_fields=["ops_token","act_address"]) @format_amount @omit_if_empty
func ParseFieldAnnotationsFromComment(comment string) *FieldAnnotation {
	annotation := &FieldAnnotation{}

	if strings.Contains(comment, "@token_metadata") {
		annotation.InjectTokenMetadata = true
		// Parse source_fields parameter
		if start := strings.Index(comment, "source_fields=["); start != -1 {
			start += len("source_fields=[")
			if end := strings.Index(comment[start:], "]"); end != -1 {
				fieldsStr := comment[start : start+end]
				// Simple parsing - split by comma and clean quotes
				fields := strings.Split(fieldsStr, ",")
				for i, field := range fields {
					fields[i] = strings.Trim(strings.TrimSpace(field), "\"'")
				}
				annotation.TokenSourceFields = fields
			}
		}
	}

	if strings.Contains(comment, "@format_amount") {
		annotation.FormatAmount = true
	}

	if strings.Contains(comment, "@omit_if_empty") {
		annotation.OmitIfEmpty = true
	}

	return annotation
}

// DefaultFieldProcessingConfig returns a sensible default configuration
func DefaultFieldProcessingConfig() *FieldProcessingConfig {
	return &FieldProcessingConfig{
		DefaultTokenSourceFields: []string{"ops_token", "act_address", "token_address", "token"},
		FieldAnnotations:         make(map[string]*FieldAnnotation),
		OmitEmptyStrings:         true,
	}
}

// AddTokenMetadataInjectionRule adds a rule for injecting token metadata
func (config *FieldProcessingConfig) AddTokenMetadataInjectionRule(targetField string, sourceFields []string) {
	if config.FieldAnnotations == nil {
		config.FieldAnnotations = make(map[string]*FieldAnnotation)
	}

	config.FieldAnnotations[targetField] = &FieldAnnotation{
		InjectTokenMetadata: true,
		TokenSourceFields:   sourceFields,
	}
}
