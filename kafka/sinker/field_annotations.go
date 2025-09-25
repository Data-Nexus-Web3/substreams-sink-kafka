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

// GetTokenSourceForMessage determines which token address to use for metadata injection (legacy single token)
func (config *FieldProcessingConfig) GetTokenSourceForMessage(message protoreflect.Message, rule *TokenMetadataInjectionRule) string {
	addresses := config.GetTokenSourcesForMessage(message, rule)
	if len(addresses) > 0 {
		return addresses[0] // Return first address for backward compatibility
	}
	return ""
}

// GetTokenSourcesForMessage determines ALL token addresses for metadata injection (supports arrays)
func (config *FieldProcessingConfig) GetTokenSourcesForMessage(message protoreflect.Message, rule *TokenMetadataInjectionRule) []string {
	// Use rule-specific fields first, then fall back to global defaults
	sourceFields := rule.TokenSourceFields
	if len(sourceFields) == 0 {
		sourceFields = config.DefaultTokenSourceFields
	}

	// Check each source field in priority order
	for _, fieldName := range sourceFields {
		// Check if this is a nested path (contains dots)
		if strings.Contains(fieldName, ".") {
			// For nested paths, convert protobuf message to JSON and use nested extraction
			addresses := config.getTokensFromNestedPath(message, fieldName)
			if len(addresses) > 0 {
				return addresses // Return all addresses found
			}
		} else {
			// Handle flat field access (original logic)
			msgDesc := message.Descriptor()
			field := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
			if field != nil && field.Kind() == protoreflect.StringKind && message.Has(field) {
				tokenAddr := message.Get(field).String()
				// Skip empty strings and zero addresses
				if tokenAddr != "" && tokenAddr != "0x0000000000000000000000000000000000000000" {
					return []string{tokenAddr}
				}
			}
		}
	}

	return []string{}
}

// getTokensFromNestedPath extracts ALL token addresses from nested path in protobuf message
func (config *FieldProcessingConfig) getTokensFromNestedPath(message protoreflect.Message, fieldPath string) []string {
	// Convert protobuf message to JSON-like map for nested path processing
	jsonData := config.convertProtoreflectToMap(message)

	// Parse the nested field path
	pathInfo := ParseNestedFieldPath(fieldPath)

	// Extract token addresses using our nested path logic
	addresses := ExtractTokenAddressesFromNestedPath(jsonData, pathInfo)

	// Return all valid addresses
	return addresses
}

// convertProtoreflectToMap converts a protoreflect.Message to a map[string]interface{}
func (config *FieldProcessingConfig) convertProtoreflectToMap(message protoreflect.Message) map[string]interface{} {
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

		result[fieldName] = config.convertProtoreflectValue(value, field)
	}

	return result
}

// convertProtoreflectValue converts a protoreflect.Value to a Go interface{}
func (config *FieldProcessingConfig) convertProtoreflectValue(value protoreflect.Value, field protoreflect.FieldDescriptor) interface{} {
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
			// Handle list of messages
			list := value.List()
			result := make([]interface{}, list.Len())
			for j := 0; j < list.Len(); j++ {
				itemValue := list.Get(j)
				if itemValue.Message().IsValid() {
					result[j] = config.convertProtoreflectToMap(itemValue.Message())
				}
			}
			return result
		} else {
			// Handle single message
			if value.Message().IsValid() {
				return config.convertProtoreflectToMap(value.Message())
			}
		}
	case protoreflect.EnumKind:
		return value.Enum()
	case protoreflect.BytesKind:
		return value.Bytes()
	}

	return nil
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

// NestedPathInfo contains information about a parsed nested field path
type NestedPathInfo struct {
	ContainerPath string // Path to the container (e.g., "metadata.tokens")
	FieldName     string // Field name within container (e.g., "address")
	IsNested      bool   // Whether this is a nested path
}

// ParseNestedFieldPath parses a field path like "metadata.tokens.address" into components
func ParseNestedFieldPath(fieldPath string) NestedPathInfo {
	if !strings.Contains(fieldPath, ".") {
		return NestedPathInfo{
			ContainerPath: fieldPath,
			FieldName:     "",
			IsNested:      false,
		}
	}

	parts := strings.Split(fieldPath, ".")
	if len(parts) < 2 {
		return NestedPathInfo{
			ContainerPath: fieldPath,
			FieldName:     "",
			IsNested:      false,
		}
	}

	// Last part is the field name, everything else is the container path
	fieldName := parts[len(parts)-1]
	containerPath := strings.Join(parts[:len(parts)-1], ".")

	return NestedPathInfo{
		ContainerPath: containerPath,
		FieldName:     fieldName,
		IsNested:      true,
	}
}

// ExtractTokenAddressesFromNestedPath extracts token addresses from a nested path in JSON data
func ExtractTokenAddressesFromNestedPath(data interface{}, pathInfo NestedPathInfo) []string {
	if !pathInfo.IsNested {
		// Handle flat field
		if dataMap, ok := data.(map[string]interface{}); ok {
			if addr, exists := dataMap[pathInfo.ContainerPath]; exists {
				if addrStr, ok := addr.(string); ok && addrStr != "" && addrStr != "0x0000000000000000000000000000000000000000" {
					return []string{addrStr}
				}
			}
		}
		return nil
	}

	// Navigate to the container using the container path
	container := GetNestedValueFromPath(data, pathInfo.ContainerPath)
	if container == nil {
		return nil
	}

	var addresses []string

	// Handle both single objects and arrays
	switch v := container.(type) {
	case map[string]interface{}:
		// Single object - extract the field
		if addr, exists := v[pathInfo.FieldName]; exists {
			if addrStr, ok := addr.(string); ok && addrStr != "" && addrStr != "0x0000000000000000000000000000000000000000" {
				addresses = append(addresses, addrStr)
			}
		}

	case []interface{}:
		// Array - extract field from each element
		for _, item := range v {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if addr, exists := itemMap[pathInfo.FieldName]; exists {
					if addrStr, ok := addr.(string); ok && addrStr != "" && addrStr != "0x0000000000000000000000000000000000000000" {
						addresses = append(addresses, addrStr)
					}
				}
			}
		}
	}

	return addresses
}

// GetNestedValueFromPath navigates to a nested value using dot notation
func GetNestedValueFromPath(data interface{}, path string) interface{} {
	if path == "" {
		return data
	}

	parts := strings.Split(path, ".")
	current := data

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			if value, exists := v[part]; exists {
				current = value
			} else {
				return nil
			}
		default:
			return nil
		}
	}

	return current
}
