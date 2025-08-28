package sinker

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	tokenmetadatav1 "github.com/streamingfast/substreams-sink-kafka/proto/token_metadata/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TokenMetadata is an alias for the generated proto type
type TokenMetadata = tokenmetadatav1.TokenMetadata

// TokenNormalizer handles token metadata lookup and amount normalization
type TokenNormalizer struct {
	grpcConn   *grpc.ClientConn
	client     TokenMetadataServiceClient
	cache      map[string]*TokenMetadata
	cacheMutex sync.RWMutex
	cacheSize  int
	logger     *zap.Logger

	// Performance metrics
	cacheHits   int64
	cacheMisses int64
	grpcCalls   int64
}

// TokenMetadataServiceClient is an alias for the generated GRPC client
type TokenMetadataServiceClient = tokenmetadatav1.TokenMetadataServiceClient

// BatchTokenRequest is an alias for the generated proto type
type BatchTokenRequest = tokenmetadatav1.BatchTokenRequest

// BatchTokenResponse is an alias for the generated proto type
type BatchTokenResponse = tokenmetadatav1.BatchTokenResponse

// NewTokenNormalizer creates a new token normalizer with GRPC connection
func NewTokenNormalizer(endpoint string, logger *zap.Logger) (*TokenNormalizer, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("GRPC endpoint cannot be empty")
	}

	// Create GRPC connection with performance optimizations
	conn, err := grpc.Dial(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16*1024*1024), // 16MB max message size
			grpc.MaxCallSendMsgSize(16*1024*1024),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to token metadata service at %s: %w", endpoint, err)
	}

	// Create the GRPC client
	client := tokenmetadatav1.NewTokenMetadataServiceClient(conn)

	normalizer := &TokenNormalizer{
		grpcConn:  conn,
		client:    client,
		cache:     make(map[string]*TokenMetadata),
		cacheSize: 10000, // Cache up to 10k tokens
		logger:    logger.With(zap.String("component", "token_normalizer")),
	}

	logger.Info("Token normalizer initialized",
		zap.String("grpc_endpoint", endpoint),
		zap.Int("cache_size", normalizer.cacheSize),
	)

	return normalizer, nil
}

// Close closes the GRPC connection
func (tn *TokenNormalizer) Close() error {
	if tn.grpcConn != nil {
		return tn.grpcConn.Close()
	}
	return nil
}

// BatchGetTokenMetadata fetches token metadata for multiple tokens at a specific block
func (tn *TokenNormalizer) BatchGetTokenMetadata(ctx context.Context, blockNumber uint64, tokenAddresses []string) (map[string]*TokenMetadata, error) {
	if len(tokenAddresses) == 0 {
		return make(map[string]*TokenMetadata), nil
	}

	// Check cache first
	result := make(map[string]*TokenMetadata)
	uncachedTokens := make([]string, 0, len(tokenAddresses))

	tn.cacheMutex.RLock()
	for _, addr := range tokenAddresses {
		if metadata, exists := tn.cache[addr]; exists {
			result[addr] = metadata
			tn.cacheHits++
		} else {
			uncachedTokens = append(uncachedTokens, addr)
			tn.cacheMisses++
		}
	}
	tn.cacheMutex.RUnlock()

	// If all tokens were cached, return immediately
	if len(uncachedTokens) == 0 {
		tn.logger.Debug("All tokens found in cache",
			zap.Int("cached_tokens", len(tokenAddresses)),
			zap.Int64("cache_hits", tn.cacheHits),
		)
		return result, nil
	}

	// Fetch uncached tokens via GRPC
	tn.grpcCalls++
	startTime := time.Now()

	// Make the actual GRPC call
	req := &tokenmetadatav1.BatchTokenRequest{
		BlockNumber:    blockNumber,
		TokenAddresses: uncachedTokens,
	}
	resp, err := tn.client.BatchGetTokenMetadata(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GRPC call failed: %w", err)
	}

	grpcDuration := time.Since(startTime)

	// Update cache with new tokens
	tn.cacheMutex.Lock()
	for addr, metadata := range resp.Tokens {
		// Implement LRU eviction if cache is full
		if len(tn.cache) >= tn.cacheSize {
			// Simple eviction: remove first item (not true LRU, but good enough for now)
			for k := range tn.cache {
				delete(tn.cache, k)
				break
			}
		}
		tn.cache[addr] = metadata
		result[addr] = metadata
	}
	tn.cacheMutex.Unlock()

	tn.logger.Debug("GRPC token metadata lookup completed",
		zap.Int("requested_tokens", len(uncachedTokens)),
		zap.Int("found_tokens", len(resp.Tokens)),
		zap.Int("not_found_tokens", len(resp.NotFound)),
		zap.Duration("grpc_duration", grpcDuration),
		zap.Int64("total_grpc_calls", tn.grpcCalls),
		zap.Int64("cache_hits", tn.cacheHits),
		zap.Int64("cache_misses", tn.cacheMisses),
	)

	return result, nil
}

// NormalizeTokenAmount converts a raw token amount to a formatted decimal string
func (tn *TokenNormalizer) NormalizeTokenAmount(rawAmount string, decimals uint32) (string, error) {
	// Parse the raw amount as a big integer
	amount := new(big.Int)
	amount, ok := amount.SetString(rawAmount, 10)
	if !ok {
		return "", fmt.Errorf("invalid raw amount: %s", rawAmount)
	}

	// Create divisor (10^decimals)
	divisor := new(big.Int)
	divisor.Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)

	// Handle negative numbers properly
	isNegative := amount.Sign() < 0
	if isNegative {
		amount.Abs(amount) // Work with absolute value
	}

	// Perform division with remainder on absolute value
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.DivMod(amount, divisor, remainder)

	// Format as decimal string
	if remainder.Sign() == 0 {
		// No fractional part
		result := quotient.String()
		if isNegative && quotient.Sign() > 0 {
			result = "-" + result
		}
		return result, nil
	}

	// Build decimal string with proper padding
	remainderStr := remainder.String()

	// Pad with leading zeros if necessary
	paddingNeeded := int(decimals) - len(remainderStr)
	if paddingNeeded > 0 {
		remainderStr = strings.Repeat("0", paddingNeeded) + remainderStr
	}

	// Remove trailing zeros
	remainderStr = strings.TrimRight(remainderStr, "0")

	if remainderStr == "" {
		result := quotient.String()
		if isNegative && quotient.Sign() > 0 {
			result = "-" + result
		}
		return result, nil
	}

	// Construct final result
	result := quotient.String() + "." + remainderStr
	if isNegative {
		result = "-" + result
	}

	return result, nil
}

// ExtractUniqueTokens extracts unique token addresses from a protobuf message
func (tn *TokenNormalizer) ExtractUniqueTokens(message protoreflect.Message, tokenAddressFields []string) []string {
	uniqueTokens := make(map[string]bool)

	// Iterate through all specified token address fields
	for _, fieldName := range tokenAddressFields {
		tn.extractTokensFromField(message, fieldName, uniqueTokens)
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

// extractTokensFromField recursively extracts token addresses from a specific field
func (tn *TokenNormalizer) extractTokensFromField(message protoreflect.Message, fieldName string, uniqueTokens map[string]bool) {
	msgDesc := message.Descriptor()

	// Try to find the field by name
	field := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
	if field == nil {
		// Field not found, try nested fields (e.g., "transaction.token_address")
		if strings.Contains(fieldName, ".") {
			parts := strings.SplitN(fieldName, ".", 2)
			nestedField := msgDesc.Fields().ByName(protoreflect.Name(parts[0]))
			if nestedField != nil && nestedField.Kind() == protoreflect.MessageKind {
				if message.Has(nestedField) {
					nestedMsg := message.Get(nestedField).Message()
					tn.extractTokensFromField(nestedMsg, parts[1], uniqueTokens)
				}
			}
		}
		return
	}

	if !message.Has(field) {
		return
	}

	value := message.Get(field)

	switch field.Kind() {
	case protoreflect.StringKind:
		// Single token address
		addr := value.String()
		if addr != "" {
			uniqueTokens[addr] = true
		}

	case protoreflect.MessageKind:
		// Nested message - recurse
		if field.IsList() {
			// List of messages
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				nestedMsg := list.Get(i).Message()
				tn.extractTokensFromField(nestedMsg, fieldName, uniqueTokens)
			}
		} else {
			// Single nested message
			nestedMsg := value.Message()
			tn.extractTokensFromField(nestedMsg, fieldName, uniqueTokens)
		}
	}
}

// GetCacheStats returns cache performance statistics
func (tn *TokenNormalizer) GetCacheStats() (hits, misses, grpcCalls int64, cacheSize int) {
	tn.cacheMutex.RLock()
	defer tn.cacheMutex.RUnlock()
	return tn.cacheHits, tn.cacheMisses, tn.grpcCalls, len(tn.cache)
}

// getNestedValueFromJSON retrieves a value from nested JSON using dot notation path
func (tn *TokenNormalizer) getNestedValueFromJSON(data interface{}, path string) interface{} {
	if path == "" {
		return nil
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

// extractTokensFromNestedJSON extracts tokens from nested JSON paths, handling arrays
func (tn *TokenNormalizer) extractTokensFromNestedJSON(data interface{}, tokenPath string, uniqueTokens map[string]bool) {
	if tokenPath == "" {
		return
	}

	// Handle nested paths with arrays
	parts := strings.Split(tokenPath, ".")
	tn.extractTokensFromNestedPath(data, parts, 0, uniqueTokens)
}

// extractTokensFromNestedPath recursively extracts tokens following a nested path
func (tn *TokenNormalizer) extractTokensFromNestedPath(data interface{}, pathParts []string, partIndex int, uniqueTokens map[string]bool) {
	if partIndex >= len(pathParts) {
		// We've reached the end of the path, extract the token value
		if tokenAddr, ok := data.(string); ok && tokenAddr != "" && tokenAddr != "0x0000000000000000000000000000000000000000" {
			uniqueTokens[tokenAddr] = true
		}
		return
	}

	currentPart := pathParts[partIndex]

	switch v := data.(type) {
	case map[string]interface{}:
		if value, exists := v[currentPart]; exists {
			tn.extractTokensFromNestedPath(value, pathParts, partIndex+1, uniqueTokens)
		}

	case []interface{}:
		// If we encounter an array, iterate through all elements
		for _, item := range v {
			tn.extractTokensFromNestedPath(item, pathParts, partIndex, uniqueTokens)
		}
	}
}

// normalizeAmountInNestedJSON normalizes amounts in nested JSON structures
func (tn *TokenNormalizer) normalizeAmountInNestedJSON(data interface{}, amountPath, tokenPath string, tokenMetadata map[string]*TokenMetadata, logger *zap.Logger, debugMode bool) {
	if amountPath == "" || tokenPath == "" {
		return
	}

	// Handle nested paths with arrays
	amountParts := strings.Split(amountPath, ".")
	tokenParts := strings.Split(tokenPath, ".")
	tn.normalizeAmountInNestedPath(data, amountParts, tokenParts, 0, tokenMetadata, logger, debugMode)
}

// normalizeAmountInNestedPath recursively normalizes amounts following nested paths
func (tn *TokenNormalizer) normalizeAmountInNestedPath(data interface{}, amountParts, tokenParts []string, partIndex int, tokenMetadata map[string]*TokenMetadata, logger *zap.Logger, debugMode bool) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check if we're at the right level to normalize
		if partIndex == len(amountParts)-1 {
			// We're at the parent level, check if both amount and token fields exist
			amountField := amountParts[partIndex]
			tokenField := tokenParts[partIndex]

			if rawAmount, amountExists := v[amountField]; amountExists {
				if tokenAddr, tokenExists := v[tokenField]; tokenExists {
					if rawStr, ok := rawAmount.(string); ok {
						if tokenAddrStr, ok := tokenAddr.(string); ok {
							if metadata, exists := tokenMetadata[tokenAddrStr]; exists {
								formattedAmount, err := tn.NormalizeTokenAmount(rawStr, metadata.Decimals)
								if err == nil {
									v[amountField+"_formatted"] = formattedAmount
									if debugMode {
										logger.Debug("Token amount normalized in nested JSON",
											zap.String("amount_path", strings.Join(amountParts, ".")),
											zap.String("token_path", strings.Join(tokenParts, ".")),
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
			return
		}

		// Continue traversing the path
		if partIndex < len(amountParts) && partIndex < len(tokenParts) {
			currentAmountPart := amountParts[partIndex]
			currentTokenPart := tokenParts[partIndex]

			// Both paths should follow the same structure until the final field
			if currentAmountPart == currentTokenPart {
				if value, exists := v[currentAmountPart]; exists {
					tn.normalizeAmountInNestedPath(value, amountParts, tokenParts, partIndex+1, tokenMetadata, logger, debugMode)
				}
			}
		}

	case []interface{}:
		// If we encounter an array, iterate through all elements
		for _, item := range v {
			tn.normalizeAmountInNestedPath(item, amountParts, tokenParts, partIndex, tokenMetadata, logger, debugMode)
		}
	}
}
