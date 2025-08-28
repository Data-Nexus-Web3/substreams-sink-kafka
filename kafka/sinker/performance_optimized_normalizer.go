package sinker

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PerformanceMetrics tracks detailed performance metrics for token normalization
type PerformanceMetrics struct {
	TokenExtractionTime time.Duration
	MetadataLookupTime  time.Duration
	NormalizationTime   time.Duration
	TotalProcessingTime time.Duration
	MessagesProcessed   int64
	TokensExtracted     int64
	AmountsNormalized   int64
	CacheHitRate        float64

	// Memory metrics
	AllocsBefore uint64
	AllocsAfter  uint64
	HeapBefore   uint64
	HeapAfter    uint64
}

// OptimizedFieldPath represents a pre-parsed field path for better performance
type OptimizedFieldPath struct {
	OriginalPath string
	Parts        []string
	IsNested     bool
	Depth        int
}

// OptimizedTokenNormalizer provides high-performance token normalization
type OptimizedTokenNormalizer struct {
	*TokenNormalizer // Embed the original normalizer

	// Pre-parsed field paths to avoid repeated string operations
	valuePaths   []OptimizedFieldPath
	addressPaths []OptimizedFieldPath

	// Performance tracking
	metrics      PerformanceMetrics
	metricsMutex sync.RWMutex

	// Optimization flags
	enableProfiling bool
	logger          *zap.Logger
}

// NewOptimizedTokenNormalizer creates a new optimized token normalizer
func NewOptimizedTokenNormalizer(base *TokenNormalizer, valueFields, addressFields []string, enableProfiling bool, logger *zap.Logger) *OptimizedTokenNormalizer {
	opt := &OptimizedTokenNormalizer{
		TokenNormalizer: base,
		enableProfiling: enableProfiling,
		logger:          logger,
	}

	// Pre-parse field paths
	opt.valuePaths = make([]OptimizedFieldPath, len(valueFields))
	opt.addressPaths = make([]OptimizedFieldPath, len(addressFields))

	for i, path := range valueFields {
		opt.valuePaths[i] = opt.parseFieldPath(path)
	}

	for i, path := range addressFields {
		opt.addressPaths[i] = opt.parseFieldPath(path)
	}

	return opt
}

// parseFieldPath pre-parses a field path for optimal performance
func (opt *OptimizedTokenNormalizer) parseFieldPath(path string) OptimizedFieldPath {
	parts := strings.Split(path, ".")
	return OptimizedFieldPath{
		OriginalPath: path,
		Parts:        parts,
		IsNested:     len(parts) > 1,
		Depth:        len(parts),
	}
}

// OptimizedNormalizeJSON performs high-performance token normalization on JSON data
func (opt *OptimizedTokenNormalizer) OptimizedNormalizeJSON(data interface{}, blockNumber uint64) error {
	if opt.enableProfiling {
		return opt.normalizeWithProfiling(data, blockNumber)
	}
	return opt.normalizeWithoutProfiling(data, blockNumber)
}

// normalizeWithProfiling includes detailed performance tracking
func (opt *OptimizedTokenNormalizer) normalizeWithProfiling(data interface{}, blockNumber uint64) error {
	startTime := time.Now()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	allocsBefore := memStats.Mallocs
	heapBefore := memStats.HeapAlloc

	// Step 1: Extract tokens
	extractStart := time.Now()
	uniqueTokens := opt.extractTokensOptimized(data)
	extractTime := time.Since(extractStart)

	if len(uniqueTokens) == 0 {
		return nil
	}

	// Step 2: Batch lookup metadata
	lookupStart := time.Now()
	ctx := context.Background()
	tokenMetadata, err := opt.TokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, uniqueTokens)
	if err != nil {
		return err
	}
	lookupTime := time.Since(lookupStart)

	// Step 3: Apply normalization
	normalizeStart := time.Now()
	amountsNormalized := opt.normalizeTokensOptimized(data, tokenMetadata)
	normalizeTime := time.Since(normalizeStart)

	// Update metrics
	runtime.ReadMemStats(&memStats)
	totalTime := time.Since(startTime)

	opt.metricsMutex.Lock()
	opt.metrics.TokenExtractionTime += extractTime
	opt.metrics.MetadataLookupTime += lookupTime
	opt.metrics.NormalizationTime += normalizeTime
	opt.metrics.TotalProcessingTime += totalTime
	opt.metrics.MessagesProcessed++
	opt.metrics.TokensExtracted += int64(len(uniqueTokens))
	opt.metrics.AmountsNormalized += int64(amountsNormalized)
	opt.metrics.AllocsBefore = allocsBefore
	opt.metrics.AllocsAfter = memStats.Mallocs
	opt.metrics.HeapBefore = heapBefore
	opt.metrics.HeapAfter = memStats.HeapAlloc

	// Calculate cache hit rate
	hits, misses, _, _ := opt.TokenNormalizer.GetCacheStats()
	if hits+misses > 0 {
		opt.metrics.CacheHitRate = float64(hits) / float64(hits+misses) * 100
	}
	opt.metricsMutex.Unlock()

	// Log performance metrics periodically
	if opt.metrics.MessagesProcessed%100 == 0 {
		opt.logPerformanceMetrics()
	}

	return nil
}

// normalizeWithoutProfiling optimized path without performance tracking overhead
func (opt *OptimizedTokenNormalizer) normalizeWithoutProfiling(data interface{}, blockNumber uint64) error {
	uniqueTokens := opt.extractTokensOptimized(data)
	if len(uniqueTokens) == 0 {
		return nil
	}

	ctx := context.Background()
	tokenMetadata, err := opt.TokenNormalizer.BatchGetTokenMetadata(ctx, blockNumber, uniqueTokens)
	if err != nil {
		return err
	}

	opt.normalizeTokensOptimized(data, tokenMetadata)
	return nil
}

// extractTokensOptimized uses pre-parsed paths for faster token extraction
func (opt *OptimizedTokenNormalizer) extractTokensOptimized(data interface{}) []string {
	uniqueTokens := make(map[string]bool)

	// Use pre-parsed paths to avoid repeated string operations
	for _, addressPath := range opt.addressPaths {
		opt.extractTokensFromPath(data, addressPath, uniqueTokens)
	}

	// Convert to slice
	result := make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		if token != "" && token != "0x0000000000000000000000000000000000000000" {
			result = append(result, token)
		}
	}

	return result
}

// extractTokensFromPath extracts tokens using pre-parsed path
func (opt *OptimizedTokenNormalizer) extractTokensFromPath(data interface{}, path OptimizedFieldPath, uniqueTokens map[string]bool) {
	if !path.IsNested {
		// Fast path for flat fields
		opt.extractTokensFlat(data, path.Parts[0], uniqueTokens)
		return
	}

	// Nested path extraction
	opt.extractTokensNested(data, path.Parts, 0, uniqueTokens)
}

// extractTokensFlat handles flat field extraction efficiently
func (opt *OptimizedTokenNormalizer) extractTokensFlat(data interface{}, fieldName string, uniqueTokens map[string]bool) {
	switch v := data.(type) {
	case map[string]interface{}:
		if tokenAddr, exists := v[fieldName]; exists {
			if addr, ok := tokenAddr.(string); ok && addr != "" {
				uniqueTokens[addr] = true
			}
		}
		// Recurse into nested objects
		for _, value := range v {
			opt.extractTokensFlat(value, fieldName, uniqueTokens)
		}
	case []interface{}:
		for _, item := range v {
			opt.extractTokensFlat(item, fieldName, uniqueTokens)
		}
	}
}

// extractTokensNested handles nested field extraction efficiently
func (opt *OptimizedTokenNormalizer) extractTokensNested(data interface{}, pathParts []string, partIndex int, uniqueTokens map[string]bool) {
	if partIndex >= len(pathParts) {
		if tokenAddr, ok := data.(string); ok && tokenAddr != "" && tokenAddr != "0x0000000000000000000000000000000000000000" {
			uniqueTokens[tokenAddr] = true
		}
		return
	}

	currentPart := pathParts[partIndex]

	switch v := data.(type) {
	case map[string]interface{}:
		if value, exists := v[currentPart]; exists {
			opt.extractTokensNested(value, pathParts, partIndex+1, uniqueTokens)
		}
	case []interface{}:
		for _, item := range v {
			opt.extractTokensNested(item, pathParts, partIndex, uniqueTokens)
		}
	}
}

// normalizeTokensOptimized performs optimized token normalization
func (opt *OptimizedTokenNormalizer) normalizeTokensOptimized(data interface{}, tokenMetadata map[string]*TokenMetadata) int {
	amountsNormalized := 0

	// Process paired fields efficiently
	minLen := len(opt.valuePaths)
	if len(opt.addressPaths) < minLen {
		minLen = len(opt.addressPaths)
	}

	for i := 0; i < minLen; i++ {
		valuePath := opt.valuePaths[i]
		addressPath := opt.addressPaths[i]

		count := opt.normalizeFieldPair(data, valuePath, addressPath, tokenMetadata)
		amountsNormalized += count
	}

	return amountsNormalized
}

// normalizeFieldPair normalizes a specific field pair efficiently
func (opt *OptimizedTokenNormalizer) normalizeFieldPair(data interface{}, valuePath, addressPath OptimizedFieldPath, tokenMetadata map[string]*TokenMetadata) int {
	if valuePath.IsNested || addressPath.IsNested {
		return opt.normalizeNestedPair(data, valuePath, addressPath, tokenMetadata, 0)
	}
	return opt.normalizeFlatPair(data, valuePath.Parts[0], addressPath.Parts[0], tokenMetadata)
}

// normalizeFlatPair handles flat field pair normalization
func (opt *OptimizedTokenNormalizer) normalizeFlatPair(data interface{}, valueField, addressField string, tokenMetadata map[string]*TokenMetadata) int {
	count := 0

	switch v := data.(type) {
	case map[string]interface{}:
		if rawValue, valueExists := v[valueField]; valueExists {
			if tokenAddr, addrExists := v[addressField]; addrExists {
				if rawStr, ok := rawValue.(string); ok {
					if tokenAddrStr, ok := tokenAddr.(string); ok {
						if metadata, exists := tokenMetadata[tokenAddrStr]; exists {
							if formattedAmount, err := opt.TokenNormalizer.NormalizeTokenAmount(rawStr, metadata.Decimals); err == nil {
								v[valueField+"_formatted"] = formattedAmount
								count++
							}
						}
					}
				}
			}
		}

		// Recurse into nested objects
		for _, value := range v {
			count += opt.normalizeFlatPair(value, valueField, addressField, tokenMetadata)
		}

	case []interface{}:
		for _, item := range v {
			count += opt.normalizeFlatPair(item, valueField, addressField, tokenMetadata)
		}
	}

	return count
}

// normalizeNestedPair handles nested field pair normalization
func (opt *OptimizedTokenNormalizer) normalizeNestedPair(data interface{}, valuePath, addressPath OptimizedFieldPath, tokenMetadata map[string]*TokenMetadata, partIndex int) int {
	count := 0

	switch v := data.(type) {
	case map[string]interface{}:
		if partIndex == valuePath.Depth-1 {
			// At the final level
			valueField := valuePath.Parts[partIndex]
			addressField := addressPath.Parts[partIndex]

			if rawValue, valueExists := v[valueField]; valueExists {
				if tokenAddr, addrExists := v[addressField]; addrExists {
					if rawStr, ok := rawValue.(string); ok {
						if tokenAddrStr, ok := tokenAddr.(string); ok {
							if metadata, exists := tokenMetadata[tokenAddrStr]; exists {
								if formattedAmount, err := opt.TokenNormalizer.NormalizeTokenAmount(rawStr, metadata.Decimals); err == nil {
									v[valueField+"_formatted"] = formattedAmount
									count++
								}
							}
						}
					}
				}
			}
		} else {
			// Continue traversing
			if partIndex < valuePath.Depth && partIndex < addressPath.Depth {
				currentValuePart := valuePath.Parts[partIndex]
				currentAddressPart := addressPath.Parts[partIndex]

				if currentValuePart == currentAddressPart {
					if value, exists := v[currentValuePart]; exists {
						count += opt.normalizeNestedPair(value, valuePath, addressPath, tokenMetadata, partIndex+1)
					}
				}
			}
		}

	case []interface{}:
		for _, item := range v {
			count += opt.normalizeNestedPair(item, valuePath, addressPath, tokenMetadata, partIndex)
		}
	}

	return count
}

// GetPerformanceMetrics returns current performance metrics
func (opt *OptimizedTokenNormalizer) GetPerformanceMetrics() PerformanceMetrics {
	opt.metricsMutex.RLock()
	defer opt.metricsMutex.RUnlock()
	return opt.metrics
}

// ResetPerformanceMetrics resets all performance counters
func (opt *OptimizedTokenNormalizer) ResetPerformanceMetrics() {
	opt.metricsMutex.Lock()
	defer opt.metricsMutex.Unlock()
	opt.metrics = PerformanceMetrics{}
}

// logPerformanceMetrics logs detailed performance information
func (opt *OptimizedTokenNormalizer) logPerformanceMetrics() {
	metrics := opt.GetPerformanceMetrics()

	if metrics.MessagesProcessed == 0 {
		return
	}

	avgTotal := metrics.TotalProcessingTime / time.Duration(metrics.MessagesProcessed)
	avgExtraction := metrics.TokenExtractionTime / time.Duration(metrics.MessagesProcessed)
	avgLookup := metrics.MetadataLookupTime / time.Duration(metrics.MessagesProcessed)
	avgNormalization := metrics.NormalizationTime / time.Duration(metrics.MessagesProcessed)

	allocsPerMessage := float64(metrics.AllocsAfter-metrics.AllocsBefore) / float64(metrics.MessagesProcessed)
	heapPerMessage := float64(metrics.HeapAfter-metrics.HeapBefore) / float64(metrics.MessagesProcessed)

	opt.logger.Info("Token normalization performance metrics",
		zap.Int64("messages_processed", metrics.MessagesProcessed),
		zap.Int64("tokens_extracted", metrics.TokensExtracted),
		zap.Int64("amounts_normalized", metrics.AmountsNormalized),
		zap.Duration("avg_total_time", avgTotal),
		zap.Duration("avg_extraction_time", avgExtraction),
		zap.Duration("avg_lookup_time", avgLookup),
		zap.Duration("avg_normalization_time", avgNormalization),
		zap.Float64("cache_hit_rate_percent", metrics.CacheHitRate),
		zap.Float64("avg_allocs_per_message", allocsPerMessage),
		zap.Float64("avg_heap_bytes_per_message", heapPerMessage),
		zap.Float64("tokens_per_message", float64(metrics.TokensExtracted)/float64(metrics.MessagesProcessed)),
		zap.Float64("amounts_per_message", float64(metrics.AmountsNormalized)/float64(metrics.MessagesProcessed)),
	)
}
