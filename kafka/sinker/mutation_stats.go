package sinker

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// MutationStats tracks statistics for field mutations
type MutationStats struct {
	// Counters (atomic for thread safety)
	TotalMutationsApplied int64
	TokenAmountsFormatted int64
	TokenMetadataInjected int64
	EmptyFieldsFiltered   int64
	MutationErrors        int64

	// Timing
	LastStatsReported time.Time
	TotalMutationTime int64 // nanoseconds
}

// NewMutationStats creates a new mutation statistics tracker
func NewMutationStats() *MutationStats {
	return &MutationStats{
		LastStatsReported: time.Now(),
	}
}

// RecordMutation records a successful mutation application
func (ms *MutationStats) RecordMutation(duration time.Duration) {
	atomic.AddInt64(&ms.TotalMutationsApplied, 1)
	atomic.AddInt64(&ms.TotalMutationTime, int64(duration))
}

// RecordTokenAmountFormatted records a token amount formatting
func (ms *MutationStats) RecordTokenAmountFormatted() {
	atomic.AddInt64(&ms.TokenAmountsFormatted, 1)
}

// RecordTokenMetadataInjected records a token metadata injection
func (ms *MutationStats) RecordTokenMetadataInjected() {
	atomic.AddInt64(&ms.TokenMetadataInjected, 1)
}

// RecordEmptyFieldFiltered records an empty field being filtered
func (ms *MutationStats) RecordEmptyFieldFiltered() {
	atomic.AddInt64(&ms.EmptyFieldsFiltered, 1)
}

// RecordMutationError records a mutation error
func (ms *MutationStats) RecordMutationError() {
	atomic.AddInt64(&ms.MutationErrors, 1)
}

// ShouldReportStats checks if it's time to report stats (every 15 seconds)
func (ms *MutationStats) ShouldReportStats() bool {
	return time.Since(ms.LastStatsReported) >= 15*time.Second
}

// ReportStats logs the current mutation statistics
func (ms *MutationStats) ReportStats(logger *zap.Logger) {
	totalMutations := atomic.LoadInt64(&ms.TotalMutationsApplied)
	tokenAmounts := atomic.LoadInt64(&ms.TokenAmountsFormatted)
	tokenMetadata := atomic.LoadInt64(&ms.TokenMetadataInjected)
	emptyFields := atomic.LoadInt64(&ms.EmptyFieldsFiltered)
	errors := atomic.LoadInt64(&ms.MutationErrors)
	totalTime := atomic.LoadInt64(&ms.TotalMutationTime)

	var avgMutationTime float64
	if totalMutations > 0 {
		avgMutationTime = float64(totalTime) / float64(totalMutations) / 1e6 // Convert to milliseconds
	}

	logger.Info("Field mutation statistics",
		zap.Int64("total_mutations", totalMutations),
		zap.Int64("token_amounts_formatted", tokenAmounts),
		zap.Int64("token_metadata_injected", tokenMetadata),
		zap.Int64("empty_fields_filtered", emptyFields),
		zap.Int64("mutation_errors", errors),
		zap.Float64("avg_mutation_time_ms", avgMutationTime),
	)

	ms.LastStatsReported = time.Now()
}
