package sinker

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

func (s *KafkaSinker) updatePerformanceMetrics() {
	blocksProcessed := atomic.LoadInt64(&s.blocksProcessed)

	// Log progress every 1000 blocks with enhanced metrics
	if blocksProcessed%1000 == 0 {
		now := time.Now()
		duration := now.Sub(s.lastStatsTime)

		if duration > 0 {
			blocksDelta := blocksProcessed - s.lastBlocksProcessed
			s.currentThroughput = float64(blocksDelta) / duration.Seconds()
			s.lastStatsTime = now
			s.lastBlocksProcessed = blocksProcessed
		}

		messagesProduced := atomic.LoadInt64(&s.messagesProduced)
		messagesConfirmed := atomic.LoadInt64(&s.messagesConfirmed)
		messagesPending := atomic.LoadInt64(&s.messagesPending)

		s.logger.Info("🚀 HIGH-PERFORMANCE PROCESSING",
			zap.Int64("blocks_processed", blocksProcessed),
			zap.Int64("messages_produced", messagesProduced),
			zap.Int64("messages_confirmed", messagesConfirmed),
			zap.Int64("messages_pending", messagesPending),
			zap.Float64("blocks_per_second", s.currentThroughput),
			zap.Float64("messages_per_second", s.currentThroughput), // 1:1 ratio for this use case
		)
	}
} 