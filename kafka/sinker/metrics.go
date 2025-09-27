package sinker

import (
	"sync/atomic"
	"time"

	"net/http"

	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	blocksProcessedGauge   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_blocks_processed_total", Help: "Total number of blocks processed"})
	messagesProducedGauge  = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_messages_produced_total", Help: "Total number of messages produced"})
	messagesConfirmedGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_messages_confirmed_total", Help: "Total number of messages confirmed"})
	messagesPendingGauge   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_messages_pending", Help: "Current number of pending messages"})
	deliveryChanLenGauge   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_delivery_chan_len", Help: "Length of Kafka delivery channel"})
	pendingMapSizeGauge    = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_pending_map_size", Help: "Number of pending message keys tracked"})

	processingQueueLenGauge       = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_processing_queue_len", Help: "Processing queue length"})
	processingQueueCapGauge       = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_processing_queue_cap", Help: "Processing queue capacity"})
	processingCurrentBytesGauge   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_processing_current_bytes", Help: "Current bytes in processing buffer"})
	processingBytesBudgetGauge    = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_processing_bytes_budget", Help: "Processing buffer bytes budget"})
	processingBytesUsedRatioGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_processing_bytes_used_ratio", Help: "Processing buffer bytes used ratio"})

	undoBufferSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_undo_buffer_size", Help: "Current undo buffer size (blocks)"})

	backpressureQueueFullGauge     = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_backpressure_queue_full", Help: "Processing queue full (1=yes, 0=no)"})
	backpressurePendingThreshGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_backpressure_pending_threshold", Help: "Pending threshold exceeded (1=yes, 0=no)"})

	blocksPerSecondGauge   = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_blocks_per_second", Help: "Blocks processed per second over last interval"})
	messagesPerSecondGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_kafka_messages_per_second", Help: "Messages processed per second over last interval (approx)"})

	serializationDurationHist = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "sink_kafka_serialization_duration_seconds", Help: "Batch serialization duration", Buckets: prometheus.DefBuckets})
	productionDurationHist    = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "sink_kafka_production_duration_seconds", Help: "Batch production duration", Buckets: prometheus.DefBuckets})
)

func init() {
	prometheus.MustRegister(
		blocksProcessedGauge,
		messagesProducedGauge,
		messagesConfirmedGauge,
		messagesPendingGauge,
		deliveryChanLenGauge,
		pendingMapSizeGauge,
		processingQueueLenGauge,
		processingQueueCapGauge,
		processingCurrentBytesGauge,
		processingBytesBudgetGauge,
		processingBytesUsedRatioGauge,
		undoBufferSizeGauge,
		backpressureQueueFullGauge,
		backpressurePendingThreshGauge,
		blocksPerSecondGauge,
		messagesPerSecondGauge,
		serializationDurationHist,
		productionDurationHist,
	)
}

// startMetricsReporter starts a periodic reporter aligned with substreams metrics (every 20s)
func (s *KafkaSinker) startMetricsReporter() {
	ticker := time.NewTicker(20 * time.Second)
	go func() {
		for range ticker.C {
			s.updatePerformanceMetrics()
		}
	}()
}

func (s *KafkaSinker) updatePerformanceMetrics() {
	now := time.Now()
	duration := now.Sub(s.lastStatsTime)

	blocksProcessed := atomic.LoadInt64(&s.blocksProcessed)
	messagesProduced := atomic.LoadInt64(&s.messagesProduced)
	messagesConfirmed := atomic.LoadInt64(&s.messagesConfirmed)
	messagesPending := atomic.LoadInt64(&s.messagesPending)

	if duration > 0 {
		blocksDelta := blocksProcessed - s.lastBlocksProcessed
		s.currentThroughput = float64(blocksDelta) / duration.Seconds()
		s.lastStatsTime = now
		s.lastBlocksProcessed = blocksProcessed
	}

	// Processing buffer stats (if enabled)
	processingQueueLen := 0
	processingQueueCap := 0
	if s.processingQueue != nil {
		processingQueueLen = len(s.processingQueue)
		processingQueueCap = cap(s.processingQueue)
	}

	// Undo buffer stats (if enabled)
	undoBufferSize := 0
	if s.undoBuffer != nil {
		undoBufferSize = s.undoBuffer.GetCurrentBufferSize()
	}

	// Derived backpressure indicators
	queueFull := s.processingQueue != nil && processingQueueCap > 0 && processingQueueLen >= processingQueueCap
	pendingThresholdExceeded := atomic.LoadInt64(&s.messagesPending) > int64(s.batchSize*10)
	bytesBudgetUsed := float64(0)
	if s.processingBufferBytes > 0 {
		bytesBudgetUsed = float64(atomic.LoadInt64(&s.processingCurrentBytes)) / float64(s.processingBufferBytes)
	}

	// export to Prometheus gauges
	blocksProcessedGauge.Set(float64(blocksProcessed))
	messagesProducedGauge.Set(float64(messagesProduced))
	messagesConfirmedGauge.Set(float64(messagesConfirmed))
	messagesPendingGauge.Set(float64(messagesPending))
	deliveryChanLenGauge.Set(float64(len(s.deliveryChan)))
	pendingMapSizeGauge.Set(float64(len(s.pendingMessages)))

	processingQueueLenGauge.Set(float64(processingQueueLen))
	processingQueueCapGauge.Set(float64(processingQueueCap))
	processingCurrentBytesGauge.Set(float64(atomic.LoadInt64(&s.processingCurrentBytes)))
	processingBytesBudgetGauge.Set(float64(s.processingBufferBytes))
	processingBytesUsedRatioGauge.Set(bytesBudgetUsed)

	undoBufferSizeGauge.Set(float64(undoBufferSize))
	if queueFull {
		backpressureQueueFullGauge.Set(1)
	} else {
		backpressureQueueFullGauge.Set(0)
	}
	if pendingThresholdExceeded {
		backpressurePendingThreshGauge.Set(1)
	} else {
		backpressurePendingThreshGauge.Set(0)
	}

	blocksPerSecondGauge.Set(s.currentThroughput)
	messagesPerSecondGauge.Set(s.currentThroughput)

	// Also log to terminal for transparency
	// Keep concise, aligned with substreams style
	s.logger.Info("sink-kafka stats",
		zap.Float64("data_msg_rate", s.currentThroughput),
		zap.Int64("messages_pending", messagesPending),
		zap.Int("delivery_chan_len", len(s.deliveryChan)),
		zap.Int("pending_map_size", len(s.pendingMessages)),
		zap.Int("processing_queue_len", processingQueueLen),
		zap.Int("processing_queue_cap", processingQueueCap),
		zap.Int64("processing_current_bytes", atomic.LoadInt64(&s.processingCurrentBytes)),
		zap.Int64("processing_bytes_budget", s.processingBufferBytes),
		zap.Float64("processing_bytes_used_ratio", bytesBudgetUsed),
		zap.Int("undo_buffer_size", undoBufferSize),
		zap.Bool("processing_queue_full", queueFull),
		zap.Bool("pending_threshold_exceeded", pendingThresholdExceeded),
	)
}

// observeSerializationDuration exports a batch serialization duration
func observeSerializationDuration(d time.Duration) { serializationDurationHist.Observe(d.Seconds()) }

// observeProductionDuration exports a batch production duration
func observeProductionDuration(d time.Duration) { productionDurationHist.Observe(d.Seconds()) }

// startPrometheusServer exposes /metrics on a default port
func startPrometheusServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":9102", Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
}
