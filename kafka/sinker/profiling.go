package sinker

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof" // Import pprof for profiling endpoints
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"go.uber.org/zap"
)

// ProfilingConfig holds profiling configuration
type ProfilingConfig struct {
	EnableCPUProfile bool
	EnableMemProfile bool
	EnableTracing    bool
	EnableHTTPPprof  bool
	ProfileDuration  time.Duration
	OutputDir        string
	HTTPPort         int
	Logger           *zap.Logger
}

// ProfileManager manages Go profiling for the sinker
type ProfileManager struct {
	config     ProfilingConfig
	cpuFile    *os.File
	traceFile  *os.File
	httpServer *http.Server
	logger     *zap.Logger
}

// NewProfileManager creates a new profile manager
func NewProfileManager(config ProfilingConfig) *ProfileManager {
	return &ProfileManager{
		config: config,
		logger: config.Logger,
	}
}

// StartProfiling starts all enabled profiling methods
func (pm *ProfileManager) StartProfiling() error {
	if pm.config.EnableHTTPPprof {
		if err := pm.startHTTPPprof(); err != nil {
			return fmt.Errorf("failed to start HTTP pprof: %w", err)
		}
	}

	if pm.config.EnableCPUProfile {
		if err := pm.startCPUProfile(); err != nil {
			return fmt.Errorf("failed to start CPU profiling: %w", err)
		}
	}

	if pm.config.EnableTracing {
		if err := pm.startTracing(); err != nil {
			return fmt.Errorf("failed to start tracing: %w", err)
		}
	}

	pm.logger.Info("Profiling started",
		zap.Bool("cpu_profile", pm.config.EnableCPUProfile),
		zap.Bool("mem_profile", pm.config.EnableMemProfile),
		zap.Bool("tracing", pm.config.EnableTracing),
		zap.Bool("http_pprof", pm.config.EnableHTTPPprof),
		zap.Int("http_port", pm.config.HTTPPort),
		zap.String("output_dir", pm.config.OutputDir),
	)

	return nil
}

// StopProfiling stops all profiling and saves results
func (pm *ProfileManager) StopProfiling() error {
	var errors []error

	if pm.cpuFile != nil {
		pprof.StopCPUProfile()
		if err := pm.cpuFile.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close CPU profile: %w", err))
		}
		pm.logger.Info("CPU profile saved", zap.String("file", pm.cpuFile.Name()))
	}

	if pm.traceFile != nil {
		trace.Stop()
		if err := pm.traceFile.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close trace file: %w", err))
		}
		pm.logger.Info("Trace file saved", zap.String("file", pm.traceFile.Name()))
	}

	if pm.config.EnableMemProfile {
		if err := pm.saveMemProfile(); err != nil {
			errors = append(errors, fmt.Errorf("failed to save memory profile: %w", err))
		}
	}

	if pm.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pm.httpServer.Shutdown(ctx); err != nil {
			errors = append(errors, fmt.Errorf("failed to shutdown HTTP server: %w", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors stopping profiling: %v", errors)
	}

	return nil
}

// startHTTPPprof starts the HTTP pprof server
func (pm *ProfileManager) startHTTPPprof() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", http.DefaultServeMux.ServeHTTP)

	pm.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", pm.config.HTTPPort),
		Handler: mux,
	}

	go func() {
		pm.logger.Info("Starting pprof HTTP server",
			zap.String("url", fmt.Sprintf("http://localhost:%d/debug/pprof/", pm.config.HTTPPort)))
		if err := pm.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			pm.logger.Error("pprof HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// startCPUProfile starts CPU profiling
func (pm *ProfileManager) startCPUProfile() error {
	filename := fmt.Sprintf("%s/cpu_profile_%d.prof", pm.config.OutputDir, time.Now().Unix())
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	pm.cpuFile = file
	return pprof.StartCPUProfile(file)
}

// startTracing starts execution tracing
func (pm *ProfileManager) startTracing() error {
	filename := fmt.Sprintf("%s/trace_%d.out", pm.config.OutputDir, time.Now().Unix())
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	pm.traceFile = file
	return trace.Start(file)
}

// saveMemProfile saves a memory profile
func (pm *ProfileManager) saveMemProfile() error {
	filename := fmt.Sprintf("%s/mem_profile_%d.prof", pm.config.OutputDir, time.Now().Unix())
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	runtime.GC() // Force garbage collection before profiling
	if err := pprof.WriteHeapProfile(file); err != nil {
		return err
	}

	pm.logger.Info("Memory profile saved", zap.String("file", filename))
	return nil
}

// ProfiledFunction wraps a function with profiling
func (pm *ProfileManager) ProfiledFunction(name string, fn func() error) error {
	if !pm.config.EnableCPUProfile && !pm.config.EnableTracing {
		return fn() // No profiling overhead
	}

	start := time.Now()
	err := fn()
	duration := time.Since(start)

	pm.logger.Debug("Function profiled",
		zap.String("function", name),
		zap.Duration("duration", duration),
		zap.Error(err),
	)

	return err
}

// GetMemoryStats returns current memory statistics
func GetMemoryStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"alloc_mb":         bToMb(m.Alloc),
		"total_alloc_mb":   bToMb(m.TotalAlloc),
		"sys_mb":           bToMb(m.Sys),
		"heap_alloc_mb":    bToMb(m.HeapAlloc),
		"heap_sys_mb":      bToMb(m.HeapSys),
		"heap_idle_mb":     bToMb(m.HeapIdle),
		"heap_inuse_mb":    bToMb(m.HeapInuse),
		"heap_released_mb": bToMb(m.HeapReleased),
		"heap_objects":     m.HeapObjects,
		"stack_inuse_mb":   bToMb(m.StackInuse),
		"stack_sys_mb":     bToMb(m.StackSys),
		"gc_sys_mb":        bToMb(m.GCSys),
		"num_gc":           m.NumGC,
		"num_forced_gc":    m.NumForcedGC,
		"gc_cpu_fraction":  m.GCCPUFraction,
	}
}

// bToMb converts bytes to megabytes
func bToMb(b uint64) float64 {
	return float64(b) / 1024 / 1024
}

// LogMemoryStats logs current memory statistics
func LogMemoryStats(logger *zap.Logger, context string) {
	stats := GetMemoryStats()

	logger.Info("Memory statistics",
		zap.String("context", context),
		zap.Float64("heap_alloc_mb", stats["heap_alloc_mb"].(float64)),
		zap.Float64("heap_sys_mb", stats["heap_sys_mb"].(float64)),
		zap.Uint64("heap_objects", stats["heap_objects"].(uint64)),
		zap.Uint32("num_gc", stats["num_gc"].(uint32)),
		zap.Float64("gc_cpu_fraction", stats["gc_cpu_fraction"].(float64)),
	)
}

// BenchmarkFunction runs a function multiple times and reports performance
func BenchmarkFunction(name string, iterations int, fn func() error, logger *zap.Logger) {
	var totalDuration time.Duration
	var errors int

	logger.Info("Starting benchmark",
		zap.String("function", name),
		zap.Int("iterations", iterations))

	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			errors++
			logger.Debug("Benchmark iteration error",
				zap.Int("iteration", i),
				zap.Error(err))
		}
		totalDuration += time.Since(start)
	}

	avgDuration := totalDuration / time.Duration(iterations)
	successRate := float64(iterations-errors) / float64(iterations) * 100

	logger.Info("Benchmark completed",
		zap.String("function", name),
		zap.Int("iterations", iterations),
		zap.Int("errors", errors),
		zap.Float64("success_rate_percent", successRate),
		zap.Duration("total_duration", totalDuration),
		zap.Duration("avg_duration", avgDuration),
		zap.Float64("ops_per_second", float64(iterations)/totalDuration.Seconds()),
	)
}
