// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package scrapers provides interval-based delta calculator for PostgreSQL slow query detection
//
// Problem Statement:
// Cumulative averages from pg_stat_statements (total_exec_time / calls)
// change very slowly when queries are optimized or degrade, causing:
// - False positives (slow queries still flagged after optimization)
// - Delayed detection of new performance issues
// - Inefficient resource usage
//
// Solution:
// Interval-based delta calculations that show what happened in the LAST polling interval,
// not cumulative since stats collection started. This provides immediate visibility into changes.
//
// Algorithm:
// 1. First Scrape: Use historical (cumulative) average, filter by threshold
// 2. Subsequent Scrapes: Calculate delta between current and previous values
//   - interval_avg = (current_total_elapsed - prev_total_elapsed) / (current_exec_count - prev_exec_count)
//
// 3. Emit both interval average AND historical average
// 4. Only emit metrics if interval average > threshold (memory efficient)
// 5. Eviction: Only TTL-based (inactive queries), NOT threshold-based
//   - This preserves delta calculation ability when queries oscillate
//
// Example (threshold = 1000ms):
// Scrape 1: 100 execs, 500ms total → cumulative avg = 5ms → not slow
// Scrape 2: 120 execs, 700ms total → interval avg = (700-500)/(120-100) = 10ms → not slow
// Scrape 5: 180 execs, 21,100ms total → interval avg = 20,000/60 = 333ms → not slow
// Scrape 7: 220 execs, 43,100ms total → interval avg = 22,000/40 = 550ms → not slow
// Scrape 9: 241 execs, 64,100ms total → interval avg = 21,000/21 = 1,000ms → SLOW! (immediate detection)
package scrapers

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// PgQueryState tracks previous scrape data for delta calculation
type PgQueryState struct {
	// Previous cumulative values from pg_stat_statements
	PrevExecutionCount     int64
	PrevTotalElapsedTimeMs float64 // milliseconds

	// Timestamps for TTL-based cleanup
	LastSeenTimestamp  time.Time
	FirstSeenTimestamp time.Time
}

// PgIntervalMetrics holds both interval and historical metrics
type PgIntervalMetrics struct {
	// Interval-based metrics (delta calculation) - ONLY for elapsed time
	IntervalAvgElapsedTimeMs float64
	IntervalExecutionCount   int64

	// Historical metrics (cumulative from pg_stat_statements)
	HistoricalAvgElapsedTimeMs float64
	HistoricalAvgCPUTimeMs     float64
	HistoricalExecutionCount   int64

	// Metadata
	IsFirstScrape    bool // True if this is the first time seeing this query
	HasNewExecutions bool // True if delta_exec_count > 0
}

// PgIntervalCalculator implements simplified delta-based calculation for PostgreSQL
type PgIntervalCalculator struct {
	logger *zap.Logger

	// State cache - tracks ALL queries, not just slow ones
	// Eviction is ONLY based on TTL (inactivity), NOT threshold
	stateCache      map[string]*PgQueryState
	stateCacheMutex sync.RWMutex

	// Configuration
	cacheTTL         time.Duration // TTL for inactive queries (default: 10 minutes)
	lastCacheCleanup time.Time
}

// NewPgIntervalCalculator creates a new PostgreSQL interval calculator
func NewPgIntervalCalculator(logger *zap.Logger, cacheTTL time.Duration) *PgIntervalCalculator {
	if cacheTTL <= 0 {
		logger.Warn("Invalid cache TTL, using default 10 minutes", zap.Duration("provided", cacheTTL))
		cacheTTL = 10 * time.Minute
	}

	return &PgIntervalCalculator{
		logger:           logger,
		stateCache:       make(map[string]*PgQueryState),
		cacheTTL:         cacheTTL,
		lastCacheCleanup: time.Now(),
	}
}

// CalculateMetrics calculates both interval and historical metrics for PostgreSQL slow queries
// Returns nil if query should be skipped (e.g., empty QueryID)
func (pic *PgIntervalCalculator) CalculateMetrics(query *models.PgSlowQueryMetric, now time.Time) *PgIntervalMetrics {
	if query == nil || query.QueryID == "" {
		return nil
	}

	queryID := query.QueryID

	// Extract current cumulative values from query
	currentExecCount := int64(0)
	if query.ExecutionCount.Valid {
		currentExecCount = query.ExecutionCount.Int64
	}

	currentTotalElapsedMs := 0.0
	if query.TotalElapsedTimeMs.Valid {
		currentTotalElapsedMs = query.TotalElapsedTimeMs.Float64
	}

	// Historical (cumulative) averages
	historicalAvgElapsedMs := 0.0
	if query.AvgElapsedTimeMs.Valid {
		historicalAvgElapsedMs = query.AvgElapsedTimeMs.Float64
	}

	historicalAvgCPUMs := 0.0
	if query.AvgCPUTimeMs.Valid {
		historicalAvgCPUMs = query.AvgCPUTimeMs.Float64
	}

	// Lock for state cache access
	pic.stateCacheMutex.Lock()
	defer pic.stateCacheMutex.Unlock()

	// Check if query exists in cache (using query_id as key)
	state, exists := pic.stateCache[queryID]

	// FIRST SCRAPE: Use historical (cumulative) average
	if !exists {
		// Handle edge case: execution_count = 0
		if currentExecCount == 0 {
			pic.logger.Warn("Query with zero execution count - skipping",
				zap.String("query_id", queryID),
				zap.Float64("total_elapsed_ms", currentTotalElapsedMs),
				zap.Float64("historical_avg_ms", historicalAvgElapsedMs))
			return nil
		}

		pic.logger.Debug("First scrape for query - using historical average",
			zap.String("query_id", queryID),
			zap.Int64("execution_count", currentExecCount),
			zap.Float64("historical_avg_ms", historicalAvgElapsedMs),
			zap.Float64("total_elapsed_ms", currentTotalElapsedMs))

		// Add to cache for next scrape
		pic.stateCache[queryID] = &PgQueryState{
			PrevExecutionCount:     currentExecCount,
			PrevTotalElapsedTimeMs: currentTotalElapsedMs,
			FirstSeenTimestamp:     now,
			LastSeenTimestamp:      now,
		}

		// For first scrape, use historical average as interval average
		// Use currentExecCount as interval count since this represents all executions since stats collection
		// Caller will filter by threshold
		return &PgIntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgElapsedMs,
			IntervalExecutionCount:     currentExecCount, // All executions since stats collection
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              true,
			HasNewExecutions:           true,
		}
	}

	// SUBSEQUENT SCRAPES: Calculate delta

	// Calculate deltas
	deltaExecCount := currentExecCount - state.PrevExecutionCount
	deltaElapsedMs := currentTotalElapsedMs - state.PrevTotalElapsedTimeMs

	// Handle no new executions (THIS REPLACES SQL TIME FILTER!)
	// If deltaExecCount == 0, query was NOT executed in the last interval
	if deltaExecCount == 0 {
		pic.logger.Debug("No new executions for query",
			zap.String("query_id", queryID))

		// Update last seen timestamp (query is still in pg_stat_statements results)
		state.LastSeenTimestamp = now

		// Return metrics but flag as no new executions
		// Caller can decide whether to emit
		return &PgIntervalMetrics{
			IntervalAvgElapsedTimeMs:   0,
			IntervalExecutionCount:     0,
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              false,
			HasNewExecutions:           false,
		}
	}

	// Handle stats reset (execution count decreased) OR stats corruption (negative delta elapsed)
	if deltaExecCount < 0 || deltaElapsedMs < 0 {
		if deltaExecCount < 0 {
			pic.logger.Warn("Stats reset detected - execution count decreased, cannot calculate valid interval delta",
				zap.String("query_id", queryID),
				zap.Int64("current_exec_count", currentExecCount),
				zap.Int64("prev_exec_count", state.PrevExecutionCount),
				zap.Int64("delta_exec_count", deltaExecCount))
		}
		if deltaElapsedMs < 0 {
			pic.logger.Warn("Negative delta elapsed time detected - possible stats corruption",
				zap.String("query_id", queryID),
				zap.Float64("current_total_ms", currentTotalElapsedMs),
				zap.Float64("prev_total_ms", state.PrevTotalElapsedTimeMs),
				zap.Float64("delta_ms", deltaElapsedMs))
		}

		// Reset state - treat as first scrape but PRESERVE FirstSeenTimestamp
		pic.stateCache[queryID] = &PgQueryState{
			PrevExecutionCount:     currentExecCount,
			PrevTotalElapsedTimeMs: currentTotalElapsedMs,
			FirstSeenTimestamp:     state.FirstSeenTimestamp, // Preserve original timestamp
			LastSeenTimestamp:      now,
		}

		// After stats reset, we cannot calculate a valid interval delta
		// Use historical average but set interval count to current count (all execs since reset)
		return &PgIntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgElapsedMs,
			IntervalExecutionCount:     currentExecCount, // All executions since stats reset
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              true, // Treat as first scrape after reset
			HasNewExecutions:           true,
		}
	}

	// NORMAL CASE: Calculate interval average (ONLY for elapsed time, no CPU)
	intervalAvgElapsedMs := deltaElapsedMs / float64(deltaExecCount)

	// Warn if we have executions but zero elapsed time
	if deltaElapsedMs == 0 && deltaExecCount > 0 {
		pic.logger.Warn("Zero elapsed time with non-zero executions - possible data issue or extremely fast query",
			zap.String("query_id", queryID),
			zap.Int64("delta_exec_count", deltaExecCount),
			zap.Float64("delta_elapsed_ms", deltaElapsedMs),
			zap.Float64("current_total_ms", currentTotalElapsedMs),
			zap.Float64("prev_total_ms", state.PrevTotalElapsedTimeMs))
	}

	pic.logger.Debug("Delta calculation for query",
		zap.String("query_id", queryID),
		zap.Int64("delta_exec_count", deltaExecCount),
		zap.Float64("delta_elapsed_ms", deltaElapsedMs),
		zap.Float64("interval_avg_elapsed_ms", intervalAvgElapsedMs),
		zap.Float64("historical_avg_elapsed_ms", historicalAvgElapsedMs))

	// Update state for next scrape
	state.PrevExecutionCount = currentExecCount
	state.PrevTotalElapsedTimeMs = currentTotalElapsedMs
	state.LastSeenTimestamp = now

	return &PgIntervalMetrics{
		IntervalAvgElapsedTimeMs:   intervalAvgElapsedMs,
		IntervalExecutionCount:     deltaExecCount,
		HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
		HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
		HistoricalExecutionCount:   currentExecCount,
		IsFirstScrape:              false,
		HasNewExecutions:           true,
	}
}

// CleanupStaleEntries removes entries based on TTL (inactivity) ONLY
func (pic *PgIntervalCalculator) CleanupStaleEntries(now time.Time) {
	pic.stateCacheMutex.Lock()
	defer pic.stateCacheMutex.Unlock()

	// Only run cleanup periodically
	if now.Sub(pic.lastCacheCleanup) < 5*time.Minute {
		return
	}

	pic.lastCacheCleanup = now
	removedCount := 0

	for queryID, state := range pic.stateCache {
		age := now.Sub(state.LastSeenTimestamp)
		if age > pic.cacheTTL {
			delete(pic.stateCache, queryID)
			removedCount++
		}
	}

	if removedCount > 0 {
		pic.logger.Info("Cleaned up stale queries from cache",
			zap.Int("removed_count", removedCount),
			zap.Int("remaining_count", len(pic.stateCache)),
			zap.Duration("cache_ttl", pic.cacheTTL))
	}
}

// GetCacheStats returns cache statistics
func (pic *PgIntervalCalculator) GetCacheStats() map[string]interface{} {
	pic.stateCacheMutex.RLock()
	defer pic.stateCacheMutex.RUnlock()

	return map[string]interface{}{
		"total_queries_tracked": len(pic.stateCache),
		"cache_ttl_minutes":     pic.cacheTTL.Minutes(),
	}
}

// Reset clears all state (useful for testing)
func (pic *PgIntervalCalculator) Reset() {
	pic.stateCacheMutex.Lock()
	defer pic.stateCacheMutex.Unlock()

	pic.stateCache = make(map[string]*PgQueryState)
	pic.logger.Info("PostgreSQL interval calculator cache reset")
}
