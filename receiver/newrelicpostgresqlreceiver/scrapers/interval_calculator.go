// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package scrapers provides simplified interval-based delta calculator for PostgreSQL slow query detection
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
// not cumulative since statistics reset. This provides immediate visibility into changes.
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
// Scrape 1: 100 calls, 500ms total → cumulative avg = 5ms → not slow
// Scrape 2: 120 calls, 700ms total → interval avg = (700-500)/(120-100) = 10ms → not slow
// Scrape 5: 180 calls, 21,100ms total → interval avg = 20,000/60 = 333ms → not slow yet
// Scrape 7: 220 calls, 61,100ms total → interval avg = 40,000/40 = 1,000ms → SLOW! (immediate detection)
package scrapers

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// PostgreSQLQueryState tracks previous scrape data for delta calculation
type PostgreSQLQueryState struct {
	// Previous cumulative values from pg_stat_statements
	PrevExecutionCount     int64
	PrevTotalElapsedTimeMs float64 // milliseconds

	// Timestamps for TTL-based cleanup
	LastSeenTimestamp  time.Time
	FirstSeenTimestamp time.Time
}

// PostgreSQLIntervalMetrics holds both interval and historical metrics
type PostgreSQLIntervalMetrics struct {
	// Interval-based metrics (delta calculation) - ONLY for elapsed time
	IntervalAvgElapsedTimeMs float64
	IntervalExecutionCount   int64

	// Historical metrics (cumulative from PostgreSQL)
	HistoricalAvgElapsedTimeMs float64
	HistoricalAvgCPUTimeMs     float64
	HistoricalExecutionCount   int64

	// Metadata
	IsFirstScrape    bool // True if this is the first time seeing this query
	HasNewExecutions bool // True if delta_exec_count > 0
}

// PostgreSQLIntervalCalculator implements simplified delta-based calculation for PostgreSQL
type PostgreSQLIntervalCalculator struct {
	logger *zap.Logger

	// State cache - tracks ALL queries, not just slow ones
	// Eviction is ONLY based on TTL (inactivity), NOT threshold
	stateCache      map[string]*PostgreSQLQueryState
	stateCacheMutex sync.RWMutex

	// Configuration
	cacheTTL         time.Duration // TTL for inactive queries (default: 10 minutes)
	lastCacheCleanup time.Time
}

// NewPostgreSQLIntervalCalculator creates a new PostgreSQL interval calculator
func NewPostgreSQLIntervalCalculator(logger *zap.Logger, cacheTTL time.Duration) *PostgreSQLIntervalCalculator {
	if cacheTTL <= 0 {
		logger.Warn("Invalid cache TTL, using default 10 minutes", zap.Duration("provided", cacheTTL))
		cacheTTL = 10 * time.Minute
	}

	return &PostgreSQLIntervalCalculator{
		logger:           logger,
		stateCache:       make(map[string]*PostgreSQLQueryState),
		cacheTTL:         cacheTTL,
		lastCacheCleanup: time.Now(),
	}
}

// CalculateMetrics calculates both interval and historical metrics for PostgreSQL slow queries
// Returns nil if query should be skipped (e.g., nil QueryID)
func (pic *PostgreSQLIntervalCalculator) CalculateMetrics(query *models.SlowQuery, now time.Time) *PostgreSQLIntervalMetrics {
	if query == nil || query.QueryID.String == "" {
		return nil
	}

	queryID := query.QueryID.String

	// Extract current cumulative values from query
	currentExecCount := int64(0)
	if query.ExecutionCount.Valid {
		currentExecCount = query.ExecutionCount.Int64
	}

	currentTotalElapsedMs := 0.0
	if query.TotalElapsedTimeMs.Valid {
		currentTotalElapsedMs = query.TotalElapsedTimeMs.Float64
	}

	// Get historical averages from pg_stat_statements
	historicalAvgElapsedMs := 0.0
	if query.AvgElapsedTimeMs.Valid {
		historicalAvgElapsedMs = query.AvgElapsedTimeMs.Float64
	}

	historicalAvgCPUMs := 0.0
	if query.AvgCPUTimeMs.Valid {
		historicalAvgCPUMs = query.AvgCPUTimeMs.Float64
	}

	pic.stateCacheMutex.Lock()
	defer pic.stateCacheMutex.Unlock()

	prevState, exists := pic.stateCache[queryID]

	// Initialize result with historical values
	result := &PostgreSQLIntervalMetrics{
		HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
		HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
		HistoricalExecutionCount:   currentExecCount,
		IsFirstScrape:              !exists,
		HasNewExecutions:           false,
	}

	if !exists {
		// First time seeing this query - use historical average as interval
		result.IntervalAvgElapsedTimeMs = historicalAvgElapsedMs
		result.IntervalExecutionCount = currentExecCount
		result.HasNewExecutions = currentExecCount > 0

		// Store current state for next scrape
		pic.stateCache[queryID] = &PostgreSQLQueryState{
			PrevExecutionCount:     currentExecCount,
			PrevTotalElapsedTimeMs: currentTotalElapsedMs,
			LastSeenTimestamp:      now,
			FirstSeenTimestamp:     now,
		}

		return result
	}

	// Calculate deltas
	deltaExecCount := currentExecCount - prevState.PrevExecutionCount
	deltaElapsedMs := currentTotalElapsedMs - prevState.PrevTotalElapsedTimeMs

	// Update last seen timestamp
	prevState.LastSeenTimestamp = now

	// Check if query was executed in this interval
	if deltaExecCount <= 0 {
		// No new executions - keep using historical average
		result.IntervalAvgElapsedTimeMs = historicalAvgElapsedMs
		result.IntervalExecutionCount = 0
		result.HasNewExecutions = false
		return result
	}

	// Calculate interval average
	result.IntervalAvgElapsedTimeMs = deltaElapsedMs / float64(deltaExecCount)
	result.IntervalExecutionCount = deltaExecCount
	result.HasNewExecutions = true

	// Update state for next scrape
	prevState.PrevExecutionCount = currentExecCount
	prevState.PrevTotalElapsedTimeMs = currentTotalElapsedMs

	return result
}

// CleanupStaleEntries removes queries that haven't been seen for longer than cacheTTL
func (pic *PostgreSQLIntervalCalculator) CleanupStaleEntries(now time.Time) {
	// Only cleanup every minute to reduce overhead
	if now.Sub(pic.lastCacheCleanup) < time.Minute {
		return
	}

	pic.stateCacheMutex.Lock()
	defer pic.stateCacheMutex.Unlock()

	pic.lastCacheCleanup = now
	removed := 0

	for queryID, state := range pic.stateCache {
		if now.Sub(state.LastSeenTimestamp) > pic.cacheTTL {
			delete(pic.stateCache, queryID)
			removed++
		}
	}

	if removed > 0 {
		pic.logger.Debug("Cleaned up stale query entries",
			zap.Int("removed", removed),
			zap.Int("remaining", len(pic.stateCache)))
	}
}

// GetCacheStats returns statistics about the cache
func (pic *PostgreSQLIntervalCalculator) GetCacheStats() map[string]interface{} {
	pic.stateCacheMutex.RLock()
	defer pic.stateCacheMutex.RUnlock()

	return map[string]interface{}{
		"total_queries": len(pic.stateCache),
		"cache_ttl_min": pic.cacheTTL.Minutes(),
	}
}
