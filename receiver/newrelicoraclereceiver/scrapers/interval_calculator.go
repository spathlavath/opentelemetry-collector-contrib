// Copyright 2025 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package scrapers provides simplified interval-based delta calculator for Oracle slow query detection
//
// Problem Statement:
// Cumulative averages from Oracle V$SQLAREA (total_elapsed_time / executions)
// change very slowly when queries are optimized or degrade, causing:
// - False positives (slow queries still flagged after optimization)
// - Delayed detection of new performance issues
// - Inefficient resource usage
//
// Solution:
// Interval-based delta calculations that show what happened in the LAST polling interval,
// not cumulative since plan cache. This provides immediate visibility into changes.
//
// Algorithm:
// 1. First Scrape: Use historical (cumulative) average, filter by threshold
// 2. Subsequent Scrapes: Calculate delta between current and previous values
//   - interval_avg_elapsed = (current_total_elapsed - prev_total_elapsed) / (current_exec_count - prev_exec_count)
//   - interval_avg_cpu = (current_total_cpu - prev_total_cpu) / (current_exec_count - prev_exec_count)
//   - interval_avg_disk_reads = (current_total_disk_reads - prev_total_disk_reads) / (current_exec_count - prev_exec_count)
//   - interval_avg_buffer_gets = (current_total_buffer_gets - prev_total_buffer_gets) / (current_exec_count - prev_exec_count)
//   - interval_avg_rows_processed = (current_total_rows_processed - prev_total_rows_processed) / (current_exec_count - prev_exec_count)
//
// 3. Emit both interval average AND historical average for all metrics
// 4. Only emit metrics if interval average > threshold (memory efficient)
// 5. Eviction: Only TTL-based (inactive queries), NOT threshold-based
//   - This preserves delta calculation ability when queries oscillate
//
// Example (threshold = 1000ms):
// Scrape 1: 100 execs, 500ms total → cumulative avg = 5ms → not slow
// Scrape 2: 120 execs, 700ms total → interval avg = (700-500)/(120-100) = 10ms → not slow
// Scrape 5: 180 execs, 21,100ms total → interval avg = 20,000/20 = 1,000ms → SLOW! (immediate detection)
// Scrape 7: 220 execs, 43,100ms total → interval avg = 2,000/20 = 100ms → at threshold
// Scrape 9: 241 execs, 45,600ms total → interval avg = 500/1 = 500ms → SLOW! (outlier detected)
package scrapers

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// OracleQueryState tracks previous scrape data for delta calculation
type OracleQueryState struct {
	// Previous cumulative values from Oracle V$SQLAREA
	PrevExecutionCount     int64
	PrevTotalElapsedTimeMs float64 // milliseconds
	PrevTotalCPUTimeMs     float64 // milliseconds
	PrevTotalDiskReads     int64   // disk reads
	PrevTotalBufferGets    int64   // buffer gets (logical reads)
	PrevTotalRowsProcessed int64   // rows processed (rows returned)

	// Timestamps for TTL-based cleanup
	LastSeenTimestamp  time.Time
	FirstSeenTimestamp time.Time
}

// OracleIntervalMetrics holds both interval and historical metrics
type OracleIntervalMetrics struct {
	// Interval-based metrics (delta calculation)
	IntervalAvgElapsedTimeMs float64
	IntervalAvgCPUTimeMs     float64
	IntervalAvgDiskReads     float64
	IntervalAvgBufferGets    float64
	IntervalAvgRowsProcessed float64
	IntervalExecutionCount   int64

	// Historical metrics (cumulative from Oracle)
	HistoricalAvgElapsedTimeMs float64
	HistoricalAvgCPUTimeMs     float64
	HistoricalAvgDiskReads     float64
	HistoricalAvgBufferGets    float64
	HistoricalAvgRowsProcessed float64
	HistoricalExecutionCount   int64

	// Metadata
	IsFirstScrape        bool    // True if this is the first time seeing this query
	HasNewExecutions     bool    // True if delta_exec_count > 0
	TimeSinceLastExecSec float64 // Time since last execution from Oracle
}

// OracleIntervalCalculator implements simplified delta-based calculation for Oracle
type OracleIntervalCalculator struct {
	logger *zap.Logger

	// State cache - tracks ALL queries, not just slow ones
	// Eviction is ONLY based on TTL (inactivity), NOT threshold
	stateCache      map[string]*OracleQueryState
	stateCacheMutex sync.RWMutex

	// Configuration
	cacheTTL         time.Duration // TTL for inactive queries (default: 10 minutes)
	lastCacheCleanup time.Time
}

// NewOracleIntervalCalculator creates a new Oracle interval calculator
func NewOracleIntervalCalculator(logger *zap.Logger, cacheTTL time.Duration) *OracleIntervalCalculator {
	if cacheTTL <= 0 {
		logger.Warn("Invalid cache TTL, using default 10 minutes", zap.Duration("provided", cacheTTL))
		cacheTTL = 10 * time.Minute
	}

	return &OracleIntervalCalculator{
		logger:           logger,
		stateCache:       make(map[string]*OracleQueryState),
		cacheTTL:         cacheTTL,
		lastCacheCleanup: time.Now(),
	}
}

// CalculateMetrics calculates both interval and historical metrics for Oracle slow queries
// Returns nil if query should be skipped (e.g., nil QueryID)
func (oic *OracleIntervalCalculator) CalculateMetrics(query *models.SlowQuery, now time.Time) *OracleIntervalMetrics {
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
	if query.TotalElapsedTimeMS.Valid {
		currentTotalElapsedMs = query.TotalElapsedTimeMS.Float64
	}

	currentTotalCPUMs := 0.0
	if query.TotalCPUTimeMS.Valid {
		currentTotalCPUMs = query.TotalCPUTimeMS.Float64
	}

	currentTotalDiskReads := int64(0)
	if query.TotalDiskReads.Valid {
		currentTotalDiskReads = query.TotalDiskReads.Int64
	}

	currentTotalBufferGets := int64(0)
	if query.TotalBufferGets.Valid {
		currentTotalBufferGets = query.TotalBufferGets.Int64
	}

	currentTotalRowsProcessed := int64(0)
	if query.TotalRowsProcessed.Valid {
		currentTotalRowsProcessed = query.TotalRowsProcessed.Int64
	}

	// Historical (cumulative) averages - computed from totals / executions
	historicalAvgElapsedMs := 0.0
	if query.AvgElapsedTimeMs.Valid {
		historicalAvgElapsedMs = query.AvgElapsedTimeMs.Float64
	}

	// Compute historical averages from totals (instead of querying from DB)
	historicalAvgCPUMs := 0.0
	if currentExecCount > 0 {
		historicalAvgCPUMs = currentTotalCPUMs / float64(currentExecCount)
	}

	historicalAvgDiskReads := 0.0
	if currentExecCount > 0 {
		historicalAvgDiskReads = float64(currentTotalDiskReads) / float64(currentExecCount)
	}

	historicalAvgBufferGets := 0.0
	if currentExecCount > 0 {
		historicalAvgBufferGets = float64(currentTotalBufferGets) / float64(currentExecCount)
	}

	historicalAvgRowsProcessed := 0.0
	if currentExecCount > 0 {
		historicalAvgRowsProcessed = float64(currentTotalRowsProcessed) / float64(currentExecCount)
	}

	// Calculate time since last execution from Oracle timestamp
	timeSinceLastExec := 0.0
	if query.LastActiveTime.Valid {
		// Oracle last_active_time is a DATE type, parse as RFC3339
		if lastExecTime, err := time.Parse("2006-01-02/15:04:05", query.LastActiveTime.String); err == nil {
			timeSinceLastExec = now.Sub(lastExecTime).Seconds()
		}
	}

	// Lock for state cache access
	oic.stateCacheMutex.Lock()
	defer oic.stateCacheMutex.Unlock()

	// Check if query exists in cache (using query_id as key)
	state, exists := oic.stateCache[queryID]

	// FIRST SCRAPE: Use historical (cumulative) average
	if !exists {
		// Handle edge case: execution_count = 0
		if currentExecCount == 0 {
			oic.logger.Warn("Query with zero execution count - skipping")
			return nil
		}

		oic.logger.Debug("First scrape for query - using historical average")

		// Add to cache for next scrape
		oic.stateCache[queryID] = &OracleQueryState{
			PrevExecutionCount:     currentExecCount,
			PrevTotalElapsedTimeMs: currentTotalElapsedMs,
			PrevTotalCPUTimeMs:     currentTotalCPUMs,
			PrevTotalDiskReads:     currentTotalDiskReads,
			PrevTotalBufferGets:    currentTotalBufferGets,
			PrevTotalRowsProcessed: currentTotalRowsProcessed,
			FirstSeenTimestamp:     now,
			LastSeenTimestamp:      now,
		}

		// For first scrape, use historical average as interval average
		// Use currentExecCount as interval count since this represents all executions since plan cache
		// Caller will filter by threshold
		return &OracleIntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgElapsedMs,
			IntervalAvgCPUTimeMs:       historicalAvgCPUMs,
			IntervalAvgDiskReads:       historicalAvgDiskReads,
			IntervalAvgBufferGets:      historicalAvgBufferGets,
			IntervalAvgRowsProcessed:   historicalAvgRowsProcessed,
			IntervalExecutionCount:     currentExecCount, // All executions since plan cache
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalAvgDiskReads:     historicalAvgDiskReads,
			HistoricalAvgBufferGets:    historicalAvgBufferGets,
			HistoricalAvgRowsProcessed: historicalAvgRowsProcessed,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              true,
			HasNewExecutions:           true,
			TimeSinceLastExecSec:       timeSinceLastExec,
		}
	}

	// SUBSEQUENT SCRAPES: Calculate delta

	// Calculate deltas
	deltaExecCount := currentExecCount - state.PrevExecutionCount
	deltaElapsedMs := currentTotalElapsedMs - state.PrevTotalElapsedTimeMs

	// Handle no new executions
	if deltaExecCount == 0 {
		oic.logger.Debug("No new executions for query")

		// Update last seen timestamp (query is still in Oracle results)
		state.LastSeenTimestamp = now

		// Return metrics but flag as no new executions
		// Caller can decide whether to emit
		return &OracleIntervalMetrics{
			IntervalAvgElapsedTimeMs:   0,
			IntervalAvgCPUTimeMs:       0,
			IntervalAvgDiskReads:       0,
			IntervalAvgBufferGets:      0,
			IntervalAvgRowsProcessed:   0,
			IntervalExecutionCount:     0,
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalAvgDiskReads:     historicalAvgDiskReads,
			HistoricalAvgBufferGets:    historicalAvgBufferGets,
			HistoricalAvgRowsProcessed: historicalAvgRowsProcessed,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              false,
			HasNewExecutions:           false,
			TimeSinceLastExecSec:       timeSinceLastExec,
		}
	}

	// Handle plan cache reset (execution count decreased) OR stats corruption (negative delta elapsed)
	if deltaExecCount < 0 || deltaElapsedMs < 0 {
		if deltaExecCount < 0 {
			oic.logger.Warn("Plan cache reset detected - execution count decreased, cannot calculate valid interval delta")
		}
		if deltaElapsedMs < 0 {
			oic.logger.Warn("Negative delta elapsed time detected - possible stats corruption")
		}

		// Reset state - treat as first scrape but PRESERVE FirstSeenTimestamp
		oic.stateCache[queryID] = &OracleQueryState{
			PrevExecutionCount:     currentExecCount,
			PrevTotalElapsedTimeMs: currentTotalElapsedMs,
			PrevTotalCPUTimeMs:     currentTotalCPUMs,
			PrevTotalDiskReads:     currentTotalDiskReads,
			PrevTotalBufferGets:    currentTotalBufferGets,
			PrevTotalRowsProcessed: currentTotalRowsProcessed,
			FirstSeenTimestamp:     state.FirstSeenTimestamp, // Preserve original timestamp
			LastSeenTimestamp:      now,
		}

		// After cache reset, we cannot calculate a valid interval delta
		// Use historical average but set interval count to current count (all execs since reset)
		// Alternative: Could skip emitting by setting HasNewExecutions: false
		return &OracleIntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgElapsedMs,
			IntervalAvgCPUTimeMs:       historicalAvgCPUMs,
			IntervalAvgDiskReads:       historicalAvgDiskReads,
			IntervalAvgBufferGets:      historicalAvgBufferGets,
			IntervalAvgRowsProcessed:   historicalAvgRowsProcessed,
			IntervalExecutionCount:     currentExecCount, // All executions since cache reset
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalAvgDiskReads:     historicalAvgDiskReads,
			HistoricalAvgBufferGets:    historicalAvgBufferGets,
			HistoricalAvgRowsProcessed: historicalAvgRowsProcessed,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              true, // Treat as first scrape after reset
			HasNewExecutions:           true,
			TimeSinceLastExecSec:       timeSinceLastExec,
		}
	}

	// NORMAL CASE: Calculate interval averages for all metrics
	deltaCPUMs := currentTotalCPUMs - state.PrevTotalCPUTimeMs
	deltaDiskReads := currentTotalDiskReads - state.PrevTotalDiskReads
	deltaBufferGets := currentTotalBufferGets - state.PrevTotalBufferGets
	deltaRowsProcessed := currentTotalRowsProcessed - state.PrevTotalRowsProcessed

	intervalAvgElapsedMs := deltaElapsedMs / float64(deltaExecCount)
	intervalAvgCPUMs := deltaCPUMs / float64(deltaExecCount)
	intervalAvgDiskReads := float64(deltaDiskReads) / float64(deltaExecCount)
	intervalAvgBufferGets := float64(deltaBufferGets) / float64(deltaExecCount)
	intervalAvgRowsProcessed := float64(deltaRowsProcessed) / float64(deltaExecCount)

	// Warn if we have executions but zero elapsed time
	if deltaElapsedMs == 0 && deltaExecCount > 0 {
		oic.logger.Warn("Zero elapsed time with non-zero executions - possible data issue or extremely fast query")
		oic.logger.Debug("Delta calculation for query")
	}

	// Update state for next scrape
	state.PrevExecutionCount = currentExecCount
	state.PrevTotalElapsedTimeMs = currentTotalElapsedMs
	state.PrevTotalCPUTimeMs = currentTotalCPUMs
	state.PrevTotalDiskReads = currentTotalDiskReads
	state.PrevTotalBufferGets = currentTotalBufferGets
	state.PrevTotalRowsProcessed = currentTotalRowsProcessed
	state.LastSeenTimestamp = now

	return &OracleIntervalMetrics{
		IntervalAvgElapsedTimeMs:   intervalAvgElapsedMs,
		IntervalAvgCPUTimeMs:       intervalAvgCPUMs,
		IntervalAvgDiskReads:       intervalAvgDiskReads,
		IntervalAvgBufferGets:      intervalAvgBufferGets,
		IntervalAvgRowsProcessed:   intervalAvgRowsProcessed,
		IntervalExecutionCount:     deltaExecCount,
		HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
		HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
		HistoricalAvgDiskReads:     historicalAvgDiskReads,
		HistoricalAvgBufferGets:    historicalAvgBufferGets,
		HistoricalAvgRowsProcessed: historicalAvgRowsProcessed,
		HistoricalExecutionCount:   currentExecCount,
		IsFirstScrape:              false,
		HasNewExecutions:           true,
		TimeSinceLastExecSec:       timeSinceLastExec,
	}
}

// CleanupStaleEntries removes entries based on TTL (inactivity) ONLY
func (oic *OracleIntervalCalculator) CleanupStaleEntries(now time.Time) {
	oic.stateCacheMutex.Lock()
	defer oic.stateCacheMutex.Unlock()

	// Only run cleanup periodically
	if now.Sub(oic.lastCacheCleanup) < 5*time.Minute {
		return
	}

	oic.lastCacheCleanup = now
	removedCount := 0

	for queryID, state := range oic.stateCache {
		age := now.Sub(state.LastSeenTimestamp)
		if age > oic.cacheTTL {
			delete(oic.stateCache, queryID)
			removedCount++
		}
	}

	if removedCount > 0 {
		oic.logger.Info("Cleaned up stale queries from cache")
	}
}

// GetCacheStats returns cache statistics
func (oic *OracleIntervalCalculator) GetCacheStats() map[string]interface{} {
	oic.stateCacheMutex.RLock()
	defer oic.stateCacheMutex.RUnlock()

	return map[string]interface{}{
		"total_queries_tracked": len(oic.stateCache),
		"cache_ttl_minutes":     oic.cacheTTL.Minutes(),
	}
}

// Reset clears all state (useful for testing)
func (oic *OracleIntervalCalculator) Reset() {
	oic.stateCacheMutex.Lock()
	defer oic.stateCacheMutex.Unlock()

	oic.stateCache = make(map[string]*OracleQueryState)
	oic.logger.Info("Oracle interval calculator cache reset")
}
