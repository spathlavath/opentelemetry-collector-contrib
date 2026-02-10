// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package scrapers // import "github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/scrapers"

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/models"
)

// OracleQueryState tracks previous scrape data for delta calculation
type OracleQueryState struct {
	PrevExecutionCount     int64
	PrevTotalElapsedTimeMs float64 // milliseconds
	PrevTotalCPUTimeMs     float64 // milliseconds
	PrevTotalWaitTimeMs    float64 // milliseconds
	PrevTotalDiskReads     int64   // disk reads
	PrevTotalDiskWrites    int64   // direct writes (bypass buffer cache)
	PrevTotalBufferGets    int64   // buffer gets (logical reads)
	PrevTotalRowsProcessed int64   // rows processed (rows returned)

	// Timestamps for TTL-based cleanup
	LastSeenTimestamp  time.Time
	FirstSeenTimestamp time.Time
}

// OracleIntervalMetrics holds both interval and historical metrics
type OracleIntervalMetrics struct {
	// Interval-based average metrics (delta calculation with per-execution averaging)
	IntervalAvgElapsedTimeMs float64
	IntervalAvgCPUTimeMs     float64
	IntervalAvgWaitTimeMs    float64
	IntervalAvgDiskReads     float64
	IntervalAvgDiskWrites    float64
	IntervalAvgBufferGets    float64
	IntervalAvgRowsProcessed float64
	IntervalExecutionCount   int64

	// Interval-based total metrics (delta calculation without averaging)
	IntervalElapsedTimeMs float64
	IntervalCPUTimeMs     float64
	IntervalWaitTimeMs    float64
	IntervalDiskReads     float64
	IntervalDiskWrites    float64
	IntervalBufferGets    float64
	IntervalRowsProcessed float64

	// Historical metrics (cumulative from Oracle)
	HistoricalAvgElapsedTimeMs float64
	HistoricalAvgCPUTimeMs     float64
	HistoricalAvgWaitTimeMs    float64
	HistoricalAvgDiskReads     float64
	HistoricalAvgDiskWrites    float64
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

	currentTotalWaitMs := 0.0
	if query.TotalWaitTimeMS.Valid {
		currentTotalWaitMs = query.TotalWaitTimeMS.Float64
	}

	currentTotalDiskReads := int64(0)
	if query.TotalDiskReads.Valid {
		currentTotalDiskReads = query.TotalDiskReads.Int64
	}

	currentTotalDiskWrites := int64(0)
	if query.TotalDiskWrites.Valid {
		currentTotalDiskWrites = query.TotalDiskWrites.Int64
	}

	currentTotalBufferGets := int64(0)
	if query.TotalBufferGets.Valid {
		currentTotalBufferGets = query.TotalBufferGets.Int64
	}

	currentTotalRowsProcessed := int64(0)
	if query.TotalRowsProcessed.Valid {
		currentTotalRowsProcessed = query.TotalRowsProcessed.Int64
	}

	// Compute historical averages from totals / executions
	historicalAvgElapsedMs := 0.0
	if currentExecCount > 0 {
		historicalAvgElapsedMs = currentTotalElapsedMs / float64(currentExecCount)
	}

	historicalAvgCPUMs := 0.0
	if currentExecCount > 0 {
		historicalAvgCPUMs = currentTotalCPUMs / float64(currentExecCount)
	}

	historicalAvgWaitMs := 0.0
	if currentExecCount > 0 {
		historicalAvgWaitMs = currentTotalWaitMs / float64(currentExecCount)
	}

	historicalAvgDiskReads := 0.0
	if currentExecCount > 0 {
		historicalAvgDiskReads = float64(currentTotalDiskReads) / float64(currentExecCount)
	}

	historicalAvgDiskWrites := 0.0
	if currentExecCount > 0 {
		historicalAvgDiskWrites = float64(currentTotalDiskWrites) / float64(currentExecCount)
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
			PrevTotalWaitTimeMs:    currentTotalWaitMs,
			PrevTotalDiskReads:     currentTotalDiskReads,
			PrevTotalDiskWrites:    currentTotalDiskWrites,
			PrevTotalBufferGets:    currentTotalBufferGets,
			PrevTotalRowsProcessed: currentTotalRowsProcessed,
			FirstSeenTimestamp:     now,
			LastSeenTimestamp:      now,
		}

		// For first scrape, use historical average as interval average
		// Use currentExecCount as interval count since this represents all executions since plan cache
		// For interval totals, use the cumulative totals from Oracle (since this is first scrape)
		// Caller will filter by threshold
		return &OracleIntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgElapsedMs,
			IntervalAvgCPUTimeMs:       historicalAvgCPUMs,
			IntervalAvgWaitTimeMs:      historicalAvgWaitMs,
			IntervalAvgDiskReads:       historicalAvgDiskReads,
			IntervalAvgDiskWrites:      historicalAvgDiskWrites,
			IntervalAvgBufferGets:      historicalAvgBufferGets,
			IntervalAvgRowsProcessed:   historicalAvgRowsProcessed,
			IntervalExecutionCount:     currentExecCount, // All executions since plan cache
			IntervalElapsedTimeMs:      currentTotalElapsedMs,
			IntervalCPUTimeMs:          currentTotalCPUMs,
			IntervalWaitTimeMs:         currentTotalWaitMs,
			IntervalDiskReads:          float64(currentTotalDiskReads),
			IntervalDiskWrites:         float64(currentTotalDiskWrites),
			IntervalBufferGets:         float64(currentTotalBufferGets),
			IntervalRowsProcessed:      float64(currentTotalRowsProcessed),
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalAvgWaitTimeMs:    historicalAvgWaitMs,
			HistoricalAvgDiskReads:     historicalAvgDiskReads,
			HistoricalAvgDiskWrites:    historicalAvgDiskWrites,
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
			IntervalAvgWaitTimeMs:      0,
			IntervalAvgDiskReads:       0,
			IntervalAvgDiskWrites:      0,
			IntervalAvgBufferGets:      0,
			IntervalAvgRowsProcessed:   0,
			IntervalExecutionCount:     0,
			IntervalElapsedTimeMs:      0,
			IntervalCPUTimeMs:          0,
			IntervalWaitTimeMs:         0,
			IntervalDiskReads:          0,
			IntervalDiskWrites:         0,
			IntervalBufferGets:         0,
			IntervalRowsProcessed:      0,
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalAvgWaitTimeMs:    historicalAvgWaitMs,
			HistoricalAvgDiskReads:     historicalAvgDiskReads,
			HistoricalAvgDiskWrites:    historicalAvgDiskWrites,
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
			PrevTotalWaitTimeMs:    currentTotalWaitMs,
			PrevTotalDiskReads:     currentTotalDiskReads,
			PrevTotalDiskWrites:    currentTotalDiskWrites,
			PrevTotalBufferGets:    currentTotalBufferGets,
			PrevTotalRowsProcessed: currentTotalRowsProcessed,
			FirstSeenTimestamp:     state.FirstSeenTimestamp, // Preserve original timestamp
			LastSeenTimestamp:      now,
		}

		// After cache reset, we cannot calculate a valid interval delta
		// Use historical average but set interval count to current count (all execs since reset)
		// For interval totals, use the cumulative totals from Oracle (since this is like first scrape)
		// Alternative: Could skip emitting by setting HasNewExecutions: false
		return &OracleIntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgElapsedMs,
			IntervalAvgCPUTimeMs:       historicalAvgCPUMs,
			IntervalAvgWaitTimeMs:      historicalAvgWaitMs,
			IntervalAvgDiskReads:       historicalAvgDiskReads,
			IntervalAvgDiskWrites:      historicalAvgDiskWrites,
			IntervalAvgBufferGets:      historicalAvgBufferGets,
			IntervalAvgRowsProcessed:   historicalAvgRowsProcessed,
			IntervalExecutionCount:     currentExecCount, // All executions since cache reset
			IntervalElapsedTimeMs:      currentTotalElapsedMs,
			IntervalCPUTimeMs:          currentTotalCPUMs,
			IntervalWaitTimeMs:         currentTotalWaitMs,
			IntervalDiskReads:          float64(currentTotalDiskReads),
			IntervalDiskWrites:         float64(currentTotalDiskWrites),
			IntervalBufferGets:         float64(currentTotalBufferGets),
			IntervalRowsProcessed:      float64(currentTotalRowsProcessed),
			HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
			HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
			HistoricalAvgWaitTimeMs:    historicalAvgWaitMs,
			HistoricalAvgDiskReads:     historicalAvgDiskReads,
			HistoricalAvgDiskWrites:    historicalAvgDiskWrites,
			HistoricalAvgBufferGets:    historicalAvgBufferGets,
			HistoricalAvgRowsProcessed: historicalAvgRowsProcessed,
			HistoricalExecutionCount:   currentExecCount,
			IsFirstScrape:              true, // Treat as first scrape after reset
			HasNewExecutions:           true,
			TimeSinceLastExecSec:       timeSinceLastExec,
		}
	}

	// NORMAL CASE: Calculate interval metrics (both averages and totals)
	deltaCPUMs := currentTotalCPUMs - state.PrevTotalCPUTimeMs
	deltaWaitMs := currentTotalWaitMs - state.PrevTotalWaitTimeMs
	deltaDiskReads := currentTotalDiskReads - state.PrevTotalDiskReads
	deltaDiskWrites := currentTotalDiskWrites - state.PrevTotalDiskWrites
	deltaBufferGets := currentTotalBufferGets - state.PrevTotalBufferGets
	deltaRowsProcessed := currentTotalRowsProcessed - state.PrevTotalRowsProcessed

	// Calculate interval averages (delta divided by execution count)
	intervalAvgElapsedMs := deltaElapsedMs / float64(deltaExecCount)
	intervalAvgCPUMs := deltaCPUMs / float64(deltaExecCount)
	intervalAvgWaitMs := deltaWaitMs / float64(deltaExecCount)
	intervalAvgDiskReads := float64(deltaDiskReads) / float64(deltaExecCount)
	intervalAvgDiskWrites := float64(deltaDiskWrites) / float64(deltaExecCount)
	intervalAvgBufferGets := float64(deltaBufferGets) / float64(deltaExecCount)
	intervalAvgRowsProcessed := float64(deltaRowsProcessed) / float64(deltaExecCount)

	// Interval totals are just the delta values themselves (not divided)
	intervalElapsedMs := deltaElapsedMs
	intervalCPUMs := deltaCPUMs
	intervalWaitMs := deltaWaitMs
	intervalDiskReads := float64(deltaDiskReads)
	intervalDiskWrites := float64(deltaDiskWrites)
	intervalBufferGets := float64(deltaBufferGets)
	intervalRowsProcessed := float64(deltaRowsProcessed)

	// Warn if we have executions but zero elapsed time
	if deltaElapsedMs == 0 && deltaExecCount > 0 {
		oic.logger.Warn("Zero elapsed time with non-zero executions - possible data issue or extremely fast query")
		oic.logger.Debug("Delta calculation for query")
	}

	// Update state for next scrape
	state.PrevExecutionCount = currentExecCount
	state.PrevTotalElapsedTimeMs = currentTotalElapsedMs
	state.PrevTotalCPUTimeMs = currentTotalCPUMs
	state.PrevTotalWaitTimeMs = currentTotalWaitMs
	state.PrevTotalDiskReads = currentTotalDiskReads
	state.PrevTotalDiskWrites = currentTotalDiskWrites
	state.PrevTotalBufferGets = currentTotalBufferGets
	state.PrevTotalRowsProcessed = currentTotalRowsProcessed
	state.LastSeenTimestamp = now

	return &OracleIntervalMetrics{
		IntervalAvgElapsedTimeMs:   intervalAvgElapsedMs,
		IntervalAvgCPUTimeMs:       intervalAvgCPUMs,
		IntervalAvgWaitTimeMs:      intervalAvgWaitMs,
		IntervalAvgDiskReads:       intervalAvgDiskReads,
		IntervalAvgDiskWrites:      intervalAvgDiskWrites,
		IntervalAvgBufferGets:      intervalAvgBufferGets,
		IntervalAvgRowsProcessed:   intervalAvgRowsProcessed,
		IntervalExecutionCount:     deltaExecCount,
		IntervalElapsedTimeMs:      intervalElapsedMs,
		IntervalCPUTimeMs:          intervalCPUMs,
		IntervalWaitTimeMs:         intervalWaitMs,
		IntervalDiskReads:          intervalDiskReads,
		IntervalDiskWrites:         intervalDiskWrites,
		IntervalBufferGets:         intervalBufferGets,
		IntervalRowsProcessed:      intervalRowsProcessed,
		HistoricalAvgElapsedTimeMs: historicalAvgElapsedMs,
		HistoricalAvgCPUTimeMs:     historicalAvgCPUMs,
		HistoricalAvgWaitTimeMs:    historicalAvgWaitMs,
		HistoricalAvgDiskReads:     historicalAvgDiskReads,
		HistoricalAvgDiskWrites:    historicalAvgDiskWrites,
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
func (oic *OracleIntervalCalculator) GetCacheStats() map[string]any {
	oic.stateCacheMutex.RLock()
	defer oic.stateCacheMutex.RUnlock()

	return map[string]any{
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
