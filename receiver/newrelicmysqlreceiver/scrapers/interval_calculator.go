// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
)

// ===========================================
// INTERVAL CALCULATOR - DELTA COMPUTATION
// ===========================================
// This calculator tracks cumulative metrics across scrapes and computes interval-based (delta) averages
// WHY: Historical averages can be misleading - a query slow "right now" needs immediate attention

// IntervalMetrics represents the calculated interval-based metrics for a single query
type IntervalMetrics struct {
	// Interval averages (delta metrics)
	IntervalAvgElapsedTimeMs float64 // Average elapsed time in the current interval
	IntervalExecutionCount   int64   // Number of executions in the current interval

	// Context fields (for logging/debugging)
	HistoricalAvgElapsedTimeMs float64 // All-time average from database (for comparison)
	HasNewExecutions           bool    // Whether this query had new executions since last scrape
	IsFirstScrape              bool    // Whether this is the first time we've seen this query
	TimeSinceLastExecSec       float64 // Time elapsed since last scrape (for staleness detection)
}

// queryState represents the last known state of a query (used for delta calculation)
type queryState struct {
	// Cumulative counters from previous scrape
	LastTotalElapsedTimeMs float64
	LastExecutionCount     int64

	// Metadata
	LastSeenTime time.Time // When we last saw this query (for TTL-based cleanup)
}

// MySQLIntervalCalculator tracks query state across scrapes and computes interval metrics
// Thread-safe: Uses mutex for concurrent access protection
type MySQLIntervalCalculator struct {
	logger   *zap.Logger
	mu       sync.RWMutex                // Protects concurrent access to state map
	state    map[string]*queryState      // Query ID -> last known state
	cacheTTL time.Duration               // How long to keep stale entries (e.g., 60 minutes)
}

// NewMySQLIntervalCalculator creates a new interval calculator
// Parameters:
//   - logger: Logger for debugging and statistics
//   - cacheTTL: Time-to-live for cached query states (e.g., 60 minutes)
//                After this period, if a query hasn't been seen, its state is removed
func NewMySQLIntervalCalculator(logger *zap.Logger, cacheTTL time.Duration) *MySQLIntervalCalculator {
	return &MySQLIntervalCalculator{
		logger:   logger,
		state:    make(map[string]*queryState),
		cacheTTL: cacheTTL,
	}
}

// CalculateMetrics computes interval-based metrics by comparing current values with previous scrape
// This is the CORE FUNCTION that enables accurate Top N selection
//
// Parameters:
//   - query: Current slow query data from database (with cumulative counters)
//   - now: Current timestamp (for TTL-based cleanup)
//
// Returns:
//   - IntervalMetrics: Computed delta metrics, or nil if query is invalid
//
// Algorithm:
//   1. First scrape: No baseline → use historical averages (will be inaccurate but necessary)
//   2. Subsequent scrapes: Compute delta = (current - previous) / (executions_delta)
//   3. Handle edge cases: No new executions, missing data, etc.
func (calc *MySQLIntervalCalculator) CalculateMetrics(query *models.SlowQuery, now time.Time) *IntervalMetrics {
	// Validation: Ensure we have the required fields
	if !query.HasValidExecutionCount() || !query.HasValidTotalElapsedTime() {
		calc.logger.Debug("Query missing required fields for interval calculation",
			zap.String("query_id", query.GetQueryID()),
			zap.Bool("has_execution_count", query.HasValidExecutionCount()),
			zap.Bool("has_total_elapsed_time", query.HasValidTotalElapsedTime()))
		return nil
	}

	queryID := query.GetQueryID()
	currentExecCount := query.ExecutionCount.Int64
	currentTotalElapsedMs := query.TotalElapsedTimeMS.Float64

	// Historical average from database (for comparison/fallback)
	historicalAvgMs := 0.0
	if query.AvgElapsedTimeMs.Valid {
		historicalAvgMs = query.AvgElapsedTimeMs.Float64
	}

	// Thread-safe state access
	calc.mu.Lock()
	defer calc.mu.Unlock()

	// Check if we've seen this query before
	previousState, exists := calc.state[queryID]

	if !exists {
		// ===========================================
		// FIRST SCRAPE FOR THIS QUERY
		// ===========================================
		// No baseline exists, so we can't compute a true interval average
		// Fallback: Use historical average from database (imperfect but necessary)

		calc.logger.Debug("First scrape for query, using historical average as interval average",
			zap.String("query_id", queryID),
			zap.Int64("execution_count", currentExecCount),
			zap.Float64("historical_avg_ms", historicalAvgMs))

		// Store current state as baseline for next scrape
		calc.state[queryID] = &queryState{
			LastTotalElapsedTimeMs: currentTotalElapsedMs,
			LastExecutionCount:     currentExecCount,
			LastSeenTime:           now,
		}

		// Return historical average as interval average (first scrape only)
		return &IntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgMs,
			IntervalExecutionCount:     currentExecCount,
			HistoricalAvgElapsedTimeMs: historicalAvgMs,
			HasNewExecutions:           true, // First scrape always counts as "new"
			IsFirstScrape:              true,
			TimeSinceLastExecSec:       0,
		}
	}

	// ===========================================
	// SUBSEQUENT SCRAPES - COMPUTE DELTA
	// ===========================================

	timeSinceLastScrape := now.Sub(previousState.LastSeenTime).Seconds()

	// Check if there were new executions since last scrape
	executionDelta := currentExecCount - previousState.LastExecutionCount
	if executionDelta <= 0 {
		// No new executions - query hasn't run since last scrape
		// Don't update state, don't return metrics
		calc.logger.Debug("No new executions since last scrape",
			zap.String("query_id", queryID),
			zap.Int64("current_exec_count", currentExecCount),
			zap.Int64("previous_exec_count", previousState.LastExecutionCount),
			zap.Float64("time_since_last_scrape_sec", timeSinceLastScrape))

		// Update LastSeenTime but keep counters unchanged
		previousState.LastSeenTime = now

		return &IntervalMetrics{
			IntervalAvgElapsedTimeMs:   0,
			IntervalExecutionCount:     0,
			HistoricalAvgElapsedTimeMs: historicalAvgMs,
			HasNewExecutions:           false,
			IsFirstScrape:              false,
			TimeSinceLastExecSec:       timeSinceLastScrape,
		}
	}

	// Compute elapsed time delta
	elapsedTimeDelta := currentTotalElapsedMs - previousState.LastTotalElapsedTimeMs

	// Handle edge case: Counter reset (server restart, performance_schema reset)
	// If current value < previous value, treat as first scrape
	if elapsedTimeDelta < 0 {
		calc.logger.Warn("Detected counter reset (total_elapsed_time decreased), treating as first scrape",
			zap.String("query_id", queryID),
			zap.Float64("current_total_ms", currentTotalElapsedMs),
			zap.Float64("previous_total_ms", previousState.LastTotalElapsedTimeMs))

		// Reset state
		calc.state[queryID] = &queryState{
			LastTotalElapsedTimeMs: currentTotalElapsedMs,
			LastExecutionCount:     currentExecCount,
			LastSeenTime:           now,
		}

		return &IntervalMetrics{
			IntervalAvgElapsedTimeMs:   historicalAvgMs,
			IntervalExecutionCount:     currentExecCount,
			HistoricalAvgElapsedTimeMs: historicalAvgMs,
			HasNewExecutions:           true,
			IsFirstScrape:              true, // Treat reset as first scrape
			TimeSinceLastExecSec:       timeSinceLastScrape,
		}
	}

	// ===========================================
	// COMPUTE INTERVAL AVERAGE
	// ===========================================
	// Interval Avg = (Delta Total Time) / (Delta Execution Count)
	// Example: (3000ms) / (100 executions) = 30ms per execution

	intervalAvgElapsedMs := elapsedTimeDelta / float64(executionDelta)

	calc.logger.Debug("Computed interval metrics",
		zap.String("query_id", queryID),
		zap.Int64("execution_delta", executionDelta),
		zap.Float64("elapsed_time_delta_ms", elapsedTimeDelta),
		zap.Float64("interval_avg_ms", intervalAvgElapsedMs),
		zap.Float64("historical_avg_ms", historicalAvgMs),
		zap.Float64("time_since_last_scrape_sec", timeSinceLastScrape))

	// Update state for next scrape
	calc.state[queryID] = &queryState{
		LastTotalElapsedTimeMs: currentTotalElapsedMs,
		LastExecutionCount:     currentExecCount,
		LastSeenTime:           now,
	}

	return &IntervalMetrics{
		IntervalAvgElapsedTimeMs:   intervalAvgElapsedMs,
		IntervalExecutionCount:     executionDelta,
		HistoricalAvgElapsedTimeMs: historicalAvgMs,
		HasNewExecutions:           true,
		IsFirstScrape:              false,
		TimeSinceLastExecSec:       timeSinceLastScrape,
	}
}

// CleanupStaleEntries removes query states that haven't been seen within the TTL period
// This prevents memory leaks from queries that are no longer executed
//
// Example: If cacheTTL = 60 minutes and a query hasn't been seen for 61 minutes, remove it
//
// Parameters:
//   - now: Current timestamp for TTL comparison
//
// Returns: Number of stale entries removed
func (calc *MySQLIntervalCalculator) CleanupStaleEntries(now time.Time) int {
	calc.mu.Lock()
	defer calc.mu.Unlock()

	removedCount := 0
	for queryID, state := range calc.state {
		// Check if entry has exceeded TTL
		timeSinceLastSeen := now.Sub(state.LastSeenTime)
		if timeSinceLastSeen > calc.cacheTTL {
			calc.logger.Debug("Removing stale query state",
				zap.String("query_id", queryID),
				zap.Float64("time_since_last_seen_minutes", timeSinceLastSeen.Minutes()),
				zap.Float64("cache_ttl_minutes", calc.cacheTTL.Minutes()))

			delete(calc.state, queryID)
			removedCount++
		}
	}

	if removedCount > 0 {
		calc.logger.Info("Cleaned up stale query states",
			zap.Int("removed_count", removedCount),
			zap.Int("remaining_count", len(calc.state)))
	}

	return removedCount
}

// GetCacheStats returns statistics about the internal state cache
// Useful for monitoring and debugging
//
// Returns: Map with statistics
func (calc *MySQLIntervalCalculator) GetCacheStats() map[string]interface{} {
	calc.mu.RLock()
	defer calc.mu.RUnlock()

	return map[string]interface{}{
		"tracked_queries":   len(calc.state),
		"cache_ttl_minutes": calc.cacheTTL.Minutes(),
	}
}
