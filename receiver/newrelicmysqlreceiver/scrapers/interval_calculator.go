// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
)

// MySQLIntervalCalculator calculates interval-based (delta) metrics for MySQL slow queries.
// This follows the Oracle receiver pattern for simplicity and proven stability.
//
// Key Features:
// - Delta calculation: Compares current vs. previous scrape to get interval metrics
// - TTL-based cache eviction: Automatically removes stale entries
// - Handles edge cases: First scrape, no new executions, cache misses
//
// Design Decision: Following Oracle's OracleIntervalCalculator pattern for simplicity
type MySQLIntervalCalculator struct {
	mu       sync.RWMutex
	cache    map[string]*QuerySnapshot // Key: query_id (DIGEST)
	logger   *zap.Logger
	cacheTTL time.Duration
}

// QuerySnapshot represents a snapshot of query metrics at a specific point in time
type QuerySnapshot struct {
	TotalElapsedTimeMS float64
	ExecutionCount     int64
	LastSeen           time.Time
	LastUpdated        time.Time
}

// IntervalMetrics represents the calculated metrics for the current interval
type IntervalMetrics struct {
	// Interval-based (delta) metrics
	IntervalAvgElapsedTimeMs float64
	IntervalExecutionCount   int64

	// Historical metrics (for reference)
	HistoricalAvgElapsedTimeMs float64
	HistoricalExecutionCount   int64

	// Flags
	HasNewExecutions bool // True if query executed in this interval
	IsFirstScrape    bool // True if this is the first time seeing this query
}

// NewMySQLIntervalCalculator creates a new interval calculator
func NewMySQLIntervalCalculator(logger *zap.Logger, cacheTTL time.Duration) *MySQLIntervalCalculator {
	return &MySQLIntervalCalculator{
		cache:    make(map[string]*QuerySnapshot),
		logger:   logger,
		cacheTTL: cacheTTL,
	}
}

// CalculateMetrics calculates interval-based metrics for a slow query
// Returns nil if the query should be skipped (invalid data)
func (c *MySQLIntervalCalculator) CalculateMetrics(query *models.SlowQuery, now time.Time) *IntervalMetrics {
	if query == nil {
		return nil
	}

	// Validate required fields
	if !query.HasValidQueryID() {
		c.logger.Debug("Query missing valid query_id, skipping")
		return nil
	}

	if !query.TotalElapsedTimeMS.Valid || !query.ExecutionCount.Valid {
		c.logger.Debug("Query missing required metrics", zap.String("query_id", query.GetQueryID()))
		return nil
	}

	queryID := query.GetQueryID()
	currentTotalElapsedMS := query.TotalElapsedTimeMS.Float64
	currentExecutionCount := query.ExecutionCount.Int64
	currentAvgElapsedMS := query.AvgElapsedTimeMS.Float64

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we have a previous snapshot
	previousSnapshot, exists := c.cache[queryID]

	// Initialize result
	result := &IntervalMetrics{
		HistoricalAvgElapsedTimeMs: currentAvgElapsedMS,
		HistoricalExecutionCount:   currentExecutionCount,
		IsFirstScrape:              !exists,
	}

	if !exists {
		// First time seeing this query - use historical averages as interval metrics
		result.IntervalAvgElapsedTimeMs = currentAvgElapsedMS
		result.IntervalExecutionCount = currentExecutionCount
		result.HasNewExecutions = true

		// Store snapshot for next interval
		c.cache[queryID] = &QuerySnapshot{
			TotalElapsedTimeMS: currentTotalElapsedMS,
			ExecutionCount:     currentExecutionCount,
			LastSeen:           now,
			LastUpdated:        now,
		}

		c.logger.Debug("First scrape for query, using historical averages",
			zap.String("query_id", queryID),
			zap.Float64("avg_elapsed_ms", currentAvgElapsedMS),
			zap.Int64("execution_count", currentExecutionCount))

		return result
	}

	// Calculate delta (new executions since last scrape)
	deltaExecutionCount := currentExecutionCount - previousSnapshot.ExecutionCount
	deltaElapsedTimeMS := currentTotalElapsedMS - previousSnapshot.TotalElapsedTimeMS

	// Check for new executions
	if deltaExecutionCount <= 0 {
		// No new executions in this interval
		result.HasNewExecutions = false
		c.logger.Debug("No new executions in interval",
			zap.String("query_id", queryID),
			zap.Int64("current_count", currentExecutionCount),
			zap.Int64("previous_count", previousSnapshot.ExecutionCount))

		// Update last seen time (keep the snapshot alive)
		previousSnapshot.LastSeen = now
		return result
	}

	// Calculate interval-based average
	intervalAvgElapsedMS := deltaElapsedTimeMS / float64(deltaExecutionCount)

	result.IntervalAvgElapsedTimeMs = intervalAvgElapsedMS
	result.IntervalExecutionCount = deltaExecutionCount
	result.HasNewExecutions = true

	c.logger.Debug("Calculated interval metrics",
		zap.String("query_id", queryID),
		zap.Int64("delta_executions", deltaExecutionCount),
		zap.Float64("delta_elapsed_ms", deltaElapsedTimeMS),
		zap.Float64("interval_avg_ms", intervalAvgElapsedMS),
		zap.Float64("historical_avg_ms", currentAvgElapsedMS))

	// Update snapshot for next interval
	c.cache[queryID] = &QuerySnapshot{
		TotalElapsedTimeMS: currentTotalElapsedMS,
		ExecutionCount:     currentExecutionCount,
		LastSeen:           now,
		LastUpdated:        now,
	}

	return result
}

// CleanupStaleEntries removes entries that haven't been seen within the TTL window
func (c *MySQLIntervalCalculator) CleanupStaleEntries(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	staleCount := 0
	for queryID, snapshot := range c.cache {
		if now.Sub(snapshot.LastSeen) > c.cacheTTL {
			delete(c.cache, queryID)
			staleCount++
		}
	}

	if staleCount > 0 {
		c.logger.Debug("Cleaned up stale entries",
			zap.Int("stale_count", staleCount),
			zap.Int("remaining_count", len(c.cache)))
	}
}

// GetCacheStats returns statistics about the cache
func (c *MySQLIntervalCalculator) GetCacheStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"cache_size": len(c.cache),
		"cache_ttl":  c.cacheTTL.String(),
	}
}
