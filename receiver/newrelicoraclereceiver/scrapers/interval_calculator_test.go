// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/models"
)

func TestNewOracleIntervalCalculator(t *testing.T) {
	logger := zap.NewNop()
	ttl := 10 * time.Minute

	calc := NewOracleIntervalCalculator(logger, ttl)

	assert.NotNil(t, calc)
	assert.Equal(t, ttl, calc.cacheTTL)
	assert.NotNil(t, calc.stateCache)
	assert.Empty(t, calc.stateCache)
}

func TestNewOracleIntervalCalculator_InvalidTTL(t *testing.T) {
	logger := zap.NewNop()
	ttl := -5 * time.Minute

	calc := NewOracleIntervalCalculator(logger, ttl)

	assert.NotNil(t, calc)
	assert.Equal(t, 10*time.Minute, calc.cacheTTL)
}

func TestNewOracleIntervalCalculator_ZeroTTL(t *testing.T) {
	logger := zap.NewNop()
	ttl := 0 * time.Minute

	calc := NewOracleIntervalCalculator(logger, ttl)

	assert.NotNil(t, calc)
	assert.Equal(t, 10*time.Minute, calc.cacheTTL)
}

func TestCalculateMetrics_NilQuery(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)

	metrics := calc.CalculateMetrics(nil, time.Now())

	assert.Nil(t, metrics)
}

func TestCalculateMetrics_EmptyQueryID(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)

	query := &models.SlowQuery{
		QueryID: sql.NullString{String: "", Valid: true},
	}

	metrics := calc.CalculateMetrics(query, time.Now())

	assert.Nil(t, metrics)
}

func TestCalculateMetrics_FirstScrape(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
		TotalCPUTimeMS:     sql.NullFloat64{Float64: 3000.0, Valid: true}, // 100 * 30 = 3000
	}

	metrics := calc.CalculateMetrics(query, now)

	assert.NotNil(t, metrics)
	assert.Equal(t, 50.0, metrics.IntervalAvgElapsedTimeMs)
	assert.Equal(t, int64(100), metrics.IntervalExecutionCount)
	assert.Equal(t, 50.0, metrics.HistoricalAvgElapsedTimeMs)
	assert.Equal(t, 30.0, metrics.HistoricalAvgCPUTimeMs)
	assert.Equal(t, int64(100), metrics.HistoricalExecutionCount)
	assert.True(t, metrics.IsFirstScrape)
	assert.True(t, metrics.HasNewExecutions)
}

func TestCalculateMetrics_FirstScrape_ZeroExecutions(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 0, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 0.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query, time.Now())

	assert.Nil(t, metrics)
}

func TestCalculateMetrics_SubsequentScrape_WithNewExecutions(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
		TotalCPUTimeMS:     sql.NullFloat64{Float64: 3000.0, Valid: true}, // 100 * 30 = 3000
	}

	// First scrape
	calc.CalculateMetrics(query1, now)

	// Second scrape with new executions
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 120, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 7000.0, Valid: true},
		TotalCPUTimeMS:     sql.NullFloat64{Float64: 4200.0, Valid: true}, // 120 * 35 = 4200
	}

	metrics := calc.CalculateMetrics(query2, now.Add(1*time.Minute))

	assert.NotNil(t, metrics)
	// Delta: (7000 - 5000) / (120 - 100) = 2000 / 20 = 100
	assert.Equal(t, 100.0, metrics.IntervalAvgElapsedTimeMs)
	assert.Equal(t, int64(20), metrics.IntervalExecutionCount)
	assert.InDelta(t, 58.33, metrics.HistoricalAvgElapsedTimeMs, 0.01)
	assert.Equal(t, 35.0, metrics.HistoricalAvgCPUTimeMs) // 4200 / 120 = 35
	assert.Equal(t, int64(120), metrics.HistoricalExecutionCount)
	assert.False(t, metrics.IsFirstScrape)
	assert.True(t, metrics.HasNewExecutions)
}

func TestCalculateMetrics_SubsequentScrape_NoNewExecutions(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query1, now)

	// Second scrape with same execution count
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query2, now.Add(1*time.Minute))

	assert.NotNil(t, metrics)
	assert.Equal(t, 0.0, metrics.IntervalAvgElapsedTimeMs)
	assert.Equal(t, int64(0), metrics.IntervalExecutionCount)
	assert.False(t, metrics.IsFirstScrape)
	assert.False(t, metrics.HasNewExecutions)
}

func TestCalculateMetrics_PlanCacheReset_NegativeExecCount(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query1, now)

	// Cache reset - execution count decreased
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 50, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 2500.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query2, now.Add(1*time.Minute))

	assert.NotNil(t, metrics)
	assert.Equal(t, 50.0, metrics.IntervalAvgElapsedTimeMs)
	assert.Equal(t, int64(50), metrics.IntervalExecutionCount)
	assert.True(t, metrics.IsFirstScrape)
	assert.True(t, metrics.HasNewExecutions)
}

func TestCalculateMetrics_NegativeElapsedTime(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query1, now)

	// Stats corruption - elapsed time decreased
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 120, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 4000.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query2, now.Add(1*time.Minute))

	assert.NotNil(t, metrics)
	assert.InDelta(t, 33.33, metrics.IntervalAvgElapsedTimeMs, 0.01)
	assert.Equal(t, int64(120), metrics.IntervalExecutionCount)
	assert.True(t, metrics.IsFirstScrape)
}

func TestCalculateMetrics_ZeroElapsedWithExecutions(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query1, now)

	// New executions but zero elapsed time
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 120, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query2, now.Add(1*time.Minute))

	assert.NotNil(t, metrics)
	assert.Equal(t, 0.0, metrics.IntervalAvgElapsedTimeMs)
	assert.Equal(t, int64(20), metrics.IntervalExecutionCount)
	assert.True(t, metrics.HasNewExecutions)
}

func TestCalculateMetrics_WithLastActiveTime(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	lastActive := now.Add(-5 * time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
		LastActiveTime:     sql.NullString{String: lastActive.Format("2006-01-02/15:04:05"), Valid: true},
	}

	metrics := calc.CalculateMetrics(query, now)

	assert.NotNil(t, metrics)
	assert.InDelta(t, 300.0, metrics.TimeSinceLastExecSec, 1.0)
}

func TestCalculateMetrics_InvalidLastActiveTime(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
		LastActiveTime:     sql.NullString{String: "invalid-date", Valid: true},
	}

	metrics := calc.CalculateMetrics(query, time.Now())

	assert.NotNil(t, metrics)
	assert.Equal(t, 0.0, metrics.TimeSinceLastExecSec)
}

func TestCalculateMetrics_NullFields(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Valid: false},
		TotalElapsedTimeMS: sql.NullFloat64{Valid: false},
		TotalCPUTimeMS:     sql.NullFloat64{Valid: false},
	}

	metrics := calc.CalculateMetrics(query, time.Now())

	assert.Nil(t, metrics)
}

func TestCleanupStaleEntries_NoCleanupNeeded(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query, now)
	calc.CleanupStaleEntries(now.Add(1 * time.Minute))

	stats := calc.GetCacheStats()
	assert.Equal(t, 1, stats["total_queries_tracked"])
}

func TestCleanupStaleEntries_RemoveStaleEntries(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query, now)

	// Wait past TTL and trigger cleanup
	calc.lastCacheCleanup = now // Force cleanup to run
	calc.CleanupStaleEntries(now.Add(15 * time.Minute))

	stats := calc.GetCacheStats()
	assert.Equal(t, 0, stats["total_queries_tracked"])
}

func TestCleanupStaleEntries_SkipIfRecentlyRan(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query, now)
	calc.lastCacheCleanup = now

	// Try cleanup too soon
	calc.CleanupStaleEntries(now.Add(1 * time.Minute))

	stats := calc.GetCacheStats()
	assert.Equal(t, 1, stats["total_queries_tracked"])
}

func TestGetCacheStats(t *testing.T) {
	logger := zap.NewNop()
	ttl := 15 * time.Minute
	calc := NewOracleIntervalCalculator(logger, ttl)

	stats := calc.GetCacheStats()

	assert.Equal(t, 0, stats["total_queries_tracked"])
	assert.Equal(t, ttl.Minutes(), stats["cache_ttl_minutes"])
}

func TestGetCacheStats_WithEntries(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	for i := 0; i < 5; i++ {
		query := &models.SlowQuery{
			QueryID:            sql.NullString{String: string(rune('a' + i)), Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
		}
		calc.CalculateMetrics(query, now)
	}

	stats := calc.GetCacheStats()
	assert.Equal(t, 5, stats["total_queries_tracked"])
}

func TestReset(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query, now)
	assert.Len(t, calc.stateCache, 1)

	calc.Reset()

	assert.Empty(t, calc.stateCache)
}

func TestCalculateMetrics_PreserveFirstSeenTimestamp(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
	}

	calc.CalculateMetrics(query1, now)

	firstTimestamp := calc.stateCache["test_query_1"].FirstSeenTimestamp

	// Cache reset scenario
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 50, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 2500.0, Valid: true},
	}

	calc.CalculateMetrics(query2, now.Add(1*time.Minute))

	// FirstSeenTimestamp should be preserved
	assert.Equal(t, firstTimestamp, calc.stateCache["test_query_1"].FirstSeenTimestamp)
}

func TestCalculateMetrics_MultipleQueries(t *testing.T) {
	logger := zap.NewNop()
	calc := NewOracleIntervalCalculator(logger, 10*time.Minute)
	now := time.Now()

	queries := []*models.SlowQuery{
		{
			QueryID:            sql.NullString{String: "query_1", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS: sql.NullFloat64{Float64: 5000.0, Valid: true},
		},
		{
			QueryID:            sql.NullString{String: "query_2", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 200, Valid: true},
			TotalElapsedTimeMS: sql.NullFloat64{Float64: 10000.0, Valid: true},
		},
	}

	for _, q := range queries {
		metrics := calc.CalculateMetrics(q, now)
		assert.NotNil(t, metrics)
		assert.True(t, metrics.IsFirstScrape)
	}

	stats := calc.GetCacheStats()
	assert.Equal(t, 2, stats["total_queries_tracked"])
}
