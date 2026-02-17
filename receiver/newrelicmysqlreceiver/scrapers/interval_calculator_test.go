// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
)

func TestNewMySQLIntervalCalculator(t *testing.T) {
	logger := zap.NewNop()
	cacheTTL := 10 * time.Minute

	calc := NewMySQLIntervalCalculator(logger, cacheTTL)

	require.NotNil(t, calc)
	assert.NotNil(t, calc.cache)
	assert.Equal(t, cacheTTL, calc.cacheTTL)
	assert.Equal(t, logger, calc.logger)
	assert.Equal(t, 0, len(calc.cache))
}

func TestMySQLIntervalCalculator_CalculateMetrics_FirstScrape(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true},
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	now := time.Now()
	metrics := calc.CalculateMetrics(query, now)

	require.NotNil(t, metrics)
	assert.True(t, metrics.IsFirstScrape, "Expected first scrape flag to be true")
	assert.True(t, metrics.HasNewExecutions, "Expected new executions flag to be true")
	assert.Equal(t, 1500.0, metrics.IntervalAvgElapsedTimeMs, "Expected interval avg to equal historical avg on first scrape")
	assert.Equal(t, int64(100), metrics.IntervalExecutionCount, "Expected interval count to equal total count on first scrape")
	assert.Equal(t, 1500.0, metrics.HistoricalAvgElapsedTimeMs)
	assert.Equal(t, int64(100), metrics.HistoricalExecutionCount)

	// Verify snapshot was stored
	assert.Equal(t, 1, len(calc.cache))
	snapshot, exists := calc.cache["test_query_1"]
	assert.True(t, exists)
	assert.Equal(t, 150000.0, snapshot.TotalElapsedTimeMS)
	assert.Equal(t, int64(100), snapshot.ExecutionCount)
}

func TestMySQLIntervalCalculator_CalculateMetrics_SubsequentScrape(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	// First scrape
	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true}, // 150 seconds total
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	now := time.Now()
	metrics1 := calc.CalculateMetrics(query1, now)
	require.NotNil(t, metrics1)
	assert.True(t, metrics1.IsFirstScrape)

	// Second scrape - 50 new executions, 90 seconds additional elapsed time
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 150, Valid: true},          // +50 executions
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 240000.0, Valid: true}, // +90 seconds = 240 seconds total
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1600.0, Valid: true},   // Historical average increased
	}

	now2 := now.Add(1 * time.Minute)
	metrics2 := calc.CalculateMetrics(query2, now2)

	require.NotNil(t, metrics2)
	assert.False(t, metrics2.IsFirstScrape, "Expected first scrape flag to be false")
	assert.True(t, metrics2.HasNewExecutions, "Expected new executions flag to be true")

	// Interval average = delta_elapsed / delta_executions = 90000ms / 50 = 1800ms
	expectedIntervalAvg := 90000.0 / 50.0
	assert.InDelta(t, expectedIntervalAvg, metrics2.IntervalAvgElapsedTimeMs, 0.01,
		"Expected interval avg to be calculated from delta")
	assert.Equal(t, int64(50), metrics2.IntervalExecutionCount, "Expected interval count of 50")
	assert.Equal(t, 1600.0, metrics2.HistoricalAvgElapsedTimeMs)
	assert.Equal(t, int64(150), metrics2.HistoricalExecutionCount)

	// Verify snapshot was updated
	snapshot, exists := calc.cache["test_query_1"]
	assert.True(t, exists)
	assert.Equal(t, 240000.0, snapshot.TotalElapsedTimeMS)
	assert.Equal(t, int64(150), snapshot.ExecutionCount)
}

func TestMySQLIntervalCalculator_CalculateMetrics_NoNewExecutions(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	// First scrape
	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true},
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	now := time.Now()
	metrics1 := calc.CalculateMetrics(query1, now)
	require.NotNil(t, metrics1)

	// Second scrape - same execution count (no new executions)
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query_1", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},          // Same count
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true}, // Same total
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	now2 := now.Add(1 * time.Minute)
	metrics2 := calc.CalculateMetrics(query2, now2)

	require.NotNil(t, metrics2)
	assert.False(t, metrics2.HasNewExecutions, "Expected no new executions flag")

	// Snapshot should still be updated with LastSeen time
	snapshot, exists := calc.cache["test_query_1"]
	assert.True(t, exists)
	assert.Equal(t, now2, snapshot.LastSeen)
}

func TestMySQLIntervalCalculator_CalculateMetrics_NilQuery(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	metrics := calc.CalculateMetrics(nil, time.Now())

	assert.Nil(t, metrics, "Expected nil metrics for nil query")
}

func TestMySQLIntervalCalculator_CalculateMetrics_InvalidQueryID(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{Valid: false}, // Invalid query ID
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true},
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query, time.Now())

	assert.Nil(t, metrics, "Expected nil metrics for invalid query ID")
}

func TestMySQLIntervalCalculator_CalculateMetrics_MissingRequiredFields(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	tests := []struct {
		name  string
		query *models.SlowQuery
	}{
		{
			name: "missing_total_elapsed_time",
			query: &models.SlowQuery{
				QueryID:            sql.NullString{String: "test_query", Valid: true},
				ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
				TotalElapsedTimeMS: sql.NullFloat64{Valid: false}, // Missing
				AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
			},
		},
		{
			name: "missing_execution_count",
			query: &models.SlowQuery{
				QueryID:            sql.NullString{String: "test_query", Valid: true},
				ExecutionCount:     sql.NullInt64{Valid: false}, // Missing
				TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true},
				AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := calc.CalculateMetrics(tt.query, time.Now())
			assert.Nil(t, metrics, "Expected nil metrics for missing required fields")
		})
	}
}

func TestMySQLIntervalCalculator_CleanupStaleEntries(t *testing.T) {
	cacheTTL := 5 * time.Minute
	calc := NewMySQLIntervalCalculator(zap.NewNop(), cacheTTL)

	now := time.Now()

	// Add multiple queries with different last seen times
	calc.cache["query1"] = &QuerySnapshot{
		TotalElapsedTimeMS: 100000.0,
		ExecutionCount:     100,
		LastSeen:           now.Add(-10 * time.Minute), // Stale (older than TTL)
		LastUpdated:        now.Add(-10 * time.Minute),
	}

	calc.cache["query2"] = &QuerySnapshot{
		TotalElapsedTimeMS: 200000.0,
		ExecutionCount:     200,
		LastSeen:           now.Add(-3 * time.Minute), // Fresh (within TTL)
		LastUpdated:        now.Add(-3 * time.Minute),
	}

	calc.cache["query3"] = &QuerySnapshot{
		TotalElapsedTimeMS: 300000.0,
		ExecutionCount:     300,
		LastSeen:           now.Add(-7 * time.Minute), // Stale (older than TTL)
		LastUpdated:        now.Add(-7 * time.Minute),
	}

	calc.cache["query4"] = &QuerySnapshot{
		TotalElapsedTimeMS: 400000.0,
		ExecutionCount:     400,
		LastSeen:           now.Add(-1 * time.Minute), // Fresh (within TTL)
		LastUpdated:        now.Add(-1 * time.Minute),
	}

	assert.Equal(t, 4, len(calc.cache), "Expected 4 entries before cleanup")

	calc.CleanupStaleEntries(now)

	// Only fresh entries should remain
	assert.Equal(t, 2, len(calc.cache), "Expected 2 entries after cleanup")
	assert.Contains(t, calc.cache, "query2", "Expected query2 to remain (fresh)")
	assert.Contains(t, calc.cache, "query4", "Expected query4 to remain (fresh)")
	assert.NotContains(t, calc.cache, "query1", "Expected query1 to be removed (stale)")
	assert.NotContains(t, calc.cache, "query3", "Expected query3 to be removed (stale)")
}

func TestMySQLIntervalCalculator_CleanupStaleEntries_EmptyCache(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 5*time.Minute)

	// Cleanup on empty cache should not panic
	calc.CleanupStaleEntries(time.Now())

	assert.Equal(t, 0, len(calc.cache))
}

func TestMySQLIntervalCalculator_CleanupStaleEntries_AllFresh(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 5*time.Minute)

	now := time.Now()

	// Add entries that are all within TTL
	calc.cache["query1"] = &QuerySnapshot{
		TotalElapsedTimeMS: 100000.0,
		ExecutionCount:     100,
		LastSeen:           now.Add(-1 * time.Minute),
		LastUpdated:        now.Add(-1 * time.Minute),
	}

	calc.cache["query2"] = &QuerySnapshot{
		TotalElapsedTimeMS: 200000.0,
		ExecutionCount:     200,
		LastSeen:           now.Add(-2 * time.Minute),
		LastUpdated:        now.Add(-2 * time.Minute),
	}

	calc.CleanupStaleEntries(now)

	// All entries should remain
	assert.Equal(t, 2, len(calc.cache))
	assert.Contains(t, calc.cache, "query1")
	assert.Contains(t, calc.cache, "query2")
}

func TestMySQLIntervalCalculator_CleanupStaleEntries_AllStale(t *testing.T) {
	cacheTTL := 5 * time.Minute
	calc := NewMySQLIntervalCalculator(zap.NewNop(), cacheTTL)

	now := time.Now()

	// Add entries that are all older than TTL
	calc.cache["query1"] = &QuerySnapshot{
		TotalElapsedTimeMS: 100000.0,
		ExecutionCount:     100,
		LastSeen:           now.Add(-10 * time.Minute),
		LastUpdated:        now.Add(-10 * time.Minute),
	}

	calc.cache["query2"] = &QuerySnapshot{
		TotalElapsedTimeMS: 200000.0,
		ExecutionCount:     200,
		LastSeen:           now.Add(-15 * time.Minute),
		LastUpdated:        now.Add(-15 * time.Minute),
	}

	calc.CleanupStaleEntries(now)

	// All entries should be removed
	assert.Equal(t, 0, len(calc.cache))
}

func TestMySQLIntervalCalculator_GetCacheStats(t *testing.T) {
	cacheTTL := 10 * time.Minute
	calc := NewMySQLIntervalCalculator(zap.NewNop(), cacheTTL)

	// Add some entries
	calc.cache["query1"] = &QuerySnapshot{
		TotalElapsedTimeMS: 100000.0,
		ExecutionCount:     100,
		LastSeen:           time.Now(),
		LastUpdated:        time.Now(),
	}

	calc.cache["query2"] = &QuerySnapshot{
		TotalElapsedTimeMS: 200000.0,
		ExecutionCount:     200,
		LastSeen:           time.Now(),
		LastUpdated:        time.Now(),
	}

	stats := calc.GetCacheStats()

	require.NotNil(t, stats)
	assert.Equal(t, 2, stats["cache_size"])
	assert.Equal(t, "10m0s", stats["cache_ttl"])
}

func TestMySQLIntervalCalculator_GetCacheStats_EmptyCache(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 5*time.Minute)

	stats := calc.GetCacheStats()

	require.NotNil(t, stats)
	assert.Equal(t, 0, stats["cache_size"])
	assert.Equal(t, "5m0s", stats["cache_ttl"])
}

func TestMySQLIntervalCalculator_MultipleQueriesConcurrency(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	// Simulate multiple queries being processed
	queries := []*models.SlowQuery{
		{
			QueryID:            sql.NullString{String: "query_1", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS: sql.NullFloat64{Float64: 100000.0, Valid: true},
			AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1000.0, Valid: true},
		},
		{
			QueryID:            sql.NullString{String: "query_2", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 200, Valid: true},
			TotalElapsedTimeMS: sql.NullFloat64{Float64: 300000.0, Valid: true},
			AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
		{
			QueryID:            sql.NullString{String: "query_3", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 50, Valid: true},
			TotalElapsedTimeMS: sql.NullFloat64{Float64: 75000.0, Valid: true},
			AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	now := time.Now()

	// First scrape
	for _, query := range queries {
		metrics := calc.CalculateMetrics(query, now)
		require.NotNil(t, metrics)
		assert.True(t, metrics.IsFirstScrape)
	}

	assert.Equal(t, 3, len(calc.cache))

	// Second scrape with updated values
	queries[0].ExecutionCount = sql.NullInt64{Int64: 150, Valid: true}
	queries[0].TotalElapsedTimeMS = sql.NullFloat64{Float64: 175000.0, Valid: true}

	queries[1].ExecutionCount = sql.NullInt64{Int64: 250, Valid: true}
	queries[1].TotalElapsedTimeMS = sql.NullFloat64{Float64: 375000.0, Valid: true}

	queries[2].ExecutionCount = sql.NullInt64{Int64: 75, Valid: true}
	queries[2].TotalElapsedTimeMS = sql.NullFloat64{Float64: 112500.0, Valid: true}

	now2 := now.Add(1 * time.Minute)

	for _, query := range queries {
		metrics := calc.CalculateMetrics(query, now2)
		require.NotNil(t, metrics)
		assert.False(t, metrics.IsFirstScrape)
		assert.True(t, metrics.HasNewExecutions)
		assert.Greater(t, metrics.IntervalExecutionCount, int64(0))
	}

	assert.Equal(t, 3, len(calc.cache))
}

func TestMySQLIntervalCalculator_NegativeDelta(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	// First scrape
	query1 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 150000.0, Valid: true},
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	now := time.Now()
	metrics1 := calc.CalculateMetrics(query1, now)
	require.NotNil(t, metrics1)

	// Second scrape - execution count decreased (e.g., performance_schema was reset)
	query2 := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 50, Valid: true}, // Lower than before
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 75000.0, Valid: true},
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 1500.0, Valid: true},
	}

	now2 := now.Add(1 * time.Minute)
	metrics2 := calc.CalculateMetrics(query2, now2)

	require.NotNil(t, metrics2)
	assert.False(t, metrics2.HasNewExecutions, "Expected no new executions for negative delta")
}

func TestMySQLIntervalCalculator_ZeroExecutionCount(t *testing.T) {
	calc := NewMySQLIntervalCalculator(zap.NewNop(), 10*time.Minute)

	query := &models.SlowQuery{
		QueryID:            sql.NullString{String: "test_query", Valid: true},
		ExecutionCount:     sql.NullInt64{Int64: 0, Valid: true}, // Zero executions
		TotalElapsedTimeMS: sql.NullFloat64{Float64: 0.0, Valid: true},
		AvgElapsedTimeMS:   sql.NullFloat64{Float64: 0.0, Valid: true},
	}

	metrics := calc.CalculateMetrics(query, time.Now())

	require.NotNil(t, metrics)
	assert.True(t, metrics.IsFirstScrape)
	assert.True(t, metrics.HasNewExecutions)
	assert.Equal(t, int64(0), metrics.IntervalExecutionCount)
}
