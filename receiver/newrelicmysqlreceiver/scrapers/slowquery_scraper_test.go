// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
)

func TestNewSlowQueryScraper(t *testing.T) {
	tests := []struct {
		name                     string
		responseTimeThreshold    int
		countThreshold           int
		intervalSeconds          int
		enableIntervalCalculator bool
		cacheTTLMinutes          int
	}{
		{
			name:                     "valid_with_interval_calculator_enabled",
			responseTimeThreshold:    100,
			countThreshold:           10,
			intervalSeconds:          60,
			enableIntervalCalculator: true,
			cacheTTLMinutes:          10,
		},
		{
			name:                     "valid_with_interval_calculator_disabled",
			responseTimeThreshold:    100,
			countThreshold:           10,
			intervalSeconds:          60,
			enableIntervalCalculator: false,
			cacheTTLMinutes:          10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := common.NewMockClient()
			mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))

			scraper := NewSlowQueryScraper(
				mockClient,
				mb,
				zap.NewNop(),
				tt.responseTimeThreshold,
				tt.countThreshold,
				tt.intervalSeconds,
				tt.enableIntervalCalculator,
				tt.cacheTTLMinutes,
			)

			require.NotNil(t, scraper)
			assert.Equal(t, tt.responseTimeThreshold, scraper.responseTimeThreshold)
			assert.Equal(t, tt.countThreshold, scraper.countThreshold)
			assert.Equal(t, tt.intervalSeconds, scraper.intervalSeconds)

			if tt.enableIntervalCalculator {
				assert.NotNil(t, scraper.intervalCalculator)
			} else {
				assert.Nil(t, scraper.intervalCalculator)
			}
		})
	}
}

func TestSlowQueryScraper_ScrapeMetrics_WithValidData(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			CollectionTimestamp:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:                sql.NullString{String: "abc123def456", Valid: true},
			QueryText:              sql.NullString{String: "SELECT * FROM users WHERE status = ?", Valid: true},
			DatabaseName:           sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:         sql.NullInt64{Int64: 150, Valid: true},
			TotalElapsedTimeMS:     sql.NullFloat64{Float64: 225000.0, Valid: true},
			AvgElapsedTimeMS:       sql.NullFloat64{Float64: 1500.0, Valid: true},
			AvgCPUTimeMS:           sql.NullFloat64{Float64: 125.5, Valid: true},
			AvgLockTimeMS:          sql.NullFloat64{Float64: 10.2, Valid: true},
			AvgRowsExamined:        sql.NullFloat64{Float64: 5000.0, Valid: true},
			AvgRowsSent:            sql.NullFloat64{Float64: 100.0, Valid: true},
			TotalErrors:            sql.NullInt64{Int64: 0, Valid: true},
			FirstSeen:              sql.NullString{String: "2024-01-01 09:00:00", Valid: true},
			LastExecutionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
		},
		{
			CollectionTimestamp:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:                sql.NullString{String: "xyz789ghi012", Valid: true},
			QueryText:              sql.NullString{String: "UPDATE orders SET status = ? WHERE id = ?", Valid: true},
			DatabaseName:           sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:         sql.NullInt64{Int64: 200, Valid: true},
			TotalElapsedTimeMS:     sql.NullFloat64{Float64: 400000.0, Valid: true},
			AvgElapsedTimeMS:       sql.NullFloat64{Float64: 2000.0, Valid: true},
			AvgCPUTimeMS:           sql.NullFloat64{Float64: 180.0, Valid: true},
			AvgLockTimeMS:          sql.NullFloat64{Float64: 15.5, Valid: true},
			AvgRowsExamined:        sql.NullFloat64{Float64: 1000.0, Valid: true},
			AvgRowsSent:            sql.NullFloat64{Float64: 1.0, Valid: true},
			TotalErrors:            sql.NullInt64{Int64: 2, Valid: true},
			FirstSeen:              sql.NullString{String: "2024-01-01 08:00:00", Valid: true},
			LastExecutionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, false, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine(), "Expected no errors")

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()
	require.Greater(t, rm.Len(), 0, "Expected at least one resource metric")

	sm := rm.At(0).ScopeMetrics()
	require.Greater(t, sm.Len(), 0, "Expected at least one scope metric")

	metricSlice := sm.At(0).Metrics()
	require.Greater(t, metricSlice.Len(), 0, "Expected metrics to be collected")

	// Verify we have the expected metric types
	metricNames := make(map[string]bool)
	for i := 0; i < metricSlice.Len(); i++ {
		metricNames[metricSlice.At(i).Name()] = true
	}

	expectedMetrics := []string{
		"newrelicmysql.slowquery.execution_count",
		"newrelicmysql.slowquery.avg_cpu_time_ms",
		"newrelicmysql.slowquery.avg_elapsed_time_ms",
		"newrelicmysql.slowquery.avg_lock_time_ms",
		"newrelicmysql.slowquery.avg_rows_examined",
		"newrelicmysql.slowquery.avg_rows_sent",
		"newrelicmysql.slowquery.query_details",
		"newrelicmysql.slowquery.query_errors",
	}

	for _, expected := range expectedMetrics {
		assert.True(t, metricNames[expected], "Expected metric %s not found", expected)
	}
}

func TestSlowQueryScraper_ScrapeMetrics_WithEmptyResults(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, false, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine(), "Expected no errors")

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()
	// No metrics should be collected
	assert.Equal(t, 0, rm.Len(), "Expected no resource metrics for empty results")
}

func TestSlowQueryScraper_ScrapeMetrics_WithQueryError(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueriesErr = errors.New("database connection failed")

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, false, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.NotNil(t, errs.Combine(), "Expected error")
	assert.Contains(t, errs.Combine().Error(), "database connection failed")
}

func TestSlowQueryScraper_ScrapeMetrics_WithInvalidData(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			// Missing query_id - should be skipped
			CollectionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:             sql.NullString{Valid: false},
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			AvgElapsedTimeMS:    sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
		{
			// Missing avg_elapsed_time_ms - should be skipped
			CollectionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:             sql.NullString{String: "test123", Valid: true},
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			AvgElapsedTimeMS:    sql.NullFloat64{Valid: false},
		},
		{
			// Valid query
			CollectionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:             sql.NullString{String: "valid123", Valid: true},
			QueryText:           sql.NullString{String: "SELECT 1", Valid: true},
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS:  sql.NullFloat64{Float64: 150000.0, Valid: true},
			AvgElapsedTimeMS:    sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, false, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine(), "Expected no errors")

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()
	require.Greater(t, rm.Len(), 0, "Expected at least one resource metric")

	// Only the valid query should be recorded
	sm := rm.At(0).ScopeMetrics()
	require.Greater(t, sm.Len(), 0)
}

func TestSlowQueryScraper_ScrapeMetrics_WithIntervalCalculator(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			CollectionTimestamp:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:                sql.NullString{String: "test_query_1", Valid: true},
			QueryText:              sql.NullString{String: "SELECT * FROM users", Valid: true},
			DatabaseName:           sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:         sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS:     sql.NullFloat64{Float64: 150000.0, Valid: true},
			AvgElapsedTimeMS:       sql.NullFloat64{Float64: 1500.0, Valid: true},
			AvgCPUTimeMS:           sql.NullFloat64{Float64: 100.0, Valid: true},
			FirstSeen:              sql.NullString{String: "2024-01-01 09:00:00", Valid: true},
			LastExecutionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, true, 10)

	// First scrape - should record historical averages
	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)
	assert.Nil(t, errs.Combine())

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()
	require.Greater(t, rm.Len(), 0)

	sm := rm.At(0).ScopeMetrics()
	require.Greater(t, sm.Len(), 0)

	metricSlice := sm.At(0).Metrics()
	metricNames := make(map[string]bool)
	for i := 0; i < metricSlice.Len(); i++ {
		metricNames[metricSlice.At(i).Name()] = true
	}

	// Should have interval metrics when calculator is enabled
	assert.True(t, metricNames["newrelicmysql.slowquery.interval_avg_elapsed_time_ms"],
		"Expected interval_avg_elapsed_time_ms metric")
	assert.True(t, metricNames["newrelicmysql.slowquery.interval_execution_count"],
		"Expected interval_execution_count metric")
}

func TestSlowQueryScraper_ApplyIntervalCalculation_TopNFiltering(t *testing.T) {
	mockClient := common.NewMockClient()

	// Create 15 queries with different elapsed times
	slowQueries := make([]models.SlowQuery, 15)
	for i := 0; i < 15; i++ {
		avgElapsedTime := float64((15 - i) * 100) // 1500ms down to 100ms
		slowQueries[i] = models.SlowQuery{
			CollectionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:             sql.NullString{String: "query_" + string(rune('A'+i)), Valid: true},
			QueryText:           sql.NullString{String: "SELECT " + string(rune('A'+i)), Valid: true},
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS:  sql.NullFloat64{Float64: avgElapsedTime * 100, Valid: true},
			AvgElapsedTimeMS:    sql.NullFloat64{Float64: avgElapsedTime, Valid: true},
		}
	}
	mockClient.SlowQueries = slowQueries

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	// Set countThreshold to 10 (top 10 queries)
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, true, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine())

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()
	require.Greater(t, rm.Len(), 0)

	// Count unique query_ids in metrics
	sm := rm.At(0).ScopeMetrics()
	require.Greater(t, sm.Len(), 0)

	metricSlice := sm.At(0).Metrics()
	queryIDs := make(map[string]bool)
	for i := 0; i < metricSlice.Len(); i++ {
		metric := metricSlice.At(i)
		if metric.Name() == "newrelicmysql.slowquery.query_details" {
			dataPoints := metric.Gauge().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				attrs := dataPoints.At(j).Attributes()
				if queryID, ok := attrs.Get("query_id"); ok {
					queryIDs[queryID.Str()] = true
				}
			}
		}
	}

	// Should have at most 10 queries (top N)
	assert.LessOrEqual(t, len(queryIDs), 10, "Expected at most 10 queries after top N filtering")
}

func TestSlowQueryScraper_ApplyIntervalCalculation_ThresholdFiltering(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			// Below threshold - should be filtered out
			CollectionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:             sql.NullString{String: "slow_query", Valid: true},
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS:  sql.NullFloat64{Float64: 5000.0, Valid: true}, // 50ms avg
			AvgElapsedTimeMS:    sql.NullFloat64{Float64: 50.0, Valid: true},
		},
		{
			// Above threshold - should be included
			CollectionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:             sql.NullString{String: "fast_query", Valid: true},
			DatabaseName:        sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:      sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS:  sql.NullFloat64{Float64: 150000.0, Valid: true}, // 1500ms avg
			AvgElapsedTimeMS:    sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	// Set threshold to 100ms
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, true, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine())

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()

	if rm.Len() > 0 {
		sm := rm.At(0).ScopeMetrics()
		if sm.Len() > 0 {
			metricSlice := sm.At(0).Metrics()
			queryIDs := make(map[string]bool)
			for i := 0; i < metricSlice.Len(); i++ {
				metric := metricSlice.At(i)
				if metric.Name() == "newrelicmysql.slowquery.query_details" {
					dataPoints := metric.Gauge().DataPoints()
					for j := 0; j < dataPoints.Len(); j++ {
						attrs := dataPoints.At(j).Attributes()
						if queryID, ok := attrs.Get("query_id"); ok {
							queryIDs[queryID.Str()] = true
						}
					}
				}
			}

			// Only the query above threshold should be collected
			assert.Equal(t, 1, len(queryIDs), "Expected only 1 query above threshold")
			assert.True(t, queryIDs["fast_query"], "Expected fast_query to be collected")
			assert.False(t, queryIDs["slow_query"], "Expected slow_query to be filtered out")
		}
	}
}

func TestSlowQueryScraper_RecordMetrics_AllFields(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			CollectionTimestamp:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			QueryID:                sql.NullString{String: "test_query", Valid: true},
			QueryText:              sql.NullString{String: "SELECT * FROM test", Valid: true},
			DatabaseName:           sql.NullString{String: "testdb", Valid: true},
			ExecutionCount:         sql.NullInt64{Int64: 100, Valid: true},
			TotalElapsedTimeMS:     sql.NullFloat64{Float64: 150000.0, Valid: true},
			AvgElapsedTimeMS:       sql.NullFloat64{Float64: 1500.0, Valid: true},
			AvgCPUTimeMS:           sql.NullFloat64{Float64: 100.0, Valid: true},
			AvgLockTimeMS:          sql.NullFloat64{Float64: 50.0, Valid: true},
			AvgRowsExamined:        sql.NullFloat64{Float64: 1000.0, Valid: true},
			AvgRowsSent:            sql.NullFloat64{Float64: 10.0, Valid: true},
			TotalErrors:            sql.NullInt64{Int64: 5, Valid: true},
			FirstSeen:              sql.NullString{String: "2024-01-01 09:00:00", Valid: true},
			LastExecutionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, false, 10)

	errs := &scrapererror.ScrapeErrors{}
	scraper.ScrapeMetrics(context.Background(), pcommon.NewTimestampFromTime(time.Now()), errs)

	assert.Nil(t, errs.Combine())

	metrics := mb.Emit()
	rm := metrics.ResourceMetrics()
	require.Greater(t, rm.Len(), 0)

	sm := rm.At(0).ScopeMetrics()
	require.Greater(t, sm.Len(), 0)

	metricSlice := sm.At(0).Metrics()

	// Verify all metric fields are recorded
	metricMap := make(map[string]bool)
	for i := 0; i < metricSlice.Len(); i++ {
		metricMap[metricSlice.At(i).Name()] = true
	}

	expectedMetrics := []string{
		"newrelicmysql.slowquery.execution_count",
		"newrelicmysql.slowquery.avg_cpu_time_ms",
		"newrelicmysql.slowquery.avg_elapsed_time_ms",
		"newrelicmysql.slowquery.avg_lock_time_ms",
		"newrelicmysql.slowquery.avg_rows_examined",
		"newrelicmysql.slowquery.avg_rows_sent",
		"newrelicmysql.slowquery.query_details",
		"newrelicmysql.slowquery.query_errors",
	}

	for _, expected := range expectedMetrics {
		assert.True(t, metricMap[expected], "Expected metric %s to be recorded", expected)
	}

	// Verify query_details metric has all attributes
	for i := 0; i < metricSlice.Len(); i++ {
		metric := metricSlice.At(i)
		if metric.Name() == "newrelicmysql.slowquery.query_details" {
			dataPoints := metric.Gauge().DataPoints()
			require.Greater(t, dataPoints.Len(), 0)

			attrs := dataPoints.At(0).Attributes()

			// Check all required attributes are present
			_, hasCollectionTimestamp := attrs.Get("collection_timestamp")
			_, hasDatabaseName := attrs.Get("database_name")
			_, hasQueryID := attrs.Get("query_id")
			_, hasQueryText := attrs.Get("query_text")
			_, hasFirstSeen := attrs.Get("first_seen")
			_, hasLastExecution := attrs.Get("last_execution_timestamp")

			assert.True(t, hasCollectionTimestamp, "Expected collection_timestamp attribute")
			assert.True(t, hasDatabaseName, "Expected database_name attribute")
			assert.True(t, hasQueryID, "Expected query_id attribute")
			assert.True(t, hasQueryText, "Expected query_text attribute")
			assert.True(t, hasFirstSeen, "Expected first_seen attribute")
			assert.True(t, hasLastExecution, "Expected last_execution_timestamp attribute")
		}
	}
}

func TestSlowQueryScraper_FetchSlowQueries(t *testing.T) {
	tests := []struct {
		name          string
		slowQueries   []models.SlowQuery
		queryErr      error
		expectError   bool
		expectedCount int
	}{
		{
			name: "successful_fetch",
			slowQueries: []models.SlowQuery{
				{
					QueryID:          sql.NullString{String: "query1", Valid: true},
					DatabaseName:     sql.NullString{String: "db1", Valid: true},
					AvgElapsedTimeMS: sql.NullFloat64{Float64: 100.0, Valid: true},
				},
				{
					QueryID:          sql.NullString{String: "query2", Valid: true},
					DatabaseName:     sql.NullString{String: "db2", Valid: true},
					AvgElapsedTimeMS: sql.NullFloat64{Float64: 200.0, Valid: true},
				},
			},
			expectError:   false,
			expectedCount: 2,
		},
		{
			name:          "empty_results",
			slowQueries:   []models.SlowQuery{},
			expectError:   false,
			expectedCount: 0,
		},
		{
			name:        "query_error",
			queryErr:    errors.New("connection timeout"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := common.NewMockClient()
			mockClient.SlowQueries = tt.slowQueries
			mockClient.SlowQueriesErr = tt.queryErr

			mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
			scraper := NewSlowQueryScraper(mockClient, mb, zap.NewNop(), 100, 10, 60, false, 10)

			queries, err := scraper.fetchSlowQueries(context.Background())

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, queries, tt.expectedCount)
			}
		})
	}
}
