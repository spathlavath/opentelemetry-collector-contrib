// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewSlowQueriesScraper(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	responseTimeThreshold := 1000
	countThreshold := 100
	enableIntervalCalculator := true
	intervalCalculatorCacheTTLMinutes := 10

	intervalSeconds := 15
	scraper := NewSlowQueriesScraper(mockClient, mb, logger, config, responseTimeThreshold, countThreshold, intervalSeconds, enableIntervalCalculator, intervalCalculatorCacheTTLMinutes)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.Equal(t, responseTimeThreshold, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, countThreshold, scraper.queryMonitoringCountThreshold)
	assert.NotNil(t, scraper.intervalCalculator)
}

func TestSlowQueriesScraper_ScrapeWithValidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "test_query_1", Valid: true},
			SchemaName:       sql.NullString{String: "TEST_SCHEMA", Valid: true},
			UserName:         sql.NullString{String: "TEST_USER", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 150, Valid: true},
			QueryText:        sql.NullString{String: "SELECT * FROM users WHERE id = 1", Valid: true},
			AvgCPUTimeMs:     sql.NullFloat64{Float64: 125.5, Valid: true},
			AvgDiskReads:     sql.NullFloat64{Float64: 50.2, Valid: true},
			AvgDiskWrites:    sql.NullFloat64{Float64: 10.3, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.75, Valid: true},
		},
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "test_query_2", Valid: true},
			UserName:         sql.NullString{String: "TEST_USER2", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 200, Valid: true},
			QueryText:        sql.NullString{String: "SELECT * FROM orders", Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSlowQueriesExecutionCount.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgDiskWrites.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgElapsedTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), config, 1000, 100, 15, false, 10)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 2)
	assert.Contains(t, queryIDs, "test_query_1")
	assert.Contains(t, queryIDs, "test_query_2")
}

func TestSlowQueriesScraper_ScrapeWithEmptyResults(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Empty(t, queryIDs)
}

func TestSlowQueriesScraper_ScrapeWithQueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("database connection failed")

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection failed")
	assert.Empty(t, queryIDs)
}

func TestSlowQueriesScraper_ScrapeWithInvalidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			// Missing required fields - should be skipped
			DatabaseName:     sql.NullString{Valid: false},
			QueryID:          sql.NullString{Valid: false},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
		{
			// Valid query
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "valid_query", Valid: true},
			UserName:         sql.NullString{String: "USER", Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)
	assert.Equal(t, "valid_query", queryIDs[0])
}

func TestSlowQueriesScraper_RecordMetrics(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "test_query", Valid: true},
			UserName:         sql.NullString{String: "TEST_USER", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
			AvgCPUTimeMs:     sql.NullFloat64{Float64: 50.5, Valid: true},
			AvgDiskReads:     sql.NullFloat64{Float64: 20.2, Valid: true},
			AvgDiskWrites:    sql.NullFloat64{Float64: 5.3, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1000.0, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSlowQueriesExecutionCount.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgDiskWrites.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgElapsedTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), config, 1000, 100, 15, false, 10)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)

	metrics := mb.Emit()
	require.Greater(t, metrics.ResourceMetrics().Len(), 0)
}

// Tests for ScrapeSlowQueries with interval calculator

func TestScrapeSlowQueries_WithIntervalCalculator(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_1", Valid: true},
			UserName:         sql.NullString{String: "USER1", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, true, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.ScrapeSlowQueries(ctx)

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)
}

func TestScrapeSlowQueries_IntervalCalculatorFiltersThreshold(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_1", Valid: true},
			UserName:         sql.NullString{String: "USER1", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 500.0, Valid: true}, // Below threshold
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, true, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.ScrapeSlowQueries(ctx)

	assert.Empty(t, errs)
	assert.Empty(t, queryIDs) // Query filtered out due to threshold
}

func TestScrapeSlowQueries_IntervalCalculatorTopN(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_1", Valid: true},
			UserName:         sql.NullString{String: "USER1", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 3000.0, Valid: true},
		},
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_2", Valid: true},
			UserName:         sql.NullString{String: "USER2", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 150, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 1, 15, true, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.ScrapeSlowQueries(ctx)

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)              // Only top 1 due to countThreshold
	assert.Equal(t, "query_1", queryIDs[0]) // Slowest query
}

func TestScrapeSlowQueries_NilIntervalCalculator(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_1", Valid: true},
			UserName:         sql.NullString{String: "USER1", Valid: true},
			ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.ScrapeSlowQueries(ctx)

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)
}

// Tests for recordMetrics

func TestRecordMetrics_NilSlowQuery(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	err := scraper.recordMetrics(0, nil, "timestamp", "db", "qid", "qtext", "user", "schema", "lastactive")

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "slow query is nil")
}

func TestRecordMetrics_AllFieldsValid(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	slowQuery := &models.SlowQuery{
		ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
		AvgCPUTimeMs:     sql.NullFloat64{Float64: 50.0, Valid: true},
		AvgDiskReads:     sql.NullFloat64{Float64: 25.0, Valid: true},
		AvgDiskWrites:    sql.NullFloat64{Float64: 10.0, Valid: true},
		AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		AvgRowsExamined:  sql.NullFloat64{Float64: 1000.0, Valid: true},
		AvgLockTimeMs:    sql.NullFloat64{Float64: 5.0, Valid: true},
	}

	err := scraper.recordMetrics(0, slowQuery, "timestamp", "db", "qid", "qtext", "user", "schema", "lastactive")

	assert.Nil(t, err)
}

func TestRecordMetrics_IntervalMetrics(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	intervalAvg := 1200.0
	intervalCount := int64(50)
	slowQuery := &models.SlowQuery{
		ExecutionCount:           sql.NullInt64{Int64: 100, Valid: true},
		AvgElapsedTimeMs:         sql.NullFloat64{Float64: 1500.0, Valid: true},
		IntervalAvgElapsedTimeMS: &intervalAvg,
		IntervalExecutionCount:   &intervalCount,
	}

	err := scraper.recordMetrics(0, slowQuery, "timestamp", "db", "qid", "qtext", "user", "schema", "lastactive")

	assert.Nil(t, err)
}

func TestRecordMetrics_PartialFields(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	slowQuery := &models.SlowQuery{
		ExecutionCount:   sql.NullInt64{Int64: 100, Valid: true},
		AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		// Other fields invalid
		AvgCPUTimeMs:  sql.NullFloat64{Valid: false},
		AvgDiskReads:  sql.NullFloat64{Valid: false},
		AvgDiskWrites: sql.NullFloat64{Valid: false},
	}

	err := scraper.recordMetrics(0, slowQuery, "timestamp", "db", "qid", "qtext", "user", "schema", "lastactive")

	assert.Nil(t, err)
}

// Tests for GetSlowQueryIDs

func TestGetSlowQueryIDs_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_1", Valid: true},
			UserName:         sql.NullString{String: "USER1", Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "query_2", Valid: true},
			UserName:         sql.NullString{String: "USER2", Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.GetSlowQueryIDs(ctx)

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 2)
	assert.Contains(t, queryIDs, "query_1")
	assert.Contains(t, queryIDs, "query_2")
}

func TestGetSlowQueryIDs_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("database error")

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.GetSlowQueryIDs(ctx)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database error")
	assert.Nil(t, queryIDs)
}

func TestGetSlowQueryIDs_EmptyResults(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.GetSlowQueryIDs(ctx)

	assert.Empty(t, errs)
	assert.Empty(t, queryIDs)
}

func TestGetSlowQueryIDs_InvalidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{Valid: false},
			QueryID:          sql.NullString{Valid: false},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{String: "valid_query", Valid: true},
			UserName:         sql.NullString{String: "USER", Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 2000.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.GetSlowQueryIDs(ctx)

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)
	assert.Equal(t, "valid_query", queryIDs[0])
}

func TestGetSlowQueryIDs_InvalidQueryID(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			QueryID:          sql.NullString{Valid: false},
			UserName:         sql.NullString{String: "USER", Valid: true},
			AvgElapsedTimeMs: sql.NullFloat64{Float64: 1500.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	ctx := context.Background()
	queryIDs, errs := scraper.GetSlowQueryIDs(ctx)

	assert.Empty(t, errs)
	assert.Empty(t, queryIDs)
}

// Tests for constructor variations

func TestNewSlowQueriesScraper_IntervalCalculatorDisabled(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSlowQueriesScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), 1000, 100, 15, false, 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.intervalCalculator)
}

func TestNewSlowQueriesScraper_DifferentThresholds(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper1 := NewSlowQueriesScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), 500, 50, 15, false, 10)
	scraper2 := NewSlowQueriesScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), 2000, 200, 30, false, 10)

	assert.Equal(t, 500, scraper1.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 50, scraper1.queryMonitoringCountThreshold)
	assert.Equal(t, 15, scraper1.queryMonitoringIntervalSeconds)

	assert.Equal(t, 2000, scraper2.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 200, scraper2.queryMonitoringCountThreshold)
	assert.Equal(t, 30, scraper2.queryMonitoringIntervalSeconds)
}
