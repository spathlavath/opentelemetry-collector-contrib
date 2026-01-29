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
			DatabaseName:       sql.NullString{String: "TESTDB", Valid: true},
			QueryID:            sql.NullString{String: "test_query_1", Valid: true},
			SchemaName:         sql.NullString{String: "TEST_SCHEMA", Valid: true},
			UserName:           sql.NullString{String: "TEST_USER", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 150, Valid: true},
			QueryText:          sql.NullString{String: "SELECT * FROM users WHERE id = 1", Valid: true},
			AvgDiskWrites:      sql.NullFloat64{Float64: 10.3, Valid: true},
			AvgElapsedTimeMs:   sql.NullFloat64{Float64: 1500.75, Valid: true},
			TotalCPUTimeMS:     sql.NullFloat64{Float64: 18825.0, Valid: true}, // 150 * 125.5
			TotalDiskReads:     sql.NullInt64{Int64: 7530, Valid: true},         // 150 * 50.2
			TotalBufferGets:    sql.NullInt64{Int64: 15000, Valid: true},
			TotalRowsProcessed: sql.NullInt64{Int64: 30000, Valid: true},
		},
		{
			DatabaseName:       sql.NullString{String: "TESTDB", Valid: true},
			QueryID:            sql.NullString{String: "test_query_2", Valid: true},
			UserName:           sql.NullString{String: "TEST_USER2", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 200, Valid: true},
			QueryText:          sql.NullString{String: "SELECT * FROM orders", Valid: true},
			AvgElapsedTimeMs:   sql.NullFloat64{Float64: 2000.0, Valid: true},
			TotalCPUTimeMS:     sql.NullFloat64{Float64: 50000.0, Valid: true},
			TotalDiskReads:     sql.NullInt64{Int64: 10000, Valid: true},
			TotalBufferGets:    sql.NullInt64{Int64: 20000, Valid: true},
			TotalRowsProcessed: sql.NullInt64{Int64: 40000, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSlowQueriesExecutionCount.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgDiskWrites.Enabled = true
	config.Metrics.NewrelicoracledbSlowQueriesAvgElapsedTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewSlowQueriesScraper(mockClient, mb, zap.NewNop(), config, 1000, 100, 15, false, 10)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 2)
	// Check that the SQL IDs are present
	assert.Equal(t, "test_query_1", queryIDs[0].SQLID)
	assert.Equal(t, "test_query_2", queryIDs[1].SQLID)
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
	assert.Equal(t, "valid_query", queryIDs[0].SQLID)
}

func TestSlowQueriesScraper_RecordMetrics(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.SlowQueries = []models.SlowQuery{
		{
			DatabaseName:       sql.NullString{String: "TESTDB", Valid: true},
			QueryID:            sql.NullString{String: "test_query", Valid: true},
			UserName:           sql.NullString{String: "TEST_USER", Valid: true},
			ExecutionCount:     sql.NullInt64{Int64: 100, Valid: true},
			AvgDiskWrites:      sql.NullFloat64{Float64: 5.3, Valid: true},
			AvgElapsedTimeMs:   sql.NullFloat64{Float64: 1000.0, Valid: true},
			TotalCPUTimeMS:     sql.NullFloat64{Float64: 5050.0, Valid: true},
			TotalDiskReads:     sql.NullInt64{Int64: 2020, Valid: true},
			TotalBufferGets:    sql.NullInt64{Int64: 10000, Valid: true},
			TotalRowsProcessed: sql.NullInt64{Int64: 5000, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSlowQueriesExecutionCount.Enabled = true
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
