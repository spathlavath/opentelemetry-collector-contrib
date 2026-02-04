// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

var receiverType = component.MustNewType("newrelicpostgresql")

func TestNewSlowQueriesScraper(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(receiverType)
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	// Test with interval calculator enabled
	scraper := NewSlowQueriesScraper(sqlClient, mb, logger, metricsBuilderConfig, 1000, 50, 60, true, 10)
	assert.NotNil(t, scraper)
	assert.NotNil(t, scraper.intervalCalculator)
	assert.Equal(t, 1000, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 50, scraper.queryMonitoringCountThreshold)
	assert.Equal(t, 60, scraper.queryMonitoringIntervalSeconds)

	// Test with interval calculator disabled
	scraper2 := NewSlowQueriesScraper(sqlClient, mb, logger, metricsBuilderConfig, 1000, 50, 60, false, 10)
	assert.NotNil(t, scraper2)
	assert.Nil(t, scraper2.intervalCalculator)
}

func TestScrapeSlowQueries_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(receiverType)
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewSlowQueriesScraper(sqlClient, mb, logger, metricsBuilderConfig, 100, 50, 60, false, 10)

	// Mock slow queries result
	rows := sqlmock.NewRows([]string{
		"collection_timestamp", "query_id", "database_name", "user_name", "execution_count",
		"query_text", "avg_elapsed_time_ms", "min_elapsed_time_ms", "max_elapsed_time_ms",
		"stddev_elapsed_time_ms", "total_elapsed_time_ms", "avg_plan_time_ms", "avg_cpu_time_ms",
		"avg_disk_reads", "total_disk_reads", "avg_buffer_hits", "total_buffer_hits",
		"avg_disk_writes", "total_disk_writes", "avg_rows_returned", "total_rows",
	}).AddRow(
		"2024-02-04 10:00:00", "123456789", "testdb", "testuser", int64(100),
		"SELECT * FROM users WHERE id = $1", 150.5, 50.0, 500.0,
		75.2, 15050.0, 10.5, 161.0,
		5.5, int64(550), 95.0, int64(9500),
		2.5, int64(250), 10.5, int64(1050),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_statements").WillReturnRows(rows)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)
	assert.Equal(t, "123456789", queryIDs[0])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestScrapeSlowQueries_EmptyResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(receiverType)
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewSlowQueriesScraper(sqlClient, mb, logger, metricsBuilderConfig, 1000, 50, 60, false, 10)

	// Mock empty result
	rows := sqlmock.NewRows([]string{
		"collection_timestamp", "query_id", "database_name", "user_name", "execution_count",
		"query_text", "avg_elapsed_time_ms", "min_elapsed_time_ms", "max_elapsed_time_ms",
		"stddev_elapsed_time_ms", "total_elapsed_time_ms", "avg_plan_time_ms", "avg_cpu_time_ms",
		"avg_disk_reads", "total_disk_reads", "avg_buffer_hits", "total_buffer_hits",
		"avg_disk_writes", "total_disk_writes", "avg_rows_returned", "total_rows",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_statements").WillReturnRows(rows)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Empty(t, queryIDs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestScrapeSlowQueries_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(receiverType)
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewSlowQueriesScraper(sqlClient, mb, logger, metricsBuilderConfig, 1000, 50, 60, false, 10)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_statements").
		WillReturnError(sql.ErrConnDone)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "sql: connection is already closed")
	assert.Nil(t, queryIDs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestScrapeSlowQueries_NullValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(receiverType)
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewSlowQueriesScraper(sqlClient, mb, logger, metricsBuilderConfig, 1000, 50, 60, false, 10)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"collection_timestamp", "query_id", "database_name", "user_name", "execution_count",
		"query_text", "avg_elapsed_time_ms", "min_elapsed_time_ms", "max_elapsed_time_ms",
		"stddev_elapsed_time_ms", "total_elapsed_time_ms", "avg_plan_time_ms", "avg_cpu_time_ms",
		"avg_disk_reads", "total_disk_reads", "avg_buffer_hits", "total_buffer_hits",
		"avg_disk_writes", "total_disk_writes", "avg_rows_returned", "total_rows",
	}).AddRow(
		"2024-02-04 10:00:00", "123456789", "testdb", "testuser", int64(100),
		"SELECT * FROM users WHERE id = $1", 150.5, nil, nil,
		nil, 15050.0, nil, 161.0,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_statements").WillReturnRows(rows)

	queryIDs, errs := scraper.ScrapeSlowQueries(context.Background())

	assert.Empty(t, errs)
	assert.Len(t, queryIDs, 1)
	assert.Equal(t, "123456789", queryIDs[0])
	assert.NoError(t, mock.ExpectationsWereMet())
}
