// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
)

// Test NewDatabaseMetricsScraper creates a scraper successfully
func TestNewDatabaseMetricsScraper(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.True(t, scraper.supportsPG12)
}

// Test ScrapeDatabaseMetrics with successful query (PG12+)
func TestScrapeDatabaseMetrics_Success_PG12(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock database metrics query with PG12+ checksum columns
	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"deadlocks", "temp_files", "temp_bytes", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
		"checksum_failures", "checksum_last_failure", "checksums_enabled",
	}).AddRow(
		"postgres", int64(10), int64(1000), int64(50),
		int64(500), int64(9500), int64(10000), int64(8000),
		int64(100), int64(50), int64(20), int64(0),
		int64(2), int64(5), int64(1024000), float64(10.5),
		float64(5.2), int64(1000000), int64(10485760),
		int64(0), nil, true,
	).AddRow(
		"testdb", int64(5), int64(500), int64(10),
		int64(200), int64(4800), int64(5000), int64(4000),
		int64(50), int64(25), int64(10), int64(0),
		int64(1), int64(2), int64(512000), float64(5.0),
		float64(2.5), int64(2000000), int64(5242880),
		int64(0), nil, true,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeDatabaseMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeDatabaseMetrics with successful query (PG11 and below)
func TestScrapeDatabaseMetrics_Success_PG11(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, false)

	// Mock database metrics query without PG12+ checksum columns
	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"deadlocks", "temp_files", "temp_bytes", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
	}).AddRow(
		"postgres", int64(10), int64(1000), int64(50),
		int64(500), int64(9500), int64(10000), int64(8000),
		int64(100), int64(50), int64(20), int64(0),
		int64(2), int64(5), int64(1024000), float64(10.5),
		float64(5.2), int64(1000000), int64(10485760),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeDatabaseMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeDatabaseMetrics with NULL values
func TestScrapeDatabaseMetrics_NullValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"deadlocks", "temp_files", "temp_bytes", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
		"checksum_failures", "checksum_last_failure", "checksums_enabled",
	}).AddRow(
		"postgres", nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeDatabaseMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeDatabaseMetrics with empty results
func TestScrapeDatabaseMetrics_EmptyResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched",
		"tup_inserted", "tup_updated", "tup_deleted", "conflicts",
		"deadlocks", "temp_files", "temp_bytes", "blk_read_time",
		"blk_write_time", "before_xid_wraparound", "database_size",
		"checksum_failures", "checksum_last_failure", "checksums_enabled",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeDatabaseMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeDatabaseMetrics with query error
func TestScrapeDatabaseMetrics_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeDatabaseMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeServerUptime with successful query
func TestScrapeServerUptime_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock server uptime query
	rows := sqlmock.NewRows([]string{"uptime"}).AddRow(float64(86400.5))

	mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM \\(now\\(\\) - pg_postmaster_start_time\\(\\)\\)\\)").WillReturnRows(rows)

	errs := scraper.ScrapeServerUptime(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeServerUptime with NULL value
func TestScrapeServerUptime_NullValue(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with NULL value
	rows := sqlmock.NewRows([]string{"uptime"}).AddRow(nil)

	mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM \\(now\\(\\) - pg_postmaster_start_time\\(\\)\\)\\)").WillReturnRows(rows)

	errs := scraper.ScrapeServerUptime(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeServerUptime with query error
func TestScrapeServerUptime_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM \\(now\\(\\) - pg_postmaster_start_time\\(\\)\\)\\)").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeServerUptime(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeDatabaseCount with successful query
func TestScrapeDatabaseCount_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock database count query
	rows := sqlmock.NewRows([]string{"database_count"}).AddRow(int64(5))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) as db_count FROM pg_database WHERE datallowconn").WillReturnRows(rows)

	errs := scraper.ScrapeDatabaseCount(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeDatabaseCount with query error
func TestScrapeDatabaseCount_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) as db_count FROM pg_database WHERE datallowconn").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeDatabaseCount(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRunningStatus with successful query
func TestScrapeRunningStatus_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock running status query
	rows := sqlmock.NewRows([]string{"running"}).AddRow(int64(1))

	mock.ExpectQuery("SELECT 1 as running").WillReturnRows(rows)

	errs := scraper.ScrapeRunningStatus(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRunningStatus with query error
func TestScrapeRunningStatus_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT 1 as running").
		WillReturnError(errors.New("connection lost"))

	errs := scraper.ScrapeRunningStatus(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "connection lost")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConnectionStats with successful query
func TestScrapeConnectionStats_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock connection stats query
	rows := sqlmock.NewRows([]string{"max_connections", "current_connections"}).
		AddRow(int64(100), int64(50))

	mock.ExpectQuery("SELECT (.+) FROM pg_settings").WillReturnRows(rows)

	errs := scraper.ScrapeConnectionStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConnectionStats with zero max connections
func TestScrapeConnectionStats_ZeroMaxConnections(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with zero max connections (edge case)
	rows := sqlmock.NewRows([]string{"max_connections", "current_connections"}).
		AddRow(int64(0), int64(0))

	mock.ExpectQuery("SELECT (.+) FROM pg_settings").WillReturnRows(rows)

	errs := scraper.ScrapeConnectionStats(context.Background())

	// Should not calculate percent usage when max_connections is 0
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConnectionStats with query error
func TestScrapeConnectionStats_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_settings").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeConnectionStats(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSessionMetrics with successful query
func TestScrapeSessionMetrics_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock session metrics query (PG 14+)
	rows := sqlmock.NewRows([]string{
		"datname", "session_time", "active_time", "idle_in_transaction_time",
		"sessions", "sessions_abandoned", "sessions_fatal", "sessions_killed",
	}).AddRow(
		"postgres", float64(10000.5), float64(5000.2), float64(500.1),
		int64(100), int64(5), int64(1), int64(0),
	).AddRow(
		"testdb", float64(5000.3), float64(2500.1), float64(250.05),
		int64(50), int64(2), int64(0), int64(0),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeSessionMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSessionMetrics with NULL values
func TestScrapeSessionMetrics_NullValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "session_time", "active_time", "idle_in_transaction_time",
		"sessions", "sessions_abandoned", "sessions_fatal", "sessions_killed",
	}).AddRow(
		"postgres", nil, nil, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeSessionMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSessionMetrics with empty results
func TestScrapeSessionMetrics_EmptyResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"datname", "session_time", "active_time", "idle_in_transaction_time",
		"sessions", "sessions_abandoned", "sessions_fatal", "sessions_killed",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").WillReturnRows(rows)

	errs := scraper.ScrapeSessionMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSessionMetrics with query error
func TestScrapeSessionMetrics_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeSessionMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConflictMetrics with successful query
func TestScrapeConflictMetrics_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock conflict metrics query
	rows := sqlmock.NewRows([]string{
		"datname", "confl_tablespace", "confl_lock", "confl_snapshot",
		"confl_bufferpin", "confl_deadlock",
	}).AddRow(
		"postgres", int64(0), int64(5), int64(10),
		int64(2), int64(1),
	).AddRow(
		"testdb", int64(0), int64(2), int64(8),
		int64(1), int64(0),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database_conflicts").WillReturnRows(rows)

	errs := scraper.ScrapeConflictMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConflictMetrics with NULL values
func TestScrapeConflictMetrics_NullValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "confl_tablespace", "confl_lock", "confl_snapshot",
		"confl_bufferpin", "confl_deadlock",
	}).AddRow(
		"postgres", nil, nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database_conflicts").WillReturnRows(rows)

	errs := scraper.ScrapeConflictMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConflictMetrics with empty results
func TestScrapeConflictMetrics_EmptyResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"datname", "confl_tablespace", "confl_lock", "confl_snapshot",
		"confl_bufferpin", "confl_deadlock",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database_conflicts").WillReturnRows(rows)

	errs := scraper.ScrapeConflictMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeConflictMetrics with query error
func TestScrapeConflictMetrics_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	logger := zaptest.NewLogger(t)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)
	sqlClient := client.NewSQLClient(db)

	scraper := NewDatabaseMetricsScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, true)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database_conflicts").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeConflictMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test helper function getInt64
func TestHelperFunction_GetInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    sql.NullInt64
		expected int64
	}{
		{
			name:     "valid value",
			input:    sql.NullInt64{Int64: 42, Valid: true},
			expected: 42,
		},
		{
			name:     "null value",
			input:    sql.NullInt64{Int64: 0, Valid: false},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test helper function getFloat64
func TestHelperFunction_GetFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    sql.NullFloat64
		expected float64
	}{
		{
			name:     "valid value",
			input:    sql.NullFloat64{Float64: 42.5, Valid: true},
			expected: 42.5,
		},
		{
			name:     "null value",
			input:    sql.NullFloat64{Float64: 0.0, Valid: false},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFloat64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test helper function getBool
func TestHelperFunction_GetBool(t *testing.T) {
	tests := []struct {
		name     string
		input    sql.NullBool
		expected int64
	}{
		{
			name:     "valid true",
			input:    sql.NullBool{Bool: true, Valid: true},
			expected: 1,
		},
		{
			name:     "valid false",
			input:    sql.NullBool{Bool: false, Valid: true},
			expected: 0,
		},
		{
			name:     "null value",
			input:    sql.NullBool{Bool: false, Valid: false},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getBool(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test helper function getString
func TestHelperFunction_GetString(t *testing.T) {
	tests := []struct {
		name     string
		input    sql.NullString
		expected string
	}{
		{
			name:     "valid value",
			input:    sql.NullString{String: "test", Valid: true},
			expected: "test",
		},
		{
			name:     "null value",
			input:    sql.NullString{String: "", Valid: false},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
