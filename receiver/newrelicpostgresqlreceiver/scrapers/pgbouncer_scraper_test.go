// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
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

// Test NewPgBouncerScraper creates a scraper successfully
func TestNewPgBouncerScraper(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
}

// Test ScrapePgBouncerStats with successful query
func TestScrapePgBouncerStats_Success(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock PgBouncer stats query
	rows := sqlmock.NewRows([]string{
		"database", "total_server_assignment_count", "total_xact_count", "total_query_count",
		"total_received", "total_sent", "total_xact_time", "total_query_time",
		"total_wait_time", "total_client_parse_count", "total_server_parse_count", "total_bind_count",
		"avg_server_assignment_count", "avg_xact_count", "avg_query_count",
		"avg_recv", "avg_sent", "avg_xact_time", "avg_query_time",
		"avg_wait_time", "avg_client_parse_count", "avg_server_parse_count", "avg_bind_count",
	}).AddRow(
		"pgbouncer", int64(10000), int64(50000), int64(100000),
		int64(1024000), int64(2048000), int64(500000), int64(250000),
		int64(10000), int64(5000), int64(5000), int64(3000),
		int64(100), int64(500), int64(1000),
		int64(10240), int64(20480), int64(5000), int64(2500),
		int64(100), int64(50), int64(50), int64(30),
	).AddRow(
		"postgres", int64(5000), int64(25000), int64(50000),
		int64(512000), int64(1024000), int64(250000), int64(125000),
		int64(5000), int64(2500), int64(2500), int64(1500),
		int64(50), int64(250), int64(500),
		int64(5120), int64(10240), int64(2500), int64(1250),
		int64(50), int64(25), int64(25), int64(15),
	)

	mock.ExpectQuery("SHOW STATS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerStats with NULL values
func TestScrapePgBouncerStats_NullValues(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"database", "total_server_assignment_count", "total_xact_count", "total_query_count",
		"total_received", "total_sent", "total_xact_time", "total_query_time",
		"total_wait_time", "total_client_parse_count", "total_server_parse_count", "total_bind_count",
		"avg_server_assignment_count", "avg_xact_count", "avg_query_count",
		"avg_recv", "avg_sent", "avg_xact_time", "avg_query_time",
		"avg_wait_time", "avg_client_parse_count", "avg_server_parse_count", "avg_bind_count",
	}).AddRow(
		"pgbouncer", nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SHOW STATS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerStats with empty results
func TestScrapePgBouncerStats_EmptyResults(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "total_server_assignment_count", "total_xact_count", "total_query_count",
		"total_received", "total_sent", "total_xact_time", "total_query_time",
		"total_wait_time", "total_client_parse_count", "total_server_parse_count", "total_bind_count",
		"avg_server_assignment_count", "avg_xact_count", "avg_query_count",
		"avg_recv", "avg_sent", "avg_xact_time", "avg_query_time",
		"avg_wait_time", "avg_client_parse_count", "avg_server_parse_count", "avg_bind_count",
	})

	mock.ExpectQuery("SHOW STATS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerStats(context.Background())

	// Empty results are okay (no databases configured)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerStats with query error (PgBouncer not available)
func TestScrapePgBouncerStats_QueryError(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error (PgBouncer not available)
	mock.ExpectQuery("SHOW STATS").
		WillReturnError(errors.New("unrecognized configuration parameter \"pgbouncer\""))

	errs := scraper.ScrapePgBouncerStats(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "unrecognized configuration parameter")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with successful query
func TestScrapePgBouncerPools_Success(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock PgBouncer pools query
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"postgres", "user1", int64(10), int64(2), int64(0), int64(0),
		int64(8), int64(0), int64(0), int64(5), int64(3),
		int64(0), int64(0), int64(0), int64(0), "transaction", int64(0),
	).AddRow(
		"testdb", "user2", int64(5), int64(1), int64(0), int64(0),
		int64(4), int64(0), int64(0), int64(2), int64(1),
		int64(0), int64(0), int64(0), int64(0), "session", int64(0),
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with maxwait_us (PgBouncer 1.21+)
func TestScrapePgBouncerPools_MaxwaitUs(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with maxwait_us (PgBouncer 1.21+)
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"postgres", "user1", int64(10), int64(2), int64(0), int64(0),
		int64(8), int64(0), int64(0), int64(5), int64(3),
		int64(0), int64(0), int64(0), int64(5000000), "transaction", int64(0), // 5 seconds in microseconds
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with fallback to maxwait (older PgBouncer)
func TestScrapePgBouncerPools_MaxwaitFallback(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with only maxwait (older PgBouncer versions)
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"postgres", "user1", int64(10), int64(2), int64(0), int64(0),
		int64(8), int64(0), int64(0), int64(5), int64(3),
		int64(0), int64(0), int64(5), int64(0), "transaction", int64(0), // 5 seconds in maxwait, 0 in maxwait_us
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with NULL values
func TestScrapePgBouncerPools_NullValues(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"postgres", "user1", nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, "transaction", nil,
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with empty results
func TestScrapePgBouncerPools_EmptyResults(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	})

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	// Empty results are okay (no pools configured)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with query error (PgBouncer not available)
func TestScrapePgBouncerPools_QueryError(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error (PgBouncer not available)
	mock.ExpectQuery("SHOW POOLS").
		WillReturnError(errors.New("unrecognized configuration parameter \"pgbouncer\""))

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "unrecognized configuration parameter")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with high client waiting (contention)
func TestScrapePgBouncerPools_HighContention(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with high client waiting (pool contention)
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"postgres", "user1", int64(100), int64(50), int64(0), int64(0), // 50 clients waiting
		int64(100), int64(0), int64(0), int64(0), int64(0), // All server connections active
		int64(0), int64(0), int64(0), int64(30000000), "transaction", int64(0), // 30 seconds wait
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgBouncerPools with different pool modes
func TestScrapePgBouncerPools_DifferentPoolModes(t *testing.T) {
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

	scraper := NewPgBouncerScraper(sqlClient, mb, logger, "test-instance")

	// Mock with different pool modes
	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "cl_active_cancel_req", "cl_waiting_cancel_req",
		"sv_active", "sv_active_cancel", "sv_being_cancel", "sv_idle", "sv_used",
		"sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode", "min_pool_size",
	}).AddRow(
		"db1", "user1", int64(10), int64(0), int64(0), int64(0),
		int64(10), int64(0), int64(0), int64(5), int64(0),
		int64(0), int64(0), int64(0), int64(0), "session", int64(0),
	).AddRow(
		"db2", "user2", int64(20), int64(1), int64(0), int64(0),
		int64(15), int64(0), int64(0), int64(3), int64(2),
		int64(0), int64(0), int64(0), int64(0), "transaction", int64(0),
	).AddRow(
		"db3", "user3", int64(5), int64(0), int64(0), int64(0),
		int64(5), int64(0), int64(0), int64(1), int64(0),
		int64(0), int64(0), int64(0), int64(0), "statement", int64(0),
	)

	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	errs := scraper.ScrapePgBouncerPools(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}
