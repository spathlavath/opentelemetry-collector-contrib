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

// Test NewActivityScraper creates a scraper successfully
func TestNewActivityScraper(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.NotNil(t, scraper.client)
	assert.NotNil(t, scraper.mb)
	assert.NotNil(t, scraper.logger)
}

// Test ScrapeActivityMetrics with successful query
func TestScrapeActivityMetrics_Success(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock the activity metrics query
	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"active_connections", "active_waiting_queries", "xact_start_age",
		"backend_xid_age", "backend_xmin_age", "max_transaction_duration",
		"sum_transaction_duration",
	}).AddRow(
		"postgres", "testuser", "psql", "client backend",
		int64(5), int64(2), 100.5,
		int64(1000), int64(2000), 50.0, 250.5,
	).AddRow(
		"testdb", "appuser", "myapp", "client backend",
		int64(10), int64(0), 25.3,
		int64(500), int64(600), 10.2, 30.5,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity").WillReturnRows(rows)

	errs := scraper.ScrapeActivityMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeActivityMetrics with NULL values
func TestScrapeActivityMetrics_NullValues(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"active_connections", "active_waiting_queries", "xact_start_age",
		"backend_xid_age", "backend_xmin_age", "max_transaction_duration",
		"sum_transaction_duration",
	}).AddRow(
		nil, nil, nil, "background worker",
		nil, nil, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity").WillReturnRows(rows)

	errs := scraper.ScrapeActivityMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeActivityMetrics with empty result set
func TestScrapeActivityMetrics_EmptyResults(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"active_connections", "active_waiting_queries", "xact_start_age",
		"backend_xid_age", "backend_xmin_age", "max_transaction_duration",
		"sum_transaction_duration",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity").WillReturnRows(rows)

	errs := scraper.ScrapeActivityMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeActivityMetrics with query error
func TestScrapeActivityMetrics_QueryError(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeActivityMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "failed to query pg_stat_activity")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWaitEvents with successful query
func TestScrapeWaitEvents_Success(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock the wait events query
	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"wait_event", "wait_event_count",
	}).AddRow(
		"postgres", "testuser", "psql", "client backend",
		"Lock", int64(3),
	).AddRow(
		"testdb", "appuser", "myapp", "client backend",
		"IO", int64(1),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity WHERE datname IS NOT NULL GROUP BY").WillReturnRows(rows)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWaitEvents with empty results
func TestScrapeWaitEvents_EmptyResults(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with empty result set (no wait events)
	rows := sqlmock.NewRows([]string{
		"datname", "usename", "application_name", "backend_type",
		"wait_event", "wait_event_count",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity WHERE datname IS NOT NULL GROUP BY").WillReturnRows(rows)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWaitEvents with query error
func TestScrapeWaitEvents_QueryError(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_activity WHERE datname IS NOT NULL GROUP BY").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "failed to query pg_stat_activity wait events")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgStatStatementsDealloc with successful query
func TestScrapePgStatStatementsDealloc_Success(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock the pg_stat_statements_info query
	rows := sqlmock.NewRows([]string{"dealloc"}).AddRow(int64(42))
	mock.ExpectQuery("SELECT dealloc FROM pg_stat_statements_info").WillReturnRows(rows)

	errs := scraper.ScrapePgStatStatementsDealloc(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgStatStatementsDealloc with zero dealloc value
func TestScrapePgStatStatementsDealloc_ZeroValue(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with zero dealloc value
	rows := sqlmock.NewRows([]string{"dealloc"}).AddRow(int64(0))
	mock.ExpectQuery("SELECT dealloc FROM pg_stat_statements_info").WillReturnRows(rows)

	errs := scraper.ScrapePgStatStatementsDealloc(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapePgStatStatementsDealloc with query error
func TestScrapePgStatStatementsDealloc_QueryError(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error (extension not installed)
	mock.ExpectQuery("SELECT dealloc FROM pg_stat_statements_info").
		WillReturnError(errors.New("extension not installed"))

	errs := scraper.ScrapePgStatStatementsDealloc(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "extension not installed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSnapshot with successful query
func TestScrapeSnapshot_Success(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock the snapshot query
	rows := sqlmock.NewRows([]string{"xmin", "xmax", "xip_count"}).
		AddRow(int64(1000), int64(1050), int64(5))
	mock.ExpectQuery("SELECT pg_snapshot_xmin").WillReturnRows(rows)

	errs := scraper.ScrapeSnapshot(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSnapshot with NULL values
func TestScrapeSnapshot_NullValues(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{"xmin", "xmax", "xip_count"}).
		AddRow(nil, nil, nil)
	mock.ExpectQuery("SELECT pg_snapshot_xmin").WillReturnRows(rows)

	errs := scraper.ScrapeSnapshot(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSnapshot with query error
func TestScrapeSnapshot_QueryError(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error
	mock.ExpectQuery("SELECT pg_snapshot_xmin").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeSnapshot(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeBuffercache with successful query
func TestScrapeBuffercache_Success(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock the buffercache query
	rows := sqlmock.NewRows([]string{
		"database", "schema", "table", "used_buffers", "unused_buffers",
		"usage_count", "dirty_buffers", "pinning_backends",
	}).AddRow(
		"postgres", "public", "users", int64(100), int64(50),
		int64(500), int64(10), int64(2),
	).AddRow(
		"testdb", "public", "orders", int64(200), int64(25),
		int64(1000), int64(5), int64(1),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_buffercache").WillReturnRows(rows)

	errs := scraper.ScrapeBuffercache(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeBuffercache with NULL values
func TestScrapeBuffercache_NullValues(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with NULL values (shared buffers)
	rows := sqlmock.NewRows([]string{
		"database", "schema", "table", "used_buffers", "unused_buffers",
		"usage_count", "dirty_buffers", "pinning_backends",
	}).AddRow(
		nil, nil, nil, int64(1000), int64(500),
		int64(5000), int64(100), int64(0),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_buffercache").WillReturnRows(rows)

	errs := scraper.ScrapeBuffercache(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeBuffercache with empty results
func TestScrapeBuffercache_EmptyResults(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schema", "table", "used_buffers", "unused_buffers",
		"usage_count", "dirty_buffers", "pinning_backends",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_buffercache").WillReturnRows(rows)

	errs := scraper.ScrapeBuffercache(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeBuffercache with query error
func TestScrapeBuffercache_QueryError(t *testing.T) {
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

	scraper := NewActivityScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error (extension not installed)
	mock.ExpectQuery("SELECT (.+) FROM pg_buffercache").
		WillReturnError(errors.New("extension not installed"))

	errs := scraper.ScrapeBuffercache(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "extension not installed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test helper functions with NULL values
func TestHelperFunctions_GetString(t *testing.T) {
	// Test valid string
	validStr := sql.NullString{String: "test", Valid: true}
	assert.Equal(t, "test", getString(validStr))

	// Test NULL string
	nullStr := sql.NullString{Valid: false}
	assert.Equal(t, "", getString(nullStr))
}

func TestHelperFunctions_GetInt64(t *testing.T) {
	// Test valid int64
	validInt := sql.NullInt64{Int64: 42, Valid: true}
	assert.Equal(t, int64(42), getInt64(validInt))

	// Test NULL int64
	nullInt := sql.NullInt64{Valid: false}
	assert.Equal(t, int64(0), getInt64(nullInt))
}

func TestHelperFunctions_GetFloat64(t *testing.T) {
	// Test valid float64
	validFloat := sql.NullFloat64{Float64: 123.45, Valid: true}
	assert.Equal(t, 123.45, getFloat64(validFloat))

	// Test NULL float64
	nullFloat := sql.NullFloat64{Valid: false}
	assert.Equal(t, 0.0, getFloat64(nullFloat))
}
