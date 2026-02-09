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

// Test NewLocksScraper creates a scraper successfully
func TestNewLocksScraper(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
}

// Test ScrapeLocks with successful query
func TestScrapeLocks_Success(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock locks query with various lock types
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"postgres", int64(100), int64(50), int64(25),
		int64(10), int64(5), int64(3),
		int64(2), int64(1),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with NULL values
func TestScrapeLocks_NullValues(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock with NULL values for all lock counts
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"postgres", nil, nil, nil,
		nil, nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with zero lock counts
func TestScrapeLocks_ZeroLockCounts(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock with zero lock counts (no locks held)
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"postgres", int64(0), int64(0), int64(0),
		int64(0), int64(0), int64(0),
		int64(0), int64(0),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with multiple databases
func TestScrapeLocks_MultipleDatabases(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock with multiple databases
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"postgres", int64(50), int64(25), int64(10),
		int64(5), int64(2), int64(1),
		int64(1), int64(0),
	).AddRow(
		"testdb", int64(30), int64(15), int64(8),
		int64(3), int64(1), int64(1),
		int64(0), int64(0),
	).AddRow(
		"proddb", int64(200), int64(100), int64(50),
		int64(20), int64(10), int64(5),
		int64(3), int64(1),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with empty results (no rows)
func TestScrapeLocks_EmptyResults(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock with empty result set (no locks)
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	// Empty result set causes an error in QueryLocks
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "no rows in result set")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with query error
func TestScrapeLocks_QueryError(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_locks").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeLocks(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with high lock contention
func TestScrapeLocks_HighLockContention(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock with high lock counts indicating contention
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"postgres", int64(1000), int64(500), int64(250),
		int64(100), int64(50), int64(25),
		int64(10), int64(5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeLocks with only exclusive locks
func TestScrapeLocks_OnlyExclusiveLocks(t *testing.T) {
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

	scraper := NewLocksScraper(sqlClient, mb, logger, "test-instance")

	// Mock with only exclusive and access exclusive locks
	rows := sqlmock.NewRows([]string{
		"database", "access_share_lock", "row_share_lock", "row_exclusive_lock",
		"share_update_exclusive_lock", "share_lock", "share_row_exclusive_lock",
		"exclusive_lock", "access_exclusive_lock",
	}).AddRow(
		"postgres", int64(0), int64(0), int64(0),
		int64(0), int64(0), int64(0),
		int64(5), int64(3),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_locks").WillReturnRows(rows)

	errs := scraper.ScrapeLocks(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}
