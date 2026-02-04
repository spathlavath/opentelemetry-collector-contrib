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

// Test NewVacuumMaintenanceScraper creates a scraper successfully
func TestNewVacuumMaintenanceScraper(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.Equal(t, 130000, scraper.pgVersion)
}

// Test ScrapeAnalyzeProgress with successful query (PostgreSQL 13+)
func TestScrapeAnalyzeProgress_Success(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	// Mock ANALYZE progress query
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "phase",
		"sample_blks_total", "sample_blks_scanned",
		"ext_stats_total", "ext_stats_computed",
		"child_tables_total", "child_tables_done",
	}).AddRow(
		"testdb", "public", "users", "computing statistics",
		int64(1000), int64(500),
		int64(10), int64(5),
		int64(3), int64(1),
	).AddRow(
		"testdb", "public", "orders", "acquiring sample rows",
		int64(2000), int64(1000),
		int64(5), int64(2),
		int64(0), int64(0),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_analyze").WillReturnRows(rows)

	errs := scraper.ScrapeAnalyzeProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeAnalyzeProgress with NULL values
func TestScrapeAnalyzeProgress_NullValues(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "phase",
		"sample_blks_total", "sample_blks_scanned",
		"ext_stats_total", "ext_stats_computed",
		"child_tables_total", "child_tables_done",
	}).AddRow(
		"testdb", "public", "users", "initializing",
		nil, nil,
		nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_analyze").WillReturnRows(rows)

	errs := scraper.ScrapeAnalyzeProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeAnalyzeProgress with empty results (no operations running)
func TestScrapeAnalyzeProgress_EmptyResults(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	// Mock with empty result set (no ANALYZE operations running)
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "phase",
		"sample_blks_total", "sample_blks_scanned",
		"ext_stats_total", "ext_stats_computed",
		"child_tables_total", "child_tables_done",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_analyze").WillReturnRows(rows)

	errs := scraper.ScrapeAnalyzeProgress(context.Background())

	// Empty results are okay (no operations running)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeAnalyzeProgress with version check (< PostgreSQL 13)
func TestScrapeAnalyzeProgress_VersionTooOld(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	errs := scraper.ScrapeAnalyzeProgress(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeAnalyzeProgress with query error
func TestScrapeAnalyzeProgress_QueryError(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_analyze").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeAnalyzeProgress(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeClusterProgress with successful query (PostgreSQL 12+)
func TestScrapeClusterProgress_Success(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock CLUSTER progress query
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "command", "phase",
		"heap_blks_total", "heap_blks_scanned",
		"heap_tuples_scanned", "heap_tuples_written",
		"index_rebuild_count",
	}).AddRow(
		"testdb", "public", "users", "CLUSTER", "seq scanning heap",
		int64(10000), int64(5000),
		int64(100000), int64(50000),
		int64(2),
	).AddRow(
		"testdb", "public", "orders", "VACUUM FULL", "writing new heap",
		int64(20000), int64(15000),
		int64(200000), int64(150000),
		int64(5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_cluster").WillReturnRows(rows)

	errs := scraper.ScrapeClusterProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeClusterProgress with NULL values
func TestScrapeClusterProgress_NullValues(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "command", "phase",
		"heap_blks_total", "heap_blks_scanned",
		"heap_tuples_scanned", "heap_tuples_written",
		"index_rebuild_count",
	}).AddRow(
		"testdb", "public", "users", "CLUSTER", "initializing",
		nil, nil,
		nil, nil,
		nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_cluster").WillReturnRows(rows)

	errs := scraper.ScrapeClusterProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeClusterProgress with empty results (no operations running)
func TestScrapeClusterProgress_EmptyResults(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock with empty result set (no CLUSTER operations running)
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "command", "phase",
		"heap_blks_total", "heap_blks_scanned",
		"heap_tuples_scanned", "heap_tuples_written",
		"index_rebuild_count",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_cluster").WillReturnRows(rows)

	errs := scraper.ScrapeClusterProgress(context.Background())

	// Empty results are okay (no operations running)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeClusterProgress with version check (< PostgreSQL 12)
func TestScrapeClusterProgress_VersionTooOld(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 110000)

	errs := scraper.ScrapeClusterProgress(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeClusterProgress with query error
func TestScrapeClusterProgress_QueryError(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_cluster").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeClusterProgress(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeCreateIndexProgress with successful query (PostgreSQL 12+)
func TestScrapeCreateIndexProgress_Success(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock CREATE INDEX progress query
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "index_relname", "command", "phase",
		"lockers_total", "lockers_done",
		"blocks_total", "blocks_done",
		"tuples_total", "tuples_done",
		"partitions_total", "partitions_done",
	}).AddRow(
		"testdb", "public", "users", "users_email_idx", "CREATE INDEX CONCURRENTLY", "building index",
		int64(5), int64(3),
		int64(1000), int64(500),
		int64(10000), int64(5000),
		int64(0), int64(0),
	).AddRow(
		"testdb", "public", "orders", "orders_date_idx", "CREATE INDEX", "building index",
		int64(0), int64(0),
		int64(2000), int64(1500),
		int64(20000), int64(15000),
		int64(4), int64(2),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_create_index").WillReturnRows(rows)

	errs := scraper.ScrapeCreateIndexProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeCreateIndexProgress with NULL values
func TestScrapeCreateIndexProgress_NullValues(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "index_relname", "command", "phase",
		"lockers_total", "lockers_done",
		"blocks_total", "blocks_done",
		"tuples_total", "tuples_done",
		"partitions_total", "partitions_done",
	}).AddRow(
		"testdb", "public", "users", "users_idx", "CREATE INDEX", "initializing",
		nil, nil,
		nil, nil,
		nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_create_index").WillReturnRows(rows)

	errs := scraper.ScrapeCreateIndexProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeCreateIndexProgress with empty results (no operations running)
func TestScrapeCreateIndexProgress_EmptyResults(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock with empty result set (no CREATE INDEX operations running)
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "index_relname", "command", "phase",
		"lockers_total", "lockers_done",
		"blocks_total", "blocks_done",
		"tuples_total", "tuples_done",
		"partitions_total", "partitions_done",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_create_index").WillReturnRows(rows)

	errs := scraper.ScrapeCreateIndexProgress(context.Background())

	// Empty results are okay (no operations running)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeCreateIndexProgress with version check (< PostgreSQL 12)
func TestScrapeCreateIndexProgress_VersionTooOld(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 110000)

	errs := scraper.ScrapeCreateIndexProgress(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeCreateIndexProgress with query error
func TestScrapeCreateIndexProgress_QueryError(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_create_index").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeCreateIndexProgress(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeVacuumProgress with successful query (PostgreSQL 12+)
func TestScrapeVacuumProgress_Success(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock VACUUM progress query
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "phase",
		"heap_blks_total", "heap_blks_scanned", "heap_blks_vacuumed",
		"index_vacuum_count",
		"max_dead_tuples", "num_dead_tuples",
	}).AddRow(
		"testdb", "public", "users", "scanning heap",
		int64(10000), int64(5000), int64(3000),
		int64(2),
		int64(1000), int64(500),
	).AddRow(
		"testdb", "public", "orders", "vacuuming indexes",
		int64(20000), int64(18000), int64(15000),
		int64(5),
		int64(2000), int64(1500),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_vacuum").WillReturnRows(rows)

	errs := scraper.ScrapeVacuumProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeVacuumProgress with NULL values
func TestScrapeVacuumProgress_NullValues(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "phase",
		"heap_blks_total", "heap_blks_scanned", "heap_blks_vacuumed",
		"index_vacuum_count",
		"max_dead_tuples", "num_dead_tuples",
	}).AddRow(
		"testdb", "public", "users", "initializing",
		nil, nil, nil,
		nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_vacuum").WillReturnRows(rows)

	errs := scraper.ScrapeVacuumProgress(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeVacuumProgress with empty results (no operations running)
func TestScrapeVacuumProgress_EmptyResults(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock with empty result set (no VACUUM operations running)
	rows := sqlmock.NewRows([]string{
		"datname", "schemaname", "relname", "phase",
		"heap_blks_total", "heap_blks_scanned", "heap_blks_vacuumed",
		"index_vacuum_count",
		"max_dead_tuples", "num_dead_tuples",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_vacuum").WillReturnRows(rows)

	errs := scraper.ScrapeVacuumProgress(context.Background())

	// Empty results are okay (no operations running)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeVacuumProgress with version check (< PostgreSQL 12)
func TestScrapeVacuumProgress_VersionTooOld(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 110000)

	errs := scraper.ScrapeVacuumProgress(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeVacuumProgress with query error
func TestScrapeVacuumProgress_QueryError(t *testing.T) {
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

	scraper := NewVacuumMaintenanceScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 120000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_progress_vacuum").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeVacuumProgress(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}
