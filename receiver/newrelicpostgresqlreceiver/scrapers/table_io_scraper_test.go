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

// Test NewTableIOScraper creates a scraper successfully
func TestNewTableIOScraper(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.Equal(t, 140000, scraper.pgVersion)
}

// Test ScrapeUserTables with successful query
func TestScrapeUserTables_Success(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock user tables query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
		"n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
		"n_live_tup", "n_dead_tup", "n_mod_since_analyze",
		"last_vacuum_age", "last_autovacuum_age", "last_analyze_age", "last_autoanalyze_age",
		"vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count",
	}).AddRow(
		"testdb", "public", "users",
		int64(100), int64(5000), int64(200), int64(3000),
		int64(1000), int64(500), int64(100), int64(400),
		int64(10000), int64(50), int64(100),
		float64(3600), float64(1800), float64(7200), float64(3600),
		int64(10), int64(50), int64(5), int64(25),
	).AddRow(
		"testdb", "public", "orders",
		int64(50), int64(2500), int64(300), int64(5000),
		int64(2000), int64(1000), int64(200), int64(800),
		int64(20000), int64(100), int64(200),
		float64(1800), float64(900), float64(3600), float64(1800),
		int64(5), int64(30), int64(3), int64(20),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeUserTables(context.Background(), []string{"public"}, []string{"users", "orders"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserTables with NULL values
func TestScrapeUserTables_NullValues(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
		"n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
		"n_live_tup", "n_dead_tup", "n_mod_since_analyze",
		"last_vacuum_age", "last_autovacuum_age", "last_analyze_age", "last_autoanalyze_age",
		"vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count",
	}).AddRow(
		"testdb", "public", "users",
		nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeUserTables(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserTables with empty results
func TestScrapeUserTables_EmptyResults(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
		"n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd",
		"n_live_tup", "n_dead_tup", "n_mod_since_analyze",
		"last_vacuum_age", "last_autovacuum_age", "last_analyze_age", "last_autoanalyze_age",
		"vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeUserTables(context.Background(), []string{"public"}, []string{})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserTables with query error
func TestScrapeUserTables_QueryError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeUserTables(context.Background(), []string{"public"}, []string{"users"})

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeIOUserTables with successful query
func TestScrapeIOUserTables_Success(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock IO user tables query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"heap_blks_read", "heap_blks_hit",
		"idx_blks_read", "idx_blks_hit",
		"toast_blks_read", "toast_blks_hit",
		"tidx_blks_read", "tidx_blks_hit",
	}).AddRow(
		"testdb", "public", "users",
		int64(1000), int64(50000),
		int64(500), int64(25000),
		int64(100), int64(5000),
		int64(50), int64(2500),
	).AddRow(
		"testdb", "public", "orders",
		int64(2000), int64(100000),
		int64(1000), int64(50000),
		int64(200), int64(10000),
		int64(100), int64(5000),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_statio_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeIOUserTables(context.Background(), []string{"public"}, []string{"users", "orders"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeIOUserTables with NULL values
func TestScrapeIOUserTables_NullValues(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"heap_blks_read", "heap_blks_hit",
		"idx_blks_read", "idx_blks_hit",
		"toast_blks_read", "toast_blks_hit",
		"tidx_blks_read", "tidx_blks_hit",
	}).AddRow(
		"testdb", "public", "users",
		nil, nil,
		nil, nil,
		nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_statio_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeIOUserTables(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeIOUserTables with empty results
func TestScrapeIOUserTables_EmptyResults(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"heap_blks_read", "heap_blks_hit",
		"idx_blks_read", "idx_blks_hit",
		"toast_blks_read", "toast_blks_hit",
		"tidx_blks_read", "tidx_blks_hit",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_statio_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeIOUserTables(context.Background(), []string{"public"}, []string{})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeIOUserTables with query error
func TestScrapeIOUserTables_QueryError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_statio_user_tables").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeIOUserTables(context.Background(), []string{"public"}, []string{"users"})

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserIndexes with successful query
func TestScrapeUserIndexes_Success(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock user indexes query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname", "indexrelname", "indexrelid",
		"idx_scan", "idx_tup_read", "idx_tup_fetch",
	}).AddRow(
		"testdb", "public", "users", "users_pkey", int64(12345),
		int64(1000), int64(5000), int64(4500),
	).AddRow(
		"testdb", "public", "users", "users_email_idx", int64(12346),
		int64(500), int64(2500), int64(2000),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_indexes").WillReturnRows(rows)

	// Mock index size queries for each index
	sizeRows1 := sqlmock.NewRows([]string{"pg_relation_size"}).AddRow(int64(8192000))
	mock.ExpectQuery("SELECT pg_relation_size").WillReturnRows(sizeRows1)

	sizeRows2 := sqlmock.NewRows([]string{"pg_relation_size"}).AddRow(int64(4096000))
	mock.ExpectQuery("SELECT pg_relation_size").WillReturnRows(sizeRows2)

	errs := scraper.ScrapeUserIndexes(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserIndexes with NULL index size
func TestScrapeUserIndexes_NullIndexRelID(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock user indexes query with NULL indexrelid
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname", "indexrelname", "indexrelid",
		"idx_scan", "idx_tup_read", "idx_tup_fetch",
	}).AddRow(
		"testdb", "public", "users", "users_pkey", nil,
		int64(1000), int64(5000), int64(4500),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_indexes").WillReturnRows(rows)

	errs := scraper.ScrapeUserIndexes(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserIndexes with index size query error
func TestScrapeUserIndexes_IndexSizeError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock user indexes query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname", "indexrelname", "indexrelid",
		"idx_scan", "idx_tup_read", "idx_tup_fetch",
	}).AddRow(
		"testdb", "public", "users", "users_pkey", int64(12345),
		int64(1000), int64(5000), int64(4500),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_indexes").WillReturnRows(rows)

	// Mock index size query error
	mock.ExpectQuery("SELECT pg_relation_size").
		WillReturnError(errors.New("size query failed"))

	errs := scraper.ScrapeUserIndexes(context.Background(), []string{"public"}, []string{"users"})

	// Index size error is logged but doesn't fail the scrape
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserIndexes with empty results
func TestScrapeUserIndexes_EmptyResults(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname", "indexrelname", "indexrelid",
		"idx_scan", "idx_tup_read", "idx_tup_fetch",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_indexes").WillReturnRows(rows)

	errs := scraper.ScrapeUserIndexes(context.Background(), []string{"public"}, []string{})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeUserIndexes with query error
func TestScrapeUserIndexes_QueryError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_indexes").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeUserIndexes(context.Background(), []string{"public"}, []string{"users"})

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeToastTables with successful query
func TestScrapeToastTables_Success(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock TOAST tables query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"toast_vacuum_count", "toast_autovacuum_count",
		"toast_last_vacuum_age", "toast_last_autovacuum_age",
	}).AddRow(
		"testdb", "public", "users",
		int64(5), int64(25),
		float64(7200), float64(3600),
	).AddRow(
		"testdb", "public", "documents",
		int64(10), int64(50),
		float64(14400), float64(7200),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeToastTables(context.Background(), []string{"public"}, []string{"users", "documents"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeToastTables with NULL values
func TestScrapeToastTables_NullValues(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with NULL values (table without TOAST)
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"toast_vacuum_count", "toast_autovacuum_count",
		"toast_last_vacuum_age", "toast_last_autovacuum_age",
	}).AddRow(
		"testdb", "public", "users",
		nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeToastTables(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeToastTables with empty results
func TestScrapeToastTables_EmptyResults(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"toast_vacuum_count", "toast_autovacuum_count",
		"toast_last_vacuum_age", "toast_last_autovacuum_age",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").WillReturnRows(rows)

	errs := scraper.ScrapeToastTables(context.Background(), []string{"public"}, []string{})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeToastTables with query error
func TestScrapeToastTables_QueryError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_user_tables").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeToastTables(context.Background(), []string{"public"}, []string{"users"})

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeTableSizes with successful query
func TestScrapeTableSizes_Success(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock table sizes query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"relation_size", "toast_size", "total_size", "indexes_size",
	}).AddRow(
		"testdb", "public", "users",
		int64(10485760), int64(2097152), int64(15728640), int64(3145728),
	).AddRow(
		"testdb", "public", "orders",
		int64(20971520), int64(4194304), int64(31457280), int64(6291456),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_class").WillReturnRows(rows)

	errs := scraper.ScrapeTableSizes(context.Background(), []string{"public"}, []string{"users", "orders"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeTableSizes with NULL values
func TestScrapeTableSizes_NullValues(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"relation_size", "toast_size", "total_size", "indexes_size",
	}).AddRow(
		"testdb", "public", "users",
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_class").WillReturnRows(rows)

	errs := scraper.ScrapeTableSizes(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeTableSizes with empty results
func TestScrapeTableSizes_EmptyResults(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"relation_size", "toast_size", "total_size", "indexes_size",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_class").WillReturnRows(rows)

	errs := scraper.ScrapeTableSizes(context.Background(), []string{"public"}, []string{})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeTableSizes with query error
func TestScrapeTableSizes_QueryError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_class").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeTableSizes(context.Background(), []string{"public"}, []string{"users"})

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRelationStats with successful query
func TestScrapeRelationStats_Success(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock relation stats query
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"relpages", "reltuples", "relallvisible", "age_xmin",
	}).AddRow(
		"testdb", "public", "users",
		int64(1000), float64(10000.5), int64(950), int64(1234567),
	).AddRow(
		"testdb", "public", "orders",
		int64(2000), float64(20000.8), int64(1900), int64(2345678),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_class").WillReturnRows(rows)

	errs := scraper.ScrapeRelationStats(context.Background(), []string{"public"}, []string{"users", "orders"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRelationStats with NULL values
func TestScrapeRelationStats_NullValues(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"relpages", "reltuples", "relallvisible", "age_xmin",
	}).AddRow(
		"testdb", "public", "users",
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_class").WillReturnRows(rows)

	errs := scraper.ScrapeRelationStats(context.Background(), []string{"public"}, []string{"users"})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRelationStats with empty results
func TestScrapeRelationStats_EmptyResults(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"database", "schemaname", "relname",
		"relpages", "reltuples", "relallvisible", "age_xmin",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_class").WillReturnRows(rows)

	errs := scraper.ScrapeRelationStats(context.Background(), []string{"public"}, []string{})

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRelationStats with query error
func TestScrapeRelationStats_QueryError(t *testing.T) {
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

	scraper := NewTableIOScraper(sqlClient, logger, mb, "test-instance", 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_class").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeRelationStats(context.Background(), []string{"public"}, []string{"users"})

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}
