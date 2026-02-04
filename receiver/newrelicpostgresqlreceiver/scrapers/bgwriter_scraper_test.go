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

// Test NewBgwriterScraper creates a scraper successfully
func TestNewBgwriterScraper(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.Equal(t, 140000, scraper.pgVersion)
}

// Test ScrapeBgwriterMetrics with successful query
func TestScrapeBgwriterMetrics_Success(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock bgwriter metrics query
	rows := sqlmock.NewRows([]string{
		"buffers_clean", "maxwritten_clean", "buffers_alloc",
		"checkpoints_timed", "checkpoints_req", "buffers_checkpoint",
		"checkpoint_write_time", "checkpoint_sync_time",
		"buffers_backend", "buffers_backend_fsync",
	}).AddRow(
		int64(1000), int64(50), int64(5000),
		int64(100), int64(10), int64(3000),
		float64(500.5), float64(100.2),
		int64(200), int64(5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_bgwriter").WillReturnRows(rows)

	errs := scraper.ScrapeBgwriterMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeBgwriterMetrics with NULL values
func TestScrapeBgwriterMetrics_NullValues(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"buffers_clean", "maxwritten_clean", "buffers_alloc",
		"checkpoints_timed", "checkpoints_req", "buffers_checkpoint",
		"checkpoint_write_time", "checkpoint_sync_time",
		"buffers_backend", "buffers_backend_fsync",
	}).AddRow(
		nil, nil, nil,
		nil, nil, nil,
		nil, nil,
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_bgwriter").WillReturnRows(rows)

	errs := scraper.ScrapeBgwriterMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeBgwriterMetrics with query error
func TestScrapeBgwriterMetrics_QueryError(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_bgwriter").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeBgwriterMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeControlCheckpoint with successful query
func TestScrapeControlCheckpoint_Success(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock checkpoint control query
	rows := sqlmock.NewRows([]string{
		"timeline_id", "checkpoint_delay", "checkpoint_delay_bytes", "redo_delay_bytes",
	}).AddRow(
		int64(1), float64(123.45), int64(1024000), int64(512000),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_control_checkpoint").WillReturnRows(rows)

	errs := scraper.ScrapeControlCheckpoint(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeControlCheckpoint with recovery mode (standby server)
func TestScrapeControlCheckpoint_RecoveryMode(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock recovery mode error (expected on standby servers)
	mock.ExpectQuery("SELECT (.+) FROM pg_control_checkpoint").
		WillReturnError(errors.New("pq: recovery is in progress"))

	errs := scraper.ScrapeControlCheckpoint(context.Background())

	// Should return no errors for recovery mode (it's expected on standby)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeControlCheckpoint with NULL values
func TestScrapeControlCheckpoint_NullValues(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"timeline_id", "checkpoint_delay", "checkpoint_delay_bytes", "redo_delay_bytes",
	}).AddRow(
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_control_checkpoint").WillReturnRows(rows)

	errs := scraper.ScrapeControlCheckpoint(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeControlCheckpoint with query error (not recovery mode)
func TestScrapeControlCheckpoint_QueryError(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock a different query error (not recovery mode)
	mock.ExpectQuery("SELECT (.+) FROM pg_control_checkpoint").
		WillReturnError(errors.New("connection timeout"))

	errs := scraper.ScrapeControlCheckpoint(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "connection timeout")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeArchiverStats with successful query
func TestScrapeArchiverStats_Success(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock archiver stats query
	rows := sqlmock.NewRows([]string{
		"archived_count", "failed_count",
	}).AddRow(
		int64(1000), int64(5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_archiver").WillReturnRows(rows)

	errs := scraper.ScrapeArchiverStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeArchiverStats with NULL values
func TestScrapeArchiverStats_NullValues(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"archived_count", "failed_count",
	}).AddRow(
		nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_archiver").WillReturnRows(rows)

	errs := scraper.ScrapeArchiverStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeArchiverStats with query error
func TestScrapeArchiverStats_QueryError(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_archiver").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeArchiverStats(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSLRUStats with successful query
func TestScrapeSLRUStats_Success(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock SLRU stats query with multiple caches
	rows := sqlmock.NewRows([]string{
		"name", "blks_zeroed", "blks_hit", "blks_read", "blks_written",
		"blks_exists", "flushes", "truncates",
	}).AddRow(
		"CommitTs", int64(100), int64(1000), int64(50), int64(75),
		int64(20), int64(10), int64(2),
	).AddRow(
		"MultiXactMember", int64(200), int64(2000), int64(100), int64(150),
		int64(40), int64(20), int64(4),
	).AddRow(
		"Subtrans", int64(150), int64(1500), int64(75), int64(100),
		int64(30), int64(15), int64(3),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_slru").WillReturnRows(rows)

	errs := scraper.ScrapeSLRUStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSLRUStats with NULL values
func TestScrapeSLRUStats_NullValues(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"name", "blks_zeroed", "blks_hit", "blks_read", "blks_written",
		"blks_exists", "flushes", "truncates",
	}).AddRow(
		"CommitTs", nil, nil, nil, nil,
		nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_slru").WillReturnRows(rows)

	errs := scraper.ScrapeSLRUStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSLRUStats with empty results
func TestScrapeSLRUStats_EmptyResults(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock with empty result set
	rows := sqlmock.NewRows([]string{
		"name", "blks_zeroed", "blks_hit", "blks_read", "blks_written",
		"blks_exists", "flushes", "truncates",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_slru").WillReturnRows(rows)

	errs := scraper.ScrapeSLRUStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSLRUStats with query error
func TestScrapeSLRUStats_QueryError(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_slru").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeSLRUStats(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRecoveryPrefetch with successful query
func TestScrapeRecoveryPrefetch_Success(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock recovery prefetch query
	rows := sqlmock.NewRows([]string{
		"prefetch", "hit", "skip_init", "skip_new", "skip_fpw",
		"skip_rep", "wal_distance", "block_distance", "io_depth",
	}).AddRow(
		int64(1000), int64(800), int64(50), int64(30), int64(20),
		int64(10), int64(1024000), int64(500), int64(5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_recovery_prefetch").WillReturnRows(rows)

	errs := scraper.ScrapeRecoveryPrefetch(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRecoveryPrefetch with NULL values
func TestScrapeRecoveryPrefetch_NullValues(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock with NULL values
	rows := sqlmock.NewRows([]string{
		"prefetch", "hit", "skip_init", "skip_new", "skip_fpw",
		"skip_rep", "wal_distance", "block_distance", "io_depth",
	}).AddRow(
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_recovery_prefetch").WillReturnRows(rows)

	errs := scraper.ScrapeRecoveryPrefetch(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRecoveryPrefetch with empty results (not on standby)
func TestScrapeRecoveryPrefetch_EmptyResults(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock with empty result set (not on standby or prefetch disabled)
	rows := sqlmock.NewRows([]string{
		"prefetch", "hit", "skip_init", "skip_new", "skip_fpw",
		"skip_rep", "wal_distance", "block_distance", "io_depth",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_recovery_prefetch").WillReturnRows(rows)

	errs := scraper.ScrapeRecoveryPrefetch(context.Background())

	// Should return no errors when no data is available
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeRecoveryPrefetch with query error
func TestScrapeRecoveryPrefetch_QueryError(t *testing.T) {
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

	scraper := NewBgwriterScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_recovery_prefetch").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeRecoveryPrefetch(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}
