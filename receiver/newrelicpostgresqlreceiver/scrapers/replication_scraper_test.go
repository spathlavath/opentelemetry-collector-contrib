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

// Test NewReplicationScraper creates a scraper successfully
func TestNewReplicationScraper(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	assert.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.Equal(t, 140000, scraper.version)
}

// Test ScrapeReplicationMetrics with successful query (PostgreSQL 9.6+)
func TestScrapeReplicationMetrics_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock replication metrics query
	rows := sqlmock.NewRows([]string{
		"application_name", "state", "sync_state", "client_addr",
		"backend_xmin_age", "sent_lsn_delay", "write_lsn_delay", "flush_lsn_delay", "replay_lsn_delay",
		"write_lag", "flush_lag", "replay_lag",
	}).AddRow(
		"walreceiver", "streaming", "async", "192.168.1.100",
		int64(1000), int64(1024), int64(512), int64(256), int64(128),
		nil, nil, nil, // NULL lag times for PG 9.6
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationMetrics with successful query (PostgreSQL 10+ with lag times)
func TestScrapeReplicationMetrics_SuccessPG10(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 100000)

	// Mock replication metrics query with lag times (PG 10+)
	rows := sqlmock.NewRows([]string{
		"application_name", "state", "sync_state", "client_addr",
		"backend_xmin_age", "sent_lsn_delay", "write_lsn_delay", "flush_lsn_delay", "replay_lsn_delay",
		"write_lag", "flush_lag", "replay_lag",
	}).AddRow(
		"walreceiver", "streaming", "sync", "192.168.1.100",
		int64(1000), int64(1024), int64(512), int64(256), int64(128),
		float64(0.5), float64(1.0), float64(2.5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationMetrics with empty results (no replicas)
func TestScrapeReplicationMetrics_EmptyResults(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock with empty result set (no replicas)
	rows := sqlmock.NewRows([]string{
		"application_name", "state", "sync_state", "client_addr",
		"backend_xmin_age", "sent_lsn_delay", "write_lsn_delay", "flush_lsn_delay", "replay_lsn_delay",
		"write_lag", "flush_lag", "replay_lag",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationMetrics(context.Background())

	// Empty result is not an error (server may be standby or have no replicas)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationMetrics with version check (< PostgreSQL 9.6)
func TestScrapeReplicationMetrics_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 90500)

	errs := scraper.ScrapeReplicationMetrics(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeReplicationMetrics with query error
func TestScrapeReplicationMetrics_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeReplicationMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationSlots with successful query (PostgreSQL 9.4+)
func TestScrapeReplicationSlots_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 94000)

	// Mock replication slots query
	rows := sqlmock.NewRows([]string{
		"slot_name", "slot_type", "plugin", "active",
		"xmin_age", "catalog_xmin_age", "restart_delay_bytes", "confirmed_flush_delay_bytes",
	}).AddRow(
		"slot1", "physical", nil, true,
		int64(100), int64(50), int64(1024), int64(512),
	).AddRow(
		"slot2", "logical", "pgoutput", false,
		int64(200), int64(100), int64(2048), int64(1024),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_replication_slots").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationSlots(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationSlots with empty results (no slots)
func TestScrapeReplicationSlots_EmptyResults(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 94000)

	// Mock with empty result set (no slots)
	rows := sqlmock.NewRows([]string{
		"slot_name", "slot_type", "plugin", "active",
		"xmin_age", "catalog_xmin_age", "restart_delay_bytes", "confirmed_flush_delay_bytes",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_replication_slots").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationSlots(context.Background())

	// Empty result is not an error (server may not have slots configured)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationSlots with version check (< PostgreSQL 9.4)
func TestScrapeReplicationSlots_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 90300)

	errs := scraper.ScrapeReplicationSlots(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeReplicationSlots with query error
func TestScrapeReplicationSlots_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 94000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_replication_slots").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeReplicationSlots(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationSlotStats with successful query (PostgreSQL 14+)
func TestScrapeReplicationSlotStats_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock replication slot stats query
	rows := sqlmock.NewRows([]string{
		"slot_name", "slot_type", "state",
		"spill_txns", "spill_count", "spill_bytes",
		"stream_txns", "stream_count", "stream_bytes",
		"total_txns", "total_bytes",
	}).AddRow(
		"logical_slot", "logical", "active",
		int64(10), int64(5), int64(1024),
		int64(20), int64(15), int64(2048),
		int64(100), int64(10240),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication_slots").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationSlotStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationSlotStats with empty results (no logical slots)
func TestScrapeReplicationSlotStats_EmptyResults(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock with empty result set (no logical slots)
	rows := sqlmock.NewRows([]string{
		"slot_name", "slot_type", "state",
		"spill_txns", "spill_count", "spill_bytes",
		"stream_txns", "stream_count", "stream_bytes",
		"total_txns", "total_bytes",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication_slots").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationSlotStats(context.Background())

	// Empty result is not an error (server may not have logical slots configured)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationSlotStats with version check (< PostgreSQL 14)
func TestScrapeReplicationSlotStats_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	errs := scraper.ScrapeReplicationSlotStats(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeReplicationSlotStats with query error
func TestScrapeReplicationSlotStats_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_replication_slots").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeReplicationSlotStats(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationDelay with successful query (standby server)
func TestScrapeReplicationDelay_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock replication delay query (standby server)
	rows := sqlmock.NewRows([]string{"replication_delay", "replication_delay_bytes"}).
		AddRow(float64(5.5), int64(1024))

	mock.ExpectQuery("SELECT (.+) replication_delay").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationDelay(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationDelay with zero values (primary server)
func TestScrapeReplicationDelay_PrimaryServer(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock replication delay query (primary server - zero values)
	rows := sqlmock.NewRows([]string{"replication_delay", "replication_delay_bytes"}).
		AddRow(float64(0), int64(0))

	mock.ExpectQuery("SELECT (.+) replication_delay").WillReturnRows(rows)

	errs := scraper.ScrapeReplicationDelay(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeReplicationDelay with version check (< PostgreSQL 9.6)
func TestScrapeReplicationDelay_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 90500)

	errs := scraper.ScrapeReplicationDelay(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeReplicationDelay with query error
func TestScrapeReplicationDelay_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) replication_delay").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeReplicationDelay(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalReceiverMetrics with successful query (standby server)
func TestScrapeWalReceiverMetrics_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock WAL receiver query (standby server - streaming)
	rows := sqlmock.NewRows([]string{
		"status", "received_tli", "last_msg_send_age", "last_msg_receipt_age", "latest_end_age",
	}).AddRow(
		"streaming", int64(1), float64(1.5), float64(2.0), float64(3.5),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal_receiver").WillReturnRows(rows)

	errs := scraper.ScrapeWalReceiverMetrics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalReceiverMetrics with nil metric (primary server)
func TestScrapeWalReceiverMetrics_NilMetric(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock WAL receiver query (primary server - no rows)
	rows := sqlmock.NewRows([]string{
		"status", "received_tli", "last_msg_send_age", "last_msg_receipt_age", "latest_end_age",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal_receiver").WillReturnRows(rows)

	errs := scraper.ScrapeWalReceiverMetrics(context.Background())

	// No error if nil metric (normal on primary servers)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalReceiverMetrics with version check (< PostgreSQL 9.6)
func TestScrapeWalReceiverMetrics_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 90500)

	errs := scraper.ScrapeWalReceiverMetrics(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeWalReceiverMetrics with query error
func TestScrapeWalReceiverMetrics_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal_receiver").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeWalReceiverMetrics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalStatistics with successful query (PostgreSQL 14-17)
func TestScrapeWalStatistics_SuccessPG14(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock WAL statistics query (PG 14-17)
	rows := sqlmock.NewRows([]string{
		"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
		"wal_write", "wal_sync", "wal_write_time", "wal_sync_time",
	}).AddRow(
		int64(10000), int64(500), int64(1048576), int64(10),
		int64(100), int64(50), float64(25.5), float64(15.0),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal").WillReturnRows(rows)

	errs := scraper.ScrapeWalStatistics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalStatistics with successful query (PostgreSQL 18+)
func TestScrapeWalStatistics_SuccessPG18(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 180000)

	// Mock WAL statistics query (PG 18+ - only 4 core metrics)
	rows := sqlmock.NewRows([]string{
		"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
	}).AddRow(
		int64(10000), int64(500), int64(1048576), int64(10),
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal").WillReturnRows(rows)

	errs := scraper.ScrapeWalStatistics(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalStatistics with empty result (no rows)
func TestScrapeWalStatistics_NilMetric(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock WAL statistics query (no rows)
	rows := sqlmock.NewRows([]string{
		"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full",
		"wal_write", "wal_sync", "wal_write_time", "wal_sync_time",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal").WillReturnRows(rows)

	errs := scraper.ScrapeWalStatistics(context.Background())

	// Empty result set causes an error
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "no rows in result set")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalStatistics with version check (< PostgreSQL 14)
func TestScrapeWalStatistics_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 130000)

	errs := scraper.ScrapeWalStatistics(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeWalStatistics with query error
func TestScrapeWalStatistics_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_wal").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeWalStatistics(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalFiles with successful query (PostgreSQL 10+)
func TestScrapeWalFiles_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 100000)

	// Mock WAL files query
	rows := sqlmock.NewRows([]string{"wal_count", "wal_size", "wal_age"}).
		AddRow(int64(10), int64(167772160), float64(300.5))

	mock.ExpectQuery("SELECT (.+) FROM pg_ls_waldir").WillReturnRows(rows)

	errs := scraper.ScrapeWalFiles(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalFiles with empty result (no rows)
func TestScrapeWalFiles_NilMetric(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 100000)

	// Mock WAL files query (no rows)
	rows := sqlmock.NewRows([]string{"wal_count", "wal_size", "wal_age"})

	mock.ExpectQuery("SELECT (.+) FROM pg_ls_waldir").WillReturnRows(rows)

	errs := scraper.ScrapeWalFiles(context.Background())

	// Empty result set causes an error
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "no rows in result set")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalFiles with permission error
func TestScrapeWalFiles_PermissionError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 100000)

	// Mock permission error (the wrapper adds "failed to query WAL files:" prefix)
	mock.ExpectQuery("SELECT (.+) FROM pg_ls_waldir").
		WillReturnError(errors.New("pq: permission denied for function pg_ls_waldir"))

	errs := scraper.ScrapeWalFiles(context.Background())

	// Permission error should not return error (logged as debug)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeWalFiles with version check (< PostgreSQL 10)
func TestScrapeWalFiles_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 96000)

	errs := scraper.ScrapeWalFiles(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeWalFiles with query error (non-permission error)
func TestScrapeWalFiles_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 100000)

	// Mock query error (not permission error)
	mock.ExpectQuery("SELECT (.+) FROM pg_ls_waldir").
		WillReturnError(errors.New("database connection failed"))

	errs := scraper.ScrapeWalFiles(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSubscriptionStats with successful query (PostgreSQL 15+)
func TestScrapeSubscriptionStats_Success(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock subscription stats query (state is the LAST column)
	rows := sqlmock.NewRows([]string{
		"subscription_name", "last_msg_send_age", "last_msg_receipt_age", "latest_end_age",
		"apply_error_count", "sync_error_count", "state",
	}).AddRow(
		"sub1", float64(1.5), float64(2.0), float64(3.5),
		int64(0), int64(0), "active",
	).AddRow(
		"sub2", float64(10.5), float64(12.0), float64(15.5),
		int64(2), int64(1), "inactive",
	)

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_subscription").WillReturnRows(rows)

	errs := scraper.ScrapeSubscriptionStats(context.Background())

	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSubscriptionStats with empty results (no subscriptions)
func TestScrapeSubscriptionStats_EmptyResults(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock with empty result set (no subscriptions) - state is the LAST column
	rows := sqlmock.NewRows([]string{
		"subscription_name", "last_msg_send_age", "last_msg_receipt_age", "latest_end_age",
		"apply_error_count", "sync_error_count", "state",
	})

	mock.ExpectQuery("SELECT (.+) FROM pg_stat_subscription").WillReturnRows(rows)

	errs := scraper.ScrapeSubscriptionStats(context.Background())

	// Empty result is not an error (server may not have subscriptions configured)
	assert.Empty(t, errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Test ScrapeSubscriptionStats with version check (< PostgreSQL 15)
func TestScrapeSubscriptionStats_VersionTooOld(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 140000)

	errs := scraper.ScrapeSubscriptionStats(context.Background())

	// Should skip without error
	assert.Empty(t, errs)
}

// Test ScrapeSubscriptionStats with query error
func TestScrapeSubscriptionStats_QueryError(t *testing.T) {
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

	scraper := NewReplicationScraper(sqlClient, mb, logger, "test-instance", metricsBuilderConfig, 150000)

	// Mock query error
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_subscription").
		WillReturnError(errors.New("query failed"))

	errs := scraper.ScrapeSubscriptionStats(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query failed")
	assert.NoError(t, mock.ExpectationsWereMet())
}
