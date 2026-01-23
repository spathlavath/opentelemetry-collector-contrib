// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// PostgreSQLClient defines the interface for PostgreSQL database operations.
// This abstraction allows for easy testing by injecting mock implementations.
type PostgreSQLClient interface {
	// Connection management
	Close() error
	Ping(ctx context.Context) error

	// Version detection
	GetVersion(ctx context.Context) (int, error)

	// Database metrics from pg_stat_database
	// The supportsPG12 parameter enables checksum metrics for PostgreSQL 12+
	QueryDatabaseMetrics(ctx context.Context, supportsPG12 bool) ([]models.PgStatDatabaseMetric, error)

	// Session metrics from pg_stat_database (PG14+)
	QuerySessionMetrics(ctx context.Context) ([]models.PgStatDatabaseSessionMetric, error)

	// Conflict metrics from pg_stat_database_conflicts (PG9.6+)
	QueryConflictMetrics(ctx context.Context) ([]models.PgStatDatabaseConflictsMetric, error)

	// QueryReplicationMetrics retrieves replication statistics from pg_stat_replication
	// Uses version-specific queries:
	// - PostgreSQL 9.6: Uses pg_xlog_location_diff, returns NULL for lag times
	// - PostgreSQL 10+: Uses pg_wal_lsn_diff, includes write_lag/flush_lag/replay_lag
	// Returns empty slice if no replication is configured or if server is a standby
	// Available in PostgreSQL 9.6+
	QueryReplicationMetrics(ctx context.Context, version int) ([]models.PgStatReplicationMetric, error)

	// QueryReplicationSlots retrieves replication slot statistics from pg_replication_slots
	// Uses version-specific queries:
	// - PostgreSQL 9.4-9.6: Uses pg_xlog_location_diff and pg_current_xlog_location
	// - PostgreSQL 10+: Uses pg_wal_lsn_diff and pg_current_wal_lsn
	// Returns empty slice if no replication slots are configured
	// Available in PostgreSQL 9.4+
	QueryReplicationSlots(ctx context.Context, version int) ([]models.PgReplicationSlotMetric, error)

	// QueryReplicationSlotStats retrieves replication slot statistics from pg_stat_replication_slots
	// This view provides statistics about logical decoding on replication slots
	// Returns empty slice if no logical replication slots are configured
	// Available in PostgreSQL 14+
	QueryReplicationSlotStats(ctx context.Context) ([]models.PgStatReplicationSlotMetric, error)

	// QueryReplicationDelay retrieves replication lag metrics on standby servers
	// Uses version-specific queries:
	// - PostgreSQL 9.6: Uses pg_xlog_location_diff and pg_last_xlog_receive/replay_location
	// - PostgreSQL 10+: Uses pg_wal_lsn_diff and pg_last_wal_receive/replay_lsn
	// Returns 0 values on primary servers (not in recovery)
	// Returns single row with replication_delay (seconds) and replication_delay_bytes
	// Available in PostgreSQL 9.6+
	QueryReplicationDelay(ctx context.Context, version int) (*models.PgReplicationDelayMetric, error)
}
