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

	// QueryServerUptime retrieves the PostgreSQL server uptime in seconds
	// Uses pg_postmaster_start_time() to calculate elapsed time since server start
	// Available in PostgreSQL 9.6+
	QueryServerUptime(ctx context.Context) (*models.PgUptimeMetric, error)

	// QueryDatabaseCount retrieves the count of databases that allow connections
	// Excludes template databases
	// Available in PostgreSQL 9.6+
	QueryDatabaseCount(ctx context.Context) (*models.PgDatabaseCountMetric, error)

	// QueryRunningStatus performs a simple health check query
	// Returns 1 if the server is running and responding
	// Available in PostgreSQL 9.6+
	QueryRunningStatus(ctx context.Context) (*models.PgRunningStatusMetric, error)

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

	// QueryWalReceiverMetrics retrieves WAL receiver statistics from pg_stat_wal_receiver
	// This view shows the state of the WAL receiver process on a standby server
	// Returns nil if no WAL receiver is running (primary servers or standby with WAL receiver stopped)
	// Available in PostgreSQL 9.6+
	QueryWalReceiverMetrics(ctx context.Context) (*models.PgStatWalReceiverMetric, error)

	// QueryWalStatistics retrieves WAL statistics from pg_stat_wal
	// Uses version-specific queries:
	// - PostgreSQL 14-17: Returns 8 metrics including write/sync timing
	// - PostgreSQL 18+: Returns 4 core metrics (timing removed)
	// Available on both primary and standby servers
	// Available in PostgreSQL 14+
	QueryWalStatistics(ctx context.Context, version int) (*models.PgStatWalMetric, error)

	// QueryWalFiles retrieves WAL file statistics from pg_ls_waldir()
	// Returns count, total size, and age of WAL files
	// Available on both primary and standby servers
	// Available in PostgreSQL 10+
	QueryWalFiles(ctx context.Context) (*models.PgWalFilesMetric, error)

	// QuerySubscriptionStats retrieves logical replication subscription statistics
	// Joins pg_stat_subscription with pg_stat_subscription_stats
	// Returns empty slice if no subscriptions are configured
	// Available on subscriber servers only
	// Available in PostgreSQL 15+
	QuerySubscriptionStats(ctx context.Context) ([]models.PgStatSubscriptionMetric, error)

	// QueryBgwriterMetrics retrieves background writer and checkpointer statistics
	// Uses version-specific queries:
	// - PostgreSQL 17+: Queries both pg_stat_bgwriter and pg_stat_checkpointer
	// - PostgreSQL < 17: Queries only pg_stat_bgwriter
	// Returns server-level statistics about background writer and checkpointer processes
	// Available in PostgreSQL 9.6+
	QueryBgwriterMetrics(ctx context.Context, version int) (*models.PgStatBgwriterMetric, error)

	// QueryControlCheckpoint retrieves checkpoint control statistics from pg_control_checkpoint()
	// Returns checkpoint timing and WAL location information
	// Available in PostgreSQL 10+
	QueryControlCheckpoint(ctx context.Context) (*models.PgControlCheckpointMetric, error)

	// QueryArchiverStats retrieves WAL archiver statistics from pg_stat_archiver
	// Returns counts of successfully archived and failed WAL files
	// Available in PostgreSQL 9.6+
	QueryArchiverStats(ctx context.Context) (*models.PgStatArchiverMetric, error)
}
