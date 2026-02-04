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

	// Activity metrics from pg_stat_activity
	// Retrieves connection activity statistics grouped by database, user, application, and backend type
	// Includes active waiting queries, transaction age, and backend transaction IDs
	// Available in PostgreSQL 9.6+
	QueryActivityMetrics(ctx context.Context) ([]models.PgStatActivity, error)

	// Wait event metrics from pg_stat_activity
	// Retrieves backend counts grouped by wait event type for performance analysis
	// grouped by database, user, application, backend type, and wait event
	// Available in PostgreSQL 9.6+
	QueryWaitEvents(ctx context.Context) ([]models.PgStatActivityWaitEvents, error)

	// pg_stat_statements deallocation metrics from pg_stat_statements_info
	// Retrieves the number of times pg_stat_statements has deallocated least-used statements
	// Requires pg_stat_statements extension to be installed and enabled
	// Returns nil if extension is not available (query will fail gracefully)
	// Available in PostgreSQL 13+
	QueryPgStatStatementsDealloc(ctx context.Context) (*models.PgStatStatementsDealloc, error)

	// Snapshot metrics from pg_snapshot functions
	// Retrieves transaction snapshot information including xmin, xmax, and in-progress transaction count
	// Provides insight into current transaction state and visibility
	// Available in PostgreSQL 13+
	QuerySnapshot(ctx context.Context) (*models.PgSnapshot, error)

	// QueryBuffercache retrieves buffer cache statistics from pg_buffercache extension
	// Returns buffer usage metrics grouped by database, schema, and table
	// Requires pg_buffercache extension to be installed and enabled
	// Returns empty slice if extension is not available (query will fail gracefully)
	// Available in PostgreSQL 9.6+
	QueryBuffercache(ctx context.Context) ([]models.PgBuffercache, error)

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

	// QueryConnectionStats retrieves connection statistics from pg_settings and pg_stat_activity
	// Returns max_connections setting and current connection count
	// Available in PostgreSQL 9.6+
	QueryConnectionStats(ctx context.Context) (*models.PgConnectionStatsMetric, error)

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

	// QuerySLRUStats retrieves SLRU (Simple LRU) cache statistics from pg_stat_slru
	// Returns per-SLRU cache performance metrics
	// Returns one row per SLRU (e.g., CommitTs, MultiXactMember, MultiXactOffset, Notify, Serial, Subtrans, Xact)
	// Available in PostgreSQL 13+
	QuerySLRUStats(ctx context.Context) ([]models.PgStatSLRUMetric, error)

	// QueryRecoveryPrefetch retrieves recovery prefetch statistics from pg_stat_recovery_prefetch
	// Returns standby server prefetch performance metrics during WAL replay
	// Returns nil if not on a standby server or if prefetch is not active
	// Available in PostgreSQL 15+
	QueryRecoveryPrefetch(ctx context.Context) (*models.PgStatRecoveryPrefetchMetric, error)

	// QueryUserTables retrieves per-table statistics from pg_stat_user_tables
	// Returns vacuum/analyze statistics and row-level activity per table
	// Filters by specified schemas and tables
	// Available in PostgreSQL 9.6+
	QueryUserTables(ctx context.Context, schemas, tables []string) ([]models.PgStatUserTablesMetric, error)

	// QueryIOUserTables retrieves per-table disk IO statistics from pg_statio_user_tables
	// Returns heap, index, and TOAST block reads from disk vs buffer cache
	// Filters by specified schemas and tables
	// Available in PostgreSQL 9.6+
	QueryIOUserTables(ctx context.Context, schemas, tables []string) ([]models.PgStatIOUserTables, error)

	// QueryUserIndexes retrieves per-index statistics from pg_stat_user_indexes
	// Returns index usage statistics for individual indexes
	// Filters by specified schemas and tables
	// Available in PostgreSQL 9.6+
	QueryUserIndexes(ctx context.Context, schemas, tables []string) ([]models.PgStatUserIndexes, error)

	// QueryToastTables retrieves TOAST table vacuum statistics
	// Returns vacuum/autovacuum statistics for TOAST tables of specified base tables
	// TOAST tables are automatically created for tables with large column values
	// Filters by specified schemas and tables (base table names)
	// Available in PostgreSQL 9.6+
	QueryToastTables(ctx context.Context, schemas, tables []string) ([]models.PgStatToastTables, error)

	// QueryTableSizes retrieves table size statistics from pg_class
	// Returns relation size and TOAST size for specified tables
	// Filters by specified schemas and tables
	// Available in PostgreSQL 9.6+
	QueryTableSizes(ctx context.Context, schemas, tables []string) ([]models.PgClassSizes, error)

	// QueryRelationStats retrieves relation statistics from pg_class
	// Returns relation metadata and transaction age for specified tables
	// Filters by specified schemas and tables
	// Available in PostgreSQL 9.6+
	QueryRelationStats(ctx context.Context, schemas, tables []string) ([]models.PgClassStats, error)

	// QueryAnalyzeProgress retrieves ANALYZE operation progress from pg_stat_progress_analyze
	// Returns real-time progress of running ANALYZE operations
	// Returns empty slice if no ANALYZE operations are currently running
	// Available in PostgreSQL 13+
	QueryAnalyzeProgress(ctx context.Context) ([]models.PgStatProgressAnalyze, error)

	// QueryClusterProgress retrieves CLUSTER/VACUUM FULL operation progress from pg_stat_progress_cluster
	// Returns real-time progress of running CLUSTER or VACUUM FULL operations
	// Returns empty slice if no CLUSTER/VACUUM FULL operations are currently running
	// Available in PostgreSQL 12+
	QueryClusterProgress(ctx context.Context) ([]models.PgStatProgressCluster, error)

	// QueryCreateIndexProgress retrieves CREATE INDEX operation progress from pg_stat_progress_create_index
	// Returns real-time progress of running CREATE INDEX operations
	// Returns empty slice if no CREATE INDEX operations are currently running
	// Available in PostgreSQL 12+
	QueryCreateIndexProgress(ctx context.Context) ([]models.PgStatProgressCreateIndex, error)

	// QueryVacuumProgress retrieves VACUUM operation progress from pg_stat_progress_vacuum
	// Returns real-time progress of running VACUUM operations
	// Returns empty slice if no VACUUM operations are currently running
	// Available in PostgreSQL 12+
	QueryVacuumProgress(ctx context.Context) ([]models.PgStatProgressVacuum, error)

	// QueryPgBouncerStats retrieves connection pool statistics from PgBouncer
	// Returns statistics from SHOW STATS command
	// Must be executed against PgBouncer admin console
	// Returns empty slice if PgBouncer is not available
	// Available in PgBouncer 1.8+
	QueryPgBouncerStats(ctx context.Context) ([]models.PgBouncerStatsMetric, error)

	// QueryPgBouncerPools retrieves per-pool connection details from PgBouncer
	// Returns connection pool status from SHOW POOLS command
	// Must be executed against PgBouncer admin console
	// Returns empty slice if PgBouncer is not available
	// Available in PgBouncer 1.8+
	QueryPgBouncerPools(ctx context.Context) ([]models.PgBouncerPoolsMetric, error)

	// QueryLocks retrieves lock statistics from pg_locks
	// Returns lock counts grouped by lock mode for the current database
	// Available in PostgreSQL 9.6+
	QueryLocks(ctx context.Context) (*models.PgLocksMetric, error)

	// QueryIndexSize retrieves the size of an index using pg_relation_size
	// Takes an index OID and returns the size in bytes
	// Available in PostgreSQL 9.6+
	QueryIndexSize(ctx context.Context, indexOID int64) (int64, error)

	// QuerySlowQueries retrieves slow query statistics from pg_stat_statements
	// Uses delta calculation for interval-based averages (similar to Oracle/MSSQL implementations)
	// Requires pg_stat_statements extension to be installed and enabled
	// Parameters:
	// - intervalSeconds: Time window for fetching queries (used for delta calculation context)
	// - responseTimeThreshold: Minimum average elapsed time in ms (filtering done in Go layer)
	// - countThreshold: Maximum number of slow queries to return (Top N selection done in Go layer)
	// Returns empty slice if extension is not available (query will fail gracefully)
	// Available in PostgreSQL 13+ (recommended for planning time metrics)
	QuerySlowQueries(ctx context.Context, intervalSeconds, responseTimeThreshold, countThreshold int) ([]models.SlowQuery, error)
}
