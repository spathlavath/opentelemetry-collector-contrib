// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/queries"
)

// SQLClient implements the PostgreSQLClient interface using database/sql
type SQLClient struct {
	db *sql.DB
}

// NewSQLClient creates a new SQLClient instance
func NewSQLClient(db *sql.DB) *SQLClient {
	return &SQLClient{db: db}
}

// Close closes the database connection
func (c *SQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the database connection is alive
func (c *SQLClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// GetVersion retrieves the PostgreSQL server version number
func (c *SQLClient) GetVersion(ctx context.Context) (int, error) {
	var version int
	err := c.db.QueryRowContext(ctx, queries.VersionQuery).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	return version, nil
}

// QueryDatabaseMetrics retrieves database statistics from pg_stat_database
// For PostgreSQL 12+, includes checksum metrics when supportsPG12 is true
func (c *SQLClient) QueryDatabaseMetrics(ctx context.Context, supportsPG12 bool) ([]models.PgStatDatabaseMetric, error) {
	// Select query based on PostgreSQL version
	query := queries.PgStatDatabaseMetricsSQL
	if supportsPG12 {
		query = queries.PgStatDatabaseMetricsWithChecksumsSQL
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseMetric

	for rows.Next() {
		var metric models.PgStatDatabaseMetric

		if supportsPG12 {
			// Scan with checksum fields (PostgreSQL 12+)
			err = rows.Scan(
				&metric.DatName,
				&metric.NumBackends,
				&metric.XactCommit,
				&metric.XactRollback,
				&metric.BlksRead,
				&metric.BlksHit,
				&metric.TupReturned,
				&metric.TupFetched,
				&metric.TupInserted,
				&metric.TupUpdated,
				&metric.TupDeleted,
				&metric.Conflicts,
				&metric.TempFiles,
				&metric.TempBytes,
				&metric.Deadlocks,
				&metric.BlkReadTime,
				&metric.BlkWriteTime,
				&metric.BeforeXIDWraparound,
				&metric.DatabaseSize,
				&metric.ChecksumFailures,
				&metric.ChecksumLastFailure,
				&metric.ChecksumsEnabled,
			)
		} else {
			// Scan without checksum fields (PostgreSQL < 12)
			err = rows.Scan(
				&metric.DatName,
				&metric.NumBackends,
				&metric.XactCommit,
				&metric.XactRollback,
				&metric.BlksRead,
				&metric.BlksHit,
				&metric.TupReturned,
				&metric.TupFetched,
				&metric.TupInserted,
				&metric.TupUpdated,
				&metric.TupDeleted,
				&metric.Conflicts,
				&metric.TempFiles,
				&metric.TempBytes,
				&metric.Deadlocks,
				&metric.BlkReadTime,
				&metric.BlkWriteTime,
				&metric.BeforeXIDWraparound,
				&metric.DatabaseSize,
			)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_database row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_database rows: %w", err)
	}

	return metrics, nil
}

// QuerySessionMetrics retrieves session statistics from pg_stat_database (PostgreSQL 14+)
func (c *SQLClient) QuerySessionMetrics(ctx context.Context) ([]models.PgStatDatabaseSessionMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatDatabaseSessionMetricsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database sessions: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseSessionMetric

	for rows.Next() {
		var metric models.PgStatDatabaseSessionMetric
		err := rows.Scan(
			&metric.DatName,
			&metric.SessionTime,
			&metric.ActiveTime,
			&metric.IdleInTransactionTime,
			&metric.SessionCount,
			&metric.SessionsAbandoned,
			&metric.SessionsFatal,
			&metric.SessionsKilled,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_database session row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_database session rows: %w", err)
	}

	return metrics, nil
}

// QueryConflictMetrics retrieves conflict statistics from pg_stat_database_conflicts (PostgreSQL 9.6+)
func (c *SQLClient) QueryConflictMetrics(ctx context.Context) ([]models.PgStatDatabaseConflictsMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatDatabaseConflictsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database_conflicts: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseConflictsMetric

	for rows.Next() {
		var metric models.PgStatDatabaseConflictsMetric
		err := rows.Scan(
			&metric.DatName,
			&metric.ConflTablespace,
			&metric.ConflLock,
			&metric.ConflSnapshot,
			&metric.ConflBufferpin,
			&metric.ConflDeadlock,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_database_conflicts row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_database_conflicts rows: %w", err)
	}

	return metrics, nil
}

// QueryServerUptime retrieves the PostgreSQL server uptime in seconds
// Calculates time elapsed since server start using pg_postmaster_start_time()
// Available in PostgreSQL 9.6+
func (c *SQLClient) QueryServerUptime(ctx context.Context) (*models.PgUptimeMetric, error) {
	var metric models.PgUptimeMetric

	err := c.db.QueryRowContext(ctx, queries.PgUptimeSQL).Scan(&metric.Uptime)
	if err != nil {
		return nil, fmt.Errorf("failed to query server uptime: %w", err)
	}

	return &metric, nil
}

// QueryDatabaseCount retrieves the count of databases that allow connections
// Excludes template databases by filtering on datallowconn
// Available in PostgreSQL 9.6+
func (c *SQLClient) QueryDatabaseCount(ctx context.Context) (*models.PgDatabaseCountMetric, error) {
	var metric models.PgDatabaseCountMetric

	err := c.db.QueryRowContext(ctx, queries.PgDatabaseCountSQL).Scan(&metric.DatabaseCount)
	if err != nil {
		return nil, fmt.Errorf("failed to query database count: %w", err)
	}

	return &metric, nil
}

// QueryRunningStatus performs a simple health check query
// Returns 1 if the server is running and responding to queries
// Available in PostgreSQL 9.6+
func (c *SQLClient) QueryRunningStatus(ctx context.Context) (*models.PgRunningStatusMetric, error) {
	var metric models.PgRunningStatusMetric

	err := c.db.QueryRowContext(ctx, queries.PgRunningStatusSQL).Scan(&metric.Running)
	if err != nil {
		return nil, fmt.Errorf("failed to query running status: %w", err)
	}

	return &metric, nil
}

// QueryReplicationMetrics retrieves replication statistics from pg_stat_replication
// Uses version-specific queries based on PostgreSQL version
func (c *SQLClient) QueryReplicationMetrics(ctx context.Context, version int) ([]models.PgStatReplicationMetric, error) {
	// Select appropriate query based on PostgreSQL version
	var query string
	if version >= 100000 { // PostgreSQL 10.0+
		query = queries.PgStatReplicationMetricsPG10SQL
	} else { // PostgreSQL 9.6
		query = queries.PgStatReplicationMetricsPG96SQL
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_replication: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatReplicationMetric

	for rows.Next() {
		var metric models.PgStatReplicationMetric

		err = rows.Scan(
			&metric.ApplicationName,
			&metric.State,
			&metric.SyncState,
			&metric.ClientAddr,
			&metric.BackendXminAge,
			&metric.SentLsnDelay,
			&metric.WriteLsnDelay,
			&metric.FlushLsnDelay,
			&metric.ReplayLsnDelay,
			&metric.WriteLag,
			&metric.FlushLag,
			&metric.ReplayLag,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_replication row: %w", err)
		}

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_replication rows: %w", err)
	}

	return metrics, nil
}

// QueryReplicationSlots retrieves replication slot statistics from pg_replication_slots
// Uses version-specific queries based on PostgreSQL version
func (c *SQLClient) QueryReplicationSlots(ctx context.Context, version int) ([]models.PgReplicationSlotMetric, error) {
	// Select appropriate query based on PostgreSQL version
	var query string
	if version >= 100000 { // PostgreSQL 10.0+
		query = queries.PgReplicationSlotsPG10SQL
	} else { // PostgreSQL 9.4-9.6
		query = queries.PgReplicationSlotsPG96SQL
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_replication_slots: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgReplicationSlotMetric

	for rows.Next() {
		var metric models.PgReplicationSlotMetric

		err = rows.Scan(
			&metric.SlotName,
			&metric.SlotType,
			&metric.Plugin,
			&metric.Active,
			&metric.XminAge,
			&metric.CatalogXminAge,
			&metric.RestartDelayBytes,
			&metric.ConfirmedFlushDelayBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_replication_slots row: %w", err)
		}

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_replication_slots rows: %w", err)
	}

	return metrics, nil
}

// QueryReplicationSlotStats retrieves replication slot statistics from pg_stat_replication_slots
// This view is only available in PostgreSQL 14+
func (c *SQLClient) QueryReplicationSlotStats(ctx context.Context) ([]models.PgStatReplicationSlotMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatReplicationSlotsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_replication_slots: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatReplicationSlotMetric

	for rows.Next() {
		var metric models.PgStatReplicationSlotMetric

		err = rows.Scan(
			&metric.SlotName,
			&metric.SlotType,
			&metric.State,
			&metric.SpillTxns,
			&metric.SpillCount,
			&metric.SpillBytes,
			&metric.StreamTxns,
			&metric.StreamCount,
			&metric.StreamBytes,
			&metric.TotalTxns,
			&metric.TotalBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_replication_slots row: %w", err)
		}

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_replication_slots rows: %w", err)
	}

	return metrics, nil
}

// QueryReplicationDelay retrieves replication lag metrics on standby servers
// Uses version-specific queries based on PostgreSQL version
// Returns single metric with replication_delay (seconds) and replication_delay_bytes
func (c *SQLClient) QueryReplicationDelay(ctx context.Context, version int) (*models.PgReplicationDelayMetric, error) {
	// Select appropriate query based on PostgreSQL version
	var query string
	if version >= 100000 { // PostgreSQL 10.0+
		query = queries.PgReplicationDelayPG10SQL
	} else { // PostgreSQL 9.6
		query = queries.PgReplicationDelayPG96SQL
	}

	var metric models.PgReplicationDelayMetric
	err := c.db.QueryRowContext(ctx, query).Scan(
		&metric.ReplicationDelay,
		&metric.ReplicationDelayBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query replication delay: %w", err)
	}

	return &metric, nil
}

// QueryWalReceiverMetrics retrieves WAL receiver statistics from pg_stat_wal_receiver
// Returns nil if no WAL receiver is running (primary servers or standby with WAL receiver stopped)
func (c *SQLClient) QueryWalReceiverMetrics(ctx context.Context) (*models.PgStatWalReceiverMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatWalReceiverSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_wal_receiver: %w", err)
	}
	defer rows.Close()

	// Check if we have any rows (WAL receiver only exists on standby servers)
	if !rows.Next() {
		// No WAL receiver found - this is normal on primary servers
		return nil, nil
	}

	var metric models.PgStatWalReceiverMetric
	err = rows.Scan(
		&metric.Status,
		&metric.ReceivedTli,
		&metric.LastMsgSendAge,
		&metric.LastMsgReceiptAge,
		&metric.LatestEndAge,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan pg_stat_wal_receiver row: %w", err)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_wal_receiver rows: %w", err)
	}

	return &metric, nil
}

// QueryWalStatistics retrieves WAL statistics from pg_stat_wal
// Uses version-specific queries:
// - PostgreSQL 14-17: Returns 8 metrics including write/sync timing
// - PostgreSQL 18+: Returns 4 core metrics (timing removed)
// Returns nil if pg_stat_wal returns no rows
func (c *SQLClient) QueryWalStatistics(ctx context.Context, version int) (*models.PgStatWalMetric, error) {
	// Select appropriate query based on PostgreSQL version
	var query string
	isPG18Plus := version >= 180000 // PostgreSQL 18.0+

	if isPG18Plus {
		query = queries.PgStatWalPG18SQL
	} else {
		query = queries.PgStatWalPG14SQL
	}

	var metric models.PgStatWalMetric

	if isPG18Plus {
		// PostgreSQL 18+: Only 4 core metrics
		err := c.db.QueryRowContext(ctx, query).Scan(
			&metric.WalRecords,
			&metric.WalFpi,
			&metric.WalBytes,
			&metric.WalBuffersFull,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to query pg_stat_wal (PG18+): %w", err)
		}
	} else {
		// PostgreSQL 14-17: All 8 metrics including timing
		err := c.db.QueryRowContext(ctx, query).Scan(
			&metric.WalRecords,
			&metric.WalFpi,
			&metric.WalBytes,
			&metric.WalBuffersFull,
			&metric.WalWrite,
			&metric.WalSync,
			&metric.WalWriteTime,
			&metric.WalSyncTime,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to query pg_stat_wal (PG14-17): %w", err)
		}
	}

	return &metric, nil
}

// QueryWalFiles retrieves WAL file statistics from pg_ls_waldir()
// Returns count, total size, and age of WAL files in the pg_wal directory
// Available in PostgreSQL 10+
func (c *SQLClient) QueryWalFiles(ctx context.Context) (*models.PgWalFilesMetric, error) {
	var metric models.PgWalFilesMetric

	err := c.db.QueryRowContext(ctx, queries.PgWalFilesSQL).Scan(
		&metric.WalCount,
		&metric.WalSize,
		&metric.WalAge,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query WAL files: %w", err)
	}

	return &metric, nil
}

// QuerySubscriptionStats retrieves logical replication subscription statistics
// Joins pg_stat_subscription with pg_stat_subscription_stats
// Returns empty slice if no subscriptions are configured
// Available in PostgreSQL 15+
func (c *SQLClient) QuerySubscriptionStats(ctx context.Context) ([]models.PgStatSubscriptionMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatSubscriptionSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_subscription: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatSubscriptionMetric

	for rows.Next() {
		var metric models.PgStatSubscriptionMetric

		err = rows.Scan(
			&metric.SubscriptionName,
			&metric.LastMsgSendAge,
			&metric.LastMsgReceiptAge,
			&metric.LatestEndAge,
			&metric.ApplyErrorCount,
			&metric.SyncErrorCount,
			&metric.State,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_subscription row: %w", err)
		}

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_subscription rows: %w", err)
	}

	return metrics, nil
}

// QueryBgwriterMetrics retrieves background writer and checkpointer statistics
// For PostgreSQL 17+, queries both pg_stat_bgwriter and pg_stat_checkpointer
// For PostgreSQL < 17, queries only pg_stat_bgwriter
func (c *SQLClient) QueryBgwriterMetrics(ctx context.Context, version int) (*models.PgStatBgwriterMetric, error) {
	// Select query based on PostgreSQL version
	// PostgreSQL 17 = 170000
	query := queries.PgStatBgwriterPrePG17SQL
	if version >= 170000 {
		query = queries.PgStatBgwriterPG17SQL
	}

	var metric models.PgStatBgwriterMetric

	err := c.db.QueryRowContext(ctx, query).Scan(
		&metric.BuffersClean,
		&metric.MaxwrittenClean,
		&metric.BuffersAlloc,
		&metric.CheckpointsTimed,
		&metric.CheckpointsRequested,
		&metric.BuffersCheckpoint,
		&metric.CheckpointWriteTime,
		&metric.CheckpointSyncTime,
		&metric.BuffersBackend,
		&metric.BuffersBackendFsync,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_bgwriter: %w", err)
	}

	return &metric, nil
}

// QueryControlCheckpoint retrieves checkpoint control statistics from pg_control_checkpoint()
// Available in PostgreSQL 10+
func (c *SQLClient) QueryControlCheckpoint(ctx context.Context) (*models.PgControlCheckpointMetric, error) {
	var metric models.PgControlCheckpointMetric

	err := c.db.QueryRowContext(ctx, queries.PgControlCheckpointSQL).Scan(
		&metric.TimelineID,
		&metric.CheckpointDelay,
		&metric.CheckpointDelayBytes,
		&metric.RedoDelayBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_control_checkpoint: %w", err)
	}

	return &metric, nil
}

// QueryArchiverStats retrieves WAL archiver statistics from pg_stat_archiver
// Available in PostgreSQL 9.6+
func (c *SQLClient) QueryArchiverStats(ctx context.Context) (*models.PgStatArchiverMetric, error) {
	var metric models.PgStatArchiverMetric

	err := c.db.QueryRowContext(ctx, queries.PgStatArchiverSQL).Scan(
		&metric.ArchivedCount,
		&metric.FailedCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_archiver: %w", err)
	}

	return &metric, nil
}

// QuerySLRUStats retrieves SLRU (Simple LRU) cache statistics from pg_stat_slru
// Returns per-SLRU cache performance metrics
// Available in PostgreSQL 13+
func (c *SQLClient) QuerySLRUStats(ctx context.Context) ([]models.PgStatSLRUMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatSLRUSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_slru: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatSLRUMetric

	for rows.Next() {
		var metric models.PgStatSLRUMetric
		err := rows.Scan(
			&metric.SLRUName,
			&metric.BlksZeroed,
			&metric.BlksHit,
			&metric.BlksRead,
			&metric.BlksWritten,
			&metric.BlksExists,
			&metric.Flushes,
			&metric.Truncates,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_slru row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_slru rows: %w", err)
	}

	return metrics, nil
}

// QueryRecoveryPrefetch retrieves recovery prefetch statistics from pg_stat_recovery_prefetch
// Returns standby server prefetch performance metrics during WAL replay
// Returns nil if not on a standby server or if the view returns no rows
// Available in PostgreSQL 15+
func (c *SQLClient) QueryRecoveryPrefetch(ctx context.Context) (*models.PgStatRecoveryPrefetchMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatRecoveryPrefetchSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_recovery_prefetch: %w", err)
	}
	defer rows.Close()

	// Check if we have any rows (recovery prefetch only exists on standby servers with prefetch enabled)
	if !rows.Next() {
		// No recovery prefetch data found - this is normal on primary servers
		return nil, nil
	}

	var metric models.PgStatRecoveryPrefetchMetric
	err = rows.Scan(
		&metric.Prefetch,
		&metric.Hit,
		&metric.SkipInit,
		&metric.SkipNew,
		&metric.SkipFpw,
		&metric.SkipRep,
		&metric.WalDistance,
		&metric.BlockDistance,
		&metric.IoDepth,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan pg_stat_recovery_prefetch row: %w", err)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_recovery_prefetch rows: %w", err)
	}

	return &metric, nil
}

// QueryUserTables retrieves per-table statistics from pg_stat_user_tables
// Returns vacuum/analyze statistics and row-level activity per table
// Filters by specified schemas and tables
// Available in PostgreSQL 9.6+
func (c *SQLClient) QueryUserTables(ctx context.Context, schemas, tables []string) ([]models.PgStatUserTablesMetric, error) {
	query := queries.PgStatUserTablesSQL(schemas, tables)
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_user_tables: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatUserTablesMetric

	for rows.Next() {
		var metric models.PgStatUserTablesMetric
		err := rows.Scan(
			&metric.Database,
			&metric.SchemaName,
			&metric.TableName,
			&metric.SeqScan,
			&metric.SeqTupRead,
			&metric.IdxScan,
			&metric.IdxTupFetch,
			&metric.NTupIns,
			&metric.NTupUpd,
			&metric.NTupDel,
			&metric.NTupHotUpd,
			&metric.NLiveTup,
			&metric.NDeadTup,
			&metric.NModSinceAnalyze,
			&metric.LastVacuumAge,
			&metric.LastAutovacuumAge,
			&metric.LastAnalyzeAge,
			&metric.LastAutoanalyzeAge,
			&metric.VacuumCount,
			&metric.AutovacuumCount,
			&metric.AnalyzeCount,
			&metric.AutoanalyzeCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_user_tables row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_user_tables rows: %w", err)
	}

	return metrics, nil
}

func (c *SQLClient) QueryAnalyzeProgress(ctx context.Context) ([]models.PgStatProgressAnalyze, error) {
	query := queries.PgStatProgressAnalyzeSQL
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_progress_analyze: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatProgressAnalyze

	for rows.Next() {
		var metric models.PgStatProgressAnalyze
		err := rows.Scan(
			&metric.Database,
			&metric.SchemaName,
			&metric.TableName,
			&metric.Phase,
			&metric.SampleBlksTotal,
			&metric.SampleBlksScanned,
			&metric.ExtStatsTotal,
			&metric.ExtStatsComputed,
			&metric.ChildTablesTotal,
			&metric.ChildTablesDone,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_progress_analyze row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_progress_analyze rows: %w", err)
	}

	return metrics, nil
}

func (c *SQLClient) QueryClusterProgress(ctx context.Context) ([]models.PgStatProgressCluster, error) {
	query := queries.PgStatProgressClusterSQL
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_progress_cluster: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatProgressCluster

	for rows.Next() {
		var metric models.PgStatProgressCluster
		err := rows.Scan(
			&metric.Database,
			&metric.SchemaName,
			&metric.TableName,
			&metric.Command,
			&metric.Phase,
			&metric.HeapBlksTotal,
			&metric.HeapBlksScanned,
			&metric.HeapTuplesScanned,
			&metric.HeapTuplesWritten,
			&metric.IndexRebuildCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_progress_cluster row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_progress_cluster rows: %w", err)
	}

	return metrics, nil
}

func (c *SQLClient) QueryCreateIndexProgress(ctx context.Context) ([]models.PgStatProgressCreateIndex, error) {
	query := queries.PgStatProgressCreateIndexSQL
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_progress_create_index: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatProgressCreateIndex

	for rows.Next() {
		var metric models.PgStatProgressCreateIndex
		err := rows.Scan(
			&metric.Database,
			&metric.SchemaName,
			&metric.TableName,
			&metric.IndexName,
			&metric.Command,
			&metric.Phase,
			&metric.LockersTotal,
			&metric.LockersDone,
			&metric.BlocksTotal,
			&metric.BlocksDone,
			&metric.TuplesTotal,
			&metric.TuplesDone,
			&metric.PartitionsTotal,
			&metric.PartitionsDone,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_progress_create_index row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_progress_create_index rows: %w", err)
	}

	return metrics, nil
}

// QueryVacuumProgress retrieves VACUUM operation progress from pg_stat_progress_vacuum
// Returns real-time progress of running VACUUM operations (PostgreSQL 12+)
func (c *SQLClient) QueryVacuumProgress(ctx context.Context) ([]models.PgStatProgressVacuum, error) {
	query := queries.PgStatProgressVacuumSQL
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_progress_vacuum: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatProgressVacuum

	for rows.Next() {
		var metric models.PgStatProgressVacuum
		err := rows.Scan(
			&metric.Database,
			&metric.SchemaName,
			&metric.TableName,
			&metric.Phase,
			&metric.HeapBlksTotal,
			&metric.HeapBlksScanned,
			&metric.HeapBlksVacuumed,
			&metric.IndexVacuumCount,
			&metric.MaxDeadTuples,
			&metric.NumDeadTuples,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_progress_vacuum row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_progress_vacuum rows: %w", err)
	}

	return metrics, nil
}
