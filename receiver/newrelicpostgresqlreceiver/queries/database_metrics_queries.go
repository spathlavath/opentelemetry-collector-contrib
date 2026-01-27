// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PostgreSQL database metrics and performance monitoring queries

// Database Statistics Queries
const (
	// PgStatDatabaseMetricsSQL returns database statistics from pg_stat_database
	// This query retrieves key database metrics including connections, transactions,
	// disk I/O, tuple operations, and timing information
	// Joins with pg_database to access datfrozenxid for XID wraparound monitoring
	PgStatDatabaseMetricsSQL = `
		SELECT
			sd.datname,
			sd.numbackends,
			sd.xact_commit,
			sd.xact_rollback,
			sd.blks_read,
			sd.blks_hit,
			sd.tup_returned,
			sd.tup_fetched,
			sd.tup_inserted,
			sd.tup_updated,
			sd.tup_deleted,
			sd.conflicts,
			sd.temp_files,
			sd.temp_bytes,
			sd.deadlocks,
			sd.blk_read_time,
			sd.blk_write_time,
			(2^31 - age(d.datfrozenxid))::bigint AS before_xid_wraparound,
			pg_database_size(sd.datname) AS database_size
		FROM pg_stat_database sd
		JOIN pg_database d ON sd.datname = d.datname
		WHERE sd.datname NOT IN ('template0', 'template1')`

	// PgStatDatabaseSessionMetricsSQL returns session statistics from pg_stat_database (PostgreSQL 14+)
	// This query retrieves session-level metrics including session time, active time,
	// idle in transaction time, and session lifecycle events
	PgStatDatabaseSessionMetricsSQL = `
		SELECT
			datname,
			session_time,
			active_time,
			idle_in_transaction_time,
			sessions AS session_count,
			sessions_abandoned,
			sessions_fatal,
			sessions_killed
		FROM pg_stat_database
		WHERE datname NOT IN ('template0', 'template1')`

	// PgStatDatabaseConflictsSQL returns conflict statistics from pg_stat_database_conflicts
	// This query retrieves replication conflict events on standby servers
	// Available in PostgreSQL 9.6+
	PgStatDatabaseConflictsSQL = `
		SELECT
			datname,
			confl_tablespace,
			confl_lock,
			confl_snapshot,
			confl_bufferpin,
			confl_deadlock
		FROM pg_stat_database_conflicts
		WHERE datname NOT IN ('template0', 'template1')`

	// PgStatDatabaseMetricsWithChecksumsSQL returns database statistics including checksum data (PostgreSQL 12+)
	// This query extends the base metrics with checksum failure tracking
	PgStatDatabaseMetricsWithChecksumsSQL = `
		SELECT
			sd.datname,
			sd.numbackends,
			sd.xact_commit,
			sd.xact_rollback,
			sd.blks_read,
			sd.blks_hit,
			sd.tup_returned,
			sd.tup_fetched,
			sd.tup_inserted,
			sd.tup_updated,
			sd.tup_deleted,
			sd.conflicts,
			sd.temp_files,
			sd.temp_bytes,
			sd.deadlocks,
			sd.blk_read_time,
			sd.blk_write_time,
			(2^31 - age(d.datfrozenxid))::bigint AS before_xid_wraparound,
			pg_database_size(sd.datname) AS database_size,
			sd.checksum_failures,
			sd.checksum_last_failure,
			(SELECT current_setting('data_checksums')::bool) as checksums_enabled
		FROM pg_stat_database sd
		JOIN pg_database d ON sd.datname = d.datname
		WHERE sd.datname NOT IN ('template0', 'template1')`

	// VersionQuery returns the PostgreSQL server version as an integer
	// Format: Major version * 10000 + Minor version * 100 + Patch version
	// Example: PostgreSQL 14.5 returns 140005
	VersionQuery = `SELECT current_setting('server_version_num')::int`

	// PgUptimeSQL returns the PostgreSQL server uptime in seconds
	// Calculates time elapsed since server start using pg_postmaster_start_time()
	// Available in PostgreSQL 9.6+
	PgUptimeSQL = `SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) as uptime`

	// PgDatabaseCountSQL returns the count of databases that allow connections
	// Excludes template databases by filtering on datallowconn
	// Available in PostgreSQL 9.6+
	PgDatabaseCountSQL = `SELECT COUNT(*) as db_count FROM pg_database WHERE datallowconn`

	// PgRunningStatusSQL returns a simple health check indicator
	// Returns 1 if the server is running and responding to queries
	// Available in PostgreSQL 9.6+
	PgRunningStatusSQL = `SELECT 1 as running`
)
