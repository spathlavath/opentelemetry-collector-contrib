// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PostgreSQL replication metrics queries

// Replication Statistics Queries
const (
	// PgStatReplicationMetricsPG96SQL returns replication statistics for PostgreSQL 9.6
	// This query does NOT include lag time columns (write_lag, flush_lag, replay_lag)
	// which were introduced in PostgreSQL 10
	// Available in PostgreSQL 9.6
	//
	// Metrics collected:
	// - backend_xmin_age: Age of oldest transaction on standby
	// - sent_lsn_delay: Bytes of WAL sent but not yet written to disk on standby
	// - write_lsn_delay: Bytes of WAL written but not yet flushed on standby
	// - flush_lsn_delay: Bytes of WAL flushed but not yet applied on standby
	// - replay_lsn_delay: Bytes of WAL not yet replayed on standby
	// - write_lag, flush_lag, replay_lag: NULL (not available in 9.6)
	//
	// Note: Uses pg_xlog_location_diff instead of pg_wal_lsn_diff (renamed in PG10)
	PgStatReplicationMetricsPG96SQL = `
		SELECT
			application_name,
			state,
			sync_state,
			client_addr,
			GREATEST(0, age(backend_xmin)) as backend_xmin_age,
			pg_xlog_location_diff(pg_current_xlog_location(), sent_location) as sent_lsn_delay,
			pg_xlog_location_diff(pg_current_xlog_location(), write_location) as write_lsn_delay,
			pg_xlog_location_diff(pg_current_xlog_location(), flush_location) as flush_lsn_delay,
			pg_xlog_location_diff(pg_current_xlog_location(), replay_location) as replay_lsn_delay,
			NULL::double precision as write_lag,
			NULL::double precision as flush_lag,
			NULL::double precision as replay_lag
		FROM pg_stat_replication`

	// PgStatReplicationMetricsPG10SQL returns replication statistics for PostgreSQL 10+
	// This query includes lag time columns (write_lag, flush_lag, replay_lag)
	// Available in PostgreSQL 10+
	//
	// Metrics collected:
	// - backend_xmin_age: Age of oldest transaction on standby
	// - sent_lsn_delay: Bytes of WAL sent but not yet written to disk on standby
	// - write_lsn_delay: Bytes of WAL written but not yet flushed on standby
	// - flush_lsn_delay: Bytes of WAL flushed but not yet applied on standby
	// - replay_lsn_delay: Bytes of WAL not yet replayed on standby
	// - write_lag: Time elapsed between WAL write on primary and standby (seconds)
	// - flush_lag: Time elapsed between WAL flush on primary and standby (seconds)
	// - replay_lag: Time elapsed between WAL replay on primary and standby (seconds)
	//
	// Notes:
	// - This query only returns rows on primary servers with active replication
	// - On standby servers or servers without replication, returns empty result set
	// - LSN delays are in bytes (use pg_wal_lsn_diff to calculate)
	// - Lag times are in seconds (extracted from interval type)
	// - GREATEST(0, ...) ensures negative values are converted to 0
	PgStatReplicationMetricsPG10SQL = `
		SELECT
			application_name,
			state,
			sync_state,
			client_addr,
			GREATEST(0, age(backend_xmin)) as backend_xmin_age,
			pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as sent_lsn_delay,
			pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn) as write_lsn_delay,
			pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) as flush_lsn_delay,
			pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replay_lsn_delay,
			GREATEST(0, EXTRACT(epoch from write_lag)) as write_lag,
			GREATEST(0, EXTRACT(epoch from flush_lag)) as flush_lag,
			GREATEST(0, EXTRACT(epoch from replay_lag)) as replay_lag
		FROM pg_stat_replication`

	// PgStatReplicationMetricsSQL is an alias for PG10+ query (backward compatibility)
	PgStatReplicationMetricsSQL = PgStatReplicationMetricsPG10SQL
)
