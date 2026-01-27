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

	// Replication Slots Queries
	// PgReplicationSlotsPG96SQL returns replication slot statistics for PostgreSQL 9.4-9.6
	// Uses pg_xlog_* functions (renamed to pg_wal_* in PostgreSQL 10)
	// Available in PostgreSQL 9.4+
	//
	// Metrics collected:
	// - xmin_age: Age of oldest transaction kept by this slot
	// - catalog_xmin_age: Age of oldest transaction affecting system catalogs
	// - restart_delay_bytes: Bytes of WAL between current location and slot's restart_lsn
	// - confirmed_flush_delay_bytes: Bytes between current location and confirmed_flush_lsn (logical slots only)
	//
	// Note: Uses pg_xlog_location_diff instead of pg_wal_lsn_diff (renamed in PG10)
	PgReplicationSlotsPG96SQL = `
		SELECT
			slot_name,
			COALESCE(slot_type, '') as slot_type,
			COALESCE(plugin, '') as plugin,
			COALESCE(active, false) as active,
			GREATEST(0, age(xmin)) as xmin_age,
			GREATEST(0, age(catalog_xmin)) as catalog_xmin_age,
			COALESCE(pg_xlog_location_diff(pg_current_xlog_location(), restart_lsn), 0) as restart_delay_bytes,
			CASE
				WHEN slot_type = 'logical' THEN COALESCE(pg_xlog_location_diff(pg_current_xlog_location(), confirmed_flush_lsn), 0)
				ELSE 0
			END as confirmed_flush_delay_bytes
		FROM pg_replication_slots`

	// PgReplicationSlotsPG10SQL returns replication slot statistics for PostgreSQL 10+
	// Uses pg_wal_* functions (renamed from pg_xlog_* in PostgreSQL 10)
	// Available in PostgreSQL 10+
	//
	// Metrics collected:
	// - xmin_age: Age of oldest transaction kept by this slot
	// - catalog_xmin_age: Age of oldest transaction affecting system catalogs
	// - restart_delay_bytes: Bytes of WAL between current location and slot's restart_lsn
	// - confirmed_flush_delay_bytes: Bytes between current location and confirmed_flush_lsn (logical slots only)
	//
	// Notes:
	// - Physical slots (used for streaming replication) have NULL confirmed_flush_lsn
	// - Logical slots (used for logical replication) have both restart_lsn and confirmed_flush_lsn
	// - GREATEST(0, ...) ensures negative values are converted to 0
	PgReplicationSlotsPG10SQL = `
		SELECT
			slot_name,
			COALESCE(slot_type, '') as slot_type,
			COALESCE(plugin, '') as plugin,
			COALESCE(active, false) as active,
			GREATEST(0, age(xmin)) as xmin_age,
			GREATEST(0, age(catalog_xmin)) as catalog_xmin_age,
			COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) as restart_delay_bytes,
			CASE
				WHEN slot_type = 'logical' THEN COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)
				ELSE 0
			END as confirmed_flush_delay_bytes
		FROM pg_replication_slots`

	// Replication Slot Stats Queries
	// PgStatReplicationSlotsSQL returns replication slot statistics from pg_stat_replication_slots
	// This view provides statistics about logical decoding on replication slots
	// Available in PostgreSQL 14+
	//
	// Metrics collected:
	// - spill_txns: Number of transactions spilled to disk
	// - spill_count: Number of times transactions were spilled to disk
	// - spill_bytes: Total bytes spilled to disk
	// - stream_txns: Number of in-progress transactions streamed
	// - stream_count: Number of times in-progress transactions were streamed
	// - stream_bytes: Total bytes streamed for in-progress transactions
	// - total_txns: Total number of decoded transactions
	// - total_bytes: Total bytes processed
	//
	// Notes:
	// - This view only contains data for logical replication slots
	// - Physical replication slots will not appear in pg_stat_replication_slots
	// - Joins with pg_replication_slots to get slot metadata (type, state)
	PgStatReplicationSlotsSQL = `
		SELECT
			slot.slot_name,
			slot.slot_type,
			CASE WHEN slot.active THEN 'active' ELSE 'inactive' END as state,
			COALESCE(stat.spill_txns, 0) as spill_txns,
			COALESCE(stat.spill_count, 0) as spill_count,
			COALESCE(stat.spill_bytes, 0) as spill_bytes,
			COALESCE(stat.stream_txns, 0) as stream_txns,
			COALESCE(stat.stream_count, 0) as stream_count,
			COALESCE(stat.stream_bytes, 0) as stream_bytes,
			COALESCE(stat.total_txns, 0) as total_txns,
			COALESCE(stat.total_bytes, 0) as total_bytes
		FROM pg_stat_replication_slots stat
		JOIN pg_replication_slots slot ON slot.slot_name = stat.slot_name`

	// Replication Delay Queries (Standby-side metrics)
	// PgReplicationDelayPG96SQL returns replication lag metrics for standby servers (PostgreSQL 9.6)
	// Uses pg_xlog_* functions (renamed to pg_wal_* in PostgreSQL 10)
	// Only returns data on standby servers (pg_is_in_recovery() = true)
	// Available in PostgreSQL 9.6
	//
	// Metrics collected:
	// - replication_delay: Time lag in seconds (how old is the last replayed transaction)
	// - replication_delay_bytes: Byte lag between received and replayed WAL
	//
	// Notes:
	// - Returns 0 for both metrics on primary servers
	// - pg_last_xact_replay_timestamp() returns the timestamp of the last transaction replayed
	// - pg_last_xlog_receive_location() is the last WAL location received from primary
	// - pg_last_xlog_replay_location() is the last WAL location replayed on standby
	// - COALESCE ensures NULL values are converted to 0
	PgReplicationDelayPG96SQL = `
		SELECT
			CASE WHEN pg_is_in_recovery()
				THEN COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)
				ELSE 0
			END as replication_delay,
			CASE WHEN pg_is_in_recovery()
				THEN COALESCE(pg_xlog_location_diff(pg_last_xlog_receive_location(), pg_last_xlog_replay_location()), 0)
				ELSE 0
			END as replication_delay_bytes`

	// PgReplicationDelayPG10SQL returns replication lag metrics for standby servers (PostgreSQL 10+)
	// Uses pg_wal_* functions (renamed from pg_xlog_* in PostgreSQL 10)
	// Only returns data on standby servers (pg_is_in_recovery() = true)
	// Available in PostgreSQL 10+
	//
	// Metrics collected:
	// - replication_delay: Time lag in seconds (how old is the last replayed transaction)
	// - replication_delay_bytes: Byte lag between received and replayed WAL
	//
	// Notes:
	// - Returns 0 for both metrics on primary servers
	// - pg_last_xact_replay_timestamp() returns the timestamp of the last transaction replayed
	// - pg_last_wal_receive_lsn() is the last WAL location received from primary
	// - pg_last_wal_replay_lsn() is the last WAL location replayed on standby
	// - COALESCE ensures NULL values are converted to 0
	PgReplicationDelayPG10SQL = `
		SELECT
			CASE WHEN pg_is_in_recovery()
				THEN COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)
				ELSE 0
			END as replication_delay,
			CASE WHEN pg_is_in_recovery()
				THEN COALESCE(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()), 0)
				ELSE 0
			END as replication_delay_bytes`

	// WAL Receiver Queries (Standby-side metrics)
	// PgStatWalReceiverSQL returns WAL receiver statistics on standby servers
	// This view shows the state of the WAL receiver process
	// Only returns data on standby servers (no rows on primary servers)
	// Available in PostgreSQL 9.6+
	//
	// Metrics collected:
	// - status: Activity status (streaming, waiting, restarting, stopped, disconnected)
	// - received_tli: Timeline number of last received WAL file
	// - last_msg_send_age: Time since last message sent from primary (seconds)
	// - last_msg_receipt_age: Time since last message received (seconds)
	// - latest_end_age: Time since last WAL location reported to primary (seconds)
	//
	// Notes:
	// - Returns no rows on primary servers or when WAL receiver is not running
	// - Time ages are calculated using clock_timestamp() for accuracy
	// - COALESCE ensures NULL values are converted to 0
	// - status is set to 'disconnected' when NULL
	PgStatWalReceiverSQL = `
		SELECT
			CASE WHEN status IS NULL THEN 'disconnected' ELSE status END as status,
			COALESCE(received_tli, 0) as received_tli,
			COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - last_msg_send_time)), 0) as last_msg_send_age,
			COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - last_msg_receipt_time)), 0) as last_msg_receipt_age,
			COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - latest_end_time)), 0) as latest_end_age
		FROM pg_stat_wal_receiver`

	// WAL Statistics Queries
	// PgStatWalPG14SQL returns WAL statistics for PostgreSQL 14-17
	// Includes all 8 metrics including timing metrics
	// Available in PostgreSQL 14-17
	//
	// Metrics collected:
	// - wal_records: Total number of WAL records generated
	// - wal_fpi: Total number of full page images generated
	// - wal_bytes: Total bytes of WAL generated
	// - wal_buffers_full: Times WAL buffers became full
	// - wal_write: Number of times WAL buffers were written to disk
	// - wal_sync: Number of times WAL files were synced to disk
	// - wal_write_time: Total time writing WAL buffers (milliseconds)
	// - wal_sync_time: Total time syncing WAL files (milliseconds)
	//
	// Notes:
	// - Timing metrics require track_wal_io_timing to be enabled
	// - These metrics are cumulative since server start
	// - Available on both primary and standby servers
	PgStatWalPG14SQL = `
		SELECT
			COALESCE(wal_records, 0) as wal_records,
			COALESCE(wal_fpi, 0) as wal_fpi,
			COALESCE(wal_bytes, 0) as wal_bytes,
			COALESCE(wal_buffers_full, 0) as wal_buffers_full,
			COALESCE(wal_write, 0) as wal_write,
			COALESCE(wal_sync, 0) as wal_sync,
			COALESCE(wal_write_time, 0) as wal_write_time,
			COALESCE(wal_sync_time, 0) as wal_sync_time
		FROM pg_stat_wal`

	// PgStatWalPG18SQL returns WAL statistics for PostgreSQL 18+
	// Simplified to 4 core metrics (timing metrics removed)
	// Available in PostgreSQL 18+
	//
	// Metrics collected:
	// - wal_records: Total number of WAL records generated
	// - wal_fpi: Total number of full page images generated
	// - wal_bytes: Total bytes of WAL generated
	// - wal_buffers_full: Times WAL buffers became full
	//
	// Notes:
	// - wal_write, wal_sync, wal_write_time, wal_sync_time columns removed in PG18
	// - These metrics are cumulative since server start
	// - Available on both primary and standby servers
	PgStatWalPG18SQL = `
		SELECT
			COALESCE(wal_records, 0) as wal_records,
			COALESCE(wal_fpi, 0) as wal_fpi,
			COALESCE(wal_bytes, 0) as wal_bytes,
			COALESCE(wal_buffers_full, 0) as wal_buffers_full
		FROM pg_stat_wal`
)
