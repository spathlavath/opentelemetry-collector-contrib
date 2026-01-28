// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PostgreSQL background writer and checkpointer statistics queries

const (
	// PgStatBgwriterPrePG17SQL returns background writer and checkpointer statistics (PostgreSQL < 17)
	// In versions before PostgreSQL 17, all background writer and checkpointer metrics
	// are available in the pg_stat_bgwriter view
	// Available in PostgreSQL 9.6+
	PgStatBgwriterPrePG17SQL = `
		SELECT
			buffers_clean,
			maxwritten_clean,
			buffers_alloc,
			checkpoints_timed,
			checkpoints_req as checkpoints_requested,
			buffers_checkpoint,
			checkpoint_write_time,
			checkpoint_sync_time,
			buffers_backend,
			buffers_backend_fsync
		FROM pg_stat_bgwriter`

	// PgStatBgwriterPG17SQL returns background writer and checkpointer statistics (PostgreSQL 17+)
	// In PostgreSQL 17+, checkpointer metrics were moved to pg_stat_checkpointer
	// This query combines data from both pg_stat_bgwriter and pg_stat_checkpointer
	// Uses CROSS JOIN since both views return a single row
	PgStatBgwriterPG17SQL = `
		SELECT
			bg.buffers_clean,
			bg.maxwritten_clean,
			bg.buffers_alloc,
			cp.checkpoints_timed,
			cp.checkpoints_req as checkpoints_requested,
			cp.buffers_checkpoint,
			cp.checkpoint_write_time,
			cp.checkpoint_sync_time,
			bg.buffers_backend,
			bg.buffers_backend_fsync
		FROM pg_stat_bgwriter bg
		CROSS JOIN pg_stat_checkpointer cp`

	// PgControlCheckpointSQL returns checkpoint control statistics (PostgreSQL 10+)
	// This query retrieves checkpoint timing and WAL location information
	// Uses pg_control_checkpoint() function and pg_current_wal_lsn() for WAL distances
	// Available in PostgreSQL 10+
	PgControlCheckpointSQL = `
		SELECT
			timeline_id,
			EXTRACT(EPOCH FROM (now() - checkpoint_time)) as checkpoint_delay,
			pg_wal_lsn_diff(pg_current_wal_lsn(), checkpoint_lsn) as checkpoint_delay_bytes,
			pg_wal_lsn_diff(pg_current_wal_lsn(), redo_lsn) as redo_delay_bytes
		FROM pg_control_checkpoint()`

	// PgStatArchiverSQL returns WAL archiver statistics (PostgreSQL 9.6+)
	// This query retrieves the count of successfully archived and failed WAL files
	// Available in PostgreSQL 9.6+
	PgStatArchiverSQL = `
		SELECT
			archived_count,
			failed_count
		FROM pg_stat_archiver`

	// PgStatSLRUSQL returns SLRU (Simple LRU) cache statistics (PostgreSQL 13+)
	// This query retrieves per-SLRU cache performance metrics
	// Returns one row per SLRU (e.g., CommitTs, MultiXactMember, MultiXactOffset, Notify, Serial, Subtrans, Xact)
	// Available in PostgreSQL 13+
	PgStatSLRUSQL = `
		SELECT
			name as slru_name,
			blks_zeroed,
			blks_hit,
			blks_read,
			blks_written,
			blks_exists,
			flushes,
			truncates
		FROM pg_stat_slru`

	// PgStatRecoveryPrefetchSQL returns recovery prefetch statistics (PostgreSQL 15+)
	// This query retrieves standby server prefetch performance metrics during WAL replay
	// Returns a single row with prefetch statistics
	// Available in PostgreSQL 15+ (standby servers only)
	PgStatRecoveryPrefetchSQL = `
		SELECT
			prefetch,
			hit,
			skip_init,
			skip_new,
			skip_fpw,
			skip_rep,
			wal_distance,
			block_distance,
			io_depth
		FROM pg_stat_recovery_prefetch`
)
