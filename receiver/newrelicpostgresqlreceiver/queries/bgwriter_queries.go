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
)
