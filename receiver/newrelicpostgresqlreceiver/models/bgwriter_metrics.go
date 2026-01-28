// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgStatBgwriterMetric represents background writer and checkpointer statistics
// This struct captures server-level background process metrics
// In PostgreSQL 17+, data comes from both pg_stat_bgwriter and pg_stat_checkpointer
// In PostgreSQL < 17, all data comes from pg_stat_bgwriter
// Available in PostgreSQL 9.6+
type PgStatBgwriterMetric struct {
	// BuffersClean is the number of buffers written by the background writer
	BuffersClean sql.NullInt64

	// MaxwrittenClean is the number of times the background writer stopped a cleaning scan
	// because it had written too many buffers
	MaxwrittenClean sql.NullInt64

	// BuffersAlloc is the number of buffers allocated
	BuffersAlloc sql.NullInt64

	// CheckpointsTimed is the number of scheduled checkpoints that have been performed
	CheckpointsTimed sql.NullInt64

	// CheckpointsRequested is the number of requested checkpoints that have been performed
	CheckpointsRequested sql.NullInt64

	// BuffersCheckpoint is the number of buffers written during checkpoints
	BuffersCheckpoint sql.NullInt64

	// CheckpointWriteTime is the total time spent writing checkpoint data to disk (milliseconds)
	CheckpointWriteTime sql.NullFloat64

	// CheckpointSyncTime is the total time spent syncing checkpoint data to disk (milliseconds)
	CheckpointSyncTime sql.NullFloat64

	// BuffersBackend is the number of buffers written directly by a backend
	BuffersBackend sql.NullInt64

	// BuffersBackendFsync is the number of times a backend had to execute its own fsync call
	// (normally the background writer handles these even when the backend does its own write)
	BuffersBackendFsync sql.NullInt64
}

// PgControlCheckpointMetric represents checkpoint control statistics from pg_control_checkpoint()
// This struct captures checkpoint timing and WAL location information
// Available in PostgreSQL 10+
type PgControlCheckpointMetric struct {
	// TimelineID is the current timeline ID
	TimelineID sql.NullInt64

	// CheckpointDelay is the time elapsed since the last checkpoint (seconds)
	CheckpointDelay sql.NullFloat64

	// CheckpointDelayBytes is the WAL distance from the last checkpoint (bytes)
	CheckpointDelayBytes sql.NullInt64

	// RedoDelayBytes is the WAL distance from the redo location (bytes)
	RedoDelayBytes sql.NullInt64
}

// PgStatArchiverMetric represents WAL archiver statistics from pg_stat_archiver
// This struct captures archiver success and failure counts
// Available in PostgreSQL 9.6+
type PgStatArchiverMetric struct {
	// ArchivedCount is the number of WAL files successfully archived
	ArchivedCount sql.NullInt64

	// FailedCount is the number of failed attempts to archive WAL files
	FailedCount sql.NullInt64
}
