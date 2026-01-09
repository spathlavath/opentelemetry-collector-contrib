// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PostgreSQL database metrics and performance monitoring queries

// Database Statistics Queries
const (
	// PgStatDatabaseMetricsSQL returns database statistics from pg_stat_database
	// This query retrieves key database metrics including connections, transactions,
	// disk I/O, tuple operations, and timing information
	PgStatDatabaseMetricsSQL = `
		SELECT
			datname,
			numbackends,
			xact_commit,
			xact_rollback,
			blks_read,
			blks_hit,
			tup_returned,
			tup_fetched,
			tup_inserted,
			tup_updated,
			tup_deleted,
			deadlocks,
			temp_files,
			temp_bytes,
			blk_read_time,
			blk_write_time,
			(2^31 - age(datfrozenxid)) AS before_xid_wraparound,
			pg_database_size(datname) AS database_size
		FROM pg_stat_database
		WHERE datname NOT IN ('template0', 'template1')`
)
