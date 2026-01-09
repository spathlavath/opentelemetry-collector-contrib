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
			sd.deadlocks,
			sd.temp_files,
			sd.temp_bytes,
			sd.blk_read_time,
			sd.blk_write_time,
			(2^31 - age(d.datfrozenxid))::bigint AS before_xid_wraparound,
			pg_database_size(sd.datname) AS database_size
		FROM pg_stat_database sd
		JOIN pg_database d ON sd.datname = d.datname
		WHERE sd.datname NOT IN ('template0', 'template1')`
)
