// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgStatDatabaseMetric represents database statistics from pg_stat_database
// This struct captures all key database metrics including connections, transactions,
// disk I/O, tuple operations, deadlocks, temp files, and timing information
type PgStatDatabaseMetric struct {
	// Database information
	DatName string // Database name

	// Connection metrics
	NumBackends sql.NullInt64 // Number of active connections

	// Transaction metrics
	XactCommit   sql.NullInt64 // Number of committed transactions
	XactRollback sql.NullInt64 // Number of rolled back transactions

	// Disk I/O metrics
	BlksRead     sql.NullInt64   // Number of disk blocks read
	BlksHit      sql.NullInt64   // Number of buffer hits (blocks found in cache)
	BlkReadTime  sql.NullFloat64 // Time spent reading data blocks (milliseconds)
	BlkWriteTime sql.NullFloat64 // Time spent writing data blocks (milliseconds)

	// Tuple (row) operation metrics
	TupReturned sql.NullInt64 // Number of rows returned by queries
	TupFetched  sql.NullInt64 // Number of rows fetched by queries
	TupInserted sql.NullInt64 // Number of rows inserted
	TupUpdated  sql.NullInt64 // Number of rows updated
	TupDeleted  sql.NullInt64 // Number of rows deleted

	// Replication and consistency metrics
	Conflicts sql.NullInt64 // Number of queries canceled due to conflicts with recovery
	Deadlocks sql.NullInt64 // Number of deadlocks detected

	// Temporary file metrics
	TempFiles sql.NullInt64 // Number of temporary files created
	TempBytes sql.NullInt64 // Total size of temporary files (bytes)

	// Transaction ID wraparound protection
	BeforeXIDWraparound sql.NullInt64 // Transactions remaining before XID wraparound

	// Database size
	DatabaseSize sql.NullInt64 // Total database size in bytes
}
