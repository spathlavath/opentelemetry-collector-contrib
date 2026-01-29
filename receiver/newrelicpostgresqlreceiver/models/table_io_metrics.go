// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgStatUserTablesMetric represents per-table statistics from pg_stat_user_tables
// This struct captures vacuum/analyze statistics and row-level activity
// Available in PostgreSQL 9.6+
type PgStatUserTablesMetric struct {
	// Database name
	Database string

	// SchemaName is the schema containing this table
	SchemaName string

	// TableName is the name of the table
	TableName string

	// Sequential scan statistics
	SeqScan    sql.NullInt64 // Number of sequential scans initiated on this table
	SeqTupRead sql.NullInt64 // Number of live rows fetched by sequential scans

	// Index scan statistics
	IdxScan     sql.NullInt64 // Number of index scans initiated on this table
	IdxTupFetch sql.NullInt64 // Number of live rows fetched by index scans

	// Row modification statistics
	NTupIns    sql.NullInt64 // Number of rows inserted
	NTupUpd    sql.NullInt64 // Number of rows updated
	NTupDel    sql.NullInt64 // Number of rows deleted
	NTupHotUpd sql.NullInt64 // Number of rows HOT updated (Heap-Only Tuple updates)

	// Live and dead tuple counts
	NLiveTup sql.NullInt64 // Estimated number of live rows
	NDeadTup sql.NullInt64 // Estimated number of dead rows

	// Analyze statistics
	NModSinceAnalyze sql.NullInt64 // Estimated number of rows modified since last analyze

	// Vacuum and analyze timing (ages in seconds)
	LastVacuumAge      sql.NullFloat64 // Seconds since last manual VACUUM
	LastAutovacuumAge  sql.NullFloat64 // Seconds since last autovacuum
	LastAnalyzeAge     sql.NullFloat64 // Seconds since last manual ANALYZE
	LastAutoanalyzeAge sql.NullFloat64 // Seconds since last autoanalyze

	// Vacuum and analyze counts
	VacuumCount      sql.NullInt64 // Number of times this table has been manually vacuumed
	AutovacuumCount  sql.NullInt64 // Number of times this table has been vacuumed by autovacuum
	AnalyzeCount     sql.NullInt64 // Number of times this table has been manually analyzed
	AutoanalyzeCount sql.NullInt64 // Number of times this table has been analyzed by autoanalyze
}

// PgStatIOUserTables represents per-table disk IO statistics from pg_statio_user_tables
// This struct captures heap, index, and TOAST block reads from disk vs buffer cache
// Available in PostgreSQL 9.6+
type PgStatIOUserTables struct {
	// Database name
	Database string

	// SchemaName is the schema containing this table
	SchemaName string

	// TableName is the name of the table
	TableName string

	// Heap block statistics - table data blocks
	HeapBlksRead sql.NullInt64 // Number of disk blocks read from the heap (table data)
	HeapBlksHit  sql.NullInt64 // Number of buffer cache hits in the heap (table data)

	// Index block statistics - all indexes combined
	IdxBlksRead sql.NullInt64 // Number of disk blocks read from indexes
	IdxBlksHit  sql.NullInt64 // Number of buffer cache hits in indexes

	// TOAST block statistics - for oversized column values
	ToastBlksRead sql.NullInt64 // Number of disk blocks read from TOAST tables
	ToastBlksHit  sql.NullInt64 // Number of buffer cache hits in TOAST tables

	// TOAST index block statistics
	TidxBlksRead sql.NullInt64 // Number of disk blocks read from TOAST indexes
	TidxBlksHit  sql.NullInt64 // Number of buffer cache hits in TOAST indexes
}

// PgStatUserIndexes represents per-index statistics from pg_stat_user_indexes
// This struct captures index usage statistics for individual indexes
// Available in PostgreSQL 9.6+
type PgStatUserIndexes struct {
	// Database name
	Database string

	// SchemaName is the schema containing this table
	SchemaName string

	// TableName is the name of the table this index belongs to
	TableName string

	// IndexName is the name of the index
	IndexName string

	// IndexRelID is the OID of the index (used to calculate size)
	IndexRelID sql.NullInt64

	// Index scan statistics
	IdxScan     sql.NullInt64 // Number of index scans initiated on this index
	IdxTupRead  sql.NullInt64 // Number of index entries returned by scans on this index
	IdxTupFetch sql.NullInt64 // Number of live table rows fetched by simple index scans using this index
}
