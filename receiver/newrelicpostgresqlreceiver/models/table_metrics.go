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

// PgStatProgressAnalyze represents progress of ANALYZE operations from pg_stat_progress_analyze
// This struct captures real-time progress of running ANALYZE operations
// Available in PostgreSQL 13+
type PgStatProgressAnalyze struct {
	// Database name
	Database string

	// SchemaName is the schema containing the table being analyzed
	SchemaName string

	// TableName is the name of the table being analyzed
	TableName string

	// Phase is the current processing phase of the ANALYZE operation
	// Common phases: "initializing", "acquiring sample rows", "acquiring inherited sample rows",
	// "computing statistics", "computing extended statistics", "finalizing analyze"
	Phase sql.NullString

	// Sample block statistics - number of blocks being sampled
	SampleBlksTotal   sql.NullInt64 // Total number of heap blocks to sample
	SampleBlksScanned sql.NullInt64 // Number of heap blocks scanned so far

	// Extended statistics progress (PostgreSQL 13+)
	ExtStatsTotal    sql.NullInt64 // Total number of extended statistics to compute
	ExtStatsComputed sql.NullInt64 // Number of extended statistics computed so far

	// Child table progress for partitioned tables
	ChildTablesTotal sql.NullInt64 // Total number of child tables to analyze (for partitioned tables)
	ChildTablesDone  sql.NullInt64 // Number of child tables analyzed so far
}

// PgStatProgressCluster represents progress of CLUSTER/VACUUM FULL operations from pg_stat_progress_cluster
// This struct captures real-time progress of running CLUSTER or VACUUM FULL operations
// Available in PostgreSQL 12+
type PgStatProgressCluster struct {
	// Database name
	Database string

	// SchemaName is the schema containing the table being clustered/vacuumed
	SchemaName string

	// TableName is the name of the table being clustered/vacuumed
	TableName string

	// Command is the command type (CLUSTER or VACUUM FULL)
	Command sql.NullString

	// Phase is the current processing phase of the operation
	// Common phases: "initializing", "seq scanning heap", "index scanning heap",
	// "sorting tuples", "writing new heap", "swapping relation files", "rebuilding index", "performing final cleanup"
	Phase sql.NullString

	// Heap block statistics - progress of scanning the old heap
	HeapBlksTotal   sql.NullInt64 // Total number of heap blocks in the table
	HeapBlksScanned sql.NullInt64 // Number of heap blocks scanned so far

	// Tuple statistics - tuples scanned and written
	HeapTuplesScanned sql.NullInt64 // Number of heap tuples scanned
	HeapTuplesWritten sql.NullInt64 // Number of heap tuples written to the new heap

	// Index rebuild progress
	IndexRebuildCount sql.NullInt64 // Number of indexes rebuilt
}
