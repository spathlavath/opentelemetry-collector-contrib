// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

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

// PgStatProgressCreateIndex represents progress of CREATE INDEX operations from pg_stat_progress_create_index
// This struct captures real-time progress of running CREATE INDEX operations
// Available in PostgreSQL 12+
type PgStatProgressCreateIndex struct {
	// Database name
	Database string

	// SchemaName is the schema containing the table being indexed
	SchemaName string

	// TableName is the name of the table being indexed
	TableName string

	// IndexName is the name of the index being created
	IndexName sql.NullString

	// Command is the command type (e.g., CREATE INDEX, CREATE INDEX CONCURRENTLY, REINDEX)
	Command sql.NullString

	// Phase is the current processing phase of the CREATE INDEX operation
	// Common phases: "initializing", "waiting for writers before build", "building index",
	// "waiting for writers before validation", "validating index", "waiting for old snapshots", "waiting for readers before marking dead"
	Phase sql.NullString

	// Locker statistics - concurrent index creation waits for locks
	LockersTotal sql.NullInt64 // Total number of lockers to wait for
	LockersDone  sql.NullInt64 // Number of lockers processed

	// Block statistics - blocks read during index build
	BlocksTotal sql.NullInt64 // Total number of blocks to be processed
	BlocksDone  sql.NullInt64 // Number of blocks processed

	// Tuple statistics - tuples indexed
	TuplesTotal sql.NullInt64 // Total number of tuples to be indexed
	TuplesDone  sql.NullInt64 // Number of tuples indexed

	// Partition statistics - for partitioned tables
	PartitionsTotal sql.NullInt64 // Total number of partitions to be indexed
	PartitionsDone  sql.NullInt64 // Number of partitions indexed
}

// PgStatProgressVacuum represents progress of VACUUM operations from pg_stat_progress_vacuum
// This struct captures real-time progress of running VACUUM operations
// Available in PostgreSQL 12+
type PgStatProgressVacuum struct {
	// Database name
	Database string

	// SchemaName is the schema containing the table being vacuumed
	SchemaName string

	// TableName is the name of the table being vacuumed
	TableName string

	// Phase is the current processing phase of the VACUUM operation
	// Common phases: "initializing", "scanning heap", "vacuuming indexes",
	// "vacuuming heap", "cleaning up indexes", "truncating heap", "performing final cleanup"
	Phase sql.NullString

	// Heap block statistics - progress of scanning and vacuuming the heap
	HeapBlksTotal    sql.NullInt64 // Total number of heap blocks in the table
	HeapBlksScanned  sql.NullInt64 // Number of heap blocks scanned so far
	HeapBlksVacuumed sql.NullInt64 // Number of heap blocks vacuumed so far

	// Index vacuum statistics
	IndexVacuumCount sql.NullInt64 // Number of completed index vacuum cycles

	// Dead tuple statistics - tuples collected for removal
	MaxDeadTuples sql.NullInt64 // Maximum number of dead tuples that can be stored before index vacuum
	NumDeadTuples sql.NullInt64 // Current number of dead tuples collected
}
