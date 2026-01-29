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
