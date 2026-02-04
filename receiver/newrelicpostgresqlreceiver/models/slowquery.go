// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// SlowQuery represents a slow query record from PostgreSQL pg_stat_statements
// This struct is compatible with Oracle V$SQLAREA and SQL Server DMV patterns
type SlowQuery struct {
	// Collection metadata
	CollectionTimestamp sql.NullString // Timestamp when query was collected

	// Query identification (stable across parameterized queries)
	QueryID sql.NullString // pg_stat_statements queryid

	// Database and user context
	DatabaseName sql.NullString
	UserName     sql.NullString

	// Execution count (for delta calculation)
	ExecutionCount sql.NullInt64 // Number of times executed (s.calls)

	// Query text (normalized/parameterized by pg_stat_statements)
	QueryText sql.NullString

	// ========================================================================
	// CROSS-DATABASE COMPATIBLE METRICS (Matches Oracle V$SQLAREA)
	// ========================================================================

	// Execution time metrics (milliseconds) - Compatible with Oracle/MSSQL
	AvgElapsedTimeMs    sql.NullFloat64 // Average total execution time (plan + exec)
	MinElapsedTimeMs    sql.NullFloat64 // Minimum execution time
	MaxElapsedTimeMs    sql.NullFloat64 // Maximum execution time
	StddevElapsedTimeMs sql.NullFloat64 // Standard deviation of execution time

	// Planning time metrics (PostgreSQL 13+)
	AvgPlanTimeMs sql.NullFloat64 // Average planning time

	// CPU time approximation (planning + execution)
	// Oracle equivalent: sa.cpu_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000
	AvgCPUTimeMs sql.NullFloat64

	// *** CRITICAL FOR DELTA CALCULATION ***
	// Total elapsed time (NOT average) - used for precise interval-based delta calculation
	// Oracle: sa.elapsed_time / 1000
	TotalElapsedTimeMs sql.NullFloat64

	// ========================================================================
	// I/O METRICS (Cross-database compatible)
	// ========================================================================

	// Disk reads (cache misses - actual I/O)
	// Oracle: sa.disk_reads / DECODE(sa.executions, 0, 1, sa.executions)
	AvgDiskReads   sql.NullFloat64
	TotalDiskReads sql.NullInt64

	// Buffer cache hits (no disk I/O)
	// Oracle: sa.buffer_gets / DECODE(sa.executions, 0, 1, sa.executions)
	AvgBufferHits   sql.NullFloat64
	TotalBufferHits sql.NullInt64

	// Disk writes (dirty pages flushed to disk)
	// Oracle: sa.direct_writes / DECODE(sa.executions, 0, 1, sa.executions)
	AvgDiskWrites   sql.NullFloat64
	TotalDiskWrites sql.NullInt64

	// ========================================================================
	// ROW STATISTICS (Cross-database compatible)
	// ========================================================================

	// Rows returned by query
	AvgRowsReturned   sql.NullFloat64
	TotalRowsReturned sql.NullInt64

	// ========================================================================
	// INTERVAL-BASED DELTA METRICS (Calculated in-memory)
	// ========================================================================

	// These are populated by the PostgreSQLIntervalCalculator
	// NOTE: Only elapsed time has delta calculation, CPU uses historical average from DB
	IntervalAvgElapsedTimeMS *float64 // Average elapsed time in the last interval (milliseconds)
	IntervalExecutionCount   *int64   // Number of executions in the last interval
}

// GetCollectionTimestamp returns the collection timestamp as a string, empty if null
func (sq *SlowQuery) GetCollectionTimestamp() string {
	if sq.CollectionTimestamp.Valid {
		return sq.CollectionTimestamp.String
	}
	return ""
}

// GetDatabaseName returns the database name as a string, empty if null
func (sq *SlowQuery) GetDatabaseName() string {
	if sq.DatabaseName.Valid {
		return sq.DatabaseName.String
	}
	return ""
}

// GetQueryID returns the query ID as a string, empty if null
func (sq *SlowQuery) GetQueryID() string {
	if sq.QueryID.Valid {
		return sq.QueryID.String
	}
	return ""
}

// GetQueryText returns the query text as a string, empty if null
func (sq *SlowQuery) GetQueryText() string {
	if sq.QueryText.Valid {
		return sq.QueryText.String
	}
	return ""
}

// GetUserName returns the username as a string, empty if null
func (sq *SlowQuery) GetUserName() string {
	if sq.UserName.Valid {
		return sq.UserName.String
	}
	return ""
}

// HasValidQueryID checks if the query has a valid query ID
func (sq *SlowQuery) HasValidQueryID() bool {
	return sq.QueryID.Valid
}

// HasValidElapsedTime checks if the query has a valid elapsed time
func (sq *SlowQuery) HasValidElapsedTime() bool {
	return sq.AvgElapsedTimeMs.Valid
}

// IsValidForMetrics checks if the slow query has the minimum required fields for metrics
func (sq *SlowQuery) IsValidForMetrics() bool {
	return sq.HasValidQueryID() && sq.HasValidElapsedTime()
}
