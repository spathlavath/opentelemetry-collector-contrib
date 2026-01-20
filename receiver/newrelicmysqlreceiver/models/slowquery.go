// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// SlowQuery represents a slow query record from MySQL performance_schema.events_statements_summary_by_digest
// This table aggregates query statistics by normalized query pattern (DIGEST)
type SlowQuery struct {
	// ===========================================
	// IDENTIFICATION FIELDS
	// ===========================================

	// QueryID: DIGEST - A hash of the normalized query pattern
	// Example: "a1b2c3d4..." (hexadecimal hash)
	// Used for: Deduplication and correlation across scrapes
	QueryID sql.NullString

	// QueryText: DIGEST_TEXT - The normalized query with literals replaced by ?
	// Example: "SELECT * FROM users WHERE id = ?"
	// Used for: Human-readable query pattern identification
	QueryText sql.NullString

	// DatabaseName: SCHEMA_NAME - The database/schema where the query was executed
	// Example: "myapp_production"
	DatabaseName sql.NullString

	// CollectionTimestamp: NOW() - When this record was collected
	// Example: "2025-01-13 10:30:45"
	CollectionTimestamp sql.NullString

	// ===========================================
	// EXECUTION METRICS (CUMULATIVE)
	// ===========================================
	// These are cumulative counters since the server started (or since performance_schema was reset)
	// Used for delta calculation by comparing values across scrapes

	// ExecutionCount: COUNT_STAR - Total number of times this query pattern has been executed
	// Example: 15000 (means this query has run 15,000 times total)
	ExecutionCount sql.NullInt64

	// TotalElapsedTimeMS: SUM_TIMER_WAIT - Total wall-clock time spent executing all instances
	// This is THE KEY FIELD for precise delta calculation
	// Example: 45000.5 ms (45 seconds total across all executions)
	// Why it's important: Historical avg can be misleading, but total allows us to compute interval avg
	TotalElapsedTimeMS sql.NullFloat64

	// TotalLockTimeMS: SUM_LOCK_TIME - Total time spent waiting for locks
	// Example: 1500.2 ms (1.5 seconds total lock wait time)
	// High value indicates lock contention issues
	TotalLockTimeMS sql.NullFloat64

	// ===========================================
	// HISTORICAL AVERAGES (FROM DATABASE)
	// ===========================================
	// These are calculated by MySQL and represent all-time averages
	// NOT used for Top N selection (we use interval averages instead)

	// AvgElapsedTimeMs: AVG_TIMER_WAIT - Historical average execution time
	// Example: 3.0 ms (average since server started)
	AvgElapsedTimeMs sql.NullFloat64

	// AvgLockTimeMs: AVG_LOCK_TIME - Historical average time waiting for locks
	// Example: 0.5 ms
	AvgLockTimeMs sql.NullFloat64

	// AvgCPUTimeMs: AVG_CPU_TIME - Historical average CPU time (MySQL 8.0.28+)
	// Example: 2.5 ms
	// Note: This field may be NULL on older MySQL versions
	AvgCPUTimeMs sql.NullFloat64

	// TotalCPUTimeMs: SUM_CPU_TIME - Total CPU time (for delta calculation if needed)
	TotalCPUTimeMs sql.NullFloat64

	// ===========================================
	// ROW METRICS
	// ===========================================
	// These help identify inefficient queries (full table scans, excessive row processing)

	// TotalRowsExamined: SUM_ROWS_EXAMINED - Total rows scanned/read
	// High value indicates potential missing indexes
	TotalRowsExamined sql.NullInt64

	// AvgRowsExamined: AVG_ROWS_EXAMINED - Average rows scanned per execution
	AvgRowsExamined sql.NullFloat64

	// TotalRowsSent: SUM_ROWS_SENT - Total rows returned to client
	TotalRowsSent sql.NullInt64

	// AvgRowsSent: AVG_ROWS_SENT - Average rows returned per execution
	AvgRowsSent sql.NullFloat64

	// TotalRowsAffected: SUM_ROWS_AFFECTED - Total rows modified (INSERT/UPDATE/DELETE)
	TotalRowsAffected sql.NullInt64

	// AvgRowsAffected: AVG_ROWS_AFFECTED - Average rows modified per execution
	AvgRowsAffected sql.NullFloat64

	// ===========================================
	// DISK I/O METRICS (PERFORMANCE INDICATORS)
	// ===========================================
	// These indicate potential optimization opportunities

	// TotalSelectScan: SUM_SELECT_SCAN - Number of full table scans
	// High value = missing indexes
	// Example: 1500 (this query did 1500 full table scans)
	TotalSelectScan sql.NullInt64

	// TotalSelectFullJoin: SUM_SELECT_FULL_JOIN - Number of joins without indexes
	// High value = missing join indexes
	// Example: 200 (this query did 200 full joins)
	TotalSelectFullJoin sql.NullInt64

	// TotalNoIndexUsed: SUM_NO_INDEX_USED - Number of queries that performed table scans without using an index
	// Non-zero value = RED FLAG - query needs an index
	// Example: 500 (this query ran 500 times without any index)
	TotalNoIndexUsed sql.NullInt64

	// TotalNoGoodIndexUsed: SUM_NO_GOOD_INDEX_USED - Number of times MySQL couldn't find a good index to use
	// Non-zero value = YELLOW FLAG - index exists but isn't optimal
	// Example: 100 (MySQL had to use suboptimal execution plan 100 times)
	TotalNoGoodIndexUsed sql.NullInt64

	// TotalSortRows: SUM_SORT_ROWS - Total rows sorted
	// High value with tmp_disk_tables = expensive sorts
	TotalSortRows sql.NullInt64

	// TotalSortMergePasses: SUM_SORT_MERGE_PASSES - Number of merge passes for sorting
	// Non-zero = sort didn't fit in memory
	TotalSortMergePasses sql.NullInt64

	// ===========================================
	// TEMPORARY TABLE METRICS (MEMORY PRESSURE)
	// ===========================================
	// Indicate memory pressure and potential query optimization opportunities

	// TotalTmpTables: SUM_CREATED_TMP_TABLES - Temporary tables created
	// High value = complex queries requiring temp storage
	TotalTmpTables sql.NullInt64

	// TotalTmpDiskTables: SUM_CREATED_TMP_DISK_TABLES - Temporary tables created on disk
	// Non-zero = memory pressure, very expensive
	// This is a RED FLAG for performance
	TotalTmpDiskTables sql.NullInt64

	// ===========================================
	// ERROR AND WARNING METRICS (RELIABILITY)
	// ===========================================
	// These help identify problematic queries that fail or generate warnings

	// TotalErrors: SUM_ERRORS - Total number of errors encountered during query execution
	// Example: 50 (this query pattern failed 50 times)
	// Common causes: syntax errors, constraint violations, permission issues
	TotalErrors sql.NullInt64

	// TotalWarnings: SUM_WARNINGS - Total number of warnings generated
	// Example: 200 (this query generated 200 warnings)
	// Common causes: type mismatches, truncated data, deprecated syntax
	TotalWarnings sql.NullInt64

	// ===========================================
	// TIMESTAMP FIELDS
	// ===========================================

	// FirstSeen: FIRST_SEEN - When this query pattern was first executed
	// Example: "2025-01-10 08:00:00"
	FirstSeen sql.NullString

	// LastExecutionTimestamp: LAST_SEEN - Most recent execution timestamp
	// Example: "2025-01-13 10:30:40"
	// Used for: Time window filtering (WHERE LAST_SEEN >= NOW() - INTERVAL X SECOND)
	LastExecutionTimestamp sql.NullString

	// ===========================================
	// PERFORMANCE VARIANCE (RCA - ROOT CAUSE ANALYSIS)
	// ===========================================
	// These help identify inconsistent query performance

	// MinElapsedTimeMs: MIN_TIMER_WAIT - Fastest execution time
	// Example: 0.5 ms
	MinElapsedTimeMs sql.NullFloat64

	// MaxElapsedTimeMs: MAX_TIMER_WAIT - Slowest execution time
	// Example: 5000 ms (5 seconds - indicates a problem)
	// Large gap between min/max = inconsistent performance (caching, locks, resource contention)
	MaxElapsedTimeMs sql.NullFloat64

	// ===========================================
	// INTERVAL-BASED DELTA METRICS (CALCULATED IN-MEMORY)
	// ===========================================
	// These fields are NOT from the database - they're calculated by the IntervalCalculator
	// They represent performance in the LAST INTERVAL (e.g., last 60 seconds)

	// IntervalAvgElapsedTimeMS: Average elapsed time in the current interval (delta)
	// Example: If total_elapsed_time went from 45000ms to 48000ms between scrapes,
	//          and execution_count went from 15000 to 15100,
	//          then interval_avg = (48000 - 45000) / (15100 - 15000) = 30ms
	// This is used for Top N selection (not historical avg)
	IntervalAvgElapsedTimeMS *float64

	// IntervalExecutionCount: Number of executions in the current interval (delta)
	// Example: 100 (query ran 100 times in the last 60 seconds)
	IntervalExecutionCount *int64
}

// ===========================================
// HELPER METHODS
// ===========================================

// IsValidForMetrics checks if the slow query has the minimum required fields for metric emission
// Returns: true if QueryID and AvgElapsedTimeMs are valid (not NULL)
// Why: We need at least these two fields to uniquely identify and measure the query
func (sq *SlowQuery) IsValidForMetrics() bool {
	return sq.QueryID.Valid && sq.AvgElapsedTimeMs.Valid
}

// GetQueryID returns the query ID (DIGEST hash) as a string, empty if NULL
// Example: "a1b2c3d4e5f6..."
func (sq *SlowQuery) GetQueryID() string {
	if sq.QueryID.Valid {
		return sq.QueryID.String
	}
	return ""
}

// GetQueryText returns the normalized query text as a string, empty if NULL
// Example: "SELECT * FROM users WHERE id = ?"
func (sq *SlowQuery) GetQueryText() string {
	if sq.QueryText.Valid {
		return sq.QueryText.String
	}
	return ""
}

// GetDatabaseName returns the database/schema name as a string, empty if NULL
// Example: "myapp_production"
func (sq *SlowQuery) GetDatabaseName() string {
	if sq.DatabaseName.Valid {
		return sq.DatabaseName.String
	}
	return ""
}

// GetCollectionTimestamp returns the collection timestamp as a string, empty if NULL
// Example: "2025-01-13 10:30:45"
func (sq *SlowQuery) GetCollectionTimestamp() string {
	if sq.CollectionTimestamp.Valid {
		return sq.CollectionTimestamp.String
	}
	return ""
}

// GetLastExecutionTimestamp returns the last execution timestamp as a string, empty if NULL
// Example: "2025-01-13 10:30:40"
func (sq *SlowQuery) GetLastExecutionTimestamp() string {
	if sq.LastExecutionTimestamp.Valid {
		return sq.LastExecutionTimestamp.String
	}
	return ""
}

// HasValidExecutionCount checks if the query has a valid execution count
// Returns: true if ExecutionCount is not NULL and > 0
func (sq *SlowQuery) HasValidExecutionCount() bool {
	return sq.ExecutionCount.Valid && sq.ExecutionCount.Int64 > 0
}

// HasValidTotalElapsedTime checks if the query has a valid total elapsed time
// Returns: true if TotalElapsedTimeMS is not NULL
// Why: This field is critical for delta calculation
func (sq *SlowQuery) HasValidTotalElapsedTime() bool {
	return sq.TotalElapsedTimeMS.Valid
}
