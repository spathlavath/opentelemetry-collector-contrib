// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// SlowQuery represents a slow query record from MySQL performance_schema
// Data source: performance_schema.events_statements_summary_by_digest
// Phase 1: 10 core metrics (aligned with Oracle receiver pattern)
type SlowQuery struct {
	// Query identification
	CollectionTimestamp sql.NullString // When the query was collected
	QueryID             sql.NullString // DIGEST (MySQL's query hash/signature)
	QueryText           sql.NullString // DIGEST_TEXT (normalized query text)
	DatabaseName        sql.NullString // SCHEMA_NAME

	// Core: Execution count (cumulative)
	ExecutionCount sql.NullInt64 // COUNT_STAR

	// Core: Elapsed time metrics (for delta calculation)
	TotalElapsedTimeMS sql.NullFloat64 // SUM_TIMER_WAIT - Used for precise delta calculation
	AvgElapsedTimeMS   sql.NullFloat64 // AVG_TIMER_WAIT - Historical average

	// Core: CPU time (cumulative average)
	AvgCPUTimeMS sql.NullFloat64 // Calculated: SUM_CPU_TIME / COUNT_STAR

	// Core: Lock time (cumulative average)
	AvgLockTimeMS sql.NullFloat64 // Calculated: SUM_LOCK_TIME / COUNT_STAR

	// Core: Row metrics (cumulative averages)
	AvgRowsExamined sql.NullFloat64 // Calculated: SUM_ROWS_EXAMINED / COUNT_STAR
	AvgRowsSent     sql.NullFloat64 // Calculated: SUM_ROWS_SENT / COUNT_STAR

	// Errors (query reliability indicator)
	TotalErrors sql.NullInt64 // SUM_ERRORS

	// Timestamps (UTC format)
	FirstSeen              sql.NullString // FIRST_SEEN
	LastExecutionTimestamp sql.NullString // LAST_SEEN

	// Interval-based delta metrics (calculated in-memory, not from DB)
	// These are populated by the MySQLIntervalCalculator
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

// GetQueryID returns the query ID (DIGEST) as a string, empty if null
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

// GetLastExecutionTimestamp returns the last execution timestamp as a string, empty if null
func (sq *SlowQuery) GetLastExecutionTimestamp() string {
	if sq.LastExecutionTimestamp.Valid {
		return sq.LastExecutionTimestamp.String
	}
	return ""
}

// GetFirstSeen returns the first seen timestamp as a string, empty if null
func (sq *SlowQuery) GetFirstSeen() string {
	if sq.FirstSeen.Valid {
		return sq.FirstSeen.String
	}
	return ""
}

// HasValidQueryID checks if the query has a valid query ID (DIGEST)
func (sq *SlowQuery) HasValidQueryID() bool {
	return sq.QueryID.Valid && sq.QueryID.String != ""
}

// HasValidElapsedTime checks if the query has a valid elapsed time
func (sq *SlowQuery) HasValidElapsedTime() bool {
	return sq.AvgElapsedTimeMS.Valid
}

// IsValidForMetrics checks if the slow query has the minimum required fields for metrics
func (sq *SlowQuery) IsValidForMetrics() bool {
	return sq.HasValidQueryID() && sq.HasValidElapsedTime()
}
