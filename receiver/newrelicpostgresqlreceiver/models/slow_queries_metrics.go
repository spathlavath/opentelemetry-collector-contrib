// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
	"time"
)

// PgSlowQueryMetric represents slow query statistics from pg_stat_statements
// This struct captures detailed query performance metrics including execution time,
// I/O operations, and resource utilization
type PgSlowQueryMetric struct {
	// Collection metadata
	CollectionTimestamp time.Time // Timestamp when the metric was collected

	// Query identification
	QueryID string // Unique query identifier (hash)

	// Database and user context
	DatabaseName sql.NullString // Database name where the query was executed
	UserName     sql.NullString // Username that executed the query

	// Execution count
	ExecutionCount sql.NullInt64 // Number of times the query has been executed

	// Query text
	QueryText sql.NullString // The actual SQL query text

	// Execution metrics
	AvgElapsedTimeMs    sql.NullFloat64 // Average execution time in milliseconds
	MinElapsedTimeMs    sql.NullFloat64 // Minimum execution time in milliseconds
	MaxElapsedTimeMs    sql.NullFloat64 // Maximum execution time in milliseconds
	StddevElapsedTimeMs sql.NullFloat64 // Standard deviation of execution time in milliseconds
	TotalElapsedTimeMs  sql.NullFloat64 // Total execution time in milliseconds

	// Planning time metrics (PostgreSQL 13+)
	AvgPlanTimeMs sql.NullFloat64 // Average planning time in milliseconds

	// CPU time approximation
	AvgCPUTimeMs sql.NullFloat64 // Average CPU time (planning + execution) in milliseconds

	// I/O metrics
	AvgDiskReads    sql.NullFloat64 // Average disk blocks read per execution
	TotalDiskReads  sql.NullInt64   // Total disk blocks read
	AvgBufferHits   sql.NullFloat64 // Average buffer cache hits per execution
	TotalBufferHits sql.NullInt64   // Total buffer cache hits
	AvgDiskWrites   sql.NullFloat64 // Average disk blocks written per execution
	TotalDiskWrites sql.NullInt64   // Total disk blocks written

	// Row statistics
	AvgRowsReturned sql.NullFloat64 // Average rows returned per execution
	TotalRows       sql.NullInt64   // Total rows returned

	// Interval-based delta metrics (calculated in-memory, not from DB)
	// These are populated by the PgIntervalCalculator
	// NOTE: Only elapsed time has delta calculation, CPU uses historical average from DB
	IntervalAvgElapsedTimeMS *float64 // Average elapsed time in the last interval (milliseconds)
	IntervalExecutionCount   *int64   // Number of executions in the last interval
}

// IsValidForMetrics checks if the slow query has the minimum required fields for metrics
func (m *PgSlowQueryMetric) IsValidForMetrics() bool {
	// QueryID must exist (required for grouping metrics)
	if m.QueryID == "" {
		return false
	}

	// Must have at least one valid timing metric (otherwise what are we measuring?)
	return m.TotalElapsedTimeMs.Valid || m.AvgElapsedTimeMs.Valid
}

// IsValidForDeltaCalculation checks if the query has required fields for delta calculation
func (m *PgSlowQueryMetric) IsValidForDeltaCalculation() bool {
	return m.QueryID != "" &&
		m.ExecutionCount.Valid &&
		m.TotalElapsedTimeMs.Valid
}
