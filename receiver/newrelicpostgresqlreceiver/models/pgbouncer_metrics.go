// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgBouncerStatsMetric represents statistics from PgBouncer SHOW STATS command
// This metric provides connection pooling performance statistics per database
// Available in PgBouncer 1.8+
type PgBouncerStatsMetric struct {
	// Database is the database name
	Database string

	// Total counters (cumulative since PgBouncer start)
	TotalXactCount  sql.NullInt64 // Total number of transactions pooled
	TotalQueryCount sql.NullInt64 // Total number of queries pooled
	TotalReceived   sql.NullInt64 // Total bytes received from clients
	TotalSent       sql.NullInt64 // Total bytes sent to clients
	TotalXactTime   sql.NullInt64 // Total transaction time in microseconds
	TotalQueryTime  sql.NullInt64 // Total query time in microseconds
	TotalWaitTime   sql.NullInt64 // Total time clients waited for a server (microseconds)

	// Average metrics (per-second averages)
	AvgXactCount  sql.NullInt64 // Average transactions per second
	AvgQueryCount sql.NullInt64 // Average queries per second
	AvgRecv       sql.NullInt64 // Average bytes received per second
	AvgSent       sql.NullInt64 // Average bytes sent per second
	AvgXactTime   sql.NullInt64 // Average transaction time in microseconds
	AvgQueryTime  sql.NullInt64 // Average query time in microseconds
	AvgWaitTime   sql.NullInt64 // Average wait time in microseconds
}
