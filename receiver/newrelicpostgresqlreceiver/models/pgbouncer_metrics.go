// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgBouncerStatsMetric represents statistics from PgBouncer SHOW STATS command
// This metric provides connection pooling performance statistics per database
// Available in PgBouncer 1.8+
// IMPORTANT: Field order must match the column order returned by SHOW STATS
type PgBouncerStatsMetric struct {
	// Database is the database name
	Database string

	// Total counters (cumulative since PgBouncer start)
	TotalServerAssignmentCount sql.NullInt64 // Total number of times a server connection was assigned
	TotalXactCount             sql.NullInt64 // Total number of transactions pooled
	TotalQueryCount            sql.NullInt64 // Total number of queries pooled
	TotalReceived              sql.NullInt64 // Total bytes received from clients
	TotalSent                  sql.NullInt64 // Total bytes sent to clients
	TotalXactTime              sql.NullInt64 // Total transaction time in microseconds
	TotalQueryTime             sql.NullInt64 // Total query time in microseconds
	TotalWaitTime              sql.NullInt64 // Total time clients waited for a server (microseconds)
	TotalClientParseCount      sql.NullInt64 // Total number of client parse operations
	TotalServerParseCount      sql.NullInt64 // Total number of server parse operations
	TotalBindCount             sql.NullInt64 // Total number of bind operations

	// Average metrics (per-second averages)
	AvgServerAssignmentCount sql.NullInt64 // Average server assignments per second
	AvgXactCount             sql.NullInt64 // Average transactions per second
	AvgQueryCount            sql.NullInt64 // Average queries per second
	AvgRecv                  sql.NullInt64 // Average bytes received per second
	AvgSent                  sql.NullInt64 // Average bytes sent per second
	AvgXactTime              sql.NullInt64 // Average transaction time in microseconds
	AvgQueryTime             sql.NullInt64 // Average query time in microseconds
	AvgWaitTime              sql.NullInt64 // Average wait time in microseconds
	AvgClientParseCount      sql.NullInt64 // Average client parse operations per second
	AvgServerParseCount      sql.NullInt64 // Average server parse operations per second
	AvgBindCount             sql.NullInt64 // Average bind operations per second
}

// PgBouncerPoolsMetric represents per-pool connection statistics from PgBouncer SHOW POOLS command
// This metric provides detailed connection pool status per database and user
// Available in PgBouncer 1.8+
// IMPORTANT: Field order must match the column order returned by SHOW POOLS
type PgBouncerPoolsMetric struct {
	// Database is the database name
	Database string

	// User is the username for this pool
	User string

	// Client connection counters
	ClActive            sql.NullInt64 // Active client connections
	ClWaiting           sql.NullInt64 // Client connections waiting for a server connection
	ClActiveCancelReq   sql.NullInt64 // Client connections with active cancel requests
	ClWaitingCancelReq  sql.NullInt64 // Client connections waiting with cancel requests

	// Server connection counters
	SvActive        sql.NullInt64 // Server connections actively linked to a client
	SvIdle          sql.NullInt64 // Server connections idle and ready
	SvUsed          sql.NullInt64 // Server connections idle more than server_check_delay
	SvTested        sql.NullInt64 // Server connections currently being tested
	SvLogin         sql.NullInt64 // Server connections currently logging in
	SvActiveCancel  sql.NullInt64 // Server connections currently being canceled
	SvBeingCancel   sql.NullInt64 // Server connections in the process of being canceled

	// Pool status
	Maxwait  sql.NullInt64 // Maximum wait time for clients in microseconds
	PoolMode string        // Pool mode: session, transaction, or statement
}
