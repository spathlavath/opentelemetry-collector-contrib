// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgStatActivity represents activity statistics from pg_stat_activity
// This struct captures connection activity, transaction age, and backend transaction IDs
// grouped by database, user, application, and backend type
// Available in PostgreSQL 9.6+
type PgStatActivity struct {
	// Grouping dimensions (can be NULL for system background processes)
	DatName         sql.NullString // Database name
	UserName        sql.NullString // User/role name
	ApplicationName sql.NullString // Application name from connection
	BackendType     sql.NullString // Type of backend process

	// Activity metrics
	ActiveConnections    sql.NullInt64 // Count of active connections (state='active')
	ActiveWaitingQueries sql.NullInt64 // Count of active queries waiting on locks or resources

	// Transaction age metrics
	XactStartAge sql.NullFloat64 // Maximum age in seconds of oldest transaction start time

	// Backend transaction ID age metrics
	BackendXIDAge  sql.NullInt64 // Maximum age of backend transaction IDs currently in use
	BackendXminAge sql.NullInt64 // Maximum age of backend xmin values (oldest visible transaction)

	// Transaction duration metrics
	MaxTransactionDuration sql.NullFloat64 // Maximum transaction duration in seconds (non-idle)
	SumTransactionDuration sql.NullFloat64 // Sum of all transaction durations in seconds (non-idle)
}

// PgStatActivityWaitEvents represents wait event statistics from pg_stat_activity
// This struct captures the count of backends grouped by wait event type
// grouped by database, user, application, backend type, and wait event
// Available in PostgreSQL 9.6+
type PgStatActivityWaitEvents struct {
	// Grouping dimensions (can be NULL for system background processes)
	DatName         sql.NullString // Database name
	UserName        sql.NullString // User/role name
	ApplicationName sql.NullString // Application name from connection
	BackendType     sql.NullString // Type of backend process
	WaitEvent       sql.NullString // Type of wait event (or NoWaitEvent)

	// Wait event metric
	WaitEventCount sql.NullInt64 // Count of backends in this wait event state
}

// PgStatStatementsDealloc represents statement deallocation statistics from pg_stat_statements_info
// This metric tracks the number of times pg_stat_statements has deallocated least-used statements
// Requires pg_stat_statements extension to be installed and enabled
// Available in PostgreSQL 13+
type PgStatStatementsDealloc struct {
	// Dealloc is the number of times statements have been deallocated
	Dealloc sql.NullInt64
}

// PgSnapshot represents transaction snapshot information from pg_snapshot functions
// This provides insight into the current transaction state and visibility
// Available in PostgreSQL 13+
type PgSnapshot struct {
	// Xmin is the earliest transaction ID still active
	Xmin sql.NullInt64
	// Xmax is the first as-yet-unassigned transaction ID
	Xmax sql.NullInt64
	// XipCount is the number of in-progress transactions
	XipCount sql.NullInt64
}

// PgBuffercache represents buffer cache statistics from pg_buffercache extension
// This provides insight into shared buffer cache usage grouped by database, schema, and table
// Requires pg_buffercache extension to be installed and enabled
// Available in PostgreSQL 9.6+
type PgBuffercache struct {
	// Grouping dimensions (can be NULL for shared buffers or system objects)
	Database sql.NullString // Database name, or 'shared' for shared buffers
	Schema   sql.NullString // Schema name
	Table    sql.NullString // Table name

	// Buffer cache metrics
	UsedBuffers     sql.NullInt64 // Count of buffers with data (relfilenode IS NOT NULL)
	UnusedBuffers   sql.NullInt64 // Count of empty buffers (relfilenode IS NULL)
	UsageCount      sql.NullInt64 // Sum of usage counts (how frequently buffers are accessed)
	DirtyBuffers    sql.NullInt64 // Count of dirty buffers needing write-back
	PinningBackends sql.NullInt64 // Count of backends currently pinning buffers
}
