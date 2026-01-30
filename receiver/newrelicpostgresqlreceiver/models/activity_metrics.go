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
