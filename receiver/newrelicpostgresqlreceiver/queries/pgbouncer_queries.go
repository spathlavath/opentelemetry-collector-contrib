// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PgBouncer monitoring queries

const (
	// PgBouncerStatsSQL returns connection pool statistics from PgBouncer
	// This command retrieves performance metrics for each database pool
	// Must be executed against the PgBouncer admin console (pgbouncer database)
	// Returns cumulative totals and per-second averages
	// Available in PgBouncer 1.8+
	PgBouncerStatsSQL = `SHOW STATS`

	// PgBouncerPoolsSQL returns per-pool connection details from PgBouncer
	// This command retrieves current connection pool status for each database/user combination
	// Must be executed against the PgBouncer admin console (pgbouncer database)
	// Returns real-time connection counts and pool mode information
	// Available in PgBouncer 1.8+
	PgBouncerPoolsSQL = `SHOW POOLS`
)
