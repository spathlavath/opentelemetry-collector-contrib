// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PostgreSQL activity monitoring queries

const (
	// PgStatActivitySQL returns connection activity statistics from pg_stat_activity
	// This query retrieves active connection metrics, transaction age, and backend transaction IDs
	// grouped by database, user, application, and backend type
	// Filters out system background processes (where datname IS NULL) since they don't have database-specific activity
	// Available in PostgreSQL 9.6+
	PgStatActivitySQL = `
		SELECT
			datname,
			usename,
			application_name,
			backend_type,
			COUNT(*) FILTER (WHERE state = 'active' AND wait_event IS NOT NULL) as active_waiting_queries,
			MAX(EXTRACT(EPOCH FROM (now() - xact_start))) FILTER (WHERE xact_start IS NOT NULL) as xact_start_age,
			MAX(GREATEST(0, age(backend_xid))) as backend_xid_age,
			MAX(GREATEST(0, age(backend_xmin))) as backend_xmin_age,
			MAX(EXTRACT(EPOCH FROM (now() - xact_start))) FILTER (WHERE state != 'idle') as max_transaction_duration,
			SUM(EXTRACT(EPOCH FROM (now() - xact_start))) FILTER (WHERE state != 'idle') as sum_transaction_duration
		FROM pg_stat_activity
		WHERE datname IS NOT NULL
		GROUP BY datname, usename, application_name, backend_type`

	// PgStatActivityWaitEventsSQL returns wait event statistics from pg_stat_activity
	// This query retrieves backend counts grouped by wait event type
	// COALESCE is used to handle NULL wait_event values (backends not waiting)
	// Filters out system background processes (where datname IS NULL) since they don't have database-specific activity
	// Available in PostgreSQL 9.6+
	PgStatActivityWaitEventsSQL = `
		SELECT
			datname,
			usename,
			application_name,
			backend_type,
			COALESCE(wait_event, 'NoWaitEvent') as wait_event,
			COUNT(*) as wait_event_count
		FROM pg_stat_activity
		WHERE datname IS NOT NULL
		GROUP BY datname, usename, application_name, backend_type, wait_event`

	// PgStatStatementsInfoSQL returns deallocation statistics from pg_stat_statements_info
	// This query retrieves the number of times pg_stat_statements has deallocated least-used statements
	// Requires pg_stat_statements extension to be installed and enabled
	// Returns NULL (handled by sql.NullInt64) if extension is not available
	// Available in PostgreSQL 13+
	PgStatStatementsInfoSQL = `SELECT dealloc FROM pg_stat_statements_info`

	// PgSnapshotSQL returns transaction snapshot information using pg_snapshot functions
	// This query retrieves the current transaction visibility snapshot including:
	// - xmin: earliest transaction ID still active
	// - xmax: first as-yet-unassigned transaction ID
	// - xip_count: number of in-progress transactions
	// Available in PostgreSQL 13+
	PgSnapshotSQL = `
		SELECT
			pg_snapshot_xmin(pg_current_snapshot()) as xmin,
			pg_snapshot_xmax(pg_current_snapshot()) as xmax,
			(SELECT COUNT(*) FROM pg_snapshot_xip(pg_current_snapshot())) as xip_count`
)
