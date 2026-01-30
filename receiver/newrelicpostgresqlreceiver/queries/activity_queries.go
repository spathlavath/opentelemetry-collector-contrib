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
)
