// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PostgreSQL lock statistics queries

const (
	// PgLocksSQL returns lock statistics broken down by lock mode
	// This query retrieves counts of active locks grouped by lock type per database
	// Uses COUNT(*) FILTER for efficient aggregation by lock mode
	// Available in PostgreSQL 9.6+
	PgLocksSQL = `
SELECT
    current_database() as database,
    COUNT(*) FILTER (WHERE mode = 'AccessShareLock') as access_share_lock,
    COUNT(*) FILTER (WHERE mode = 'RowShareLock') as row_share_lock,
    COUNT(*) FILTER (WHERE mode = 'RowExclusiveLock') as row_exclusive_lock,
    COUNT(*) FILTER (WHERE mode = 'ShareUpdateExclusiveLock') as share_update_exclusive_lock,
    COUNT(*) FILTER (WHERE mode = 'ShareLock') as share_lock,
    COUNT(*) FILTER (WHERE mode = 'ShareRowExclusiveLock') as share_row_exclusive_lock,
    COUNT(*) FILTER (WHERE mode = 'ExclusiveLock') as exclusive_lock,
    COUNT(*) FILTER (WHERE mode = 'AccessExclusiveLock') as access_exclusive_lock
FROM pg_locks
WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())`
)
