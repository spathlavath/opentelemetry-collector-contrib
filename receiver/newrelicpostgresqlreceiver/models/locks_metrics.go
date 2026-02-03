// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// PgLocksMetric represents lock statistics from pg_locks view
// This metric provides breakdown of active locks by lock mode per database
// Available in PostgreSQL 9.6+
type PgLocksMetric struct {
	// Database is the database name
	Database string

	// Lock counts by mode
	AccessShareLock          sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'AccessShareLock')
	RowShareLock             sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'RowShareLock')
	RowExclusiveLock         sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'RowExclusiveLock')
	ShareUpdateExclusiveLock sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'ShareUpdateExclusiveLock')
	ShareLock                sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'ShareLock')
	ShareRowExclusiveLock    sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'ShareRowExclusiveLock')
	ExclusiveLock            sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'ExclusiveLock')
	AccessExclusiveLock      sql.NullInt64 // COUNT(*) FILTER (WHERE mode = 'AccessExclusiveLock')
}
