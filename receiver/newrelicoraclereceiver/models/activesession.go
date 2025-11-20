// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// ActiveSession represents an active session running a specific SQL ID from V$SESSION
type ActiveSession struct {
	Username       sql.NullString
	SID            sql.NullInt64
	Serial         sql.NullInt64
	QueryID        sql.NullString
	SQLChildNumber sql.NullInt64
	SQLExecStart   sql.NullString
	SQLExecID      sql.NullInt64
}
