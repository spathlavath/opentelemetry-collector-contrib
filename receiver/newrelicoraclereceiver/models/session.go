// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// SessionCount represents the count of user sessions
type SessionCount struct {
	Count int64
}

// UserSessionDetail represents detailed information about user sessions
type UserSessionDetail struct {
	Username        sql.NullString
	SID             sql.NullInt64
	Serial          sql.NullInt64
	Machine         sql.NullString
	Program         sql.NullString
	LogonTime       sql.NullTime
	Status          sql.NullString
	TotalExecutions sql.NullInt64
	ActiveLockCount sql.NullInt64
}
