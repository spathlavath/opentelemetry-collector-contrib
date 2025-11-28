// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
)

// ChildCursor represents a child cursor from V$SQL with average execution statistics
type ChildCursor struct {
	DatabaseName   sql.NullString
	SQLID          sql.NullString
	ChildNumber    sql.NullInt64
	CPUTime        sql.NullInt64 // Average CPU time per execution (microseconds)
	ElapsedTime    sql.NullInt64 // Average elapsed time per execution (microseconds)
	UserIOWaitTime sql.NullInt64 // Average I/O wait time per execution (microseconds)
	DiskReads      sql.NullInt64 // Average disk reads per execution
	BufferGets     sql.NullInt64 // Average buffer gets per execution
	Executions     sql.NullInt64 // Total number of executions
	Invalidations  sql.NullInt64
	FirstLoadTime  sql.NullString
	LastLoadTime   sql.NullString
}

// GetDatabaseName returns the database name as a string, empty if null
func (cc *ChildCursor) GetDatabaseName() string {
	if cc.DatabaseName.Valid {
		return cc.DatabaseName.String
	}
	return ""
}

// GetSQLID returns the SQL ID as a string, empty if null
func (cc *ChildCursor) GetSQLID() string {
	if cc.SQLID.Valid {
		return cc.SQLID.String
	}
	return ""
}

// GetChildNumber returns the child number as int64, 0 if null
func (cc *ChildCursor) GetChildNumber() int64 {
	if cc.ChildNumber.Valid {
		return cc.ChildNumber.Int64
	}
	return 0
}

// GetCPUTime returns the CPU time in microseconds as int64, 0 if null
func (cc *ChildCursor) GetCPUTime() int64 {
	if cc.CPUTime.Valid {
		return cc.CPUTime.Int64
	}
	return 0
}

// GetElapsedTime returns the elapsed time in microseconds as int64, 0 if null
func (cc *ChildCursor) GetElapsedTime() int64 {
	if cc.ElapsedTime.Valid {
		return cc.ElapsedTime.Int64
	}
	return 0
}

// GetUserIOWaitTime returns the user IO wait time in microseconds as int64, 0 if null
func (cc *ChildCursor) GetUserIOWaitTime() int64 {
	if cc.UserIOWaitTime.Valid {
		return cc.UserIOWaitTime.Int64
	}
	return 0
}

// GetExecutions returns the number of executions as int64, 0 if null
func (cc *ChildCursor) GetExecutions() int64 {
	if cc.Executions.Valid {
		return cc.Executions.Int64
	}
	return 0
}

// GetDiskReads returns the number of disk reads as int64, 0 if null
func (cc *ChildCursor) GetDiskReads() int64 {
	if cc.DiskReads.Valid {
		return cc.DiskReads.Int64
	}
	return 0
}

// GetBufferGets returns the number of buffer gets as int64, 0 if null
func (cc *ChildCursor) GetBufferGets() int64 {
	if cc.BufferGets.Valid {
		return cc.BufferGets.Int64
	}
	return 0
}

// GetInvalidations returns the number of invalidations as int64, 0 if null
func (cc *ChildCursor) GetInvalidations() int64 {
	if cc.Invalidations.Valid {
		return cc.Invalidations.Int64
	}
	return 0
}

// GetFirstLoadTime returns the first load time as a string, empty if null
func (cc *ChildCursor) GetFirstLoadTime() string {
	if cc.FirstLoadTime.Valid {
		return cc.FirstLoadTime.String
	}
	return ""
}

// GetLastLoadTime returns the last load time as a string, empty if null
func (cc *ChildCursor) GetLastLoadTime() string {
	if cc.LastLoadTime.Valid {
		return cc.LastLoadTime.String
	}
	return ""
}

// HasValidIdentifier checks if the child cursor has valid SQL_ID and child_number
func (cc *ChildCursor) HasValidIdentifier() bool {
	return cc.SQLID.Valid && cc.ChildNumber.Valid
}
