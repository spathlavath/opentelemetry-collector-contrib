// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
	"time"
)

// PlanHashMetrics represents aggregated execution metrics by plan hash value for a SQL statement
type PlanHashMetrics struct {
	CollectionTimestamp sql.NullTime
	SQLID               sql.NullString  // SQL identifier
	PlanHashValue       sql.NullInt64   // Plan hash value - identifies the execution plan
	TotalExecutions     sql.NullInt64   // Total number of executions across all child cursors with this plan hash
	AvgElapsedTimeMs    sql.NullFloat64 // Average elapsed time per execution (milliseconds)
	AvgCPUTimeMs        sql.NullFloat64 // Average CPU time per execution (milliseconds)
	AvgDiskReads        sql.NullFloat64 // Average disk reads per execution
	AvgBufferGets       sql.NullFloat64 // Average buffer gets per execution
	AvgRowsReturned     sql.NullFloat64 // Average rows returned per execution
	FirstLoadTime       sql.NullString  // Earliest first load time across all child cursors with this plan hash (VARCHAR2)
	LastActiveTime      sql.NullTime    // Most recent active time across all child cursors (DATE)
}

// GetCollectionTimestamp returns the collection timestamp as time.Time
func (phm *PlanHashMetrics) GetCollectionTimestamp() time.Time {
	if phm.CollectionTimestamp.Valid {
		return phm.CollectionTimestamp.Time
	}
	return time.Time{}
}

// GetSQLID returns the SQL ID as a string, empty if null
func (phm *PlanHashMetrics) GetSQLID() string {
	if phm.SQLID.Valid {
		return phm.SQLID.String
	}
	return ""
}

// GetPlanHashValue returns the plan hash value as int64, 0 if null
func (phm *PlanHashMetrics) GetPlanHashValue() int64 {
	if phm.PlanHashValue.Valid {
		return phm.PlanHashValue.Int64
	}
	return 0
}

// GetTotalExecutions returns the total number of executions as int64, 0 if null
func (phm *PlanHashMetrics) GetTotalExecutions() int64 {
	if phm.TotalExecutions.Valid {
		return phm.TotalExecutions.Int64
	}
	return 0
}

// GetAvgElapsedTimeMs returns the average elapsed time in milliseconds as float64, 0 if null
func (phm *PlanHashMetrics) GetAvgElapsedTimeMs() float64 {
	if phm.AvgElapsedTimeMs.Valid {
		return phm.AvgElapsedTimeMs.Float64
	}
	return 0
}

// GetAvgCPUTimeMs returns the average CPU time in milliseconds as float64, 0 if null
func (phm *PlanHashMetrics) GetAvgCPUTimeMs() float64 {
	if phm.AvgCPUTimeMs.Valid {
		return phm.AvgCPUTimeMs.Float64
	}
	return 0
}

// GetAvgDiskReads returns the average number of disk reads as float64, 0 if null
func (phm *PlanHashMetrics) GetAvgDiskReads() float64 {
	if phm.AvgDiskReads.Valid {
		return phm.AvgDiskReads.Float64
	}
	return 0
}

// GetAvgBufferGets returns the average number of buffer gets as float64, 0 if null
func (phm *PlanHashMetrics) GetAvgBufferGets() float64 {
	if phm.AvgBufferGets.Valid {
		return phm.AvgBufferGets.Float64
	}
	return 0
}

// GetAvgRowsReturned returns the average number of rows returned as float64, 0 if null
func (phm *PlanHashMetrics) GetAvgRowsReturned() float64 {
	if phm.AvgRowsReturned.Valid {
		return phm.AvgRowsReturned.Float64
	}
	return 0
}

// GetFirstLoadTime returns the first load time as a string, empty if null
func (phm *PlanHashMetrics) GetFirstLoadTime() string {
	if phm.FirstLoadTime.Valid {
		return phm.FirstLoadTime.String
	}
	return ""
}

// GetLastActiveTime returns the last active time as time.Time
func (phm *PlanHashMetrics) GetLastActiveTime() time.Time {
	if phm.LastActiveTime.Valid {
		return phm.LastActiveTime.Time
	}
	return time.Time{}
}

// HasValidIdentifier checks if the plan hash metrics has valid SQL_ID and plan_hash_value
func (phm *PlanHashMetrics) HasValidIdentifier() bool {
	return phm.SQLID.Valid && phm.PlanHashValue.Valid
}
