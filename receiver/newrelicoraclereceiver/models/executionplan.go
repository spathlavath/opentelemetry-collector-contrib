// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// ExecutionPlanRow represents a single row from V$SQL_PLAN
type ExecutionPlanRow struct {
	SQLID            sql.NullString
	Timestamp        sql.NullString
	TempSpace        sql.NullInt64
	AccessPredicates sql.NullString
	Projection       sql.NullString
	Time             sql.NullInt64
	FilterPredicates sql.NullString
	ChildNumber      sql.NullInt64
	ID               sql.NullInt64
	ParentID         sql.NullInt64
	Depth            sql.NullInt64
	Operation        sql.NullString
	Options          sql.NullString
	ObjectOwner      sql.NullString
	ObjectName       sql.NullString
	Position         sql.NullInt64
	PlanHashValue    sql.NullInt64
	Cost             sql.NullInt64
	Cardinality      sql.NullInt64
	Bytes            sql.NullInt64
	CPUCost          sql.NullString // Changed to String to handle large values that exceed int64 max
	IOCost           sql.NullInt64
}
