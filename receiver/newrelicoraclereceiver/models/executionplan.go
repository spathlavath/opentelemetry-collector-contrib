// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// ExecutionPlan represents an execution plan record from Oracle using DBMS_XPLAN.DISPLAY_CURSOR
type ExecutionPlan struct {
	DatabaseName      sql.NullString
	QueryID           sql.NullString
	PlanHashValue     sql.NullInt64
	ExecutionPlanText sql.NullString // The formatted execution plan from DBMS_XPLAN.DISPLAY_CURSOR
}

// GetDatabaseName returns the database name as a string, empty if null
func (ep *ExecutionPlan) GetDatabaseName() string {
	if ep.DatabaseName.Valid {
		return ep.DatabaseName.String
	}
	return ""
}

// GetQueryID returns the query ID as a string, empty if null
func (ep *ExecutionPlan) GetQueryID() string {
	if ep.QueryID.Valid {
		return ep.QueryID.String
	}
	return ""
}

// GetPlanHashValue returns the plan hash value as int64, 0 if null
func (ep *ExecutionPlan) GetPlanHashValue() int64 {
	if ep.PlanHashValue.Valid {
		return ep.PlanHashValue.Int64
	}
	return 0
}

// GetExecutionPlanText returns the execution plan text as a string, empty if null
func (ep *ExecutionPlan) GetExecutionPlanText() string {
	if ep.ExecutionPlanText.Valid {
		return ep.ExecutionPlanText.String
	}
	return ""
}

// HasValidQueryID checks if the execution plan has a valid query ID
func (ep *ExecutionPlan) HasValidQueryID() bool {
	return ep.QueryID.Valid
}

// HasValidPlanHashValue checks if the execution plan has a valid plan hash value
func (ep *ExecutionPlan) HasValidPlanHashValue() bool {
	return ep.PlanHashValue.Valid
}

// HasValidExecutionPlanText checks if the execution plan has valid plan text
func (ep *ExecutionPlan) HasValidExecutionPlanText() bool {
	return ep.ExecutionPlanText.Valid && len(ep.ExecutionPlanText.String) > 0
}

// IsValidForMetrics checks if the execution plan has the minimum required fields for metrics
func (ep *ExecutionPlan) IsValidForMetrics() bool {
	return ep.HasValidQueryID() && ep.HasValidPlanHashValue() && ep.HasValidExecutionPlanText()
}
