// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import "database/sql"

// ExecPlan represents an Oracle query execution plan
type ExecPlan struct {
	QueryID         sql.NullString
	PlanTableOutput sql.NullString
}

// GetQueryID returns the query ID as a string, empty if null
func (ep *ExecPlan) GetQueryID() string {
	if ep.QueryID.Valid {
		return ep.QueryID.String
	}
	return ""
}

// GetPlanTableOutput returns the execution plan output as a string, empty if null
func (ep *ExecPlan) GetPlanTableOutput() string {
	if ep.PlanTableOutput.Valid {
		return ep.PlanTableOutput.String
	}
	return ""
}

// IsValid returns true if the execution plan has valid data
func (ep *ExecPlan) IsValid() bool {
	return ep.QueryID.Valid && ep.PlanTableOutput.Valid
}
