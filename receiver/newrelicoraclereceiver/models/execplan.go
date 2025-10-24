// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
	"encoding/json"
	"strings"
)

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

// ExecPlanJSON represents a JSON-serializable execution plan
type ExecPlanJSON struct {
	QueryID       string   `json:"query_id"`
	SQLID         string   `json:"sql_id"`
	ChildNumber   string   `json:"child_number"`
	QueryText     string   `json:"query_text,omitempty"`
	PlanHashValue string   `json:"plan_hash_value,omitempty"`
	PlanOutput    []string `json:"plan_output"`
	FullPlan      string   `json:"full_plan"`
	HasPlan       bool     `json:"has_plan"`
	Note          string   `json:"note,omitempty"`
}

// ToJSON converts the execution plan to a JSON-serializable structure
func (ep *ExecPlan) ToJSON() ExecPlanJSON {
	planJSON := ExecPlanJSON{
		QueryID:  ep.GetQueryID(),
		FullPlan: ep.GetPlanTableOutput(),
		HasPlan:  true,
	}

	// Parse the plan output to extract key information
	planOutput := ep.GetPlanTableOutput()
	if planOutput == "" {
		planJSON.HasPlan = false
		return planJSON
	}

	lines := strings.Split(planOutput, "\n")
	planJSON.PlanOutput = lines

	// Extract SQL_ID, child number, query text, and plan hash value
	for i, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		
		// Extract SQL_ID and child number
		if strings.HasPrefix(trimmedLine, "SQL_ID") && strings.Contains(trimmedLine, "child number") {
			parts := strings.Fields(trimmedLine)
			if len(parts) >= 2 {
				planJSON.SQLID = strings.TrimRight(parts[1], ",")
			}
			if len(parts) >= 5 {
				planJSON.ChildNumber = parts[4]
			}
		}
		
		// Extract query text (usually appears after the dashed line)
		if i > 0 && strings.Contains(lines[i-1], "-----") && 
			!strings.HasPrefix(trimmedLine, "Plan hash") && 
			!strings.HasPrefix(trimmedLine, "Note") &&
			!strings.Contains(trimmedLine, "-----") &&
			trimmedLine != "" && planJSON.QueryText == "" {
			planJSON.QueryText = trimmedLine
		}
		
		// Extract plan hash value
		if strings.HasPrefix(trimmedLine, "Plan hash value:") {
			parts := strings.Fields(trimmedLine)
			if len(parts) >= 4 {
				planJSON.PlanHashValue = parts[3]
			}
		}
		
		// Check for "cannot fetch plan" note
		if strings.Contains(trimmedLine, "cannot fetch plan") || 
			strings.Contains(trimmedLine, "no longer in cursor cache") {
			planJSON.HasPlan = false
			planJSON.Note = trimmedLine
		}
	}

	return planJSON
}

// ToJSONString returns the execution plan as a formatted JSON string
func (ep *ExecPlan) ToJSONString() (string, error) {
	planJSON := ep.ToJSON()
	jsonBytes, err := json.MarshalIndent(planJSON, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}
