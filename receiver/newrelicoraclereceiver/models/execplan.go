// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

// PlanOperation represents a single operation in the execution plan
type PlanOperation struct {
	ID         int               `json:"id"`
	Operation  string            `json:"operation"`
	Options    string            `json:"options,omitempty"`
	Name       string            `json:"name,omitempty"`
	Rows       *int64            `json:"rows,omitempty"`
	Bytes      *int64            `json:"bytes,omitempty"`
	Cost       *int              `json:"cost,omitempty"`
	CPUCost    string            `json:"cpu_cost,omitempty"`
	Time       string            `json:"time,omitempty"`
	Predicates map[string]string `json:"predicates,omitempty"`
	Children   []PlanOperation   `json:"children,omitempty"`
}

// ExecPlanJSON represents a JSON-serializable execution plan
type ExecPlanJSON struct {
	QueryID       string         `json:"query_id"`
	SQLID         string         `json:"sql_id"`
	ChildNumber   int            `json:"child_number"`
	SQLText       string         `json:"sql_text,omitempty"`
	PlanHashValue int64          `json:"plan_hash_value,omitempty"`
	Plan          *PlanOperation `json:"plan,omitempty"`
	HasPlan       bool           `json:"has_plan"`
	Note          string         `json:"note,omitempty"`
}

// ToJSON converts the execution plan to a JSON-serializable structure
func (ep *ExecPlan) ToJSON() ExecPlanJSON {
	planJSON := ExecPlanJSON{
		QueryID: ep.GetQueryID(),
		HasPlan: true,
	}

	// Parse the plan output to extract key information
	planOutput := ep.GetPlanTableOutput()
	if planOutput == "" {
		planJSON.HasPlan = false
		return planJSON
	}

	lines := strings.Split(planOutput, "\n")

	// Parse the execution plan
	parsePlan(lines, &planJSON)

	return planJSON
}

// parsePlan extracts structured information from the execution plan text
func parsePlan(lines []string, planJSON *ExecPlanJSON) {
	var planTableStart, planTableEnd int
	var sqlTextLines []string
	collectingSQLText := false

	for i, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// Extract SQL_ID and child number
		if strings.HasPrefix(trimmedLine, "SQL_ID") && strings.Contains(trimmedLine, "child number") {
			parts := strings.Fields(trimmedLine)
			if len(parts) >= 2 {
				planJSON.SQLID = strings.TrimRight(parts[1], ",")
			}
			if len(parts) >= 5 {
				if childNum := parseIntSafe(parts[4]); childNum != nil {
					planJSON.ChildNumber = *childNum
				}
			}
			collectingSQLText = true
			continue
		}

		// Collect SQL text (between dashed lines)
		if collectingSQLText {
			if strings.Contains(line, "-----") {
				if len(sqlTextLines) == 0 {
					continue // First dashed line after SQL_ID
				} else {
					collectingSQLText = false // Second dashed line, end of SQL text
				}
			} else if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "SQL_ID") {
				sqlTextLines = append(sqlTextLines, trimmedLine)
			}
		}

		// Extract plan hash value
		if strings.HasPrefix(trimmedLine, "Plan hash value:") {
			parts := strings.Fields(trimmedLine)
			if len(parts) >= 4 {
				if hashVal := parseInt64Safe(parts[3]); hashVal != nil {
					planJSON.PlanHashValue = *hashVal
				}
			}
		}

		// Find plan table boundaries
		if strings.Contains(line, "| Id") && strings.Contains(line, "| Operation") {
			planTableStart = i + 2 // Skip header and separator line
		}

		if planTableStart > 0 && planTableEnd == 0 {
			if strings.HasPrefix(trimmedLine, "---") && i > planTableStart {
				planTableEnd = i
			}
		}

		// Check for "cannot fetch plan" note
		if strings.Contains(trimmedLine, "cannot fetch plan") ||
			strings.Contains(trimmedLine, "no longer in cursor cache") {
			planJSON.HasPlan = false
			planJSON.Note = trimmedLine
			return
		}
	}

	// Set SQL text
	if len(sqlTextLines) > 0 {
		planJSON.SQLText = strings.Join(sqlTextLines, " ")
	}

	// Parse the plan table
	if planTableStart > 0 && planTableEnd > planTableStart {
		planRows := parsePlanTable(lines[planTableStart:planTableEnd])
		if len(planRows) > 0 {
			// Build the plan tree
			planJSON.Plan = buildPlanTree(planRows)
		}
	}

	// Extract predicates
	extractPredicates(lines, planJSON)
}

// parsePlanTable parses the plan table rows
func parsePlanTable(lines []string) []map[string]string {
	var rows []map[string]string

	for _, line := range lines {
		if strings.TrimSpace(line) == "" || !strings.Contains(line, "|") {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) < 4 {
			continue
		}

		row := make(map[string]string)

		// Parse each column
		for i, part := range parts {
			trimmed := strings.TrimSpace(part)
			switch i {
			case 0:
				// Skip empty first column
			case 1:
				row["id"] = strings.TrimLeft(trimmed, "*")
				if strings.HasPrefix(strings.TrimSpace(part), "*") {
					row["has_predicate"] = "true"
				}
			case 2:
				// Operation may contain both operation and options
				opParts := strings.Fields(trimmed)
				if len(opParts) > 0 {
					row["operation"] = opParts[0]
					if len(opParts) > 1 {
						row["options"] = strings.Join(opParts[1:], " ")
					}
				}
			case 3:
				row["name"] = trimmed
			case 4:
				row["rows"] = trimmed
			case 5:
				row["bytes"] = trimmed
			case 6:
				// Parse cost and CPU percentage
				costStr := strings.TrimSpace(trimmed)
				if strings.Contains(costStr, "(") {
					costParts := strings.Split(costStr, "(")
					row["cost"] = strings.TrimSpace(costParts[0])
					if len(costParts) > 1 {
						row["cpu_cost"] = strings.TrimRight(costParts[1], ")")
					}
				} else {
					row["cost"] = costStr
				}
			case 7:
				row["time"] = trimmed
			}
		}

		if row["id"] != "" {
			rows = append(rows, row)
		}
	}

	return rows
}

// buildPlanTree constructs a hierarchical plan structure
func buildPlanTree(rows []map[string]string) *PlanOperation {
	if len(rows) == 0 {
		return nil
	}

	// Create a map of all operations by ID
	operations := make(map[int]*PlanOperation)
	var rootOp *PlanOperation

	for _, row := range rows {
		id := parseIntSafe(row["id"])
		if id == nil {
			continue
		}

		op := &PlanOperation{
			ID:        *id,
			Operation: row["operation"],
			Options:   row["options"],
			Name:      row["name"],
			Time:      row["time"],
			CPUCost:   row["cpu_cost"],
		}

		if rows := parseInt64Safe(row["rows"]); rows != nil {
			op.Rows = rows
		}
		if bytes := parseInt64Safe(row["bytes"]); bytes != nil {
			op.Bytes = bytes
		}
		if cost := parseIntSafe(row["cost"]); cost != nil {
			op.Cost = cost
		}

		operations[*id] = op

		if *id == 0 {
			rootOp = op
		}
	}

	// Build the tree by determining parent-child relationships based on indentation/ID
	for i := len(rows) - 1; i > 0; i-- {
		currentID := parseIntSafe(rows[i]["id"])
		if currentID == nil {
			continue
		}

		// Find the parent (first operation with lower ID above current)
		for j := i - 1; j >= 0; j-- {
			parentID := parseIntSafe(rows[j]["id"])
			if parentID != nil && *parentID < *currentID {
				if parent, ok := operations[*parentID]; ok {
					if child, ok := operations[*currentID]; ok {
						parent.Children = append(parent.Children, *child)
					}
				}
				break
			}
		}
	}

	return rootOp
}

// extractPredicates extracts predicate information and adds to operations
func extractPredicates(lines []string, planJSON *ExecPlanJSON) {
	predicateSection := false
	predicates := make(map[int]map[string]string)

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		if strings.HasPrefix(trimmedLine, "Predicate Information") {
			predicateSection = true
			continue
		}

		if predicateSection && trimmedLine == "" {
			break
		}

		if predicateSection && strings.Contains(trimmedLine, " - ") {
			parts := strings.SplitN(trimmedLine, " - ", 2)
			if len(parts) == 2 {
				idStr := strings.TrimSpace(parts[0])
				predStr := strings.TrimSpace(parts[1])

				if id := parseIntSafe(idStr); id != nil {
					if _, ok := predicates[*id]; !ok {
						predicates[*id] = make(map[string]string)
					}

					// Determine predicate type (access, filter, etc.)
					if strings.HasPrefix(predStr, "access(") {
						predicates[*id]["access"] = strings.TrimSuffix(strings.TrimPrefix(predStr, "access("), ")")
					} else if strings.HasPrefix(predStr, "filter(") {
						predicates[*id]["filter"] = strings.TrimSuffix(strings.TrimPrefix(predStr, "filter("), ")")
					} else {
						predicates[*id]["info"] = predStr
					}
				}
			}
		}
	}

	// Add predicates to the plan
	if planJSON.Plan != nil {
		addPredicatesToPlan(planJSON.Plan, predicates)
	}
}

// addPredicatesToPlan recursively adds predicates to plan operations
func addPredicatesToPlan(op *PlanOperation, predicates map[int]map[string]string) {
	if preds, ok := predicates[op.ID]; ok {
		op.Predicates = preds
	}

	for i := range op.Children {
		addPredicatesToPlan(&op.Children[i], predicates)
	}
}

// Helper functions for safe parsing
func parseIntSafe(s string) *int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var val int
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return nil
	}
	return &val
}

func parseInt64Safe(s string) *int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return nil
	}
	return &val
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
