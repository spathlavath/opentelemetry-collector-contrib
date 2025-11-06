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
	ObjectName       sql.NullString
	PlanHashValue    sql.NullInt64
	Cost             sql.NullInt64
	Cardinality      sql.NullInt64
	Bytes            sql.NullInt64
	CPUCost          sql.NullInt64
	IOCost           sql.NullInt64
}

// PlanNode represents a node in the execution plan tree
type PlanNode struct {
	ID               int64       `json:"id"`
	Operation        string      `json:"operation"`
	Options          *string     `json:"options"`
	ObjectName       *string     `json:"object_name"`
	Cost             *int64      `json:"cost"`
	Cardinality      *int64      `json:"cardinality"`
	Bytes            *int64      `json:"bytes"`
	CPUCost          *int64      `json:"cpu_cost"`
	IOCost           *int64      `json:"io_cost"`
	Timestamp        *string     `json:"timestamp"`
	TempSpace        *int64      `json:"temp_space"`
	AccessPredicates *string     `json:"access_predicates"`
	Projection       *string     `json:"projection"`
	Time             *int64      `json:"time"`
	FilterPredicates *string     `json:"filter_predicates"`
	Children         []*PlanNode `json:"children"`
}

// ExecutionPlan represents a complete execution plan in hierarchical format
type ExecutionPlan struct {
	SQLID         string    `json:"sql_id"`
	ChildNumber   int64     `json:"child_number"`
	PlanHashValue int64     `json:"plan_hash_value"`
	PlanTree      *PlanNode `json:"plan_tree"`
}

// BuildExecutionPlansFromRows converts flat V$SQL_PLAN rows into hierarchical execution plans
func BuildExecutionPlansFromRows(rows []ExecutionPlanRow) []ExecutionPlan {
	if len(rows) == 0 {
		return []ExecutionPlan{}
	}

	// Group rows by SQL_ID and CHILD_NUMBER
	planMap := make(map[string]map[int64][]ExecutionPlanRow)
	for _, row := range rows {
		sqlID := ""
		if row.SQLID.Valid {
			sqlID = row.SQLID.String
		}
		childNum := int64(0)
		if row.ChildNumber.Valid {
			childNum = row.ChildNumber.Int64
		}

		if planMap[sqlID] == nil {
			planMap[sqlID] = make(map[int64][]ExecutionPlanRow)
		}
		planMap[sqlID][childNum] = append(planMap[sqlID][childNum], row)
	}

	var result []ExecutionPlan

	// Build hierarchical plans for each SQL_ID and CHILD_NUMBER
	for sqlID, childPlans := range planMap {
		for childNum, planRows := range childPlans {
			if len(planRows) == 0 {
				continue
			}

			plan := ExecutionPlan{
				SQLID:         sqlID,
				ChildNumber:   childNum,
				PlanHashValue: planRows[0].PlanHashValue.Int64,
			}

			// Build the tree structure
			plan.PlanTree = buildPlanTree(planRows, 0)
			result = append(result, plan)
		}
	}

	return result
}

// buildPlanTree recursively builds a plan tree from flat rows
func buildPlanTree(rows []ExecutionPlanRow, parentID int64) *PlanNode {
	// Find the node with the given ID as parent
	var currentNode *PlanNode

	for _, row := range rows {
		nodeID := int64(-1)
		if row.ID.Valid {
			nodeID = row.ID.Int64
		}

		nodeParentID := int64(-1)
		if row.ParentID.Valid {
			nodeParentID = row.ParentID.Int64
		}

		if nodeParentID == parentID {
			if currentNode == nil && nodeID == parentID {
				// This is the root node (ID == PARENT_ID == 0)
				currentNode = createPlanNode(row)
			} else if currentNode == nil {
				// First child
				currentNode = createPlanNode(row)
			}
		}
	}

	if currentNode == nil {
		// Find node with ID == parentID
		for _, row := range rows {
			nodeID := int64(-1)
			if row.ID.Valid {
				nodeID = row.ID.Int64
			}
			if nodeID == parentID {
				currentNode = createPlanNode(row)
				break
			}
		}
	}

	if currentNode != nil {
		// Build children
		childMap := make(map[int64]bool)
		for _, row := range rows {
			nodeParentID := int64(-1)
			if row.ParentID.Valid {
				nodeParentID = row.ParentID.Int64
			}

			if nodeParentID == currentNode.ID && !childMap[row.ID.Int64] {
				child := buildPlanTree(rows, row.ID.Int64)
				if child != nil {
					currentNode.Children = append(currentNode.Children, child)
					childMap[row.ID.Int64] = true
				}
			}
		}
	}

	return currentNode
}

// createPlanNode creates a PlanNode from an ExecutionPlanRow
func createPlanNode(row ExecutionPlanRow) *PlanNode {
	node := &PlanNode{
		Children: []*PlanNode{},
	}

	if row.ID.Valid {
		node.ID = row.ID.Int64
	}
	if row.Operation.Valid {
		node.Operation = row.Operation.String
	}
	if row.Options.Valid && row.Options.String != "" {
		node.Options = &row.Options.String
	}
	if row.ObjectName.Valid && row.ObjectName.String != "" {
		node.ObjectName = &row.ObjectName.String
	}
	if row.Cost.Valid {
		node.Cost = &row.Cost.Int64
	}
	if row.Cardinality.Valid {
		node.Cardinality = &row.Cardinality.Int64
	}
	if row.Bytes.Valid {
		node.Bytes = &row.Bytes.Int64
	}
	if row.CPUCost.Valid {
		node.CPUCost = &row.CPUCost.Int64
	}
	if row.IOCost.Valid {
		node.IOCost = &row.IOCost.Int64
	}
	if row.Timestamp.Valid && row.Timestamp.String != "" {
		node.Timestamp = &row.Timestamp.String
	}
	if row.TempSpace.Valid {
		node.TempSpace = &row.TempSpace.Int64
	}
	if row.AccessPredicates.Valid && row.AccessPredicates.String != "" {
		node.AccessPredicates = &row.AccessPredicates.String
	}
	if row.Projection.Valid && row.Projection.String != "" {
		node.Projection = &row.Projection.String
	}
	if row.Time.Valid {
		node.Time = &row.Time.Int64
	}
	if row.FilterPredicates.Valid && row.FilterPredicates.String != "" {
		node.FilterPredicates = &row.FilterPredicates.String
	}

	return node
}
