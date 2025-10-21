package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ExecutionPlanScraper handles fetching Oracle execution plans
type ExecutionPlanScraper struct {
	db     *sql.DB
	logger *zap.Logger
}

// ExecutionPlan represents an Oracle execution plan in XML format
type ExecutionPlan struct {
	QueryID     string
	PlanXML     string
	PlanText    string // Human-readable format
	PlanHash    string
	ChildNumber int
	Error       error
}

// ExecutionPlanResult contains the results of execution plan fetching
type ExecutionPlanResult struct {
	Plans        []ExecutionPlan
	SuccessCount int
	ErrorCount   int
	TotalCount   int
}

// NewExecutionPlanScraper creates a new execution plan scraper
func NewExecutionPlanScraper(db *sql.DB, logger *zap.Logger) (*ExecutionPlanScraper, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &ExecutionPlanScraper{
		db:     db,
		logger: logger,
	}, nil
}

// GetExecutionPlansXML fetches Oracle execution plans in raw XML format for the given query IDs
func (eps *ExecutionPlanScraper) GetExecutionPlansXML(ctx context.Context, queryIDs []string) *ExecutionPlanResult {
	if len(queryIDs) == 0 {
		eps.logger.Debug("No query IDs provided for execution plan retrieval")
		fmt.Printf("⚠️  No query IDs provided for execution plan retrieval\n")
		return &ExecutionPlanResult{Plans: nil, SuccessCount: 0, ErrorCount: 0, TotalCount: 0}
	}

	eps.logger.Info("Starting execution plan retrieval",
		zap.Int("total_query_ids", len(queryIDs)),
		zap.Strings("query_ids", queryIDs))

	fmt.Printf("\n🔍 Starting execution plan retrieval for %d query IDs\n", len(queryIDs))
	fmt.Printf("Query IDs: %v\n", queryIDs)

	// Oracle SQL query to get execution plan in raw XML format (no XMLSERIALIZE wrapper)
	const executionPlanXMLSQL = `
		SELECT 
			sql_id,
			child_number,
			plan_hash_value,
			DBMS_XPLAN.DISPLAY_CURSOR(sql_id, NULL, 'ADVANCED +OUTLINE +PROJECTION +ALIAS +PREDICATE +NOTE +IOSTATS +MEMSTATS +ALLSTATS LAST') AS plan_xml
		FROM v$sql 
		WHERE sql_id = ? 
		AND rownum = 1`

	var executionPlans []ExecutionPlan
	successCount := 0
	errorCount := 0

	for i, queryID := range queryIDs {
		plan := ExecutionPlan{QueryID: queryID}

		eps.logger.Info("Processing execution plan",
			zap.Int("current", i+1),
			zap.Int("total", len(queryIDs)),
			zap.String("query_id", queryID))

		fmt.Printf("📋 [%d/%d] Processing execution plan for Query ID: %s\n", i+1, len(queryIDs), queryID)

		var planXML sql.NullString
		var childNumber sql.NullInt64
		var planHashValue sql.NullInt64
		var returnedQueryID string

		eps.logger.Debug("Executing SQL query", zap.String("sql", executionPlanXMLSQL))
		fmt.Printf("   🔄 Executing DBMS_XPLAN.DISPLAY_CURSOR for query ID: %s\n", queryID)

		err := eps.db.QueryRowContext(ctx, executionPlanXMLSQL, queryID).Scan(
			&returnedQueryID,
			&childNumber,
			&planHashValue,
			&planXML,
		)

		if err != nil {
			if err == sql.ErrNoRows {
				plan.Error = fmt.Errorf("no execution plan found for query ID: %s", queryID)
				eps.logger.Warn("No execution plan found in v$sql", zap.String("query_id", queryID))
				fmt.Printf("   ❌ No execution plan found in v$sql for Query ID: %s\n", queryID)
			} else {
				plan.Error = fmt.Errorf("failed to fetch execution plan for query ID %s: %w", queryID, err)
				eps.logger.Error("SQL execution failed", zap.String("query_id", queryID), zap.Error(err))
				fmt.Printf("   ❌ SQL execution failed for Query ID: %s, Error: %v\n", queryID, err)
			}
			errorCount++
		} else {
			eps.logger.Info("SQL execution successful",
				zap.String("query_id", queryID),
				zap.String("returned_query_id", returnedQueryID),
				zap.Bool("plan_xml_valid", planXML.Valid))

			fmt.Printf("   ✅ SQL execution successful for Query ID: %s\n", queryID)
			fmt.Printf("   📊 Returned Query ID: %s, Plan XML Valid: %v\n", returnedQueryID, planXML.Valid)

			if planXML.Valid && planXML.String != "" {
				plan.PlanXML = planXML.String
				if childNumber.Valid {
					plan.ChildNumber = int(childNumber.Int64)
					fmt.Printf("   🔢 Child Number: %d\n", plan.ChildNumber)
				}
				if planHashValue.Valid {
					plan.PlanHash = fmt.Sprintf("%d", planHashValue.Int64)
					fmt.Printf("   🏷️  Plan Hash: %s\n", plan.PlanHash)
				}

				eps.logger.Info("Successfully retrieved raw XML execution plan",
					zap.String("query_id", queryID),
					zap.Int("plan_size_bytes", len(plan.PlanXML)),
					zap.String("plan_hash", plan.PlanHash),
					zap.Int("child_number", plan.ChildNumber))

				fmt.Printf("   📝 Raw XML Plan Size: %d bytes\n", len(plan.PlanXML))
				fmt.Printf("   🎯 Plan preview: %s...\n", truncateString(plan.PlanXML, 100))

				successCount++

				// Record the execution plan
				eps.recordExecutionPlan(plan)
			} else {
				plan.Error = fmt.Errorf("execution plan is empty for query ID: %s", queryID)
				eps.logger.Warn("Execution plan content is empty", zap.String("query_id", queryID))
				fmt.Printf("   ⚠️  Execution plan content is empty for Query ID: %s\n", queryID)
				errorCount++
			}
		}

		executionPlans = append(executionPlans, plan)
		fmt.Printf("   ✅ Completed processing Query ID: %s\n\n", queryID)
	}

	result := &ExecutionPlanResult{
		Plans:        executionPlans,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
		TotalCount:   len(queryIDs),
	}

	eps.logger.Info("Execution plan retrieval completed",
		zap.Int("total_queries", result.TotalCount),
		zap.Int("successful_plans", result.SuccessCount),
		zap.Int("failed_plans", result.ErrorCount),
		zap.Float64("success_rate", float64(result.SuccessCount)/float64(result.TotalCount)*100))

	fmt.Printf("\n🏁 Execution plan retrieval completed!\n")
	fmt.Printf("📈 Results Summary:\n")
	fmt.Printf("   Total Queries: %d\n", result.TotalCount)
	fmt.Printf("   Successful Plans: %d\n", result.SuccessCount)
	fmt.Printf("   Failed Plans: %d\n", result.ErrorCount)
	if result.TotalCount > 0 {
		successRate := float64(result.SuccessCount) / float64(result.TotalCount) * 100
		fmt.Printf("   Success Rate: %.1f%%\n", successRate)
	}
	fmt.Printf("==========================================\n")

	eps.logExecutionPlanResults(result)
	return result
}

// GetExecutionPlansText fetches Oracle execution plans in human-readable text format
func (eps *ExecutionPlanScraper) GetExecutionPlansText(ctx context.Context, queryIDs []string) *ExecutionPlanResult {
	if len(queryIDs) == 0 {
		eps.logger.Debug("No query IDs provided for execution plan retrieval")
		return &ExecutionPlanResult{Plans: nil, SuccessCount: 0, ErrorCount: 0, TotalCount: 0}
	}

	eps.logger.Info("Fetching text execution plans for query IDs", zap.Int("count", len(queryIDs)))

	// Oracle SQL query to get execution plan in text format
	const executionPlanTextSQL = `
		SELECT 
			plan_table_output
		FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(:1, NULL, 'ALLSTATS LAST +OUTLINE +PROJECTION +ALIAS +PREDICATE +NOTE'))`

	var executionPlans []ExecutionPlan
	successCount := 0
	errorCount := 0

	for _, queryID := range queryIDs {
		plan := ExecutionPlan{QueryID: queryID}

		eps.logger.Debug("Fetching text execution plan", zap.String("query_id", queryID))

		rows, err := eps.db.QueryContext(ctx, executionPlanTextSQL, queryID)
		if err != nil {
			plan.Error = fmt.Errorf("failed to fetch text execution plan for query ID %s: %w", queryID, err)
			eps.logger.Error("Failed to fetch text execution plan", zap.String("query_id", queryID), zap.Error(err))
			errorCount++
			executionPlans = append(executionPlans, plan)
			continue
		}
		defer rows.Close()

		var planLines []string
		for rows.Next() {
			var planLine sql.NullString
			if err := rows.Scan(&planLine); err != nil {
				eps.logger.Error("Failed to scan plan line", zap.String("query_id", queryID), zap.Error(err))
				continue
			}
			if planLine.Valid {
				planLines = append(planLines, planLine.String)
			}
		}

		if err := rows.Err(); err != nil {
			plan.Error = fmt.Errorf("error reading plan rows for query ID %s: %w", queryID, err)
			errorCount++
		} else if len(planLines) > 0 {
			plan.PlanText = strings.Join(planLines, "\n")
			eps.logger.Debug("Successfully retrieved text execution plan",
				zap.String("query_id", queryID),
				zap.Int("plan_lines", len(planLines)))
			successCount++
		} else {
			plan.Error = fmt.Errorf("no execution plan data found for query ID: %s", queryID)
			eps.logger.Warn("No execution plan data found", zap.String("query_id", queryID))
			errorCount++
		}

		executionPlans = append(executionPlans, plan)
	}

	result := &ExecutionPlanResult{
		Plans:        executionPlans,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
		TotalCount:   len(queryIDs),
	}

	eps.logExecutionPlanResults(result)
	return result
}

// logExecutionPlanResults logs the execution plan results to console
func (eps *ExecutionPlanScraper) logExecutionPlanResults(result *ExecutionPlanResult) {
	fmt.Printf("\n=== Execution Plans Report ===\n")
	fmt.Printf("Total queries: %d\n", result.TotalCount)
	fmt.Printf("Successful: %d\n", result.SuccessCount)
	fmt.Printf("Errors: %d\n", result.ErrorCount)
	fmt.Printf("==============================\n")

	for _, plan := range result.Plans {
		if plan.Error != nil {
			fmt.Printf("❌ Query ID: %s\n", plan.QueryID)
			fmt.Printf("   Error: %s\n\n", plan.Error.Error())
		} else {
			fmt.Printf("✅ Query ID: %s\n", plan.QueryID)
			if plan.PlanHash != "" {
				fmt.Printf("   Plan Hash: %s\n", plan.PlanHash)
			}
			if plan.ChildNumber > 0 {
				fmt.Printf("   Child Number: %d\n", plan.ChildNumber)
			}
			if plan.PlanXML != "" {
				fmt.Printf("   XML Plan: %d characters\n", len(plan.PlanXML))
				// Show preview of XML
				eps.showXMLPreview(plan.PlanXML)
			}
			if plan.PlanText != "" {
				fmt.Printf("   Text Plan: %d lines\n", strings.Count(plan.PlanText, "\n")+1)
				// Show preview of text plan
				eps.showTextPreview(plan.PlanText)
			}
			fmt.Printf("\n")
		}
	}

	eps.logger.Info("Completed execution plan retrieval",
		zap.Int("total_plans", result.TotalCount),
		zap.Int("successful", result.SuccessCount),
		zap.Int("errors", result.ErrorCount))
}

// showXMLPreview shows a preview of the XML execution plan
func (eps *ExecutionPlanScraper) showXMLPreview(xmlPlan string) {
	lines := splitIntoLines(xmlPlan, 3)
	if len(lines) > 0 {
		fmt.Printf("   XML Preview:\n")
		for _, line := range lines {
			if strings.TrimSpace(line) != "" {
				fmt.Printf("   > %s\n", strings.TrimSpace(line))
			}
		}
		if len(xmlPlan) > 200 {
			fmt.Printf("   > ... (truncated)\n")
		}
	}
}

// showTextPreview shows a preview of the text execution plan
func (eps *ExecutionPlanScraper) showTextPreview(textPlan string) {
	lines := strings.Split(textPlan, "\n")
	fmt.Printf("   Text Preview:\n")

	// Show up to 5 meaningful lines (skip empty lines)
	shown := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && shown < 5 {
			fmt.Printf("   > %s\n", trimmed)
			shown++
		}
		if shown >= 5 {
			break
		}
	}

	if len(lines) > 5 {
		fmt.Printf("   > ... (%d more lines)\n", len(lines)-5)
	}
}

// Helper function to split text into lines and return the first n lines
func splitIntoLines(text string, maxLines int) []string {
	if text == "" {
		return nil
	}

	var lines []string
	currentLine := ""
	lineCount := 0

	for _, char := range text {
		if char == '\n' {
			if lineCount < maxLines {
				lines = append(lines, currentLine)
				lineCount++
			} else {
				break
			}
			currentLine = ""
		} else {
			currentLine += string(char)
		}
	}

	// Add the last line if we haven't reached the limit
	if lineCount < maxLines && currentLine != "" {
		lines = append(lines, currentLine)
	}

	return lines
}

// Helper function to truncate string for preview
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// recordExecutionPlan processes and records the execution plan data
func (eps *ExecutionPlanScraper) recordExecutionPlan(plan ExecutionPlan) {
	eps.logger.Info("Recording execution plan",
		zap.String("query_id", plan.QueryID),
		zap.String("plan_hash", plan.PlanHash),
		zap.Int("child_number", plan.ChildNumber))

	fmt.Printf("📊 Recording execution plan for Query ID: %s\n", plan.QueryID)

	// Here you can add your specific recording logic
	// For example, save to file, send to metrics, etc.

	if plan.PlanXML != "" {
		fmt.Printf("   📄 Raw XML Plan Content Available: %d characters\n", len(plan.PlanXML))

		// Show first few lines of raw XML for verification
		lines := strings.Split(plan.PlanXML, "\n")
		fmt.Printf("   🔍 Raw XML Preview (first 5 lines):\n")
		for i, line := range lines {
			if i >= 5 {
				break
			}
			if strings.TrimSpace(line) != "" {
				fmt.Printf("      %s\n", strings.TrimSpace(line))
			}
		}
		if len(lines) > 5 {
			fmt.Printf("      ... (%d more lines)\n", len(lines)-5)
		}
	}

	fmt.Printf("   ✅ Successfully recorded execution plan for Query ID: %s\n", plan.QueryID)
}

// GetPlanDetailsFromCache gets additional plan details from Oracle's plan cache
func (eps *ExecutionPlanScraper) GetPlanDetailsFromCache(ctx context.Context, queryIDs []string) map[string]map[string]interface{} {
	if len(queryIDs) == 0 {
		return nil
	}

	// Create placeholders for IN clause
	placeholders := make([]string, len(queryIDs))
	args := make([]interface{}, len(queryIDs))
	for i, id := range queryIDs {
		placeholders[i] = fmt.Sprintf(":arg%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			sql_id,
			child_number,
			plan_hash_value,
			executions,
			elapsed_time,
			cpu_time,
			disk_reads,
			buffer_gets,
			rows_processed,
			optimizer_cost,
			optimizer_mode,
			parsing_schema_name,
			module,
			action
		FROM v$sql 
		WHERE sql_id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := eps.db.QueryContext(ctx, query, args...)
	if err != nil {
		eps.logger.Error("Failed to fetch plan details from cache", zap.Error(err))
		return nil
	}
	defer rows.Close()

	result := make(map[string]map[string]interface{})

	for rows.Next() {
		var sqlID string
		var childNumber, planHashValue, executions, elapsedTime, cpuTime, diskReads, bufferGets, rowsProcessed, optimizerCost sql.NullInt64
		var optimizerMode, parsingSchemaName, module, action sql.NullString

		err := rows.Scan(
			&sqlID, &childNumber, &planHashValue, &executions, &elapsedTime,
			&cpuTime, &diskReads, &bufferGets, &rowsProcessed, &optimizerCost,
			&optimizerMode, &parsingSchemaName, &module, &action,
		)
		if err != nil {
			eps.logger.Error("Failed to scan plan details", zap.String("sql_id", sqlID), zap.Error(err))
			continue
		}

		planDetails := make(map[string]interface{})

		if childNumber.Valid {
			planDetails["child_number"] = childNumber.Int64
		}
		if planHashValue.Valid {
			planDetails["plan_hash_value"] = planHashValue.Int64
		}
		if executions.Valid {
			planDetails["executions"] = executions.Int64
		}
		if elapsedTime.Valid {
			planDetails["elapsed_time"] = elapsedTime.Int64
		}
		if cpuTime.Valid {
			planDetails["cpu_time"] = cpuTime.Int64
		}
		if diskReads.Valid {
			planDetails["disk_reads"] = diskReads.Int64
		}
		if bufferGets.Valid {
			planDetails["buffer_gets"] = bufferGets.Int64
		}
		if rowsProcessed.Valid {
			planDetails["rows_processed"] = rowsProcessed.Int64
		}
		if optimizerCost.Valid {
			planDetails["optimizer_cost"] = optimizerCost.Int64
		}
		if optimizerMode.Valid {
			planDetails["optimizer_mode"] = optimizerMode.String
		}
		if parsingSchemaName.Valid {
			planDetails["parsing_schema_name"] = parsingSchemaName.String
		}
		if module.Valid {
			planDetails["module"] = module.String
		}
		if action.Valid {
			planDetails["action"] = action.String
		}

		result[sqlID] = planDetails
	}

	fmt.Printf("\n=== Plan Cache Details ===\n")
	for sqlID, details := range result {
		fmt.Printf("Query ID: %s\n", sqlID)
		for key, value := range details {
			fmt.Printf("  %s: %v\n", key, value)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("=========================\n")

	return result
}
