// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// ScrapeActiveRunningQueriesMetrics fetches active running queries from SQL Server
// Returns the list of active queries for further processing (metrics emission and execution plan fetching)
func (s *QueryPerformanceScraper) ScrapeActiveRunningQueriesMetrics(ctx context.Context, limit, textTruncateLimit, elapsedTimeThreshold int, slowQueryIDs []string) ([]models.ActiveRunningQuery, error) {
	// Skip active query scraping if no slow queries found (nothing to correlate)
	if len(slowQueryIDs) == 0 {
		s.logger.Info("No slow queries found, skipping active query scraping (nothing to correlate)")
		return nil, nil
	}

	// Build database filter (empty string = all databases)
	dbFilter := "" // TODO: Add config option for monitored_databases filter
	
	// Build query with slow query correlation filter
	queryIDFilter := "AND r_wait.query_hash IN (" + strings.Join(slowQueryIDs, ",") + ")"
	query := fmt.Sprintf(queries.ActiveRunningQueriesQuery, dbFilter, limit, textTruncateLimit, elapsedTimeThreshold, queryIDFilter)

	s.logger.Debug("Executing active running queries fetch",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("limit", limit),
		zap.Int("text_truncate_limit", textTruncateLimit),
		zap.Int("elapsed_time_threshold_ms", elapsedTimeThreshold),
		zap.Int("slow_query_id_count", len(slowQueryIDs)))

	var results []models.ActiveRunningQuery
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return nil, fmt.Errorf("failed to execute active running queries query: %w", err)
	}

	s.logger.Info("Active running queries fetched from database",
		zap.Int("result_count", len(results)))

	return results, nil
}

// EmitActiveRunningQueriesMetrics emits metrics for active running queries
// This processes the active queries and emits metrics (no execution plans)
func (s *QueryPerformanceScraper) EmitActiveRunningQueriesMetrics(ctx context.Context, activeQueries []models.ActiveRunningQuery, slowQueryPlanDataMap map[string]models.SlowQueryPlanData) error {
	if len(activeQueries) == 0 {
		s.logger.Info("No active queries to emit metrics for")
		return nil
	}

	filteredCount := 0
	processedCount := 0
	skippedNoSlowQueryMatch := 0

	for i, result := range activeQueries {
		// Defensive checks for required fields
		if result.WaitType == nil || *result.WaitType == "" {
			filteredCount++
			s.logger.Warn("Active query has NULL/empty wait_type, skipping metric emission",
				zap.Any("session_id", result.CurrentSessionID))
			continue
		}

		if result.QueryID == nil || result.QueryID.IsEmpty() {
			filteredCount++
			s.logger.Warn("Active query has NULL/empty query_id, skipping metric emission",
				zap.Any("session_id", result.CurrentSessionID))
			continue
		}

		// Get plan_handle from lightweight plan data using query_id
		var slowQueryPlanHandle *models.QueryID
		if result.QueryID != nil && !result.QueryID.IsEmpty() {
			queryIDStr := result.QueryID.String()
			if planData, found := slowQueryPlanDataMap[queryIDStr]; found {
				slowQueryPlanHandle = planData.PlanHandle
			}
		}

		// Skip if no matching slow query plan_handle found
		if slowQueryPlanHandle == nil || slowQueryPlanHandle.IsEmpty() {
			skippedNoSlowQueryMatch++
			s.logger.Debug("No matching slow query plan_handle found for active query, skipping metric emission",
				zap.Any("session_id", result.CurrentSessionID),
				zap.Any("query_id", result.QueryID))
			continue
		}

		processedCount++

		// Emit metrics for this active query (no execution plan XML) using slow query plan_handle
		if err := s.processActiveRunningQueryMetricsWithPlan(result, i, "", slowQueryPlanHandle); err != nil {
			s.logger.Error("Failed to emit active running query metrics", zap.Error(err), zap.Int("index", i))
		}
	}

	s.logger.Info("Active running queries metrics emission complete",
		zap.Int("total_queries", len(activeQueries)),
		zap.Int("filtered_out", filteredCount),
		zap.Int("skipped_no_slow_query_match", skippedNoSlowQueryMatch),
		zap.Int("metrics_emitted", processedCount))

	return nil
}

// EmitActiveRunningExecutionPlansAsLogs fetches execution plans for active queries and emits as OTLP logs
// This is called from ScrapeLogs to emit execution plans with slow query correlation
// Uses plan_handle from slow queries instead of active query plan_handle
func (s *QueryPerformanceScraper) EmitActiveRunningExecutionPlansAsLogs(ctx context.Context, logs plog.Logs, activeQueries []models.ActiveRunningQuery, slowQueryPlanDataMap map[string]models.SlowQueryPlanData) error {
	if len(activeQueries) == 0 {
		s.logger.Info("No active queries to fetch execution plans for")
		return nil
	}

	fetchedCount := 0
	errorCount := 0
	skippedNoSlowQueryMatch := 0

	for i, result := range activeQueries {
		// Get plan_handle from lightweight plan data using query_id
		// This ensures we only show plans for queries that exist in dm_exec_query_stats
		var planHandle *models.QueryID
		var queryIDStr string
		if result.QueryID != nil && !result.QueryID.IsEmpty() {
			queryIDStr = result.QueryID.String()
			if planData, found := slowQueryPlanDataMap[queryIDStr]; found {
				planHandle = planData.PlanHandle
			}
			s.logger.Info(fmt.Sprintf("[%d/%d] Processing active query for execution plan", i+1, len(activeQueries)),
				zap.Any("session_id", result.CurrentSessionID),
				zap.String("query_hash", queryIDStr),
				zap.Bool("found_in_slow_query_map", planHandle != nil),
				zap.String("plan_handle", func() string {
					if planHandle != nil {
						return planHandle.String()
					}
					return "NULL"
				}()))
		} else {
			s.logger.Warn("Active query has NULL/empty query_hash - cannot correlate with slow query",
				zap.Any("session_id", result.CurrentSessionID))
		}

		// Skip if no matching slow query plan_handle found
		// The warmup phase ensures all operations are cached in dm_exec_query_stats first
		if planHandle == nil || planHandle.IsEmpty() {
			skippedNoSlowQueryMatch++
			s.logger.Warn("No matching slow query plan_handle found for active query, skipping execution plan fetch",
				zap.Any("session_id", result.CurrentSessionID),
				zap.Any("query_id", result.QueryID),
				zap.String("query_hash", queryIDStr),
				zap.Int("plan_data_map_size", len(slowQueryPlanDataMap)))
			continue
		}

		s.logger.Debug("Fetching execution plan XML for active query",
			zap.Any("session_id", result.CurrentSessionID),
			zap.String("plan_handle", planHandle.String()))

		// Fetch execution plan XML using plan_handle from slow query
		executionPlanXML, err := s.fetchExecutionPlanXML(ctx, planHandle)
		if err != nil {
			errorCount++
			s.logger.Error("Failed to fetch execution plan XML for active query",
				zap.Error(err),
				zap.Any("session_id", result.CurrentSessionID),
				zap.String("plan_handle", planHandle.String()))
			continue
		}

		if executionPlanXML == "" {
			s.logger.Warn("Empty execution plan XML returned for active query (plan evicted or not available)",
				zap.Any("session_id", result.CurrentSessionID),
				zap.String("plan_handle", planHandle.String()),
				zap.String("query_hash", queryIDStr))
			continue
		}

		s.logger.Debug("Successfully fetched execution plan XML",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int("xml_length", len(executionPlanXML)))

		// Parse and emit execution plan as logs (pass the slow query plan_handle we used to fetch XML)
		if err := s.parseAndEmitExecutionPlanAsLogs(ctx, result, executionPlanXML, planHandle, logs); err != nil {
			errorCount++
			s.logger.Error("Failed to parse and emit execution plan as logs",
				zap.Error(err),
				zap.Any("session_id", result.CurrentSessionID),
				zap.Int("index", i))
			continue
		}

		fetchedCount++
	}

	s.logger.Info("Execution plans emission complete",
		zap.Int("total_active_queries", len(activeQueries)),
		zap.Int("plans_successfully_fetched_and_emitted", fetchedCount),
		zap.Int("errors_during_fetch_or_parse", errorCount),
		zap.Int("skipped_no_slow_query_match", skippedNoSlowQueryMatch),
		zap.Int("plan_data_map_size", len(slowQueryPlanDataMap)))

	// Warning if no execution plans were emitted
	if fetchedCount == 0 && len(activeQueries) > 0 {
		s.logger.Warn("⚠️  NO EXECUTION PLANS EMITTED - Check logs above for reasons:",
			zap.Int("active_queries_found", len(activeQueries)),
			zap.Int("skipped_no_plan_handle_match", skippedNoSlowQueryMatch),
			zap.Int("errors", errorCount))
	}

	return nil
}

func (s *QueryPerformanceScraper) processActiveRunningQueryMetricsWithPlan(result models.ActiveRunningQuery, index int, executionPlanXML string, slowQueryPlanHandle *models.QueryID) error {
	if result.CurrentSessionID == nil {
		s.logger.Debug("Skipping active running query with nil session ID", zap.Int("index", index))
		return nil
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Helper functions for attribute extraction
	getSessionID := func() int64 {
		if result.CurrentSessionID != nil {
			return *result.CurrentSessionID
		}
		return 0
	}
	getRequestID := func() int64 {
		if result.RequestID != nil {
			return *result.RequestID
		}
		return 0
	}
	getDatabaseName := func() string {
		if result.DatabaseName != nil {
			return *result.DatabaseName
		}
		return ""
	}
	getLoginName := func() string {
		if result.LoginName != nil {
			return *result.LoginName
		}
		return ""
	}
	getHostName := func() string {
		if result.HostName != nil {
			return *result.HostName
		}
		return ""
	}
	// Stub functions for deleted fields (return empty values)
	getProgramName := func() string { return "" }
	getRequestCommand := func() string { return "" }
	getRequestStatus := func() string { return "" }
	getSessionStatus := func() string { return "" }
	getClientInterfaceName := func() string { return "" }
	getWaitResourceDatabaseName := func() string { return "" }
	getWaitResourceSchemaName := func() string { return "" }
	getWaitResourceObjectType := func() string { return "" }
	getWaitResourceIndexName := func() string { return "" }
	getWaitResourceIndexType := func() string { return "" }
	getTransactionIsolationLevel := func() int64 { return 0 }
	getDegreeOfParallelism := func() int64 { return 0 }
	getParallelWorkerCount := func() int64 { return 0 }
	getBlockingHostName := func() string { return "" }
	getBlockingProgramName := func() string { return "" }
	getBlockingStatus := func() string { return "" }
	getBlockingIsolationLevel := func() int64 { return 0 }
	getBlockingOpenTransactionCount := func() int64 { return 0 }

	getWaitType := func() string {
		if result.WaitType != nil {
			return *result.WaitType
		}
		return ""
	}
	getWaitResource := func() string {
		if result.WaitResource != nil {
			return *result.WaitResource
		}
		return ""
	}
	getWaitResourceObjectName := func() string {
		if result.WaitResourceObjectName != nil {
			return *result.WaitResourceObjectName
		}
		return ""
	}

	getLastWaitType := func() string {
		if result.LastWaitType != nil {
			return *result.LastWaitType
		}
		return ""
	}
	getRequestStartTime := func() string {
		if result.RequestStartTime != nil {
			return *result.RequestStartTime
		}
		return ""
	}
	getCollectionTimestamp := func() string {
		if result.CollectionTimestamp != nil {
			return *result.CollectionTimestamp
		}
		return ""
	}
	getTransactionID := func() int64 {
		if result.TransactionID != nil {
			return *result.TransactionID
		}
		return 0
	}
	getOpenTransactionCount := func() int64 {
		if result.OpenTransactionCount != nil {
			return *result.OpenTransactionCount
		}
		return 0
	}

	getBlockingSessionID := func() int64 {
		if result.BlockingSessionID != nil {
			return *result.BlockingSessionID
		}
		return 0
	}
	getBlockingLoginName := func() string {
		if result.BlockerLoginName != nil {
			return *result.BlockerLoginName
		}
		return ""
	}

	getBlockingQueryHash := func() string {
		if result.BlockingQueryHash != nil && !result.BlockingQueryHash.IsEmpty() {
			return result.BlockingQueryHash.String()
		}
		return ""
	}
	getBlockingQueryText := func() string {
		if result.BlockingQueryStatementText != nil {
			return *result.BlockingQueryStatementText
		}
		return ""
	}
	getQueryText := func() string {
		if result.QueryStatementText != nil {
			return *result.QueryStatementText
		}
		return ""
	}
	getQueryID := func() string {
		if result.QueryID != nil && !result.QueryID.IsEmpty() {
			return result.QueryID.String()
		}
		return ""
	}
	getPlanHandle := func() string {
		// Use slow query plan_handle for consistency across all metrics and logs
		if slowQueryPlanHandle != nil && !slowQueryPlanHandle.IsEmpty() {
			return slowQueryPlanHandle.String()
		}
		return ""
	}
	// getWaitTypeDescription returns human-readable wait event name (never blank)
	// Falls back to raw wait_type if description is unavailable
	getWaitTypeDescription := func() string {
		waitType := getWaitType()
		if waitType == "" {
			waitType = "N/A"
		}
		description := helpers.DecodeWaitType(waitType)
		// DecodeWaitType already handles empty/N/A and returns "Not Waiting"
		// It also returns the original waitType for unknown types
		// So this should never be blank, but we double-check
		if description == "" {
			return waitType
		}
		return description
	}
	// getWaitTypeCategory returns wait category (never blank)
	// Falls back to "Other" if category cannot be determined
	getWaitTypeCategory := func() string {
		waitType := getWaitType()
		if waitType == "" {
			waitType = "N/A"
		}
		category := helpers.GetWaitTypeCategory(waitType)
		// GetWaitTypeCategory returns "None" for empty/N/A and "Other" for unknown
		// So this should never be blank, but we double-check
		if category == "" {
			return "Other"
		}
		return category
	}
	getWaitResourceType := func() string {
		// This would be computed from wait_resource parsing
		if result.WaitResource != nil {
			resourceType, _ := helpers.DecodeWaitResource(*result.WaitResource)
			return resourceType
		}
		return ""
	}
	getWaitResourceDescription := func() string {
		// This would be computed with enhanced logic from addActiveQueryAttributes
		if result.WaitResource != nil {
			_, resourceDesc := helpers.DecodeWaitResource(*result.WaitResource)
			return resourceDesc
		}
		return ""
	}
	getWaitResourceTableName := func() string {
		if result.WaitResourceObjectName != nil {
			return *result.WaitResourceObjectName
		}
		return ""
	}
	getLastWaitTypeDescription := func() string {
		if result.LastWaitType != nil {
			return helpers.DecodeWaitType(*result.LastWaitType)
		}
		return ""
	}

	// Active query wait time
	if result.WaitTimeS != nil && *result.WaitTimeS > 0 {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.wait_time_seconds",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Float64("value", *result.WaitTimeS),
			zap.Any("wait_type", result.WaitType),
			zap.Any("database_name", result.DatabaseName))

		s.mb.RecordSqlserverActivequeryWaitTimeSecondsDataPoint(
			timestamp,
			*result.WaitTimeS,
			getSessionID(),
			getRequestID(),
			getDatabaseName(),
			getLoginName(),
			getHostName(),
			getProgramName(),
			getRequestCommand(),
			getRequestStatus(),
			getSessionStatus(),
			getClientInterfaceName(),
			getWaitType(),
			getWaitTypeDescription(),
			getWaitTypeCategory(),
			getWaitResource(),
			getWaitResourceObjectName(),
			getWaitResourceDatabaseName(),
			getWaitResourceType(),
			getWaitResourceDescription(),
			getWaitResourceSchemaName(),
			getWaitResourceTableName(),
			getWaitResourceObjectType(),
			getWaitResourceIndexName(),
			getWaitResourceIndexType(),
			getLastWaitType(),
			getLastWaitTypeDescription(),
			getRequestStartTime(),
			getCollectionTimestamp(),
			getTransactionID(),
			getOpenTransactionCount(),
			getTransactionIsolationLevel(),
			getDegreeOfParallelism(),
			getParallelWorkerCount(),
			getBlockingHostName(),
			getBlockingIsolationLevel(),
			getBlockingLoginName(),
			getBlockingOpenTransactionCount(),
			getBlockingProgramName(),
			getBlockingQueryHash(),
			getBlockingQueryText(),
			getBlockingSessionID(),
			getBlockingStatus(),
			getQueryText(),
			getQueryID(),
			getPlanHandle(),
		)
	} else {
		s.logger.Warn("❌ SKIPPED wait_time metric (wait_time_s <= 0 or nil)",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Any("wait_time_s", result.WaitTimeS),
			zap.Any("wait_type", result.WaitType))
	}

	return nil
}

// fetchExecutionPlanXML fetches the execution plan XML for a given plan_handle
// Simple wrapper for use by logs endpoint
func (s *QueryPerformanceScraper) fetchExecutionPlanXML(ctx context.Context, planHandle *models.QueryID) (string, error) {
	if planHandle == nil || planHandle.IsEmpty() {
		s.logger.Warn("fetchExecutionPlanXML called with NULL/empty plan_handle")
		return "", nil
	}

	planHandleHex := planHandle.String()
	query := fmt.Sprintf(queries.ActiveQueryExecutionPlanQuery, planHandleHex)

	s.logger.Debug("Fetching execution plan XML from sys.dm_exec_query_plan",
		zap.String("plan_handle", planHandleHex),
		zap.String("query", query))

	var results []struct {
		ExecutionPlanXML *string `db:"execution_plan_xml"`
	}

	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("SQL query failed when fetching execution plan XML",
			zap.Error(err),
			zap.String("plan_handle", planHandleHex),
			zap.String("query", query))
		return "", fmt.Errorf("failed to fetch execution plan: %w", err)
	}

	s.logger.Debug("Execution plan query returned",
		zap.Int("result_count", len(results)),
		zap.Bool("xml_is_null", len(results) == 0 || results[0].ExecutionPlanXML == nil))

	if len(results) == 0 {
		s.logger.Warn("sys.dm_exec_query_plan returned 0 rows (plan not in cache or invalid plan_handle)",
			zap.String("plan_handle", planHandleHex))
		return "", nil
	}

	if results[0].ExecutionPlanXML == nil {
		s.logger.Warn("sys.dm_exec_query_plan returned NULL for query_plan (plan evicted from cache or not available)",
			zap.String("plan_handle", planHandleHex))
		return "", nil
	}

	xmlLength := len(*results[0].ExecutionPlanXML)
	s.logger.Info("Successfully fetched execution plan XML",
		zap.String("plan_handle", planHandleHex),
		zap.Int("xml_length_bytes", xmlLength))

	return *results[0].ExecutionPlanXML, nil
}

// parseAndEmitExecutionPlanAsLogs parses execution plan XML and emits as OTLP logs
// Converts XML execution plan to custom events using attributes (not JSON body)
func (s *QueryPerformanceScraper) parseAndEmitExecutionPlanAsLogs(
	ctx context.Context,
	activeQuery models.ActiveRunningQuery,
	executionPlanXML string,
	slowQueryPlanHandle *models.QueryID,
	logs plog.Logs,
) error {
	// Build execution plan analysis with metadata
	var queryID string
	if activeQuery.QueryID != nil {
		queryID = activeQuery.QueryID.String()
	}

	// Use the slow query plan_handle (this is what we used to fetch the XML)
	// NOT the active query's plan_handle
	var planHandle string
	if slowQueryPlanHandle != nil && !slowQueryPlanHandle.IsEmpty() {
		planHandle = slowQueryPlanHandle.String()
	}

	// Parse the XML execution plan
	analysis, err := models.ParseExecutionPlanXML(executionPlanXML, queryID, planHandle)
	if err != nil {
		return fmt.Errorf("failed to parse execution plan XML: %w", err)
	}

	// Emit as OTLP logs
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Emit one log record per operator node using ATTRIBUTES (not JSON body)
	for i := range analysis.Nodes {
		node := &analysis.Nodes[i]
		logRecord := scopeLogs.LogRecords().AppendEmpty()
		logRecord.SetTimestamp(timestamp)
		logRecord.SetObservedTimestamp(timestamp)
		logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
		logRecord.SetSeverityText("INFO")

		// Set body as simple string (not JSON)
		logRecord.Body().SetStr(fmt.Sprintf("Execution Plan Node: %s (NodeID=%d, Parent=%d)",
			node.PhysicalOp, node.NodeID, node.ParentNodeID))

		// Set ALL fields as ATTRIBUTES (this is what makes it a proper custom event)
		attrs := logRecord.Attributes()

		// IMPORTANT: Signal New Relic to ingest this as a Custom Event
		attrs.PutStr("newrelic.event.type", "SqlServerActiveQueryExecutionPlan")

		// Correlation keys
		attrs.PutStr("query_id", node.QueryID)
		attrs.PutStr("plan_handle", node.PlanHandle)
		if activeQuery.CurrentSessionID != nil {
			attrs.PutInt("session_id", *activeQuery.CurrentSessionID)
		}
		if activeQuery.RequestID != nil {
			attrs.PutInt("request_id", *activeQuery.RequestID)
		}
		if activeQuery.DatabaseName != nil {
			attrs.PutStr("database_name", *activeQuery.DatabaseName)
		}
		if activeQuery.RequestStartTime != nil {
			attrs.PutStr("start_time", *activeQuery.RequestStartTime)
		}

		// Node structure
		attrs.PutInt("node_id", int64(node.NodeID))
		attrs.PutInt("parent_node_id", int64(node.ParentNodeID))
		attrs.PutStr("input_type", node.InputType)

		// Operator information
		attrs.PutStr("physical_op", node.PhysicalOp)
		attrs.PutStr("logical_op", node.LogicalOp)
		attrs.PutStr("sql_text", node.SQLText)

		// Object information (for Index Scan/Seek operators)
		if node.SchemaName != "" {
			attrs.PutStr("schema_name", node.SchemaName)
		}
		if node.TableName != "" {
			attrs.PutStr("table_name", node.TableName)
		}
		if node.IndexName != "" {
			attrs.PutStr("index_name", node.IndexName)
		}
		if node.ReferencedColumns != "" {
			attrs.PutStr("referenced_columns", node.ReferencedColumns)
		}

		// Cost estimates
		attrs.PutDouble("estimate_rows", node.EstimateRows)
		attrs.PutDouble("estimate_io", node.EstimateIO)
		attrs.PutDouble("estimate_cpu", node.EstimateCPU)
		attrs.PutDouble("avg_row_size", node.AvgRowSize)
		attrs.PutDouble("total_subtree_cost", node.TotalSubtreeCost)
		attrs.PutDouble("estimated_operator_cost", node.EstimatedOperatorCost)

		// Execution details
		attrs.PutStr("estimated_execution_mode", node.EstimatedExecutionMode)
		attrs.PutInt("granted_memory_kb", node.GrantedMemoryKb)
		attrs.PutBool("spill_occurred", node.SpillOccurred)
		attrs.PutBool("no_join_predicate", node.NoJoinPredicate)

		// Performance metrics
		attrs.PutDouble("total_worker_time", node.TotalWorkerTime)
		attrs.PutDouble("total_elapsed_time", node.TotalElapsedTime)
		attrs.PutInt("total_logical_reads", node.TotalLogicalReads)
		attrs.PutInt("total_logical_writes", node.TotalLogicalWrites)
		attrs.PutInt("execution_count", node.ExecutionCount)
		attrs.PutDouble("avg_elapsed_time_ms", node.AvgElapsedTimeMs)

		// Timestamps
		if node.LastExecutionTime != "" {
			attrs.PutStr("last_execution_time", node.LastExecutionTime)
		}
	}

	s.logger.Info("Emitted execution plan as OTLP logs with attributes",
		zap.String("query_id", queryID),
		zap.String("plan_handle", planHandle),
		zap.Int("operator_count", len(analysis.Nodes)))

	return nil
}

// REMOVED (2025-11-28): Old execution plan functions that created node-level custom events
// - emitActiveQueryExecutionPlanLogs() - created one SqlServerActiveQueryExecutionPlan event per operator node
// - createActiveQueryExecutionPlanNodeLog() - helper function for node-level events
// - emitActiveQueryExecutionPlanMetrics() - created summary metrics from parsed XML
// These have been replaced with aggregated stats approach using dm_exec_query_stats

// REMOVED: Old execution plan fetching functions for active queries
// - fetchTop5PlanHandlesForActiveQuery
// - emitAggregatedExecutionPlanAsMetrics
// - createAggregatedExecutionPlanMetrics
//
// These functions have been replaced by the new approach in scraper_query_performance_montoring_metrics.go:
// - ScrapeSlowQueryExecutionPlans: Fetches plans for ALL slow queries (not just active ones)
// - emitActiveQueryPlanMetrics: Emits metrics with active query correlation (session_id, request_id, request_start_time)
//
// Benefits of the new approach:
// 1. Plans available even when query is NOT currently running
// 2. No duplicate fetches per active execution (fetched once per unique query_id)
// 3. Clean historical context without misleading active session attributes
