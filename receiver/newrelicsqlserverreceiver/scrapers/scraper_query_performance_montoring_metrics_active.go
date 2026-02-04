// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// ScrapeActiveRunningQueriesMetrics fetches active running queries from SQL Server
// Returns the list of active queries for further processing (metrics emission and execution plan fetching)
func (s *QueryPerformanceScraper) ScrapeActiveRunningQueriesMetrics(ctx context.Context, limit, elapsedTimeThreshold int, slowQueryIDs []string) ([]models.ActiveRunningQuery, error) {
	// Skip active query scraping if no slow queries found (nothing to correlate)
	if len(slowQueryIDs) == 0 {
		s.logger.Info("No slow queries found, skipping active query scraping (nothing to correlate)")
		return nil, nil
	}

	// Build database filter for KEY/OBJECT lock resolution from monitored_databases
	dbFilter := ""
	if s.metadataCache != nil {
		monitoredDBs := s.metadataCache.GetMonitoredDatabases()
		if len(monitoredDBs) > 0 {
			// Build IN clause with properly escaped database names
			var quotedDBs []string
			for _, dbName := range monitoredDBs {
				// Escape single quotes by doubling them (SQL standard)
				escapedName := strings.ReplaceAll(dbName, "'", "''")
				quotedDBs = append(quotedDBs, fmt.Sprintf("'%s'", escapedName))
			}
			dbFilter = fmt.Sprintf(" AND name IN (%s)", strings.Join(quotedDBs, ", "))
		}
	}

	// Build query with slow query correlation filter
	queryIDFilter := "AND r_wait.query_hash IN (" + strings.Join(slowQueryIDs, ",") + ")"
	query := fmt.Sprintf(queries.ActiveRunningQueriesQuery, dbFilter, limit, elapsedTimeThreshold, queryIDFilter)

	s.logger.Debug("Executing active running queries fetch",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("limit", limit),
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

// processActiveRunningQueryMetricsWithPlan emits metrics for a single active running query
// Uses slow query plan_handle for consistency across all metrics and logs
func (s *QueryPerformanceScraper) processActiveRunningQueryMetricsWithPlan(result models.ActiveRunningQuery, index int, executionPlanXML string, slowQueryPlanHandle *models.QueryID) error {
	if result.CurrentSessionID == nil {
		s.logger.Debug("Skipping active running query with nil session ID", zap.Int("index", index))
		return nil
	}

	// Extract New Relic metadata and normalize SQL for cross-language correlation
	// This enables APM integration and query correlation across different language agents
	if result.QueryText != nil && *result.QueryText != "" {
		// Log the first 500 characters of query text to check for comments
		queryPreview := *result.QueryText
		if len(queryPreview) > 500 {
			queryPreview = queryPreview[:500] + "..."
		}

		hasNrGuidComment := strings.Contains(*result.QueryText, "nr_service_guid=")
		hasNrServiceComment := strings.Contains(*result.QueryText, "nr_service=")

		sessionIDStr := "unknown"
		if result.CurrentSessionID != nil {
			sessionIDStr = fmt.Sprintf("%d", *result.CurrentSessionID)
		}

		s.logger.Info("Processing active query text for APM metadata extraction",
			zap.String("session_id", sessionIDStr),
			zap.Int("query_text_length", len(*result.QueryText)),
			zap.Bool("has_nr_guid_comment", hasNrGuidComment),
			zap.Bool("has_nr_service_comment", hasNrServiceComment),
			zap.String("query_text_preview", queryPreview),
			zap.String("full_query_text", *result.QueryText))

		// Extract metadata from New Relic query comments (e.g., /* nr_apm_guid="ABC123", nr_service="order-service" */)
		nrApmGuid, clientName := helpers.ExtractNewRelicMetadata(*result.QueryText)

		s.logger.Info("Extracted APM metadata from active query text",
			zap.String("session_id", sessionIDStr),
			zap.String("extracted_nr_service_guid", nrApmGuid),
			zap.String("extracted_client_name", clientName),
			zap.Bool("extraction_successful", nrApmGuid != "" || clientName != ""))

		// Normalize SQL and generate MD5 hash for cross-language query correlation
		normalizedSQL, sqlHash := helpers.NormalizeSqlAndHash(*result.QueryText)

		s.logger.Debug("Normalized active query SQL",
			zap.String("session_id", sessionIDStr),
			zap.String("normalized_sql_hash", sqlHash),
			zap.Int("normalized_length", len(normalizedSQL)))

		// Populate model fields with extracted metadata
		if nrApmGuid != "" {
			result.NrServiceGuid = &nrApmGuid
		}
		if clientName != "" {
			result.ClientName = &clientName
		}
		if sqlHash != "" {
			result.NormalisedSqlHash = &sqlHash
		}

		// Cache APM metadata for slow query enrichment
		// This allows slow queries (from plan cache) to be enriched with APM correlation data
		// captured from active running queries (which have the APM comments)
		if result.QueryID != nil && !result.QueryID.IsEmpty() && (nrApmGuid != "" || clientName != "" || sqlHash != "") {
			queryHashStr := result.QueryID.String()
			s.apmMetadataCache.Set(queryHashStr, nrApmGuid, clientName, sqlHash)
			s.logger.Debug("Cached APM metadata from active query",
				zap.String("query_hash", queryHashStr),
				zap.String("nr_service_guid", nrApmGuid),
				zap.String("client_name", clientName),
				zap.String("normalised_sql_hash", sqlHash))
		}

		// Replace QueryText with normalized version for privacy and consistency
		// This removes literals while preserving query structure
		result.QueryText = &normalizedSQL
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Define getter functions for all attributes based on the actual ActiveRunningQuery model
	// Only including fields that exist in the simplified ActiveRunningQuery struct
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
			// Truncate to 4095 characters first (before anonymization for efficiency)
			truncatedText := *result.BlockingQueryStatementText
			if len(truncatedText) > 4095 {
				truncatedText = truncatedText[:4095]
			}
			// Then anonymize only what we're sending
			return helpers.AnonymizeQueryText(truncatedText)
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
	getWaitTypeDescription := func() string {
		waitType := getWaitType()
		if waitType == "" {
			waitType = "N/A"
		}
		description := helpers.DecodeWaitType(waitType)
		if description == "" {
			return waitType
		}
		return description
	}
	getWaitTypeCategory := func() string {
		waitType := getWaitType()
		if waitType == "" {
			waitType = "N/A"
		}
		category := helpers.GetWaitTypeCategory(waitType)
		if category == "" {
			return "Other"
		}
		return category
	}
	getWaitResourceType := func() string {
		if result.WaitResource != nil {
			resourceType, _ := helpers.DecodeWaitResource(*result.WaitResource)
			return resourceType
		}
		return ""
	}
	getLastWaitTypeDescription := func() string {
		if result.LastWaitType != nil {
			return helpers.DecodeWaitType(*result.LastWaitType)
		}
		return ""
	}
	getClientName := func() string {
		if result.ClientName != nil {
			return *result.ClientName
		}
		return ""
	}
	getQueryText := func() string {
		if result.QueryText != nil {
			// Truncate to 4095 characters first (before anonymization for efficiency)
			truncatedText := *result.QueryText
			if len(truncatedText) > 4095 {
				truncatedText = truncatedText[:4095]
			}
			// Then anonymize only what we're sending
			return helpers.AnonymizeQueryText(truncatedText)
		}
		return ""
	}
	getNormalisedSqlHash := func() string {
		if result.NormalisedSqlHash != nil {
			return *result.NormalisedSqlHash
		}
		return ""
	}
	getNrServiceGuid := func() string {
		if result.NrServiceGuid != nil {
			return *result.NrServiceGuid
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
			getClientName(),
			getQueryID(),
			getQueryText(),
			getNormalisedSqlHash(),
			getNrServiceGuid(),
			getWaitType(),
			getWaitTypeDescription(),
			getWaitTypeCategory(),
			getWaitResource(),
			getWaitResourceType(),
			getWaitResourceObjectName(),
			getLastWaitType(),
			getLastWaitTypeDescription(),
			getRequestStartTime(),
			getCollectionTimestamp(),
			getTransactionID(),
			getOpenTransactionCount(),
			getPlanHandle(),
			getBlockingSessionID(),
			getBlockingLoginName(),
			getBlockingQueryText(),
			getBlockingQueryHash(),
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

	if len(results) == 0 {
		s.logger.Warn("No execution plan found in database - plan evicted from cache or invalid plan_handle",
			zap.String("plan_handle", planHandleHex))
		return "", nil
	}

	// Defensive check (should never happen due to WHERE clause, but safety first)
	if results[0].ExecutionPlanXML == nil {
		s.logger.Warn("Execution plan XML is NULL (unexpected - WHERE clause should filter this)",
			zap.String("plan_handle", planHandleHex))
		return "", nil
	}

	xmlLength := len(*results[0].ExecutionPlanXML)
	s.logger.Info("Successfully fetched execution plan XML",
		zap.String("plan_handle", planHandleHex),
		zap.Int("xml_length_bytes", xmlLength))

	return *results[0].ExecutionPlanXML, nil
}

// REMOVED: Old logs-based execution plan functions (EmitActiveRunningExecutionPlansAsLogs, parseAndEmitExecutionPlanAsLogs)
// Execution plans now emitted as sqlserver.execution.plan metrics, converted to logs via metricsaslogs connector.

// REMOVED: Legacy execution plan functions (fetchTop5PlanHandlesForActiveQuery, emitAggregatedExecutionPlanAsMetrics)
// Replaced by ScrapeSlowQueryExecutionPlans in scraper_query_performance_montoring_metrics.go
