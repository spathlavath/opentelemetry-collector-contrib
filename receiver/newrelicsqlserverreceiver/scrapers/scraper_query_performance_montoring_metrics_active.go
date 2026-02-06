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
	// Log whether we have slow query IDs for correlation (but continue scraping regardless)
	if len(slowQueryIDs) == 0 {
		s.logger.Info("No slow queries found, active queries will be scraped without plan data correlation")
	} else {
		s.logger.Info("Scraping active queries with slow query correlation",
			zap.Int("slow_query_count", len(slowQueryIDs)))
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
	queryIDFilter := ""
	if len(slowQueryIDs) > 0 {
		queryIDFilter = "AND r_wait.query_hash IN (" + strings.Join(slowQueryIDs, ",") + ")"
	}
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
func (s *QueryPerformanceScraper) EmitActiveRunningQueriesMetrics(ctx context.Context, activeQueries []models.ActiveRunningQuery, slowQueryPlanDataMap map[string]models.SlowQueryPlanData, apmMetadataCache *helpers.APMMetadataCache) error {
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
		if err := s.processActiveRunningQueryMetricsWithPlan(result, i, "", slowQueryPlanHandle, apmMetadataCache); err != nil {
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
func (s *QueryPerformanceScraper) processActiveRunningQueryMetricsWithPlan(result models.ActiveRunningQuery, index int, executionPlanXML string, slowQueryPlanHandle *models.QueryID, apmMetadataCache *helpers.APMMetadataCache) error {
	if result.CurrentSessionID == nil {
		s.logger.Debug("Skipping active running query with nil session ID", zap.Int("index", index))
		return nil
	}

	// Extract New Relic metadata and normalize SQL for cross-language correlation
	// This enables APM integration and query correlation across different language agents
	var nrApmGuid, sqlHash, normalizedSQL string
	var blockingNrApmGuid string

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

		s.logger.Info("🔍 ACTIVE QUERY: Processing query text from dm_exec_requests",
			zap.String("session_id", sessionIDStr),
			zap.Int("query_text_length", len(*result.QueryText)),
			zap.Bool("has_nr_guid_comment", hasNrGuidComment),
			zap.Bool("has_nr_service_comment", hasNrServiceComment),
			zap.String("query_text_preview", queryPreview))

		// Log full query text at debug level
		s.logger.Debug("ACTIVE QUERY: Full query text",
			zap.String("session_id", sessionIDStr),
			zap.String("full_query_text", *result.QueryText))

		// Extract metadata from New Relic query comments (e.g., /* nr_apm_guid="ABC123", nr_service="order-service" */)
		nrApmGuid, _ = helpers.ExtractNewRelicMetadata(*result.QueryText)

		s.logger.Info("🏷️  ACTIVE QUERY: Extracted APM metadata from query text",
			zap.String("session_id", sessionIDStr),
			zap.String("extracted_nr_service_guid", nrApmGuid),
			zap.Bool("extraction_successful", nrApmGuid != ""))

		// Normalize SQL and generate MD5 hash for cross-language query correlation
		normalizedSQL, sqlHash = helpers.NormalizeSqlAndHash(*result.QueryText)

		s.logger.Info("🔐 ACTIVE QUERY: Normalized SQL and generated hash",
			zap.String("session_id", sessionIDStr),
			zap.String("normalized_sql_hash", sqlHash),
			zap.Int("normalized_length", len(normalizedSQL)),
			zap.String("normalized_sql_preview", func() string {
				if len(normalizedSQL) > 200 {
					return normalizedSQL[:200] + "..."
				}
				return normalizedSQL
			}()))

		// Replace QueryText with normalized version for privacy and consistency
		// This removes literals while preserving query structure
		result.QueryText = &normalizedSQL
	}

	// Try to enrich from cache if metadata not found in query text
	// This handles cases where:
	// 1. Query text is missing NR comments (e.g., historical queries from plan cache)
	// 2. Active query captured before APM agent injected comments
	// 3. Another execution of same query had metadata earlier in this scrape
	if (nrApmGuid == "" || sqlHash == "") && result.QueryID != nil && !result.QueryID.IsEmpty() && apmMetadataCache != nil {
		queryHashStr := result.QueryID.String()

		sessionIDStr := "unknown"
		if result.CurrentSessionID != nil {
			sessionIDStr = fmt.Sprintf("%d", *result.CurrentSessionID)
		}

		s.logger.Info("💾 ACTIVE QUERY: Checking cache for missing metadata",
			zap.String("session_id", sessionIDStr),
			zap.String("query_hash", queryHashStr),
			zap.Bool("missing_nr_service_guid", nrApmGuid == ""),
			zap.Bool("missing_sql_hash", sqlHash == ""))

		if cachedMetadata, found := apmMetadataCache.Get(queryHashStr); found {
			s.logger.Info("✅ ACTIVE QUERY: Enriching with cached APM metadata",
				zap.String("session_id", sessionIDStr),
				zap.String("query_hash", queryHashStr),
				zap.String("cached_nr_service_guid", cachedMetadata.NrServiceGuid),
				zap.String("cached_normalized_sql_hash", cachedMetadata.NormalisedSqlHash))

			// Use cached values if current extraction didn't find them
			if nrApmGuid == "" && cachedMetadata.NrServiceGuid != "" {
				nrApmGuid = cachedMetadata.NrServiceGuid
			}
			if sqlHash == "" && cachedMetadata.NormalisedSqlHash != "" {
				sqlHash = cachedMetadata.NormalisedSqlHash
			}
		}
	}

	// Populate model fields with extracted or cached metadata
	if nrApmGuid != "" {
		result.NrServiceGuid = &nrApmGuid
	}
	if sqlHash != "" {
		result.NormalisedSqlHash = &sqlHash
	}

	// Extract New Relic metadata from BLOCKING query text (blocker's query)
	// This enables APM correlation for the blocker session as well
	if result.BlockingQueryStatementText != nil && *result.BlockingQueryStatementText != "" {
		sessionIDStr := "unknown"
		if result.CurrentSessionID != nil {
			sessionIDStr = fmt.Sprintf("%d", *result.CurrentSessionID)
		}
		blockerSessionIDStr := "unknown"
		if result.BlockingSessionID != nil {
			blockerSessionIDStr = fmt.Sprintf("%d", *result.BlockingSessionID)
		}

		s.logger.Info("🔍 BLOCKING QUERY: Processing blocker query text",
			zap.String("victim_session_id", sessionIDStr),
			zap.String("blocker_session_id", blockerSessionIDStr),
			zap.Int("blocking_query_text_length", len(*result.BlockingQueryStatementText)))

		// Extract metadata from blocker's query comments
		blockingNrApmGuid, _ = helpers.ExtractNewRelicMetadata(*result.BlockingQueryStatementText)

		// Normalize and hash the blocking query for cross-language correlation
		blockingNormalizedSQL := helpers.AnonymizeQueryText(*result.BlockingQueryStatementText)
		blockingSqlHash := helpers.GenerateMD5Hash(blockingNormalizedSQL)

		// Store blocking query metadata in model
		if blockingNrApmGuid != "" {
			result.BlockingNrServiceGuid = &blockingNrApmGuid
		}
		if blockingSqlHash != "" {
			result.BlockingNormalisedSqlHash = &blockingSqlHash
		}

		s.logger.Info("🏷️  BLOCKING QUERY: Extracted and normalized blocker metadata",
			zap.String("victim_session_id", sessionIDStr),
			zap.String("blocker_session_id", blockerSessionIDStr),
			zap.String("blocking_nr_service_guid", blockingNrApmGuid),
			zap.String("blocking_normalised_sql_hash", blockingSqlHash),
			zap.Bool("has_blocking_guid", blockingNrApmGuid != ""),
			zap.Bool("has_blocking_hash", blockingSqlHash != ""))

		// Cache blocking query metadata for future correlation
		if result.BlockingQueryHash != nil && !result.BlockingQueryHash.IsEmpty() && (blockingNrApmGuid != "" || blockingSqlHash != "") && apmMetadataCache != nil {
			blockingQueryHashStr := result.BlockingQueryHash.String()
			apmMetadataCache.Set(blockingQueryHashStr, blockingNrApmGuid, blockingSqlHash)

			s.logger.Info("💾 BLOCKING QUERY: Cached blocker APM metadata",
				zap.String("blocking_query_hash", blockingQueryHashStr),
				zap.String("blocking_nr_service_guid", blockingNrApmGuid),
				zap.String("blocking_normalised_sql_hash", blockingSqlHash))
		}
	}

	// Cache APM metadata for slow query enrichment (in same scrape) and future active query enrichment
	// This allows both slow queries (from plan cache) and other active queries in this scrape
	// to be enriched with APM correlation data
	if result.QueryID != nil && !result.QueryID.IsEmpty() && (nrApmGuid != "" || sqlHash != "") && apmMetadataCache != nil {
		queryHashStr := result.QueryID.String()
		apmMetadataCache.Set(queryHashStr, nrApmGuid, sqlHash)

		sessionIDStr := "unknown"
		if result.CurrentSessionID != nil {
			sessionIDStr = fmt.Sprintf("%d", *result.CurrentSessionID)
		}

		s.logger.Info("💾 ACTIVE QUERY: Cached APM metadata for slow query enrichment",
			zap.String("session_id", sessionIDStr),
			zap.String("query_hash", queryHashStr),
			zap.String("nr_service_guid", nrApmGuid),
			zap.String("normalized_sql_hash", sqlHash))
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
	getBlockingNrServiceGuid := func() string {
		if result.BlockingNrServiceGuid != nil {
			return *result.BlockingNrServiceGuid
		}
		return ""
	}
	getBlockingNormalisedSqlHash := func() string {
		if result.BlockingNormalisedSqlHash != nil {
			return *result.BlockingNormalisedSqlHash
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
		s.logger.Info("📤 ACTIVE QUERY: Emitting metric with final metadata",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Float64("wait_time_seconds", *result.WaitTimeS),
			zap.Any("wait_type", result.WaitType),
			zap.Any("database_name", result.DatabaseName),
			zap.String("nr_service_guid", getNrServiceGuid()),
			zap.String("normalized_sql_hash", getNormalisedSqlHash()),
			zap.Bool("has_apm_correlation", getNrServiceGuid() != "" && getNormalisedSqlHash() != ""),
			zap.String("metric_name", "sqlserver.activequery.wait_time_seconds"))

		s.mb.RecordSqlserverActivequeryWaitTimeSecondsDataPoint(
			timestamp,
			*result.WaitTimeS,
			getSessionID(),
			getRequestID(),
			getDatabaseName(),
			getLoginName(),
			getHostName(),
			getQueryID(),
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
			getBlockingQueryHash(),
			getBlockingNrServiceGuid(),
			getBlockingNormalisedSqlHash(),
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

// EmitBlockingQueriesAsCustomEvents extracts unique blocking queries from active queries
// and emits them as metrics (which get converted to custom events/logs via metricsaslogs connector)
// Uses composite key: session_id + request_id + request_start_time + blocking_session_id
func (s *QueryPerformanceScraper) EmitBlockingQueriesAsCustomEvents(activeQueries []models.ActiveRunningQuery) error {
	// Build a map of unique blocking events
	// Key: session_id|request_id|request_start_time|blocking_session_id
	blockingEventsMap := make(map[string]models.BlockingQueryEvent)

	for _, activeQuery := range activeQueries {
		// Skip if no blocking session
		if activeQuery.BlockingSessionID == nil || *activeQuery.BlockingSessionID == 0 {
			continue
		}

		// Skip if blocking query text is N/A or empty
		if activeQuery.BlockingQueryStatementText == nil ||
			*activeQuery.BlockingQueryStatementText == "" ||
			*activeQuery.BlockingQueryStatementText == "N/A" {
			continue
		}

		// Skip if required victim identifiers are missing
		if activeQuery.CurrentSessionID == nil ||
			activeQuery.RequestID == nil ||
			activeQuery.RequestStartTime == nil {
			continue
		}

		// Build composite key for deduplication
		key := fmt.Sprintf("%d|%d|%s|%d",
			*activeQuery.CurrentSessionID,
			*activeQuery.RequestID,
			*activeQuery.RequestStartTime,
			*activeQuery.BlockingSessionID)

		// Only add if not already in map (deduplicate)
		if _, exists := blockingEventsMap[key]; !exists {
			// Extract APM metadata fields (use empty string if nil)
			blockingNrServiceGuid := ""
			if activeQuery.BlockingNrServiceGuid != nil {
				blockingNrServiceGuid = *activeQuery.BlockingNrServiceGuid
			}
			blockingNormalisedSqlHash := ""
			if activeQuery.BlockingNormalisedSqlHash != nil {
				blockingNormalisedSqlHash = *activeQuery.BlockingNormalisedSqlHash
			}

			blockingEventsMap[key] = models.BlockingQueryEvent{
				SessionID:                 *activeQuery.CurrentSessionID,
				RequestID:                 *activeQuery.RequestID,
				RequestStartTime:          *activeQuery.RequestStartTime,
				BlockingSessionID:         *activeQuery.BlockingSessionID,
				BlockingQueryText:         *activeQuery.BlockingQueryStatementText, // Full text, no truncation
				BlockingNrServiceGuid:     blockingNrServiceGuid,                   // APM service GUID from blocking query
				BlockingNormalisedSqlHash: blockingNormalisedSqlHash,               // Normalized SQL hash from blocking query
			}
		}
	}

	s.logger.Info("Extracted unique blocking query events from active queries",
		zap.Int("total_active_queries", len(activeQueries)),
		zap.Int("unique_blocking_events", len(blockingEventsMap)))

	// Emit metrics for each unique blocking event
	// These will be converted to logs/custom events via the metricsaslogs connector
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	emittedCount := 0

	for _, event := range blockingEventsMap {
		// Anonymize the blocking query text before emission
		anonymizedText := helpers.AnonymizeQueryText(event.BlockingQueryText)

		s.mb.RecordSqlserverBlockingQueryDetailsDataPoint(
			timestamp,
			1, // Value is always 1 for dimensional metrics
			event.SessionID,
			event.RequestID,
			event.RequestStartTime,
			event.BlockingSessionID,
			anonymizedText,
			event.BlockingNrServiceGuid,     // APM service GUID for correlation
			event.BlockingNormalisedSqlHash, // Normalized SQL hash for cross-language correlation
			"SqlServerSlowQueryDetails",     // event.name for New Relic custom events
		)
		emittedCount++
	}

	s.logger.Info("Emitted blocking query events as metrics",
		zap.Int("emitted_count", emittedCount))

	return nil
}
