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

	// Get APM metadata from cache (populated by slow query scraper)
	// This enables APM integration and query correlation across different language agents
	var nrApmGuid, sqlHash string
	var blockingNrApmGuid string

	// Try to get APM metadata from cache first (populated by slow query scraper)
	if result.QueryID != nil && !result.QueryID.IsEmpty() && apmMetadataCache != nil {
		queryHashStr := result.QueryID.String()
		if cachedMetadata, found := apmMetadataCache.Get(queryHashStr); found {
			nrApmGuid = cachedMetadata.NrServiceGuid
			sqlHash = cachedMetadata.NormalisedSqlHash

			sessionIDStr := "unknown"
			if result.CurrentSessionID != nil {
				sessionIDStr = fmt.Sprintf("%d", *result.CurrentSessionID)
			}

			s.logger.Info("✅ ACTIVE QUERY: Using cached APM metadata from slow query",
				zap.String("session_id", sessionIDStr),
				zap.String("query_id", queryHashStr),
				zap.String("cached_nr_service_guid", nrApmGuid),
				zap.String("cached_normalised_sql_hash", sqlHash))
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

	// Helper functions for safe string extraction
	stringValue := func(s *string) string {
		if s != nil {
			return *s
		}
		return ""
	}
	int64Value := func(i *int64) int64 {
		if i != nil {
			return *i
		}
		return 0
	}
	queryIDValue := func(qid *models.QueryID) string {
		if qid != nil && !qid.IsEmpty() {
			return qid.String()
		}
		return ""
	}

	// Extract values
	sessionID := int64Value(result.CurrentSessionID)
	requestID := int64Value(result.RequestID)
	databaseName := stringValue(result.DatabaseName)
	loginName := stringValue(result.LoginName)
	hostName := stringValue(result.HostName)
	queryID := queryIDValue(result.QueryID)
	normalisedSqlHash := stringValue(result.NormalisedSqlHash)
	nrServiceGuidVal := stringValue(result.NrServiceGuid)
	waitType := stringValue(result.WaitType)
	waitResource := stringValue(result.WaitResource)
	waitResourceObjectName := stringValue(result.WaitResourceObjectName)
	lastWaitType := stringValue(result.LastWaitType)
	requestStartTime := stringValue(result.RequestStartTime)
	collectionTimestamp := stringValue(result.CollectionTimestamp)
	transactionID := int64Value(result.TransactionID)
	openTransactionCount := int64Value(result.OpenTransactionCount)
	blockingSessionID := int64Value(result.BlockingSessionID)
	blockingLoginName := stringValue(result.BlockerLoginName)
	blockingQueryHash := queryIDValue(result.BlockingQueryHash)
	blockingNrServiceGuid := stringValue(result.BlockingNrServiceGuid)
	blockingNormalisedSqlHash := stringValue(result.BlockingNormalisedSqlHash)

	// Use slow query plan_handle for consistency
	planHandle := ""
	if slowQueryPlanHandle != nil && !slowQueryPlanHandle.IsEmpty() {
		planHandle = slowQueryPlanHandle.String()
	}

	// Decode wait types
	waitTypeForDecoding := waitType
	if waitTypeForDecoding == "" {
		waitTypeForDecoding = "N/A"
	}
	waitTypeDescription := helpers.DecodeWaitType(waitTypeForDecoding)
	if waitTypeDescription == "" {
		waitTypeDescription = waitTypeForDecoding
	}
	waitTypeCategory := helpers.GetWaitTypeCategory(waitTypeForDecoding)
	if waitTypeCategory == "" {
		waitTypeCategory = "Other"
	}

	// Decode wait resource
	waitResourceType := ""
	if result.WaitResource != nil {
		waitResourceType, _ = helpers.DecodeWaitResource(*result.WaitResource)
	}

	// Decode last wait type
	lastWaitTypeDescription := ""
	if result.LastWaitType != nil {
		lastWaitTypeDescription = helpers.DecodeWaitType(*result.LastWaitType)
	}

	// Active query wait time
	if result.WaitTimeS != nil && *result.WaitTimeS > 0 {
		s.logger.Info("📤 ACTIVE QUERY: Emitting metric with final metadata",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Float64("wait_time_seconds", *result.WaitTimeS),
			zap.Any("wait_type", result.WaitType),
			zap.Any("database_name", result.DatabaseName),
			zap.String("nr_service_guid", nrServiceGuidVal),
			zap.String("normalized_sql_hash", normalisedSqlHash),
			zap.Bool("has_apm_correlation", nrServiceGuidVal != "" && normalisedSqlHash != ""),
			zap.String("metric_name", "sqlserver.activequery.wait_time_seconds"))

		s.mb.RecordSqlserverActivequeryWaitTimeSecondsDataPoint(
			timestamp,
			*result.WaitTimeS,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			queryID,
			normalisedSqlHash,
			nrServiceGuidVal,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceType,
			waitResourceObjectName,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			planHandle,
			blockingSessionID,
			blockingLoginName,
			blockingQueryHash,
			blockingNrServiceGuid,
			blockingNormalisedSqlHash,
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
