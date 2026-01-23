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

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Extract attribute values
	var sessionID int64
	if result.CurrentSessionID != nil {
		sessionID = *result.CurrentSessionID
	}

	var requestID int64
	if result.RequestID != nil {
		requestID = *result.RequestID
	}

	var databaseName string
	if result.DatabaseName != nil {
		databaseName = *result.DatabaseName
	}

	var loginName string
	if result.LoginName != nil {
		loginName = *result.LoginName
	}

	var hostName string
	if result.HostName != nil {
		hostName = *result.HostName
	}

	var programName string
	if result.ProgramName != nil {
		programName = *result.ProgramName
	}

	var requestCommand string
	if result.RequestCommand != nil {
		requestCommand = *result.RequestCommand
	}

	var requestStatus string
	if result.RequestStatus != nil {
		requestStatus = *result.RequestStatus
	}

	var sessionStatus string
	if result.SessionStatus != nil {
		sessionStatus = *result.SessionStatus
	}

	var clientInterfaceName string
	if result.ClientInterfaceName != nil {
		clientInterfaceName = *result.ClientInterfaceName
	}

	var waitType string
	if result.WaitType != nil {
		waitType = *result.WaitType
	}

	var waitResource string
	if result.WaitResource != nil {
		waitResource = *result.WaitResource
	}

	var waitResourceObjectName string
	if result.WaitResourceObjectName != nil {
		waitResourceObjectName = *result.WaitResourceObjectName
	}

	var waitResourceDatabaseName string
	if result.WaitResourceDatabaseName != nil {
		waitResourceDatabaseName = *result.WaitResourceDatabaseName
	}

	var lastWaitType string
	if result.LastWaitType != nil {
		lastWaitType = *result.LastWaitType
	}

	var requestStartTime string
	if result.RequestStartTime != nil {
		requestStartTime = *result.RequestStartTime
	}

	var collectionTimestamp string
	if result.CollectionTimestamp != nil {
		collectionTimestamp = *result.CollectionTimestamp
	}

	var transactionID int64
	if result.TransactionID != nil {
		transactionID = *result.TransactionID
	}

	var openTransactionCount int64
	if result.OpenTransactionCount != nil {
		openTransactionCount = *result.OpenTransactionCount
	}

	var transactionIsolationLevel int64
	if result.TransactionIsolationLevel != nil {
		transactionIsolationLevel = *result.TransactionIsolationLevel
	}

	var degreeOfParallelism int64
	if result.DegreeOfParallelism != nil {
		degreeOfParallelism = *result.DegreeOfParallelism
	}

	var parallelWorkerCount int64
	if result.ParallelWorkerCount != nil {
		parallelWorkerCount = *result.ParallelWorkerCount
	}

	var blockingSessionID int64
	if result.BlockingSessionID != nil {
		blockingSessionID = *result.BlockingSessionID
	}

	var blockingLoginName string
	if result.BlockerLoginName != nil {
		blockingLoginName = *result.BlockerLoginName
	}

	var blockingHostName string
	if result.BlockerHostName != nil {
		blockingHostName = *result.BlockerHostName
	}

	var blockingProgramName string
	if result.BlockerProgramName != nil {
		blockingProgramName = *result.BlockerProgramName
	}

	var blockingStatus string
	if result.BlockerStatus != nil {
		blockingStatus = *result.BlockerStatus
	}

	var blockingIsolationLevel int64
	if result.BlockerIsolationLevel != nil {
		blockingIsolationLevel = *result.BlockerIsolationLevel
	}

	var blockingOpenTransactionCount int64
	if result.BlockerOpenTransactionCount != nil {
		blockingOpenTransactionCount = *result.BlockerOpenTransactionCount
	}

	var blockingQueryHash string
	if result.BlockingQueryHash != nil && !result.BlockingQueryHash.IsEmpty() {
		blockingQueryHash = result.BlockingQueryHash.String()
	}

	var blockingQueryText string
	if result.BlockingQueryStatementText != nil {
		blockingQueryText = *result.BlockingQueryStatementText
	}

	var queryText string
	if result.QueryStatementText != nil {
		queryText = *result.QueryStatementText
	}

	var queryID string
	if result.QueryID != nil && !result.QueryID.IsEmpty() {
		queryID = result.QueryID.String()
	}

	var planHandle string
	if slowQueryPlanHandle != nil && !slowQueryPlanHandle.IsEmpty() {
		planHandle = slowQueryPlanHandle.String()
	}

	// Computed wait type attributes
	waitTypeForDescription := waitType
	if waitTypeForDescription == "" {
		waitTypeForDescription = "N/A"
	}
	waitTypeDescription := helpers.DecodeWaitType(waitTypeForDescription)
	if waitTypeDescription == "" {
		waitTypeDescription = waitTypeForDescription
	}

	waitTypeForCategory := waitType
	if waitTypeForCategory == "" {
		waitTypeForCategory = "N/A"
	}
	waitTypeCategory := helpers.GetWaitTypeCategory(waitTypeForCategory)
	if waitTypeCategory == "" {
		waitTypeCategory = "Other"
	}

	var waitResourceType, waitResourceDescription string
	if result.WaitResource != nil {
		waitResourceType, waitResourceDescription = helpers.DecodeWaitResource(*result.WaitResource)
	}

	var waitResourceSchemaName string
	if result.WaitResourceSchemaNameObject != nil {
		waitResourceSchemaName = *result.WaitResourceSchemaNameObject
	}

	var waitResourceTableName string
	if result.WaitResourceObjectName != nil {
		waitResourceTableName = *result.WaitResourceObjectName
	}

	var waitResourceObjectType string
	if result.WaitResourceObjectType != nil {
		waitResourceObjectType = *result.WaitResourceObjectType
	}

	var waitResourceIndexName string
	if result.WaitResourceIndexName != nil {
		waitResourceIndexName = *result.WaitResourceIndexName
	}

	var waitResourceIndexType string
	if result.WaitResourceIndexType != nil {
		waitResourceIndexType = *result.WaitResourceIndexType
	}

	var lastWaitTypeDescription string
	if result.LastWaitType != nil {
		lastWaitTypeDescription = helpers.DecodeWaitType(*result.LastWaitType)
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
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Warn("❌ SKIPPED wait_time metric (wait_time_s <= 0 or nil)",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Any("wait_time_s", result.WaitTimeS),
			zap.Any("wait_type", result.WaitType))
	}

	// Active query CPU time
	if result.CPUTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.cpu_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.CPUTimeMs))

		s.mb.RecordSqlserverActivequeryCPUTimeMsDataPoint(
			timestamp,
			*result.CPUTimeMs,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED cpu_time metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Active query elapsed time
	if result.TotalElapsedTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.elapsed_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.TotalElapsedTimeMs))

		s.mb.RecordSqlserverActivequeryElapsedTimeMsDataPoint(
			timestamp,
			*result.TotalElapsedTimeMs,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED elapsed_time metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Active query reads
	if result.Reads != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.reads",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.Reads))

		s.mb.RecordSqlserverActivequeryReadsDataPoint(
			timestamp,
			*result.Reads,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED reads metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Active query writes
	if result.Writes != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.writes",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.Writes))

		s.mb.RecordSqlserverActivequeryWritesDataPoint(
			timestamp,
			*result.Writes,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED writes metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Active query logical reads
	if result.LogicalReads != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.logical_reads",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.LogicalReads))

		s.mb.RecordSqlserverActivequeryLogicalReadsDataPoint(
			timestamp,
			*result.LogicalReads,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED logical_reads metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Active query row count
	if result.RowCount != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.row_count",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.RowCount))

		s.mb.RecordSqlserverActivequeryRowCountDataPoint(
			timestamp,
			*result.RowCount,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED row_count metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	if result.GrantedQueryMemoryPages != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.granted_query_memory_pages",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.GrantedQueryMemoryPages))

		s.mb.RecordSqlserverActivequeryGrantedQueryMemoryPagesDataPoint(
			timestamp,
			*result.GrantedQueryMemoryPages,
			sessionID,
			requestID,
			databaseName,
			loginName,
			hostName,
			programName,
			requestCommand,
			requestStatus,
			sessionStatus,
			clientInterfaceName,
			waitType,
			waitTypeDescription,
			waitTypeCategory,
			waitResource,
			waitResourceObjectName,
			waitResourceDatabaseName,
			waitResourceType,
			waitResourceDescription,
			waitResourceSchemaName,
			waitResourceTableName,
			waitResourceObjectType,
			waitResourceIndexName,
			waitResourceIndexType,
			lastWaitType,
			lastWaitTypeDescription,
			requestStartTime,
			collectionTimestamp,
			transactionID,
			openTransactionCount,
			transactionIsolationLevel,
			degreeOfParallelism,
			parallelWorkerCount,
			blockingHostName,
			blockingIsolationLevel,
			blockingLoginName,
			blockingOpenTransactionCount,
			blockingProgramName,
			blockingQueryHash,
			blockingQueryText,
			blockingSessionID,
			blockingStatus,
			queryText,
			queryID,
			planHandle,
		)
	} else {
		s.logger.Debug("SKIPPED granted_query_memory_pages metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
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

// REMOVED: Old logs-based execution plan functions (EmitActiveRunningExecutionPlansAsLogs, parseAndEmitExecutionPlanAsLogs)
// Execution plans now emitted as sqlserver.execution.plan metrics, converted to logs via metricsaslogs connector.

// REMOVED: Legacy execution plan functions (fetchTop5PlanHandlesForActiveQuery, emitAggregatedExecutionPlanAsMetrics)
// Replaced by ScrapeSlowQueryExecutionPlans in scraper_query_performance_montoring_metrics.go
