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
func (s *QueryPerformanceScraper) ScrapeActiveRunningQueriesMetrics(ctx context.Context, limit, textTruncateLimit, elapsedTimeThreshold int, slowQueryIDs []string) ([]models.ActiveRunningQuery, error) {
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
	getProgramName := func() string {
		if result.ProgramName != nil {
			return *result.ProgramName
		}
		return ""
	}
	getRequestCommand := func() string {
		if result.RequestCommand != nil {
			return *result.RequestCommand
		}
		return ""
	}
	getRequestStatus := func() string {
		if result.RequestStatus != nil {
			return *result.RequestStatus
		}
		return ""
	}
	getSessionStatus := func() string {
		if result.SessionStatus != nil {
			return *result.SessionStatus
		}
		return ""
	}
	getClientInterfaceName := func() string {
		if result.ClientInterfaceName != nil {
			return *result.ClientInterfaceName
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
	getWaitResourceDatabaseName := func() string {
		if result.WaitResourceDatabaseName != nil {
			return *result.WaitResourceDatabaseName
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
	getTransactionIsolationLevel := func() int64 {
		if result.TransactionIsolationLevel != nil {
			return *result.TransactionIsolationLevel
		}
		return 0
	}
	getDegreeOfParallelism := func() int64 {
		if result.DegreeOfParallelism != nil {
			return *result.DegreeOfParallelism
		}
		return 0
	}
	getParallelWorkerCount := func() int64 {
		if result.ParallelWorkerCount != nil {
			return *result.ParallelWorkerCount
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
	getBlockingHostName := func() string {
		if result.BlockerHostName != nil {
			return *result.BlockerHostName
		}
		return ""
	}
	getBlockingProgramName := func() string {
		if result.BlockerProgramName != nil {
			return *result.BlockerProgramName
		}
		return ""
	}
	getBlockingStatus := func() string {
		if result.BlockerStatus != nil {
			return *result.BlockerStatus
		}
		return ""
	}
	getBlockingIsolationLevel := func() int64 {
		if result.BlockerIsolationLevel != nil {
			return *result.BlockerIsolationLevel
		}
		return 0
	}
	getBlockingOpenTransactionCount := func() int64 {
		if result.BlockerOpenTransactionCount != nil {
			return *result.BlockerOpenTransactionCount
		}
		return 0
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
	getWaitResourceSchemaName := func() string {
		if result.WaitResourceSchemaNameObject != nil {
			return *result.WaitResourceSchemaNameObject
		}
		return ""
	}
	getWaitResourceTableName := func() string {
		if result.WaitResourceObjectName != nil {
			return *result.WaitResourceObjectName
		}
		return ""
	}
	getWaitResourceObjectType := func() string {
		if result.WaitResourceObjectType != nil {
			return *result.WaitResourceObjectType
		}
		return ""
	}
	getWaitResourceIndexName := func() string {
		if result.WaitResourceIndexName != nil {
			return *result.WaitResourceIndexName
		}
		return ""
	}
	getWaitResourceIndexType := func() string {
		if result.WaitResourceIndexType != nil {
			return *result.WaitResourceIndexType
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

	// Active query CPU time
	if result.CPUTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.cpu_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.CPUTimeMs))

		s.mb.RecordSqlserverActivequeryCPUTimeMsDataPoint(
			timestamp,
			*result.CPUTimeMs,
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
