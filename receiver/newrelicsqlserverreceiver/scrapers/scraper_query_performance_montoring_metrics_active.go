// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// ScrapeActiveRunningQueriesMetrics collects currently executing queries with wait and blocking details
// This scraper captures real-time query execution state from sys.dm_exec_requests
// If a plan_handle is available, it fetches, parses, and emits execution plan as OTLP logs
// Filters queries by:
//   - elapsedTimeThreshold: minimum elapsed time in milliseconds (0 = capture all)
//   - Must have valid wait_type (not null)
//   - Must have valid plan_handle (not null and not empty)
func (s *QueryPerformanceScraper) ScrapeActiveRunningQueriesMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, logs plog.Logs, limit, textTruncateLimit, elapsedTimeThreshold int) error {
	// Refresh metadata cache if needed (auto-throttled by refresh interval)
	if s.metadataCache != nil {
		if err := s.metadataCache.Refresh(ctx); err != nil {
			s.logger.Warn("Failed to refresh metadata cache", zap.Error(err))
			// Continue with stale cache data
		}
	}

	query := fmt.Sprintf(queries.ActiveRunningQueriesQuery, limit, textTruncateLimit)

	s.logger.Debug("Executing active running queries metrics collection",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("limit", limit),
		zap.Int("text_truncate_limit", textTruncateLimit),
		zap.Int("elapsed_time_threshold_ms", elapsedTimeThreshold))

	var results []models.ActiveRunningQuery
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute active running queries metrics query: %w", err)
	}

	s.logger.Debug("Active running queries metrics fetched", zap.Int("result_count", len(results)))

	// Track filtered queries for logging
	filteredCount := 0
	processedCount := 0

	// Log each query result from SQL Server
	for i, result := range results {
		// FILTERING CRITERIA:
		// 1. Elapsed time must meet threshold (if threshold > 0)
		// 2. Must have valid wait_type (not null)
		// 3. Must have valid plan_handle (not null and not empty)

		// Check elapsed time threshold (skip very short queries if threshold > 0)
		if elapsedTimeThreshold > 0 && result.TotalElapsedTimeMs != nil && *result.TotalElapsedTimeMs < int64(elapsedTimeThreshold) {
			filteredCount++
			s.logger.Debug("Filtering active query: below elapsed time threshold",
				zap.Any("session_id", result.CurrentSessionID),
				zap.Int64("elapsed_time_ms", *result.TotalElapsedTimeMs),
				zap.Int("threshold_ms", elapsedTimeThreshold))
			continue
		}

		// Check for valid wait_type (skip queries with no wait information)
		if result.WaitType == nil || *result.WaitType == "" {
			filteredCount++
			s.logger.Debug("Filtering active query: no wait_type",
				zap.Any("session_id", result.CurrentSessionID))
			continue
		}

		// Check for valid plan_handle (skip queries without execution plans)
		if result.PlanHandle == nil || result.PlanHandle.IsEmpty() {
			filteredCount++
			s.logger.Debug("Filtering active query: no plan_handle",
				zap.Any("session_id", result.CurrentSessionID),
				zap.Any("query_id", result.QueryID))
			continue
		}

		// Query passed all filters, process it
		processedCount++
		s.logger.Info("=== SCRAPED ACTIVE QUERY FROM DB ===",
			zap.Int("index", i),
			zap.Any("session_id", result.CurrentSessionID),
			zap.Any("request_id", result.RequestID),
			zap.Any("database_name", result.DatabaseName),
			zap.Any("request_status", result.RequestStatus),
			zap.Any("wait_type", result.WaitType),
			zap.Any("wait_time_s", result.WaitTimeS),
			zap.Any("last_wait_type", result.LastWaitType),
			zap.Any("cpu_time_ms", result.CPUTimeMs),
			zap.Any("elapsed_time_ms", result.TotalElapsedTimeMs),
			zap.Any("reads", result.Reads),
			zap.Any("logical_reads", result.LogicalReads),
			zap.Any("writes", result.Writes),
			zap.Any("row_count", result.RowCount),
			zap.Any("granted_query_memory_pages", result.GrantedQueryMemoryPages),
			zap.Any("blocking_session_id", result.BlockingSessionID),
			zap.String("query_text_preview", func() string {
				if result.QueryStatementText != nil && len(*result.QueryStatementText) > 100 {
					return (*result.QueryStatementText)[:100] + "..."
				} else if result.QueryStatementText != nil {
					return *result.QueryStatementText
				}
				return "N/A"
			}()))

		// Fetch and parse execution plan for active queries with plan_handle
		var executionPlanXML string

		// NEW: Fetch top 5 most recently used plan_handles for this query_hash with aggregated stats
		// This provides historical execution plan analysis for performance troubleshooting
		if result.QueryID != nil && !result.QueryID.IsEmpty() {
			top5PlanHandles, err := s.fetchTop5PlanHandlesForActiveQuery(ctx, result)
			if err != nil {
				s.logger.Warn("Failed to fetch top 5 plan_handles for active query",
					zap.Error(err),
					zap.Any("session_id", result.CurrentSessionID))
			} else if len(top5PlanHandles) > 0 {
				// Emit aggregated execution plan stats as metrics
				s.emitAggregatedExecutionPlanAsMetrics(ctx, result, top5PlanHandles, scopeMetrics)

				s.logger.Info("Successfully fetched and emitted top 5 aggregated execution plans as metrics",
					zap.Any("session_id", result.CurrentSessionID),
					zap.Int("plan_count", len(top5PlanHandles)))
			}
		}

		// REMOVED (2025-11-28): Old execution plan XML parsing and node-level emission
		// This has been replaced with aggregated stats from dm_exec_query_stats (see lines 121-137 above)

		// Process active query metrics with execution plan
		if err := s.processActiveRunningQueryMetricsWithPlan(result, scopeMetrics, i, executionPlanXML); err != nil {
			s.logger.Error("Failed to process active running query metric", zap.Error(err), zap.Int("index", i))
		}
	}

	// Log filtering summary
	s.logger.Info("Active running queries filtering summary",
		zap.Int("total_fetched", len(results)),
		zap.Int("filtered_out", filteredCount),
		zap.Int("processed", processedCount),
		zap.Int("elapsed_time_threshold_ms", elapsedTimeThreshold))

	return nil
}

// processActiveRunningQueryMetricsWithPlan processes and emits metrics for a single active running query
// with optional execution plan XML
func (s *QueryPerformanceScraper) processActiveRunningQueryMetricsWithPlan(result models.ActiveRunningQuery, scopeMetrics pmetric.ScopeMetrics, index int, executionPlanXML string) error {
	if result.CurrentSessionID == nil {
		s.logger.Debug("Skipping active running query with nil session ID", zap.Int("index", index))
		return nil
	}

	// Create metrics for each active running query
	// We'll emit each query as a separate data point with all its attributes

	// Metric 1: Active query wait time
	if result.WaitTimeS != nil && *result.WaitTimeS > 0 {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.wait_time_seconds",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Float64("value", *result.WaitTimeS),
			zap.Any("wait_type", result.WaitType),
			zap.Any("database_name", result.DatabaseName))

		waitTimeMetric := scopeMetrics.Metrics().AppendEmpty()
		waitTimeMetric.SetName("sqlserver.activequery.wait_time_seconds")
		waitTimeMetric.SetDescription("Wait time for currently executing query")
		waitTimeMetric.SetUnit("s")
		gauge := waitTimeMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(*result.WaitTimeS)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Warn("❌ SKIPPED wait_time metric (wait_time_s <= 0 or nil)",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Any("wait_time_s", result.WaitTimeS),
			zap.Any("wait_type", result.WaitType))
	}

	// Metric 2: Active query CPU time
	if result.CPUTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.cpu_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.CPUTimeMs))

		cpuTimeMetric := scopeMetrics.Metrics().AppendEmpty()
		cpuTimeMetric.SetName("sqlserver.activequery.cpu_time_ms")
		cpuTimeMetric.SetDescription("CPU time for currently executing query")
		cpuTimeMetric.SetUnit("ms")
		gauge := cpuTimeMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.CPUTimeMs)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED cpu_time metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 3: Active query elapsed time
	if result.TotalElapsedTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.elapsed_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.TotalElapsedTimeMs))

		elapsedTimeMetric := scopeMetrics.Metrics().AppendEmpty()
		elapsedTimeMetric.SetName("sqlserver.activequery.elapsed_time_ms")
		elapsedTimeMetric.SetDescription("Total elapsed time for currently executing query")
		elapsedTimeMetric.SetUnit("ms")
		gauge := elapsedTimeMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.TotalElapsedTimeMs)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED elapsed_time metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 4: Active query reads (physical reads from disk)
	if result.Reads != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.reads",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.Reads))

		readsMetric := scopeMetrics.Metrics().AppendEmpty()
		readsMetric.SetName("sqlserver.activequery.reads")
		readsMetric.SetDescription("Number of physical reads from disk for currently executing query")
		readsMetric.SetUnit("{reads}")
		gauge := readsMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.Reads)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED reads metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 5: Active query writes
	if result.Writes != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.writes",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.Writes))

		writesMetric := scopeMetrics.Metrics().AppendEmpty()
		writesMetric.SetName("sqlserver.activequery.writes")
		writesMetric.SetDescription("Number of writes for currently executing query")
		writesMetric.SetUnit("{writes}")
		gauge := writesMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.Writes)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED writes metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 6: Active query logical reads (reads from buffer cache/memory)
	if result.LogicalReads != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.logical_reads",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.LogicalReads))

		logicalReadsMetric := scopeMetrics.Metrics().AppendEmpty()
		logicalReadsMetric.SetName("sqlserver.activequery.logical_reads")
		logicalReadsMetric.SetDescription("Number of logical reads from buffer cache for currently executing query")
		logicalReadsMetric.SetUnit("{reads}")
		gauge := logicalReadsMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.LogicalReads)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED logical_reads metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 7: Active query row count
	if result.RowCount != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.row_count",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.RowCount))

		rowCountMetric := scopeMetrics.Metrics().AppendEmpty()
		rowCountMetric.SetName("sqlserver.activequery.row_count")
		rowCountMetric.SetDescription("Number of rows returned by currently executing query")
		rowCountMetric.SetUnit("{rows}")
		gauge := rowCountMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.RowCount)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED row_count metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 8: Active query granted memory
	if result.GrantedQueryMemoryPages != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.granted_query_memory_pages",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.GrantedQueryMemoryPages))

		grantedMemoryMetric := scopeMetrics.Metrics().AppendEmpty()
		grantedMemoryMetric.SetName("sqlserver.activequery.granted_query_memory_pages")
		grantedMemoryMetric.SetDescription("Number of memory pages granted to currently executing query")
		grantedMemoryMetric.SetUnit("{pages}")
		gauge := grantedMemoryMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.GrantedQueryMemoryPages)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED granted_query_memory_pages metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	s.logger.Info("=== FINISHED PROCESSING ACTIVE QUERY ===",
		zap.Any("session_id", result.CurrentSessionID),
		zap.Int("total_metrics_in_scope", scopeMetrics.Metrics().Len()))

	return nil
}

// addActiveQueryAttributes adds all attributes from an active running query to a data point
func (s *QueryPerformanceScraper) addActiveQueryAttributes(attrs pcommon.Map, result models.ActiveRunningQuery, executionPlanXML string) {
	// NOTE: execution_plan_xml is NOT added as a metric attribute to avoid large data payloads
	// Execution plans are sent as parsed OTLP logs with individual operators
	// Query: FROM Log WHERE event.name = 'sqlserver.execution_plan_operator'

	// Session details
	if result.CurrentSessionID != nil {
		attrs.PutInt("session_id", *result.CurrentSessionID)
	}
	if result.RequestID != nil {
		attrs.PutInt("request_id", *result.RequestID)
	}
	if result.DatabaseName != nil {
		attrs.PutStr("database_name", *result.DatabaseName)
	}
	if result.LoginName != nil {
		attrs.PutStr("login_name", *result.LoginName)
	}
	if result.HostName != nil {
		attrs.PutStr("host_name", *result.HostName)
	}
	if result.ProgramName != nil {
		attrs.PutStr("program_name", *result.ProgramName)
	}
	if result.RequestCommand != nil {
		attrs.PutStr("request_command", *result.RequestCommand)
	}
	if result.RequestStatus != nil {
		attrs.PutStr("request_status", *result.RequestStatus)
	}
	if result.SessionStatus != nil {
		attrs.PutStr("session_status", *result.SessionStatus)
	}
	if result.ClientInterfaceName != nil {
		attrs.PutStr("client_interface_name", *result.ClientInterfaceName)
	}

	// Wait details (raw and decoded)
	if result.WaitType != nil {
		waitType := *result.WaitType
		attrs.PutStr("wait_type", waitType)
		attrs.PutStr("wait_type_description", helpers.DecodeWaitType(waitType))
		attrs.PutStr("wait_type_category", helpers.GetWaitTypeCategory(waitType))
	}
	if result.WaitResource != nil {
		waitResource := *result.WaitResource
		attrs.PutStr("wait_resource", waitResource)
		// Add SQL Server's decoded wait resource (with database/table/index names)
		if result.WaitResourceDecoded != nil {
			attrs.PutStr("wait_resource_decoded", *result.WaitResourceDecoded)
		}
		// Add Go helper's parsed resource type and description (with enrichment if enabled)
		resourceType, resourceDesc := helpers.DecodeWaitResourceWithMetadata(waitResource, s.metadataCache)
		attrs.PutStr("wait_resource_type", resourceType)
		attrs.PutStr("wait_resource_description", resourceDesc)
	}
	if result.LastWaitType != nil {
		lastWaitType := *result.LastWaitType
		attrs.PutStr("last_wait_type", lastWaitType)
		attrs.PutStr("last_wait_type_description", helpers.DecodeWaitType(lastWaitType))
	}

	// Timestamps
	if result.RequestStartTime != nil {
		attrs.PutStr("request_start_time", *result.RequestStartTime)
	}
	if result.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
	}

	// Transaction context (Phase 1 RCA)
	if result.TransactionID != nil {
		attrs.PutInt("transaction_id", *result.TransactionID)
	}
	if result.OpenTransactionCount != nil {
		attrs.PutInt("open_transaction_count", *result.OpenTransactionCount)
	}
	if result.TransactionIsolationLevel != nil {
		attrs.PutInt("transaction_isolation_level", *result.TransactionIsolationLevel)
	}

	// Parallel execution details (Phase 1 RCA)
	if result.DegreeOfParallelism != nil {
		attrs.PutInt("degree_of_parallelism", *result.DegreeOfParallelism)
	}
	if result.ParallelWorkerCount != nil {
		attrs.PutInt("parallel_worker_count", *result.ParallelWorkerCount)
	}

	// Blocking details (LINKING FIX: Now int64 for proper joins)
	if result.BlockingSessionID != nil {
		attrs.PutInt("blocking_session_id", *result.BlockingSessionID)
	}
	if result.BlockerLoginName != nil {
		attrs.PutStr("blocker_login_name", *result.BlockerLoginName)
	}
	if result.BlockerHostName != nil {
		attrs.PutStr("blocker_host_name", *result.BlockerHostName)
	}
	if result.BlockerProgramName != nil {
		attrs.PutStr("blocker_program_name", *result.BlockerProgramName)
	}

	// Query text (anonymized) and query ID for correlation
	if result.QueryStatementText != nil {
		anonymizedQuery := helpers.AnonymizeQueryText(*result.QueryStatementText)
		attrs.PutStr("query_text", anonymizedQuery)
	}

	// Phase 1 RCA: Correlation keys
	// Add SQL Server's native query_id if available (from dm_exec_query_stats)
	// This enables direct correlation with slow queries
	if result.QueryID != nil && !result.QueryID.IsEmpty() {
		attrs.PutStr("query_id", result.QueryID.String())
	}

	// Add plan_handle for execution plan correlation and fetching
	if result.PlanHandle != nil && !result.PlanHandle.IsEmpty() {
		attrs.PutStr("plan_handle", result.PlanHandle.String())
	}

	// ALSO compute and add query_signature (client-side hash) for backwards compatibility
	// This provides a secondary correlation method for queries not yet cached
	if result.QueryStatementText != nil {
		querySignature := helpers.ComputeQueryHash(*result.QueryStatementText)
		if querySignature != "" {
			attrs.PutStr("query_signature", querySignature)
		}
	}
	if result.BlockingQueryStatementText != nil && *result.BlockingQueryStatementText != "N/A" {
		anonymizedBlockingQuery := helpers.AnonymizeQueryText(*result.BlockingQueryStatementText)
		attrs.PutStr("blocking_query_text", anonymizedBlockingQuery)

		// Also compute hash for blocking query for additional correlation
		blockingQueryHash := helpers.ComputeQueryHash(*result.BlockingQueryStatementText)
		if blockingQueryHash != "" {
			attrs.PutStr("blocking_query_hash", blockingQueryHash)
		}
	}
}

// ScrapeLockedObjectsMetrics collects detailed information about objects locked by a specific session
// This provides table/object names for locked resources, enabling better lock contention troubleshooting
func (s *QueryPerformanceScraper) ScrapeLockedObjectsMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, sessionID int) error {
	query := fmt.Sprintf(queries.LockedObjectsBySessionQuery, sessionID)

	s.logger.Debug("Executing locked objects metrics collection",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("session_id", sessionID))

	var results []models.LockedObject
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute locked objects metrics query: %w", err)
	}

	s.logger.Debug("Locked objects metrics fetched", zap.Int("result_count", len(results)))

	for i, result := range results {
		if err := s.processLockedObjectMetrics(result, scopeMetrics, i); err != nil {
			s.logger.Error("Failed to process locked object metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processLockedObjectMetrics processes and emits metrics for a single locked object
func (s *QueryPerformanceScraper) processLockedObjectMetrics(result models.LockedObject, scopeMetrics pmetric.ScopeMetrics, index int) error {
	if result.SessionID == nil {
		s.logger.Debug("Skipping locked object with nil session ID", zap.Int("index", index))
		return nil
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Create a metric for locked object tracking
	// Value = 1 to indicate presence of lock, attributes contain all details
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("sqlserver.locked_object")
	metric.SetDescription("Database object locked by a session")
	metric.SetUnit("1")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetIntValue(1) // Presence indicator
	dp.SetTimestamp(timestamp)
	dp.SetStartTimestamp(s.startTime)

	// Add all locked object attributes
	attrs := dp.Attributes()

	if result.SessionID != nil {
		attrs.PutInt("session_id", *result.SessionID)
	}
	if result.DatabaseName != nil {
		attrs.PutStr("database_name", *result.DatabaseName)
	}
	if result.SchemaName != nil && *result.SchemaName != "" {
		attrs.PutStr("schema_name", *result.SchemaName)
	}
	if result.LockedObjectName != nil && *result.LockedObjectName != "" {
		attrs.PutStr("locked_object_name", *result.LockedObjectName)
	} else {
		// If object name is NULL, this might be a database or file lock
		attrs.PutStr("locked_object_name", "N/A")
	}
	if result.ResourceType != nil {
		attrs.PutStr("resource_type", *result.ResourceType)
	}
	if result.LockGranularity != nil {
		attrs.PutStr("lock_granularity", *result.LockGranularity)
	}
	if result.LockMode != nil {
		attrs.PutStr("lock_mode", *result.LockMode)
	}
	if result.LockStatus != nil {
		attrs.PutStr("lock_status", *result.LockStatus)
	}
	if result.LockRequestType != nil {
		attrs.PutStr("lock_request_type", *result.LockRequestType)
	}
	if result.ResourceDescription != nil && *result.ResourceDescription != "" {
		attrs.PutStr("resource_description", *result.ResourceDescription)
	}
	if result.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
	}

	s.logger.Debug("Processed locked object metric",
		zap.Any("session_id", result.SessionID),
		zap.Any("locked_object_name", result.LockedObjectName),
		zap.Any("lock_granularity", result.LockGranularity),
		zap.Any("lock_mode", result.LockMode))

	return nil
}

// fetchExecutionPlanForActiveQuery fetches the execution plan XML for an active query using plan_handle only
// This ensures we get the execution plan for the CURRENTLY RUNNING query, not from historical stats
// Returns the execution plan XML string or empty string if not found
func (s *QueryPerformanceScraper) fetchExecutionPlanForActiveQuery(ctx context.Context, activeQuery models.ActiveRunningQuery) (string, error) {
	var executionPlanXML string

	// Fetch using plan_handle (for currently executing queries only)
	if activeQuery.PlanHandle == nil || activeQuery.PlanHandle.IsEmpty() {
		s.logger.Debug("No plan_handle available for active query, skipping execution plan fetch",
			zap.Any("session_id", activeQuery.CurrentSessionID),
			zap.Any("query_id", activeQuery.QueryID))
		return "", nil
	}

	planHandleHex := activeQuery.PlanHandle.String()
	query := fmt.Sprintf(queries.ActiveQueryExecutionPlanQuery, planHandleHex)

	s.logger.Debug("Fetching execution plan for active query using plan_handle",
		zap.String("plan_handle", planHandleHex),
		zap.Any("session_id", activeQuery.CurrentSessionID),
		zap.Any("query_id", activeQuery.QueryID))

	// Execute the query to fetch the execution plan XML
	var results []struct {
		ExecutionPlanXML *string `db:"execution_plan_xml"`
	}

	if err := s.connection.Query(ctx, &results, query); err != nil {
		return "", fmt.Errorf("failed to fetch execution plan using plan_handle: %w", err)
	}

	if len(results) == 0 || results[0].ExecutionPlanXML == nil {
		s.logger.Debug("No execution plan found for plan_handle",
			zap.String("plan_handle", planHandleHex),
			zap.Any("session_id", activeQuery.CurrentSessionID))
		return "", nil
	}

	executionPlanXML = *results[0].ExecutionPlanXML
	s.logger.Debug("Successfully fetched execution plan XML using plan_handle",
		zap.String("plan_handle", planHandleHex),
		zap.Any("session_id", activeQuery.CurrentSessionID),
		zap.Int("xml_length", len(executionPlanXML)))

	// Return the execution plan XML
	return executionPlanXML, nil
}

// REMOVED (2025-11-28): Old execution plan functions that created node-level custom events
// - emitActiveQueryExecutionPlanLogs() - created one SqlServerActiveQueryExecutionPlan event per operator node
// - createActiveQueryExecutionPlanNodeLog() - helper function for node-level events
// - emitActiveQueryExecutionPlanMetrics() - created summary metrics from parsed XML
// These have been replaced with aggregated stats approach using dm_exec_query_stats

// fetchTop5PlanHandlesForActiveQuery fetches the top 5 most recently used plan_handles
// for the query_hash of the currently active query from dm_exec_query_stats
// This provides historical execution plans for performance analysis
func (s *QueryPerformanceScraper) fetchTop5PlanHandlesForActiveQuery(ctx context.Context, activeQuery models.ActiveRunningQuery) ([]models.PlanHandleResult, error) {
	// Check if query_id (query_hash) is available
	if activeQuery.QueryID == nil || activeQuery.QueryID.IsEmpty() {
		s.logger.Debug("No query_hash available for active query, skipping top 5 plan_handles fetch",
			zap.Any("session_id", activeQuery.CurrentSessionID))
		return nil, nil
	}

	queryHashHex := activeQuery.QueryID.String()
	query := fmt.Sprintf(queries.Top5PlanHandlesForActiveQueryQuery, queryHashHex)

	s.logger.Debug("Fetching top 5 plan_handles for active query using query_hash",
		zap.String("query_hash", queryHashHex),
		zap.Any("session_id", activeQuery.CurrentSessionID))

	// Execute the query to fetch plan_handles
	var results []models.PlanHandleResult
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return nil, fmt.Errorf("failed to fetch top 5 plan_handles for query_hash: %w", err)
	}

	s.logger.Debug("Successfully fetched plan_handles for query_hash",
		zap.String("query_hash", queryHashHex),
		zap.Int("plan_count", len(results)),
		zap.Any("session_id", activeQuery.CurrentSessionID))

	return results, nil
}

// emitAggregatedExecutionPlanAsMetrics emits aggregated execution plan statistics as metrics
// Emits metrics for each plan_handle with aggregated stats from dm_exec_query_stats
// Each plan_handle for the query_hash becomes separate metric data points with attributes
func (s *QueryPerformanceScraper) emitAggregatedExecutionPlanAsMetrics(ctx context.Context, activeQuery models.ActiveRunningQuery, planHandleResults []models.PlanHandleResult, scopeMetrics pmetric.ScopeMetrics) {
	if len(planHandleResults) == 0 {
		return
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Process each plan_handle and emit as metrics
	for _, planResult := range planHandleResults {
		// Create aggregated metrics for this plan_handle with stats from dm_exec_query_stats
		s.createAggregatedExecutionPlanMetrics(planResult, activeQuery, scopeMetrics, timestamp)
	}

	s.logger.Info("Emitted aggregated execution plan stats as metrics",
		zap.Any("session_id", activeQuery.CurrentSessionID),
		zap.Int("plan_count", len(planHandleResults)))
}

// createAggregatedExecutionPlanMetrics creates metrics for aggregated execution plan statistics
// Emits metrics with plan_handle, query_hash, and query_plan_hash as attributes
// This provides historical performance data for the active query's plan_handle
func (s *QueryPerformanceScraper) createAggregatedExecutionPlanMetrics(planResult models.PlanHandleResult, activeQuery models.ActiveRunningQuery, scopeMetrics pmetric.ScopeMetrics, timestamp pcommon.Timestamp) {
	// Helper function to create common attributes for all metrics
	createPlanAttributes := func() pcommon.Map {
		attrs := pcommon.NewMap()

		// Correlation keys
		if activeQuery.QueryID != nil {
			attrs.PutStr("query_hash", activeQuery.QueryID.String())
		}
		if planResult.PlanHandle != nil {
			attrs.PutStr("plan_handle", planResult.PlanHandle.String())
		}
		if planResult.QueryPlanHash != nil {
			attrs.PutStr("query_plan_hash", planResult.QueryPlanHash.String())
		}
		if activeQuery.CurrentSessionID != nil {
			attrs.PutInt("session_id", *activeQuery.CurrentSessionID)
		}
		if activeQuery.RequestID != nil {
			attrs.PutInt("request_id", *activeQuery.RequestID)
		}
		if activeQuery.DatabaseName != nil {
			attrs.PutStr("database_name", *activeQuery.DatabaseName)
		}
		if planResult.LastExecutionTime != nil {
			attrs.PutStr("last_execution_time", *planResult.LastExecutionTime)
		}
		if planResult.CreationTime != nil {
			attrs.PutStr("creation_time", *planResult.CreationTime)
		}

		return attrs
	}

	// Metric 1: Execution Count (from dm_exec_query_stats)
	if planResult.ExecutionCount != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.execution_count")
		metric.SetDescription("Number of times this execution plan was executed")
		metric.SetUnit("{executions}")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(*planResult.ExecutionCount)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 2: Total Elapsed Time (from dm_exec_query_stats)
	if planResult.TotalElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.total_elapsed_time_ms")
		metric.SetDescription("Total elapsed time across all executions of this plan")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.TotalElapsedTimeMs)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 3: Average Elapsed Time (from dm_exec_query_stats)
	if planResult.AvgElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.avg_elapsed_time_ms")
		metric.SetDescription("Average elapsed time per execution of this plan")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.AvgElapsedTimeMs)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 4: Total Worker Time (from dm_exec_query_stats)
	if planResult.TotalWorkerTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.total_worker_time_ms")
		metric.SetDescription("Total CPU time across all executions of this plan")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.TotalWorkerTimeMs)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 5: Average Worker Time (from dm_exec_query_stats)
	if planResult.AvgWorkerTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.avg_worker_time_ms")
		metric.SetDescription("Average CPU time per execution of this plan")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.AvgWorkerTimeMs)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 6: Current Wait Time (from active query)
	if activeQuery.WaitTimeS != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.current_wait_time_s")
		metric.SetDescription("Current wait time for active query using this plan")
		metric.SetUnit("s")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*activeQuery.WaitTimeS)
		attrs := createPlanAttributes()
		if activeQuery.WaitType != nil {
			attrs.PutStr("current_wait_type", *activeQuery.WaitType)
		}
		attrs.CopyTo(dp.Attributes())
	}

	// Metric 7: Current Elapsed Time (from active query)
	if activeQuery.TotalElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.current_elapsed_time_ms")
		metric.SetDescription("Current elapsed time for active query using this plan")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(*activeQuery.TotalElapsedTimeMs)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 8: Current CPU Time (from active query)
	if activeQuery.CPUTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.current_cpu_time_ms")
		metric.SetDescription("Current CPU time for active query using this plan")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(*activeQuery.CPUTimeMs)
		createPlanAttributes().CopyTo(dp.Attributes())
	}

	// Metric 9: Current Granted Memory (from active query)
	if activeQuery.GrantedQueryMemoryPages != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.current_granted_memory_kb")
		metric.SetDescription("Current granted memory for active query using this plan")
		metric.SetUnit("KB")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		// Convert pages to KB (1 page = 8KB in SQL Server)
		dp.SetIntValue(*activeQuery.GrantedQueryMemoryPages * 8)
		createPlanAttributes().CopyTo(dp.Attributes())
	}
}
