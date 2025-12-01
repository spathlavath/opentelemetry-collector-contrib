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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// ScrapeActiveRunningQueriesMetrics collects currently executing queries with wait and blocking details
// This scraper captures real-time query execution state from sys.dm_exec_requests
// If a plan_handle is available, it fetches, parses, and emits execution plan as OTLP logs
// SQL-level filters (applied in WHERE clause):
//   - slowQueryIDs: filter to only queries matching slow query query_hash values (correlation)
//   - elapsedTimeThreshold: minimum elapsed time in milliseconds (filtered in SQL)
//   - Must have valid wait_type (filtered in SQL)
//   - Must have valid plan_handle (filtered in SQL)
//   - Must have valid query_hash (filtered in SQL)
//
// Results are sorted by total_elapsed_time DESC (slowest first) and limited to Top N
// Returns the active queries for further processing (e.g., fetching top 5 plan handles per active query)
func (s *QueryPerformanceScraper) ScrapeActiveRunningQueriesMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, logs plog.Logs, limit, textTruncateLimit, elapsedTimeThreshold int, slowQueryIDs []string) ([]models.ActiveRunningQuery, error) {
	// Early return if no slow queries to correlate with
	if len(slowQueryIDs) == 0 {
		s.logger.Info("No slow queries found, skipping active query scraping (nothing to correlate)")
		return nil, nil
	}

	// Build query_id IN clause for SQL filtering
	queryIDFilter := "AND r_wait.query_hash IN (" + strings.Join(slowQueryIDs, ",") + ")"

	query := fmt.Sprintf(queries.ActiveRunningQueriesQuery, limit, textTruncateLimit, elapsedTimeThreshold) + " " + queryIDFilter

	s.logger.Debug("Executing active running queries metrics collection (filtered by slow query IDs)",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("limit", limit),
		zap.Int("text_truncate_limit", textTruncateLimit),
		zap.Int("elapsed_time_threshold_ms", elapsedTimeThreshold),
		zap.Int("slow_query_id_count", len(slowQueryIDs)),
		zap.String("slow_query_ids_preview", queries.TruncateQuery(strings.Join(slowQueryIDs, ","), 100)))

	var results []models.ActiveRunningQuery
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return nil, fmt.Errorf("failed to execute active running queries metrics query: %w", err)
	}

	s.logger.Debug("Active running queries metrics fetched", zap.Int("result_count", len(results)))

	// Track filtered queries for logging
	filteredCount := 0
	processedCount := 0

	// Log each query result from SQL Server
	for i, result := range results {
		// SQL-LEVEL FILTERING: Most filters already applied in SQL WHERE clause
		// - elapsed_time >= threshold
		// - wait_type IS NOT NULL
		// - plan_handle IS NOT NULL
		// - query_hash IS NOT NULL
		//
		// Go-level checks below are defensive (should not trigger if SQL is correct)

		// Defensive check: wait_type should never be null (filtered in SQL)
		if result.WaitType == nil || *result.WaitType == "" {
			filteredCount++
			s.logger.Warn("Active query has NULL/empty wait_type despite SQL filter (should not happen)",
				zap.Any("session_id", result.CurrentSessionID))
			continue
		}

		// Defensive check: plan_handle should never be null (filtered in SQL)
		if result.PlanHandle == nil || result.PlanHandle.IsEmpty() {
			filteredCount++
			s.logger.Warn("Active query has NULL/empty plan_handle despite SQL filter (should not happen)",
				zap.Any("session_id", result.CurrentSessionID),
				zap.Any("query_id", result.QueryID))
			continue
		}

		// Defensive check: query_id should never be null (filtered in SQL)
		if result.QueryID == nil || result.QueryID.IsEmpty() {
			filteredCount++
			s.logger.Warn("Active query has NULL/empty query_id despite SQL filter (should not happen)",
				zap.Any("session_id", result.CurrentSessionID))
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

		// Fetch execution plan XML for logs endpoint ONLY
		// For metrics endpoint: We use aggregate plan stats from slow query scraper (dm_exec_query_stats)
		// For logs endpoint: We fetch detailed XML execution plan for operator-level analysis
		var executionPlanXML string
		shouldFetchXML := logs.ResourceLogs().Len() >= 0 // Check if logs collection is active (not nil)

		if shouldFetchXML && result.PlanHandle != nil && !result.PlanHandle.IsEmpty() {
			s.logger.Debug("Fetching execution plan XML for active query (logs endpoint)",
				zap.Any("session_id", result.CurrentSessionID),
				zap.String("plan_handle", result.PlanHandle.String()))

			planXML, err := s.fetchExecutionPlanXML(ctx, result.PlanHandle)
			if err != nil {
				s.logger.Warn("Failed to fetch execution plan XML for active query",
					zap.Error(err),
					zap.Any("session_id", result.CurrentSessionID),
					zap.String("plan_handle", result.PlanHandle.String()))
			} else if planXML != "" {
				executionPlanXML = planXML
				s.logger.Debug("Successfully fetched execution plan XML",
					zap.Any("session_id", result.CurrentSessionID),
					zap.Int("xml_length", len(planXML)))
			}
		}

		// Process active query metrics with execution plan
		if err := s.processActiveRunningQueryMetricsWithPlan(result, scopeMetrics, i, executionPlanXML); err != nil {
			s.logger.Error("Failed to process active running query metric", zap.Error(err), zap.Int("index", i))
		}

		// If we have execution plan XML, parse and emit as logs
		if executionPlanXML != "" && shouldFetchXML {
			if err := s.parseAndEmitExecutionPlanAsLogs(ctx, result, executionPlanXML, logs); err != nil {
				s.logger.Error("Failed to parse and emit execution plan as logs",
					zap.Error(err),
					zap.Any("session_id", result.CurrentSessionID))
			}
		}
	}

	// Log filtering summary
	s.logger.Info("Active running queries filtering summary",
		zap.Int("total_fetched", len(results)),
		zap.Int("filtered_out", filteredCount),
		zap.Int("processed", processedCount),
		zap.Int("elapsed_time_threshold_ms", elapsedTimeThreshold))

	// Return the active queries for further processing (e.g., fetching top 5 plan handles)
	return results, nil
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

		// Add SQL Server's decoded wait resource names (from SQL query)
		if result.WaitResourceObjectName != nil {
			attrs.PutStr("wait_resource_object_name", *result.WaitResourceObjectName)
		}
		if result.WaitResourceDatabaseName != nil {
			attrs.PutStr("wait_resource_database_name", *result.WaitResourceDatabaseName)
		}

		// Add Go helper's parsed resource type and base description
		resourceType, resourceDesc := helpers.DecodeWaitResource(waitResource)
		attrs.PutStr("wait_resource_type", resourceType)

		// Build enhanced wait_resource_description based on lock type
		enhancedDesc := resourceDesc // Start with base description

		// Enhanced wait resource decoding from SQL joins
		// For OBJECT locks (table-level locks)
		if result.WaitResourceSchemaNameObject != nil && result.WaitResourceObjectName != nil {
			attrs.PutStr("wait_resource_schema_name", *result.WaitResourceSchemaNameObject)
			attrs.PutStr("wait_resource_table_name", *result.WaitResourceObjectName)

			// Build enhanced description: "Database: X | Schema: Y | Table: Z | Type: USER_TABLE"
			dbName := ""
			if result.WaitResourceDatabaseName != nil {
				dbName = *result.WaitResourceDatabaseName
			}
			objectType := ""
			if result.WaitResourceObjectType != nil {
				attrs.PutStr("wait_resource_object_type", *result.WaitResourceObjectType)
				objectType = *result.WaitResourceObjectType
			}

			enhancedDesc = fmt.Sprintf("Database: %s | Schema: %s | Table: %s | Type: %s",
				dbName, *result.WaitResourceSchemaNameObject, *result.WaitResourceObjectName, objectType)
		}

		// For INDEX locks (KEY locks - row-level locks)
		// Parse HOBT ID from wait_resource and look up in MetadataCache
		if resourceType == "Key Lock" && s.metadataCache != nil {
			// KEY wait_resource format: "KEY: <db_id>:<hobt_id> (<key_hash>)"
			// Example: "KEY: 5:72057594042908672 (e5e11ab44f5d)"
			hobtID := helpers.ParseHOBTIDFromWaitResource(waitResource)
			s.logger.Debug("Parsing KEY lock wait_resource",
				zap.String("wait_resource", waitResource),
				zap.Int64("parsed_hobt_id", hobtID))
			if hobtID > 0 {
				if hobtMetadata, ok := s.metadataCache.GetHOBTMetadata(hobtID); ok {
					s.logger.Debug("Found HOBT metadata in cache",
						zap.Int64("hobt_id", hobtID),
						zap.String("database", hobtMetadata.DatabaseName),
						zap.String("schema", hobtMetadata.SchemaName),
						zap.String("table", hobtMetadata.ObjectName),
						zap.String("index", hobtMetadata.IndexName))
					// Add attributes from MetadataCache lookup
					attrs.PutStr("wait_resource_schema_name", hobtMetadata.SchemaName)
					attrs.PutStr("wait_resource_table_name", hobtMetadata.ObjectName)
					attrs.PutStr("wait_resource_index_name", hobtMetadata.IndexName)
					attrs.PutStr("wait_resource_index_type", hobtMetadata.IndexType)

					// Build enhanced description using cached metadata
					dbName := hobtMetadata.DatabaseName
					if result.WaitResourceDatabaseName != nil {
						dbName = *result.WaitResourceDatabaseName // Prefer real-time database name if available
					}

					enhancedDesc = fmt.Sprintf("Database: %s | Schema: %s | Table: %s | Index: %s (%s)",
						dbName, hobtMetadata.SchemaName, hobtMetadata.ObjectName, hobtMetadata.IndexName, hobtMetadata.IndexType)
				} else {
					s.logger.Debug("HOBT ID not found in metadata cache",
						zap.Int64("hobt_id", hobtID),
						zap.String("wait_resource", waitResource))
				}
			}
		}

		// Set the final enhanced description
		attrs.PutStr("wait_resource_description", enhancedDesc)
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

// fetchExecutionPlanXML fetches the execution plan XML for a given plan_handle
// Simple wrapper for use by logs endpoint
func (s *QueryPerformanceScraper) fetchExecutionPlanXML(ctx context.Context, planHandle *models.QueryID) (string, error) {
	if planHandle == nil || planHandle.IsEmpty() {
		return "", nil
	}

	planHandleHex := planHandle.String()
	query := fmt.Sprintf(queries.ActiveQueryExecutionPlanQuery, planHandleHex)

	s.logger.Debug("Fetching execution plan XML",
		zap.String("plan_handle", planHandleHex))

	var results []struct {
		ExecutionPlanXML *string `db:"execution_plan_xml"`
	}

	if err := s.connection.Query(ctx, &results, query); err != nil {
		return "", fmt.Errorf("failed to fetch execution plan: %w", err)
	}

	if len(results) == 0 || results[0].ExecutionPlanXML == nil {
		return "", nil
	}

	return *results[0].ExecutionPlanXML, nil
}

// parseAndEmitExecutionPlanAsLogs parses execution plan XML and emits as OTLP logs
// Converts XML execution plan to custom events using attributes (not JSON body)
func (s *QueryPerformanceScraper) parseAndEmitExecutionPlanAsLogs(
	ctx context.Context,
	activeQuery models.ActiveRunningQuery,
	executionPlanXML string,
	logs plog.Logs,
) error {
	// Build execution plan analysis with metadata
	var queryID string
	if activeQuery.QueryID != nil {
		queryID = activeQuery.QueryID.String()
	}
	var planHandle string
	if activeQuery.PlanHandle != nil {
		planHandle = activeQuery.PlanHandle.String()
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
// - emitSlowQueryPlanMetrics: Emits metrics with clean historical context (no session_id/request_id)
//
// Benefits of the new approach:
// 1. Plans available even when query is NOT currently running
// 2. No duplicate fetches per active execution (fetched once per unique query_id)
// 3. Clean historical context without misleading active session attributes
