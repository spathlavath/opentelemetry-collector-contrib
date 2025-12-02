// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package scrapers provides the performance monitoring scraper for SQL Server.
// This file implements comprehensive query performance monitoring including
// slow query analysis, wait time statistics, blocking session detection, and execution plan analysis.
//
// Performance Monitoring Features:
//
// 1. Top N Slow Queries:
//   - Query dm_exec_query_stats for execution statistics
//   - Rank by total_elapsed_time, avg_elapsed_time, execution_count
//   - Include query text from dm_exec_sql_text()
//   - Track CPU time, logical reads, physical reads, writes
//   - Monitor plan generation time and recompiles
//
// 2. Blocking Session Detection:
//   - Query dm_exec_requests for blocked sessions
//   - Identify blocking session hierarchy (head blockers)
//   - Track blocked session duration and wait types
//   - Include session details (login, program, host)
//   - Monitor lock resource information
//
// 3. Execution Plan Analysis:
//   - Query dm_exec_cached_plans for plan cache statistics
//   - Track plan reuse ratio and cache hit rates
//   - Monitor plan cache memory consumption
//   - Identify ad-hoc vs prepared statement ratios
//   - Include parameterized vs non-parameterized queries
//
// Scraper Structure:
//
//	type PerformanceScraper struct {
//	    config   *Config
//	    mb       *metadata.MetricsBuilder
//	    queries  *queries.PerformanceQueries
//	    logger   *zap.Logger
//	}
//
// Metrics Generated:
// - sqlserver.slowquery.avg_cpu_time_ms
// - sqlserver.slowquery.avg_disk_reads
// - sqlserver.slowquery.avg_disk_writes
// - sqlserver.slowquery.rows_processed
// - sqlserver.slowquery.avg_elapsed_time_ms
// - sqlserver.slowquery.execution_count
// - sqlserver.slowquery.query_text
// - sqlserver.slowquery.query_id
// - sqlserver.blocking_query.wait_time_seconds
// - sqlserver.blocked_query.wait_time_seconds
// - sqlserver.wait_analysis.query_text
// - sqlserver.wait_analysis.total_wait_time_ms
// - sqlserver.wait_analysis.avg_wait_time_ms
// - sqlserver.wait_analysis.wait_event_count
// - sqlserver.wait_analysis.last_execution_time
//
// Engine-Specific Considerations:
// - Azure SQL Database: Limited access to some DMVs, use Azure-specific alternatives
// - Azure SQL Managed Instance: Most performance DMVs available
// - Standard SQL Server: Full access to all performance monitoring DMVs
package scrapers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// QueryPerformanceScraper handles SQL Server query performance monitoring metrics collection
type QueryPerformanceScraper struct {
	connection          SQLConnectionInterface
	logger              *zap.Logger
	startTime           pcommon.Timestamp
	engineEdition       int
	executionPlanLogger *models.ExecutionPlanLogger
	logConsumer         plog.Logs                     // For emitting execution plan logs
	slowQuerySmoother   *SlowQuerySmoother            // EWMA-based smoothing algorithm for slow queries
	intervalCalculator  *SimplifiedIntervalCalculator // Simplified delta-based interval calculator
	metadataCache       *helpers.MetadataCache        // Metadata cache for wait resource enrichment
}

// NewQueryPerformanceScraper creates a new query performance scraper with smoothing and interval calculator configuration
func NewQueryPerformanceScraper(
	conn SQLConnectionInterface,
	logger *zap.Logger,
	engineEdition int,
	smoothingEnabled bool,
	smoothingFactor float64,
	decayThreshold int,
	maxAgeMinutes int,
	intervalCalcEnabled bool,
	intervalCalcCacheTTLMinutes int,
	metadataCache *helpers.MetadataCache,
) *QueryPerformanceScraper {
	var smoother *SlowQuerySmoother
	var intervalCalc *SimplifiedIntervalCalculator

	// Initialize EWMA-based smoother if enabled
	if smoothingEnabled {
		// Initialize smoother with configured parameters
		maxAge := time.Duration(maxAgeMinutes) * time.Minute
		smoother = NewSlowQuerySmoother(logger, smoothingFactor, decayThreshold, maxAge)
		logger.Info("Slow query EWMA smoothing enabled",
			zap.Float64("smoothing_factor", smoothingFactor),
			zap.Int("decay_threshold", decayThreshold),
			zap.Int("max_age_minutes", maxAgeMinutes))
	} else {
		logger.Info("Slow query EWMA smoothing disabled")
	}

	// Initialize simplified interval-based calculator if enabled
	if intervalCalcEnabled {
		cacheTTL := time.Duration(intervalCalcCacheTTLMinutes) * time.Minute
		intervalCalc = NewSimplifiedIntervalCalculator(logger, cacheTTL)
		logger.Info("Simplified interval-based delta calculator enabled",
			zap.Int("cache_ttl_minutes", intervalCalcCacheTTLMinutes))
	} else {
		logger.Info("Simplified interval-based delta calculator disabled")
	}

	return &QueryPerformanceScraper{
		connection:          conn,
		logger:              logger,
		startTime:           pcommon.NewTimestampFromTime(time.Now()),
		engineEdition:       engineEdition,
		executionPlanLogger: models.NewExecutionPlanLogger(),
		slowQuerySmoother:   smoother,
		intervalCalculator:  intervalCalc,
		metadataCache:       metadataCache,
	}
}

// ScrapeSlowQueryMetrics collects slow query performance monitoring metrics with interval-based averaging and/or EWMA smoothing
// Returns the processed slow queries for downstream correlation (e.g., filtering active queries by slow query IDs)
func (s *QueryPerformanceScraper) ScrapeSlowQueryMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit int) ([]models.SlowQuery, error) {

	query := fmt.Sprintf(queries.SlowQuery, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit)

	s.logger.Debug("Executing slow query metrics collection",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("interval_seconds", intervalSeconds),
		zap.Int("top_n", topN),
		zap.Int("elapsed_time_threshold", elapsedTimeThreshold),
		zap.Bool("interval_calc_enabled", s.intervalCalculator != nil),
		zap.Bool("ewma_smoothing_enabled", s.slowQuerySmoother != nil))

	var rawResults []models.SlowQuery
	if err := s.connection.Query(ctx, &rawResults, query); err != nil {
		return nil, fmt.Errorf("failed to execute slow query metrics query: %w", err)
	}

	s.logger.Debug("Raw slow query metrics fetched", zap.Int("raw_result_count", len(rawResults)))

	// Fetch currently active queries to backfill slow query metrics
	// This ensures all active queries have corresponding slow query metrics for complete correlation
	activeQueryStatsQuery := fmt.Sprintf(queries.ActiveQueryStatsForSlowQueryUnion, textTruncateLimit)
	var activeQueryStats []models.SlowQuery
	if err := s.connection.Query(ctx, &activeQueryStats, activeQueryStatsQuery); err != nil {
		s.logger.Warn("Failed to fetch active query stats for union, continuing without them",
			zap.Error(err))
	} else {
		s.logger.Debug("Active query stats fetched for union", zap.Int("active_count", len(activeQueryStats)))

		// Merge active queries with slow queries
		// Use a map to deduplicate by (query_hash, plan_handle)
		// Priority: Keep slow query stat if it exists, otherwise use active query stat
		queryMap := make(map[string]models.SlowQuery)

		// First, add all slow queries from dm_exec_query_stats
		for _, sq := range rawResults {
			if sq.QueryID != nil && sq.PlanHandle != nil {
				key := sq.QueryID.String() + "|" + sq.PlanHandle.String()
				queryMap[key] = sq
			}
		}

		// Then, add active queries that aren't already in the map
		addedCount := 0
		for _, aq := range activeQueryStats {
			if aq.QueryID != nil && aq.PlanHandle != nil {
				key := aq.QueryID.String() + "|" + aq.PlanHandle.String()
				if _, exists := queryMap[key]; !exists {
					queryMap[key] = aq
					addedCount++
				}
			}
		}

		s.logger.Debug("Merged active queries with slow queries",
			zap.Int("slow_query_count", len(rawResults)),
			zap.Int("active_query_count", len(activeQueryStats)),
			zap.Int("added_from_active", addedCount),
			zap.Int("total_after_merge", len(queryMap)))

		// Convert map back to slice
		rawResults = make([]models.SlowQuery, 0, len(queryMap))
		for _, sq := range queryMap {
			rawResults = append(rawResults, sq)
		}
	}

	// Apply simplified interval-based delta calculation if enabled
	var resultsWithIntervalMetrics []models.SlowQuery
	if s.intervalCalculator != nil {
		now := time.Now()

		// Pre-allocate slice capacity to avoid reallocations (performance optimization)
		// Note: Length is still 0, capacity reserves space for worst case (all queries pass filters)
		resultsWithIntervalMetrics = make([]models.SlowQuery, 0, len(rawResults))

		// Calculate interval metrics for each query
		for _, rawQuery := range rawResults {
			metrics := s.intervalCalculator.CalculateMetrics(&rawQuery, now)

			if metrics == nil {
				s.logger.Debug("Skipping query with nil metrics")
				continue
			}

			// Skip queries with no new executions
			if !metrics.HasNewExecutions {
				s.logger.Debug("Skipping query with no new executions",
					zap.String("query_id", rawQuery.QueryID.String()),
					zap.Float64("time_since_last_exec_sec", metrics.TimeSinceLastExecSec))
				continue
			}

			// Apply interval-based threshold filtering
			// First scrape: interval = historical (no baseline), filter still applies
			// Subsequent scrapes: interval = delta, filter on delta performance
			if metrics.IntervalAvgElapsedTimeMs < float64(elapsedTimeThreshold) {
				s.logger.Debug("Skipping query below interval threshold",
					zap.String("query_id", rawQuery.QueryID.String()),
					zap.Float64("interval_avg_ms", metrics.IntervalAvgElapsedTimeMs),
					zap.Float64("historical_avg_ms", metrics.HistoricalAvgElapsedTimeMs),
					zap.Int("threshold_ms", elapsedTimeThreshold),
					zap.Bool("is_first_scrape", metrics.IsFirstScrape))
				continue
			}

			// Store interval metrics in new fields (preserve historical values)
			// Historical values (AvgElapsedTimeMS, AvgCPUTimeMS, ExecutionCount) remain unchanged from DB
			// NOTE: Only elapsed time has delta calculation, CPU uses historical average
			rawQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			rawQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

			resultsWithIntervalMetrics = append(resultsWithIntervalMetrics, rawQuery)
		}

		// Cleanup stale entries periodically (TTL-based only)
		s.intervalCalculator.CleanupStaleEntries(now)

		s.logger.Debug("Simplified interval-based delta calculation applied",
			zap.Int("raw_count", len(rawResults)),
			zap.Int("processed_count", len(resultsWithIntervalMetrics)))

		// Log calculator statistics
		stats := s.intervalCalculator.GetCacheStats()
		s.logger.Debug("Interval calculator statistics", zap.Any("stats", stats))

		// Use interval-calculated results as input for next step
		rawResults = resultsWithIntervalMetrics
	}

	// Apply Go-level sorting and top-N selection AFTER delta calculation and filtering
	// This ensures we get the slowest queries based on interval (delta) averages, not historical
	if s.intervalCalculator != nil && len(rawResults) > 0 {
		// Sort by interval average elapsed time (delta) - slowest first
		sort.Slice(rawResults, func(i, j int) bool {
			// Handle nil cases - queries without interval metrics go to the end
			if rawResults[i].IntervalAvgElapsedTimeMS == nil {
				return false
			}
			if rawResults[j].IntervalAvgElapsedTimeMS == nil {
				return true
			}
			// Sort descending (highest delta average first)
			return *rawResults[i].IntervalAvgElapsedTimeMS > *rawResults[j].IntervalAvgElapsedTimeMS
		})

		s.logger.Debug("Sorted queries by interval average elapsed time (delta)",
			zap.Int("total_count", len(rawResults)))

		// Take top N queries after sorting
		if len(rawResults) > topN {
			s.logger.Debug("Applying top N selection",
				zap.Int("before_count", len(rawResults)),
				zap.Int("top_n", topN))
			rawResults = rawResults[:topN]
		}
	}

	// Apply EWMA smoothing algorithm if enabled (can work on top of interval metrics or raw)
	var resultsToProcess []models.SlowQuery
	if s.slowQuerySmoother != nil {
		// Smoothing is enabled - apply the algorithm
		resultsToProcess = s.slowQuerySmoother.Smooth(rawResults)

		s.logger.Debug("EWMA smoothing applied",
			zap.Int("input_count", len(rawResults)),
			zap.Int("smoothed_count", len(resultsToProcess)))

		// Log smoother statistics
		stats := s.slowQuerySmoother.GetHistoryStats()
		s.logger.Debug("EWMA smoother statistics", zap.Any("stats", stats))
	} else {
		// Smoothing is disabled - use results from interval calculator or raw
		resultsToProcess = rawResults
		s.logger.Debug("EWMA smoothing disabled, using interval-calculated or raw results")
	}

	// Process results (interval-calculated + smoothed, or just one, or raw)
	for i, result := range resultsToProcess {
		if err := s.processSlowQueryMetrics(result, scopeMetrics, i); err != nil {
			s.logger.Error("Failed to process slow query metric", zap.Error(err), zap.Int("index", i))
		}
	}

	// Return processed results for downstream correlation (e.g., filtering active queries)
	return resultsToProcess, nil
}

// ScrapeSlowQueryExecutionPlans - REMOVED: No longer fetch top 5 plans per slow query
// Plan metrics are now only emitted for active queries via ScrapeActiveQueryExecutionPlans
// This function is now a no-op to maintain interface compatibility
func (s *QueryPerformanceScraper) ScrapeSlowQueryExecutionPlans(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, slowQueries []models.SlowQuery) error {
	s.logger.Debug("ScrapeSlowQueryExecutionPlans called but disabled - plan metrics only for active queries")
	return nil
}

// ScrapeActiveQueryExecutionPlans fetches execution statistics for each unique plan_handle from active running queries
// This is called AFTER active query scraping to get historical plan statistics with active query correlation
// KEY CHANGE: Instead of fetching top 5 plans per query_hash, we fetch stats for the EXACT plan_handle currently executing
// Benefits:
//   - No redundancy: ActiveRunningQueriesQuery already includes execution_plan_xml
//   - Direct correlation: Fetch stats for the plan that's actually running (not historical top 5)
//   - Emits metrics WITH active query correlation (session_id, request_id, request_start_time)
func (s *QueryPerformanceScraper) ScrapeActiveQueryExecutionPlans(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, activeQueries []models.ActiveRunningQuery) error {
	if len(activeQueries) == 0 {
		s.logger.Debug("No active queries to fetch execution statistics for")
		return nil
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())
	totalStatsFound := 0

	// Collect unique plan_handles from active queries to avoid duplicate fetches
	uniquePlanHandles := make(map[string]models.ActiveRunningQuery)
	for _, activeQuery := range activeQueries {
		if activeQuery.PlanHandle == nil || activeQuery.PlanHandle.IsEmpty() {
			continue
		}
		planHandleStr := activeQuery.PlanHandle.String()
		// Keep first occurrence for correlation attributes
		if _, exists := uniquePlanHandles[planHandleStr]; !exists {
			uniquePlanHandles[planHandleStr] = activeQuery
		}
	}

	s.logger.Info("Fetching execution statistics for active running query plan_handles",
		zap.Int("active_query_count", len(activeQueries)),
		zap.Int("unique_plan_handle_count", len(uniquePlanHandles)))

	// Fetch execution statistics for each unique plan_handle
	for _, activeQuery := range uniquePlanHandles {
		// Extract correlation attributes for logging
		sessionID := int64(0)
		if activeQuery.CurrentSessionID != nil {
			sessionID = *activeQuery.CurrentSessionID
		}
		requestID := int64(0)
		if activeQuery.RequestID != nil {
			requestID = *activeQuery.RequestID
		}

		planHandleHex := activeQuery.PlanHandle.String()
		query := fmt.Sprintf(queries.ExecutionStatsForActivePlanHandleQuery, planHandleHex)

		s.logger.Debug("Fetching execution statistics for active query plan_handle",
			zap.String("plan_handle", planHandleHex),
			zap.Int64("session_id", sessionID),
			zap.Int64("request_id", requestID))

		var planResults []models.PlanHandleResult
		if err := s.connection.Query(ctx, &planResults, query); err != nil {
			s.logger.Warn("Failed to fetch execution statistics for active query plan_handle - continuing with others",
				zap.Error(err),
				zap.String("plan_handle", planHandleHex),
				zap.Int64("session_id", sessionID))
			continue
		}

		if len(planResults) == 0 {
			s.logger.Debug("No execution statistics found for active query plan_handle",
				zap.String("plan_handle", planHandleHex),
				zap.Int64("session_id", sessionID))
			continue
		}

		// Should only be 1 result (query by plan_handle, not query_hash)
		if len(planResults) > 1 {
			s.logger.Warn("Expected 1 result for plan_handle query, got multiple - using first",
				zap.String("plan_handle", planHandleHex),
				zap.Int("result_count", len(planResults)))
		}

		// Emit metrics for this plan_handle WITH active query correlation
		planResult := planResults[0]
		s.emitActiveQueryPlanMetrics(planResult, activeQuery, scopeMetrics, timestamp)

		totalStatsFound++
		s.logger.Debug("Fetched and emitted execution statistics for active query plan_handle",
			zap.String("plan_handle", planHandleHex),
			zap.Int64("session_id", sessionID))
	}

	s.logger.Info("Successfully fetched execution statistics for active running query plan_handles",
		zap.Int("active_query_count", len(activeQueries)),
		zap.Int("unique_plan_handles", len(uniquePlanHandles)),
		zap.Int("stats_found", totalStatsFound))

	return nil
}

// emitActiveQueryPlanMetrics emits execution plan metrics for an active running query
// These metrics represent HISTORICAL statistics from dm_exec_query_stats (same as slow query plans)
// Uses namespace: sqlserver.plan.* (SAME as slow query plans)
// Context: Active query drill-down (WITH session_id/request_id/request_start_time for correlation)
// The key difference from slow query plans is the presence of session correlation attributes
func (s *QueryPerformanceScraper) emitActiveQueryPlanMetrics(planResult models.PlanHandleResult, activeQuery models.ActiveRunningQuery, scopeMetrics pmetric.ScopeMetrics, timestamp pcommon.Timestamp) {
	// Helper to create common attributes with active query correlation
	createAttrs := func() pcommon.Map {
		attrs := pcommon.NewMap()

		// Correlation keys
		if planResult.QueryID != nil {
			attrs.PutStr("query_id", planResult.QueryID.String())
		}
		if planResult.PlanHandle != nil {
			attrs.PutStr("plan_handle", planResult.PlanHandle.String())
		}
		if planResult.QueryPlanHash != nil {
			attrs.PutStr("query_plan_hash", planResult.QueryPlanHash.String())
		}

		// ACTIVE QUERY CORRELATION ATTRIBUTES (key requirement!)
		if activeQuery.CurrentSessionID != nil {
			attrs.PutInt("session_id", *activeQuery.CurrentSessionID)
		}
		if activeQuery.RequestID != nil {
			attrs.PutInt("request_id", *activeQuery.RequestID)
		}
		if activeQuery.RequestStartTime != nil {
			attrs.PutStr("request_start_time", *activeQuery.RequestStartTime)
		}

		// Timestamps
		if planResult.LastExecutionTime != nil {
			attrs.PutStr("last_execution_time", *planResult.LastExecutionTime)
		}
		if planResult.CreationTime != nil {
			attrs.PutStr("creation_time", *planResult.CreationTime)
		}

		// Context from active query
		if activeQuery.DatabaseName != nil {
			attrs.PutStr("database_name", *activeQuery.DatabaseName)
		}
		if activeQuery.SchemaName != nil {
			attrs.PutStr("schema_name", *activeQuery.SchemaName)
		}

		return attrs
	}

	// Metric 1: Execution count
	if planResult.ExecutionCount != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.execution_count")
		metric.SetDescription("Total number of executions for this execution plan (historical)")
		metric.SetUnit("{executions}")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(*planResult.ExecutionCount)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 2: Average elapsed time
	if planResult.AvgElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.avg_elapsed_time_ms")
		metric.SetDescription("Average elapsed time per execution of this plan (historical)")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.AvgElapsedTimeMs)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 3: Total elapsed time
	if planResult.TotalElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.total_elapsed_time_ms")
		metric.SetDescription("Total elapsed time across all executions of this plan (historical)")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.TotalElapsedTimeMs)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 4: Min/Max elapsed time
	if planResult.MinElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.min_elapsed_time_ms")
		metric.SetDescription("Minimum elapsed time for this plan (historical)")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.MinElapsedTimeMs)
		createAttrs().CopyTo(dp.Attributes())
	}

	if planResult.MaxElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.max_elapsed_time_ms")
		metric.SetDescription("Maximum elapsed time for this plan (historical)")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.MaxElapsedTimeMs)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 5: Average worker time (CPU)
	if planResult.AvgWorkerTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.avg_worker_time_ms")
		metric.SetDescription("Average CPU time per execution of this plan (historical)")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.AvgWorkerTimeMs)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 6: Average logical reads
	if planResult.AvgLogicalReads != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.avg_logical_reads")
		metric.SetDescription("Average logical reads per execution of this plan (historical)")
		metric.SetUnit("{reads}")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.AvgLogicalReads)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 7: Average logical writes
	if planResult.AvgLogicalWrites != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.avg_logical_writes")
		metric.SetDescription("Average logical writes per execution of this plan (historical)")
		metric.SetUnit("{writes}")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.AvgLogicalWrites)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 8: Last DOP (degree of parallelism)
	if planResult.LastDOP != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.last_dop")
		metric.SetDescription("Degree of parallelism for last execution of this plan")
		metric.SetUnit("{threads}")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(int64(*planResult.LastDOP))
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 9: Last grant KB (memory grant)
	if planResult.LastGrantKB != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.last_grant_kb")
		metric.SetDescription("Memory grant for last execution of this plan")
		metric.SetUnit("KB")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(*planResult.LastGrantKB)
		createAttrs().CopyTo(dp.Attributes())
	}

	// Metric 10: Last spills (tempdb spills)
	if planResult.LastSpills != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.plan.last_spills")
		metric.SetDescription("TempDB spills for last execution of this plan")
		metric.SetUnit("{pages}")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(*planResult.LastSpills)
		createAttrs().CopyTo(dp.Attributes())
	}
}

// ScrapeBlockingSessionMetrics collects blocking session metrics
// func (s *QueryPerformanceScraper) ScrapeBlockingSessionMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, limit, textTruncateLimit int) error {
// 	query := fmt.Sprintf(queries.BlockingSessionsQuery, limit, textTruncateLimit)

// 	s.logger.Debug("Executing blocking session metrics collection",
// 		zap.String("query", queries.TruncateQuery(query, 100)),
// 		zap.Int("limit", limit),
// 		zap.Int("text_truncate_limit", textTruncateLimit))

// 	var results []models.BlockingSession
// 	if err := s.connection.Query(ctx, &results, query); err != nil {
// 		return fmt.Errorf("failed to execute blocking session metrics query: %w", err)
// 	}

// 	s.logger.Debug("Blocking session metrics fetched", zap.Int("result_count", len(results)))

// 	for i, result := range results {
// 		if err := s.processBlockingSessionMetrics(result, scopeMetrics, i); err != nil {
// 			s.logger.Error("Failed to process blocking session metric", zap.Error(err), zap.Int("index", i))
// 		}
// 	}

// 	return nil
// }

// REMOVED: ScrapeWaitTimeAnalysisMetrics - Used Query Store views
// This functionality has been replaced by the new active query monitoring approach
// which gets wait information directly from sys.dm_exec_requests
//
// func (s *QueryPerformanceScraper) ScrapeWaitTimeAnalysisMetrics(...) error { ... }

// REMOVED: ScrapeQueryExecutionPlanMetrics - Redundant fallback scraper
//
// This function has been removed because execution plans are already included in:
// 1. SlowQuery - via CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) in the SQL query
// 2. ActiveRunningQuery - via OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) in the SQL query
//
// The separate scraper was redundant and added unnecessary complexity.
// All execution plan data is now collected directly in the primary scrapers.

// processSlowQueryMetrics processes slow query metrics and creates separate OpenTelemetry metrics for each measurement
// CARDINALITY-SAFE: Implements controlled attribute strategy to prevent metric explosion
func (s *QueryPerformanceScraper) processSlowQueryMetrics(result models.SlowQuery, scopeMetrics pmetric.ScopeMetrics, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// CARDINALITY-SAFE: Create limited common attributes (query_id + plan_handle as composite key)
	// plan_handle is included because delta calculation tracks state per (query_id, plan_handle)
	// and same query can have multiple execution plans with different performance characteristics
	createSafeAttributes := func() pcommon.Map {
		attrs := pcommon.NewMap()
		if result.QueryID != nil {
			attrs.PutStr("query_id", result.QueryID.String())
		}
		if result.PlanHandle != nil {
			attrs.PutStr("plan_handle", result.PlanHandle.String())
		}
		if result.DatabaseName != nil {
			attrs.PutStr("database_name", *result.DatabaseName)
		}
		if result.SchemaName != nil {
			attrs.PutStr("schema_name", *result.SchemaName)
		}
		if result.StatementType != nil {
			attrs.PutStr("statement_type", *result.StatementType)
		}
		// Include query_text for observability - already truncated by SQL query (@TextTruncateLimit)
		// Cardinality is controlled by query_id grouping (parameterized queries share same query_id)
		if result.QueryText != nil {
			attrs.PutStr("query_text", *result.QueryText)
		}
		// NOTE: NOT including CollectionTimestamp as attribute to prevent additional cardinality
		return attrs
	}

	// Create attributes WITH timestamps - ONLY for primary metric (avg_elapsed_time_ms)
	// This enables time-based filtering on the most commonly queried performance metric
	createSafeAttributesWithTimestamps := func() pcommon.Map {
		attrs := createSafeAttributes()
		// Add ISO 8601 timestamp strings (not Unix epoch numbers)
		if result.CollectionTimestamp != nil {
			attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
		}
		if result.LastExecutionTimestamp != nil {
			attrs.PutStr("last_execution_timestamp", *result.LastExecutionTimestamp)
		}
		return attrs
	}

	// Create detailed attributes for logging/debugging (not used in metrics)
	logAttributes := func() []zap.Field {
		var fields []zap.Field
		if result.QueryID != nil {
			fields = append(fields, zap.String("query_id", result.QueryID.String()))
		}
		if result.PlanHandle != nil {
			fields = append(fields, zap.String("plan_handle", result.PlanHandle.String()))
		}
		if result.DatabaseName != nil {
			fields = append(fields, zap.String("database_name", *result.DatabaseName))
		}
		if result.QueryText != nil {
			// Anonymize and truncate query text for logging
			anonymizedSQL := helpers.SafeAnonymizeQueryText(result.QueryText)
			if len(anonymizedSQL) > 100 {
				anonymizedSQL = anonymizedSQL[:100] + "..."
			}
			fields = append(fields, zap.String("query_text_preview", anonymizedSQL))
		}
		if result.CollectionTimestamp != nil {
			fields = append(fields, zap.String("collection_timestamp", *result.CollectionTimestamp))
		}
		if result.LastExecutionTimestamp != nil {
			fields = append(fields, zap.String("last_execution_timestamp", *result.LastExecutionTimestamp))
		}
		return fields
	}

	// Create avg_cpu_time_ms metric (historical/cumulative only, no delta) - CARDINALITY SAFE
	if result.AvgCPUTimeMS != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.historical_avg_cpu_time_ms")
		metric.SetDescription("Historical average CPU time in milliseconds (cumulative since plan cached)")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.AvgCPUTimeMS)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Create avg_disk_reads metric - CARDINALITY SAFE
	if result.AvgDiskReads != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.avg_disk_reads")
		metric.SetDescription("Average disk reads for slow query")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.AvgDiskReads)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Create avg_disk_writes metric - CARDINALITY SAFE
	if result.AvgDiskWrites != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.avg_disk_writes")
		metric.SetDescription("Average disk writes for slow query")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.AvgDiskWrites)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Create avg_rows_processed metric - CARDINALITY SAFE
	if result.AvgRowsProcessed != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.avg_rows_processed")
		metric.SetDescription("Average rows processed (returned) per execution for slow query")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.AvgRowsProcessed)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	} else {
		s.logger.Debug("AvgRowsProcessed is nil for slow query", zap.Int("index", index))
	}

	// Create avg_elapsed_time_ms metric (historical/cumulative) - CARDINALITY SAFE
	if result.AvgElapsedTimeMS != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.historical_avg_elapsed_time_ms")
		metric.SetDescription("Historical average elapsed time in milliseconds (cumulative since plan cached)")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.AvgElapsedTimeMS)
		// Use timestamp attributes for this primary metric only
		createSafeAttributesWithTimestamps().CopyTo(dataPoint.Attributes())
	}

	// Create interval_avg_elapsed_time_ms metric (delta) - CARDINALITY SAFE
	if result.IntervalAvgElapsedTimeMS != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.interval_avg_elapsed_time_ms")
		metric.SetDescription("Interval average elapsed time in milliseconds (delta for this collection interval)")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.IntervalAvgElapsedTimeMS)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Create execution_count metric (historical/cumulative) - CARDINALITY SAFE
	if result.ExecutionCount != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.historical_execution_count")
		metric.SetDescription("Historical execution count (cumulative since plan cached)")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetIntValue(*result.ExecutionCount)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Create interval_execution_count metric (delta) - CARDINALITY SAFE
	if result.IntervalExecutionCount != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.interval_execution_count")
		metric.SetDescription("Interval execution count (delta for this collection interval)")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetIntValue(*result.IntervalExecutionCount)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// NOTE: Removed separate query_id and plan_handle metrics as these attributes
	// are now included in ALL slow query metrics via createSafeAttributes()
	// This eliminates redundant metric emission

	// Create query_text metric with cardinality control
	if result.QueryText != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.query_text")
		metric.SetDescription("Query text for slow query")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetIntValue(1) // Dummy value since this is primarily for the string attribute

		attrs := createSafeAttributes()
		// Safely anonymize query text with size limits to control cardinality
		anonymizedText := helpers.SafeAnonymizeQueryText(result.QueryText)
		// Truncate to prevent attribute size issues
		if len(anonymizedText) > 1000 {
			anonymizedText = anonymizedText[:1000] + "...[truncated]"
		}
		attrs.PutStr("query_text", anonymizedText)

		// Add computed query_signature for cross-instance correlation
		// This is a SHA256 hash of the normalized query, providing a secondary correlation method
		querySignature := helpers.ComputeQueryHash(*result.QueryText)
		if querySignature != "" {
			attrs.PutStr("query_signature", querySignature)
		}

		attrs.CopyTo(dataPoint.Attributes())
	}

	// ========================================
	// RCA ENHANCEMENT METRICS
	// ========================================

	// Min/Max/Last Elapsed Time Metrics
	if result.MinElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.min_elapsed_time_ms")
		metric.SetDescription("Minimum elapsed time in milliseconds")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.MinElapsedTimeMs)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	if result.MaxElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.max_elapsed_time_ms")
		metric.SetDescription("Maximum elapsed time in milliseconds")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.MaxElapsedTimeMs)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	if result.LastElapsedTimeMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.last_elapsed_time_ms")
		metric.SetDescription("Last elapsed time in milliseconds")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.LastElapsedTimeMs)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Memory Grant Metrics
	if result.LastGrantKB != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.last_grant_kb")
		metric.SetDescription("Last memory grant in KB")
		metric.SetUnit("KB")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.LastGrantKB)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	if result.LastUsedGrantKB != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.last_used_grant_kb")
		metric.SetDescription("Last used memory grant in KB")
		metric.SetUnit("KB")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.LastUsedGrantKB)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// TempDB Spill Metrics
	if result.LastSpills != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.last_spills")
		metric.SetDescription("Last TempDB spills count")
		metric.SetUnit("1")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.LastSpills)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	if result.MaxSpills != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.max_spills")
		metric.SetDescription("Maximum TempDB spills count")
		metric.SetUnit("1")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.MaxSpills)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Parallelism Metrics
	if result.LastDOP != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.slowquery.last_dop")
		metric.SetDescription("Last degree of parallelism")
		metric.SetUnit("1")
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.LastDOP)
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Use dedicated logging function with cardinality-safe approach
	s.logger.Debug("Processed slow query metrics with cardinality safety", logAttributes()...)

	return nil
}



// processQueryExecutionPlanMetrics processes query execution plan metrics with cardinality safety
func (s *QueryPerformanceScraper) processQueryExecutionPlanMetrics(result models.QueryExecutionPlan, scopeMetrics pmetric.ScopeMetrics, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// CARDINALITY-SAFE: Create limited common attributes
	// Only include query_id as the primary identifier, avoid full SQL text and multiple hashes
	createSafeAttributes := func() pcommon.Map {
		attrs := pcommon.NewMap()
		if result.QueryID != nil {
			attrs.PutStr("query_id", result.QueryID.String())
		}
		if result.PlanHandle != nil {
			attrs.PutStr("plan_handle", result.PlanHandle.String())
		}
		if result.QueryPlanID != nil {
			attrs.PutStr("query_plan_id", result.QueryPlanID.String())
		}
		// NOTE: execution_plan_xml is NOT included in metrics to avoid data duplication
		// Execution plans are already sent as parsed logs via scrapeLogs() with full operator details
		// If you need the XML, query the logs: FROM Log WHERE eventName = 'sqlserver.execution_plan_operator'

		// Add SQL text (anonymized)
		if result.SQLText != nil {
			anonymizedSQL := helpers.AnonymizeQueryText(*result.SQLText)
			attrs.PutStr("query_text", anonymizedSQL)
		}
		// Add timestamps in ISO 8601 format
		if result.CreationTime != nil {
			attrs.PutStr("creation_time", *result.CreationTime)
		}
		if result.LastExecutionTime != nil {
			attrs.PutStr("last_execution_time", *result.LastExecutionTime)
		}
		return attrs
	}

	// Create detailed attributes for logging/debugging (not used in metrics)
	logAttributes := func() []zap.Field {
		var fields []zap.Field
		if result.QueryID != nil {
			fields = append(fields, zap.String("query_id", result.QueryID.String()))
		}
		if result.PlanHandle != nil {
			fields = append(fields, zap.String("plan_handle", result.PlanHandle.String()))
		}
		if result.QueryPlanID != nil {
			fields = append(fields, zap.String("query_plan_id", result.QueryPlanID.String()))
		}
		if result.SQLText != nil {
			// Anonymize and truncate SQL text for logging
			anonymizedSQL := helpers.AnonymizeQueryText(*result.SQLText)
			if len(anonymizedSQL) > 100 {
				anonymizedSQL = anonymizedSQL[:100] + "..."
			}
			fields = append(fields, zap.String("sql_text_preview", anonymizedSQL))
		}
		return fields
	}

	// Create TotalCPUMs metric - CARDINALITY SAFE
	if result.TotalCPUMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.individual_query.total_cpu_ms")
		metric.SetDescription("Total CPU time in milliseconds for individual query analysis")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.TotalCPUMs)

		// Use all attributes including execution plan XML
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Create TotalElapsedMs metric - CARDINALITY SAFE
	// This metric includes all attributes: execution plan XML, timestamps, query details
	if result.TotalElapsedMs != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.individual_query.total_elapsed_ms")
		metric.SetDescription("Total elapsed time in milliseconds for individual query analysis")
		metric.SetUnit("ms")

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(timestamp)
		dataPoint.SetStartTimestamp(s.startTime)
		dataPoint.SetDoubleValue(*result.TotalElapsedMs)

		// Use all attributes including execution plan XML and timestamps
		createSafeAttributes().CopyTo(dataPoint.Attributes())
	}

	// Log detailed information for debugging/analysis
	s.logger.Debug("Processed query execution plan metrics with execution plan XML and timestamps included",
		logAttributes()...)

	return nil
}

// getSlowQueryResults fetches slow query results to extract QueryIDs for execution plan analysis
func (s *QueryPerformanceScraper) getSlowQueryResults(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold,
	textTruncateLimit int,
) ([]models.SlowQuery, error) {
	// Format the slow query with parameters
	formattedQuery := fmt.Sprintf(queries.SlowQuery, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit)

	s.logger.Debug("Executing slow query to extract QueryIDs for execution plan analysis",
		zap.String("query", queries.TruncateQuery(formattedQuery, 100)),
		zap.Int("interval_seconds", intervalSeconds),
		zap.Int("top_n", topN),
		zap.Int("elapsed_time_threshold", elapsedTimeThreshold))

	var results []models.SlowQuery
	if err := s.connection.Query(ctx, &results, formattedQuery); err != nil {
		return nil, fmt.Errorf("failed to execute slow query for QueryID extraction: %w", err)
	}

	s.logger.Debug("Successfully fetched slow queries for QueryID extraction",
		zap.Int("result_count", len(results)))

	return results, nil
}

// REMOVED: emitExecutionPlanLogs, categorizeOperator, and assessPerformanceImpact functions
// These functions created SqlServerExecutionPlan and SqlServerExecutionPlanNode custom events
// which have been removed in favor of SqlServerActiveQueryExecutionPlan with aggregated stats

// REMOVED (2025-11-28): logExecutionPlanToNewRelic function
// This function created structured logs for execution plan data
// which has been removed in favor of SqlServerActiveQueryExecutionPlan custom events with aggregated stats

// ExtractQueryIDsFromSlowQueries extracts unique QueryIDs from slow query results
func (s *QueryPerformanceScraper) ExtractQueryIDsFromSlowQueries(slowQueries []models.SlowQuery) []string {
	queryIDMap := make(map[string]bool)
	var queryIDs []string

	for _, slowQuery := range slowQueries {
		if slowQuery.QueryID != nil && !slowQuery.QueryID.IsEmpty() {
			queryIDStr := slowQuery.QueryID.String()
			if !queryIDMap[queryIDStr] {
				queryIDMap[queryIDStr] = true
				queryIDs = append(queryIDs, queryIDStr)
			}
		}
	}

	s.logger.Debug("Extracted unique QueryIDs from slow queries",
		zap.Int("total_slow_queries", len(slowQueries)),
		zap.Int("unique_query_ids", len(queryIDs)))

	return queryIDs
}

// formatQueryIDsForSQL converts QueryID slice to comma-separated string for SQL IN clause
// Follows nri-mssql pattern for QueryID formatting
func (s *QueryPerformanceScraper) formatQueryIDsForSQL(queryIDs []string) string {
	if len(queryIDs) == 0 {
		return "0x0" // Return placeholder if no QueryIDs
	}

	// Join QueryIDs with commas for SQL STRING_SPLIT
	// QueryIDs are already in hex format (0x...), so we can use them directly
	queryIDsString := ""
	for i, queryID := range queryIDs {
		if i > 0 {
			queryIDsString += ","
		}
		queryIDsString += queryID
	}

	s.logger.Debug("Formatted QueryIDs for SQL query",
		zap.Int("query_id_count", len(queryIDs)),
		zap.String("formatted_query_ids", queries.TruncateQuery(queryIDsString, 100)))

	return queryIDsString
}

// extractPlanHandlesFromSlowQueries extracts unique PlanHandles from slow query results
func (s *QueryPerformanceScraper) extractPlanHandlesFromSlowQueries(slowQueries []models.SlowQuery) []string {
	planHandleMap := make(map[string]bool)
	var planHandles []string

	for _, slowQuery := range slowQueries {
		if slowQuery.PlanHandle != nil && !slowQuery.PlanHandle.IsEmpty() {
			planHandleStr := slowQuery.PlanHandle.String()
			if !planHandleMap[planHandleStr] {
				planHandleMap[planHandleStr] = true
				planHandles = append(planHandles, planHandleStr)
			}
		}
	}

	s.logger.Debug("Extracted unique PlanHandles from slow queries",
		zap.Int("total_slow_queries", len(slowQueries)),
		zap.Int("unique_plan_handles", len(planHandles)))

	return planHandles
}

// formatPlanHandlesForSQL converts PlanHandle slice to comma-separated string for SQL IN clause
func (s *QueryPerformanceScraper) formatPlanHandlesForSQL(planHandles []string) string {
	if len(planHandles) == 0 {
		return "0x0" // Return placeholder if no PlanHandles
	}

	// Join PlanHandles with commas for SQL STRING_SPLIT
	// PlanHandles are already in hex format (0x...), so we can use them directly
	planHandlesString := ""
	for i, planHandle := range planHandles {
		if i > 0 {
			planHandlesString += ","
		}
		planHandlesString += planHandle
	}

	s.logger.Debug("Formatted PlanHandles for SQL query",
		zap.Int("plan_handle_count", len(planHandles)),
		zap.String("formatted_plan_handles", queries.TruncateQuery(planHandlesString, 100)))

	return planHandlesString
}
