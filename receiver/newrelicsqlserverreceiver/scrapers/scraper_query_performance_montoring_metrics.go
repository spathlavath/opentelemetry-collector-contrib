// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// QueryPerformanceScraper handles SQL Server query performance monitoring metrics collection
type QueryPerformanceScraper struct {
	connection         SQLConnectionInterface
	logger             *zap.Logger
	mb                 *metadata.MetricsBuilder
	engineEdition      int
	slowQuerySmoother  *SlowQuerySmoother
	intervalCalculator *SimplifiedIntervalCalculator
	metadataCache      *helpers.MetadataCache
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
	mb *metadata.MetricsBuilder,
) *QueryPerformanceScraper {
	// Initialize interval calculator if enabled
	var intervalCalculator *SimplifiedIntervalCalculator
	if intervalCalcEnabled {
		cacheTTL := time.Duration(intervalCalcCacheTTLMinutes) * time.Minute
		intervalCalculator = NewSimplifiedIntervalCalculator(logger, cacheTTL)
		logger.Info("Interval calculator initialized",
			zap.Duration("cache_ttl", cacheTTL))
	}

	// TODO: Initialize smoother when implementing
	return &QueryPerformanceScraper{
		connection:         conn,
		logger:             logger,
		mb:                 mb,
		engineEdition:      engineEdition,
		slowQuerySmoother:  nil, // TODO: Initialize when implementing
		intervalCalculator: intervalCalculator,
		metadataCache:      metadataCache,
	}
}

// TODO: Implement query performance monitoring scrapers with MetricsBuilder pattern
//
// Methods to implement (3 total):
// 1. ScrapeSlowQueryMetrics - top N slow queries with execution statistics
// 2. ScrapeSlowQueryExecutionPlans - execution plan analysis for slow queries
// 3. ScrapeActiveQueryExecutionPlans - execution plan analysis for active queries
//
// Each method should:
// - Remove scopeMetrics parameter from signature
// - Query SQL Server using dm_exec_query_stats and other DMVs
// - Use s.mb.Record*DataPoint() for metrics
// - Integrate with SlowQuerySmoother for EWMA smoothing
// - Integrate with SimplifiedIntervalCalculator for delta calculations
// - Integrate with ExecutionPlanLogger for execution plan logging
// - Handle errors and logging appropriately
//
// Original file was 1200 lines with complex query performance tracking and execution plan analysis.

func (s *QueryPerformanceScraper) ScrapeSlowQueryMetrics(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit int) ([]models.SlowQuery, error) {
	s.logger.Debug("Scraping slow query metrics",
		zap.Int("interval_seconds", intervalSeconds),
		zap.Int("top_n", topN),
		zap.Int("elapsed_threshold", elapsedTimeThreshold),
		zap.Int("text_truncate_limit", textTruncateLimit))

	// Use the properly defined SlowQuery from queries package
	query := fmt.Sprintf(queries.SlowQuery, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit)

	var results []models.SlowQuery
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute slow query metrics query", zap.Error(err))
		return nil, fmt.Errorf("failed to execute slow query metrics query: %w", err)
	}

	s.logger.Debug("Slow query metrics collected", zap.Int("result_count", len(results)))

	// Apply interval-based delta calculation if enabled
	var resultsWithIntervalMetrics []models.SlowQuery
	if s.intervalCalculator != nil {
		now := time.Now()

		// Pre-allocate slice capacity to avoid reallocations
		resultsWithIntervalMetrics = make([]models.SlowQuery, 0, len(results))

		// Calculate interval metrics for each query
		for _, rawQuery := range results {
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
			// Historical values remain unchanged from DB
			rawQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			rawQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

			resultsWithIntervalMetrics = append(resultsWithIntervalMetrics, rawQuery)
		}

		// Cleanup stale entries periodically (TTL-based only)
		s.intervalCalculator.CleanupStaleEntries(now)

		s.logger.Debug("Interval-based delta calculation applied",
			zap.Int("raw_count", len(results)),
			zap.Int("processed_count", len(resultsWithIntervalMetrics)))

		// Log calculator statistics
		stats := s.intervalCalculator.GetCacheStats()
		s.logger.Debug("Interval calculator statistics", zap.Any("stats", stats))

		// Use interval-calculated results as input for next step
		results = resultsWithIntervalMetrics
	}

	// Apply Go-level sorting and top-N selection AFTER delta calculation and filtering
	// This ensures we get the slowest queries based on interval (delta) averages, not historical
	if s.intervalCalculator != nil && len(results) > 0 {
		// Sort by interval average elapsed time (delta) - slowest first
		sort.Slice(results, func(i, j int) bool {
			// Handle nil cases - queries without interval metrics go to the end
			if results[i].IntervalAvgElapsedTimeMS == nil {
				return false
			}
			if results[j].IntervalAvgElapsedTimeMS == nil {
				return true
			}
			// Sort descending (highest delta average first)
			return *results[i].IntervalAvgElapsedTimeMS > *results[j].IntervalAvgElapsedTimeMS
		})

		s.logger.Debug("Sorted queries by interval average elapsed time (delta)",
			zap.Int("total_count", len(results)))

		// Take top N queries after sorting
		if len(results) > topN {
			s.logger.Debug("Applying top N selection",
				zap.Int("before_count", len(results)),
				zap.Int("top_n", topN))
			results = results[:topN]
		}
	}

	// TODO: Apply smoothing if enabled
	// if s.slowQuerySmoother != nil {
	//     results = s.slowQuerySmoother.SmoothResults(results)
	// }

	// Record metrics using MetricsBuilder pattern
	if s.mb != nil {
		now := pcommon.NewTimestampFromTime(time.Now())
		metricsRecorded := 0

		for _, result := range results {
			// Extract attributes for all metrics (with safe nil handling)
			queryID := ""
			if result.QueryID != nil {
				queryID = result.QueryID.String()
			}
			databaseName := ""
			if result.DatabaseName != nil {
				databaseName = *result.DatabaseName
			}
			queryText := ""
			if result.QueryText != nil {
				queryText = *result.QueryText
			}
			lastExecutionTimestamp := ""
			if result.LastExecutionTimestamp != nil {
				lastExecutionTimestamp = *result.LastExecutionTimestamp
			}

			// Get interval metrics if available (already calculated above)
			var intervalMetrics *SimplifiedIntervalMetrics
			if s.intervalCalculator != nil {
				// Recalculate to get the SimplifiedIntervalMetrics struct
				// (we only stored the values in the model above)
				intervalMetrics = s.intervalCalculator.CalculateMetrics(&result, time.Now())
			}

			// CPU time (historical only)
			if result.AvgCPUTimeMS != nil {
				s.mb.RecordSqlserverSlowqueryAvgCPUTimeMsDataPoint(now, *result.AvgCPUTimeMS, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Elapsed time - historical (avg since plan cached)
			if intervalMetrics != nil {
				s.mb.RecordSqlserverSlowqueryHistoricalAvgElapsedTimeMsDataPoint(now, intervalMetrics.HistoricalAvgElapsedTimeMs, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			} else if result.AvgElapsedTimeMS != nil {
				// Fallback if no interval calculator
				s.mb.RecordSqlserverSlowqueryHistoricalAvgElapsedTimeMsDataPoint(now, *result.AvgElapsedTimeMS, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Elapsed time - interval (delta-based average)
			if intervalMetrics != nil && intervalMetrics.HasNewExecutions {
				s.mb.RecordSqlserverSlowqueryIntervalAvgElapsedTimeMsDataPoint(now, intervalMetrics.IntervalAvgElapsedTimeMs, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			} else if result.AvgElapsedTimeMS != nil {
				// Fallback if no interval calculator or no new executions
				s.mb.RecordSqlserverSlowqueryIntervalAvgElapsedTimeMsDataPoint(now, *result.AvgElapsedTimeMS, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Execution count - historical
			if intervalMetrics != nil {
				s.mb.RecordSqlserverSlowqueryHistoricalExecutionCountDataPoint(now, intervalMetrics.HistoricalExecutionCount, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			} else if result.ExecutionCount != nil {
				// Fallback if no interval calculator
				s.mb.RecordSqlserverSlowqueryHistoricalExecutionCountDataPoint(now, *result.ExecutionCount, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Execution count - interval (delta)
			if intervalMetrics != nil && intervalMetrics.HasNewExecutions {
				s.mb.RecordSqlserverSlowqueryIntervalExecutionCountDataPoint(now, intervalMetrics.IntervalExecutionCount, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			} else if result.ExecutionCount != nil {
				// Fallback if no interval calculator or no new executions
				s.mb.RecordSqlserverSlowqueryIntervalExecutionCountDataPoint(now, *result.ExecutionCount, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Disk reads
			if result.AvgDiskReads != nil {
				s.mb.RecordSqlserverSlowqueryAvgDiskReadsDataPoint(now, *result.AvgDiskReads, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Disk writes
			if result.AvgDiskWrites != nil {
				s.mb.RecordSqlserverSlowqueryAvgDiskWritesDataPoint(now, *result.AvgDiskWrites, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Rows processed
			if result.AvgRowsProcessed != nil {
				s.mb.RecordSqlserverSlowqueryAvgRowsProcessedDataPoint(now, *result.AvgRowsProcessed, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Memory grant
			if result.LastGrantKB != nil {
				s.mb.RecordSqlserverSlowqueryLastGrantKbDataPoint(now, *result.LastGrantKB, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// TempDB spills
			if result.LastSpills != nil {
				s.mb.RecordSqlserverSlowqueryLastSpillsDataPoint(now, int64(*result.LastSpills), queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}

			// Degree of parallelism
			if result.LastDOP != nil {
				s.mb.RecordSqlserverSlowqueryLastDopDataPoint(now, *result.LastDOP, queryID, databaseName, queryText, lastExecutionTimestamp)
				metricsRecorded++
			}
		}

		s.logger.Info("Slow query metrics recorded",
			zap.Int("query_count", len(results)),
			zap.Int("metrics_recorded", metricsRecorded),
			zap.Bool("interval_calc_enabled", s.intervalCalculator != nil))
	} else {
		s.logger.Warn("MetricsBuilder is nil, skipping metric recording for slow queries")
	}

	return results, nil
}

func (s *QueryPerformanceScraper) ScrapeSlowQueryExecutionPlans(ctx context.Context, slowQueries []models.SlowQuery) error {
	if len(slowQueries) == 0 {
		s.logger.Debug("No slow queries to fetch execution plans for")
		return nil
	}

	s.logger.Debug("Scraping slow query execution plans", zap.Int("query_count", len(slowQueries)))

	// For each slow query, fetch its execution plan
	for i, slowQuery := range slowQueries {
		if i >= 10 { // Limit to top 10 to avoid excessive queries
			break
		}

		if slowQuery.PlanHandle == nil {
			continue
		}

		// Use the properly defined ActiveQueryExecutionPlanQuery from queries package
		query := fmt.Sprintf(queries.ActiveQueryExecutionPlanQuery, slowQuery.PlanHandle.String())

		var planResults []struct {
			ExecutionPlanXML *string `db:"execution_plan_xml"`
		}

		if err := s.connection.Query(ctx, &planResults, query); err != nil {
			s.logger.Warn("Failed to fetch execution plan", zap.Error(err))
			continue
		}

		if len(planResults) > 0 && planResults[0].ExecutionPlanXML != nil && *planResults[0].ExecutionPlanXML != "" {
			s.logger.Debug("Execution plan retrieved")
			// TODO: Log execution plan to metadata cache or file
			// if s.metadataCache != nil {
			//     s.metadataCache.StoreExecutionPlan(slowQuery.PlanHandle, *planResults[0].ExecutionPlanXML)
			// }
		}
	}

	return nil
}

func (s *QueryPerformanceScraper) ScrapeActiveQueryExecutionPlans(ctx context.Context, activeQueries []models.ActiveRunningQuery) error {
	if len(activeQueries) == 0 {
		s.logger.Debug("No active queries to fetch execution statistics for")
		return nil
	}

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

		// Emit metrics for the historical statistics retrieved
		planResult := planResults[0]
		totalStatsFound++

		s.logger.Info("Fetched execution statistics for active query plan_handle",
			zap.String("plan_handle", planHandleHex),
			zap.Int64("session_id", sessionID),
			zap.Any("execution_count", planResult.ExecutionCount),
			zap.Any("avg_elapsed_time_ms", planResult.AvgElapsedTimeMs),
			zap.Any("last_elapsed_time_ms", planResult.LastElapsedTimeMs))

		// Emit metrics using MetricsBuilder
		// These historical statistics provide context about how this plan has performed over time
		if s.mb != nil {
			now := pcommon.NewTimestampFromTime(time.Now())
			metricsEmitted := 0

			// Execution count
			if planResult.ExecutionCount != nil {
				s.mb.RecordSqlserverPlanExecutionCountDataPoint(now, *planResult.ExecutionCount)
				metricsEmitted++
			}

			// Average elapsed time
			if planResult.AvgElapsedTimeMs != nil {
				s.mb.RecordSqlserverPlanAvgElapsedTimeMsDataPoint(now, *planResult.AvgElapsedTimeMs)
				metricsEmitted++
			}

			// Total elapsed time (convert float64 to int64)
			if planResult.TotalElapsedTimeMs != nil {
				s.mb.RecordSqlserverPlanTotalElapsedTimeMsDataPoint(now, int64(*planResult.TotalElapsedTimeMs))
				metricsEmitted++
			}

			// Average worker time (CPU)
			if planResult.AvgWorkerTimeMs != nil {
				s.mb.RecordSqlserverPlanAvgWorkerTimeMsDataPoint(now, *planResult.AvgWorkerTimeMs)
				metricsEmitted++
			}

			// Logical reads
			if planResult.AvgLogicalReads != nil {
				s.mb.RecordSqlserverPlanAvgLogicalReadsDataPoint(now, *planResult.AvgLogicalReads)
				metricsEmitted++
			}

			// Logical writes
			if planResult.AvgLogicalWrites != nil {
				s.mb.RecordSqlserverPlanAvgLogicalWritesDataPoint(now, *planResult.AvgLogicalWrites)
				metricsEmitted++
			}

			s.logger.Debug("Emitted plan execution statistics metrics",
				zap.String("plan_handle", planHandleHex),
				zap.Int64("session_id", sessionID),
				zap.Int("metrics_emitted", metricsEmitted))
		}
	}

	s.logger.Info("Successfully fetched execution statistics for active running query plan_handles",
		zap.Int("active_query_count", len(activeQueries)),
		zap.Int("unique_plan_handles", len(uniquePlanHandles)),
		zap.Int("stats_found", totalStatsFound))

	return nil
}

// ExtractQueryIDsFromSlowQueries extracts query IDs from slow queries for correlation
func (s *QueryPerformanceScraper) ExtractQueryIDsFromSlowQueries(slowQueries []models.SlowQuery) []string {
	if len(slowQueries) == 0 {
		return nil
	}

	queryIDs := make([]string, 0, len(slowQueries))
	for _, slowQuery := range slowQueries {
		if slowQuery.QueryID != nil {
			queryIDs = append(queryIDs, string(*slowQuery.QueryID))
		}
	}

	s.logger.Debug("Extracted query IDs from slow queries", zap.Int("count", len(queryIDs)))
	return queryIDs
}
