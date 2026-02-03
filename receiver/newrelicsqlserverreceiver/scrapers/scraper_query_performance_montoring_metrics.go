// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// QueryPerformanceScraper handles SQL Server query performance monitoring metrics collection
type QueryPerformanceScraper struct {
	connection          SQLConnectionInterface
	logger              *zap.Logger
	mb                  *metadata.MetricsBuilder
	startTime           pcommon.Timestamp // Used by slow query metrics (not yet migrated)
	engineEdition       int
	executionPlanLogger *models.ExecutionPlanLogger
	logConsumer         plog.Logs                     // For emitting execution plan logs
	slowQuerySmoother   *SlowQuerySmoother            // EWMA-based smoothing algorithm for slow queries
	intervalCalculator  *SimplifiedIntervalCalculator // Simplified delta-based interval calculator
	metadataCache       *helpers.MetadataCache        // Metadata cache for wait resource enrichment
	executionPlanCache  *helpers.ExecutionPlanCache   // Cache for execution plan deduplication
}

// NewQueryPerformanceScraper creates a new query performance scraper with smoothing and interval calculator configuration
func NewQueryPerformanceScraper(
	conn SQLConnectionInterface,
	logger *zap.Logger,
	mb *metadata.MetricsBuilder,
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

	if smoothingEnabled {
		maxAge := time.Duration(maxAgeMinutes) * time.Minute
		smoother = NewSlowQuerySmoother(logger, smoothingFactor, decayThreshold, maxAge)
	}

	if intervalCalcEnabled {
		cacheTTL := time.Duration(intervalCalcCacheTTLMinutes) * time.Minute
		intervalCalc = NewSimplifiedIntervalCalculator(logger, cacheTTL)
	}

	// Execution plan caching is always enabled (hardcoded 24 hour TTL)
	execPlanCache := helpers.NewExecutionPlanCache(logger)
	logger.Info("Execution plan caching enabled (TTL: 24 hours hardcoded)")

	return &QueryPerformanceScraper{
		connection:          conn,
		logger:              logger,
		mb:                  mb,
		startTime:           pcommon.NewTimestampFromTime(time.Now()),
		engineEdition:       engineEdition,
		executionPlanLogger: models.NewExecutionPlanLogger(),
		slowQuerySmoother:   smoother,
		intervalCalculator:  intervalCalc,
		metadataCache:       metadataCache,
		executionPlanCache:  execPlanCache,
	}
}

// SetMetricsBuilder sets the metrics builder for this scraper
// This is called before each scrape operation to provide the current metrics builder
func (s *QueryPerformanceScraper) SetMetricsBuilder(mb *metadata.MetricsBuilder) {
	s.mb = mb
}

// CleanupExecutionPlanCache removes expired entries from the execution plan cache
func (s *QueryPerformanceScraper) CleanupExecutionPlanCache() {
	if s.executionPlanCache != nil {
		s.executionPlanCache.CleanupStaleEntries()
	}
}

func (s *QueryPerformanceScraper) ScrapeSlowQueryMetrics(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold int, emitMetrics bool) ([]models.SlowQuery, error) {
	query := fmt.Sprintf(queries.SlowQuery, intervalSeconds)

	var rawResults []models.SlowQuery
	if err := s.connection.Query(ctx, &rawResults, query); err != nil {
		return nil, fmt.Errorf("failed to execute slow query metrics query: %w", err)
	}

	s.logger.Debug("Raw slow query metrics fetched", zap.Int("raw_result_count", len(rawResults)))

	// Apply simplified interval-based delta calculation if enabled
	var resultsWithIntervalMetrics []models.SlowQuery
	if s.intervalCalculator != nil {
		now := time.Now()
		resultsWithIntervalMetrics = make([]models.SlowQuery, 0, len(rawResults))
		for _, rawQuery := range rawResults {
			metrics := s.intervalCalculator.CalculateMetrics(&rawQuery, now)

			if metrics == nil || !metrics.HasNewExecutions {

				s.logger.Debug("Skipping query with no new executions",
					zap.String("query_id", rawQuery.QueryID.String()),
					zap.Float64("time_since_last_exec_sec", metrics.TimeSinceLastExecSec))
				continue

			}

			if metrics.IntervalAvgElapsedTimeMs < float64(elapsedTimeThreshold) {
				continue
			}

			// Populate interval metrics in the model
			rawQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			rawQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

			// Populate new interval metrics
			rawQuery.IntervalWorkerTimeMS = &metrics.IntervalWorkerTimeMs
			rawQuery.IntervalAvgWorkerTimeMS = &metrics.IntervalAvgWorkerTimeMs
			rawQuery.IntervalRows = &metrics.IntervalRows
			rawQuery.IntervalAvgRows = &metrics.IntervalAvgRows
			rawQuery.IntervalLogicalReads = &metrics.IntervalLogicalReads
			rawQuery.IntervalAvgLogicalReads = &metrics.IntervalAvgLogicalReads
			rawQuery.IntervalPhysicalReads = &metrics.IntervalPhysicalReads
			rawQuery.IntervalAvgPhysicalReads = &metrics.IntervalAvgPhysicalReads
			rawQuery.IntervalLogicalWrites = &metrics.IntervalLogicalWrites
			rawQuery.IntervalAvgLogicalWrites = &metrics.IntervalAvgLogicalWrites
			rawQuery.IntervalWaitTimeMS = &metrics.IntervalWaitTimeMs
			rawQuery.IntervalAvgWaitTimeMS = &metrics.IntervalAvgWaitTimeMs

			// Calculate and populate historical wait time (total_elapsed - total_worker)
			// Note: Both are float64 in milliseconds, convert result to int64
			if rawQuery.TotalElapsedTimeMS != nil && rawQuery.TotalWorkerTimeMS != nil {
				waitTimeMs := int64(*rawQuery.TotalElapsedTimeMS - *rawQuery.TotalWorkerTimeMS)
				rawQuery.TotalWaitTimeMS = &waitTimeMs
			}

			resultsWithIntervalMetrics = append(resultsWithIntervalMetrics, rawQuery)
		}

		s.intervalCalculator.CleanupStaleEntries(now)

		stats := s.intervalCalculator.GetCacheStats()
		s.logger.Debug("Interval calculator cache stats", zap.Any("queries are in the cache", stats))
		rawResults = resultsWithIntervalMetrics
	}

	if s.intervalCalculator != nil && len(rawResults) > 0 {
		sort.Slice(rawResults, func(i, j int) bool {
			if rawResults[i].IntervalAvgElapsedTimeMS == nil {
				return false
			}
			if rawResults[j].IntervalAvgElapsedTimeMS == nil {
				return true
			}
			return *rawResults[i].IntervalAvgElapsedTimeMS > *rawResults[j].IntervalAvgElapsedTimeMS
		})

		if len(rawResults) > topN {
			rawResults = rawResults[:topN]
		}
	}

	var resultsToProcess []models.SlowQuery
	if s.slowQuerySmoother != nil {
		resultsToProcess = s.slowQuerySmoother.Smooth(rawResults)
		stats := s.slowQuerySmoother.GetHistoryStats()
		s.logger.Debug("EWMA smoother statistics", zap.Any("queries in history", stats))
	} else {
		resultsToProcess = rawResults
		s.logger.Debug("EWMA smoothing disabled, using interval-calculated or raw results")
	}

	if emitMetrics {
		for i, result := range resultsToProcess {
			if err := s.processSlowQueryMetrics(result, i); err != nil {
				s.logger.Error("Failed to process slow query metric", zap.Error(err), zap.Int("index", i))
			}
		}
	}
	return resultsToProcess, nil
}

func (s *QueryPerformanceScraper) ScrapeActiveQueryPlanStatistics(ctx context.Context, activeQueries []models.ActiveRunningQuery, slowQueryPlanDataMap map[string]models.SlowQueryPlanData) error {
	if len(activeQueries) == 0 {
		return nil
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())
	totalStatsEmitted := 0
	totalExecutionPlansEmitted := 0
	skippedNoSlowQueryMatch := 0
	skippedPlanFetch := 0

	for _, activeQuery := range activeQueries {
		var planData models.SlowQueryPlanData
		var found bool
		if activeQuery.QueryID != nil && !activeQuery.QueryID.IsEmpty() {
			queryIDStr := activeQuery.QueryID.String()
			planData, found = slowQueryPlanDataMap[queryIDStr]
		}

		if !found {
			skippedNoSlowQueryMatch++
			continue
		}

		if planData.PlanHandle == nil || planData.PlanHandle.IsEmpty() {
			skippedNoSlowQueryMatch++
			continue
		}

		planResult := s.convertPlanDataToPlanHandleResult(planData)
		s.emitActiveQueryPlanMetrics(planResult, activeQuery, timestamp)

		totalStatsEmitted++

		// Check execution plan cache - skip fetching/parsing if already sent recently
		if s.executionPlanCache != nil {
			queryHash := ""
			if planData.QueryHash != nil {
				queryHash = planData.QueryHash.String()
			}
			planHandle := planData.PlanHandle.String()

			if !s.executionPlanCache.ShouldEmit(queryHash, planHandle) {
				// ShouldEmit already logged the cache hit, just skip
				skippedPlanFetch++
				continue // Skip DB fetch, parsing, and emission for this plan
			}

		} else {
			// Cache disabled - always fetch
			s.logger.Debug("Execution plan cache disabled, will fetch from database")
		}

		executionPlanXML, err := s.fetchExecutionPlanXML(ctx, planData.PlanHandle)
		if err != nil {
			s.logger.Warn("Failed to fetch execution plan XML",
				zap.Error(err),
				zap.String("plan_handle", planData.PlanHandle.String()))
			skippedPlanFetch++
			continue
		}

		if executionPlanXML == "" {
			skippedPlanFetch++
			continue
		}
		queryID := ""
		if activeQuery.QueryID != nil {
			queryID = activeQuery.QueryID.String()
		}
		planHandle := planData.PlanHandle.String()

		executionPlan, err := models.ParseExecutionPlanXML(executionPlanXML, queryID, planHandle)
		if err != nil {
			s.logger.Warn("Failed to parse execution plan XML",
				zap.Error(err),
				zap.String("plan_handle", planHandle))
			skippedPlanFetch++
			continue
		}

		s.emitExecutionPlanNodeMetrics(*executionPlan, activeQuery, timestamp)
		totalExecutionPlansEmitted++
	}

	s.logger.Info("Emitted execution plan statistics and detailed operator metrics",
		zap.Int("active_query_count", len(activeQueries)),
		zap.Int("plan_stats_emitted", totalStatsEmitted),
		zap.Int("execution_plans_emitted", totalExecutionPlansEmitted),
		zap.Int("skipped_no_slow_query_match", skippedNoSlowQueryMatch),
		zap.Int("skipped_plan_fetch", skippedPlanFetch))

	return nil
}

func (s *QueryPerformanceScraper) convertPlanDataToPlanHandleResult(planData models.SlowQueryPlanData) models.PlanHandleResult {
	return models.PlanHandleResult{
		QueryID:           planData.QueryHash,
		PlanHandle:        planData.PlanHandle,
		LastExecutionTime: planData.LastExecutionTime,
		CreationTime:      planData.CreationTime,
		AvgElapsedTimeMs:  planData.AvgElapsedTimeMs,
	}
}

func (s *QueryPerformanceScraper) emitActiveQueryPlanMetrics(planResult models.PlanHandleResult, activeQuery models.ActiveRunningQuery, timestamp pcommon.Timestamp) {
	// Extract only the attributes needed for plan metrics
	queryID := ""
	if planResult.QueryID != nil {
		queryID = planResult.QueryID.String()
	}

	planHandle := ""
	if planResult.PlanHandle != nil {
		planHandle = planResult.PlanHandle.String()
	}

	lastExecutionTime := ""
	if planResult.LastExecutionTime != nil {
		lastExecutionTime = *planResult.LastExecutionTime
	}

	creationTime := ""
	if planResult.CreationTime != nil {
		creationTime = *planResult.CreationTime
	}

	if planResult.AvgElapsedTimeMs != nil {
		s.mb.RecordSqlserverPlanAvgElapsedTimeMsDataPoint(
			timestamp,
			*planResult.AvgElapsedTimeMs,
			queryID,
			planHandle,
			lastExecutionTime,
			creationTime,
		)
	}
}

func (s *QueryPerformanceScraper) emitExecutionPlanNodeMetrics(executionPlan models.ExecutionPlanAnalysis, activeQuery models.ActiveRunningQuery, timestamp pcommon.Timestamp) {
	// Cache check now happens before DB fetch - this method only emits
	if s.executionPlanCache != nil {
		// Log emission (we only reach here if cache check passed earlier)
		s.logger.Info("Emitting execution plan metrics",
			zap.String("query_hash", executionPlan.QueryID),
			zap.String("plan_handle", executionPlan.PlanHandle),
			zap.Int("node_count", len(executionPlan.Nodes)))
	}

	// Compute all attribute values once
	requestStartTime := ""
	if activeQuery.RequestStartTime != nil {
		requestStartTime = *activeQuery.RequestStartTime
	}

	for _, node := range executionPlan.Nodes {
		s.mb.RecordSqlserverExecutionPlanDataPoint(
			timestamp,
			1, // Value is always 1 for dimensional metrics
			node.QueryID,
			node.PlanHandle,
			int64(node.NodeID),
			int64(node.ParentNodeID),
			node.PhysicalOp,
			node.LogicalOp,
			node.InputType,
			node.SchemaName,
			node.TableName,
			node.IndexName,
			node.ReferencedColumns,
			node.EstimateRows,
			node.EstimateIO,
			node.EstimateCPU,
			node.AvgRowSize,
			node.TotalSubtreeCost,
			node.EstimatedOperatorCost,
			node.EstimatedExecutionMode,
			node.GrantedMemoryKb,
			node.SpillOccurred,
			node.NoJoinPredicate,
			node.TotalWorkerTime,
			node.TotalElapsedTime,
			node.TotalLogicalReads,
			node.TotalLogicalWrites,
			node.ExecutionCount,
			requestStartTime,
			node.LastExecutionTime,
			"SqlServerExecutionPlan",
		)
	}
}

func (s *QueryPerformanceScraper) processSlowQueryMetrics(result models.SlowQuery, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Compute all attribute values once
	queryID := ""
	if result.QueryID != nil {
		queryID = result.QueryID.String()
	}

	// Extract New Relic metadata and normalize SQL for cross-language correlation
	// This enables APM integration and query correlation across different language agents
	if result.QueryText != nil && *result.QueryText != "" {
		// Extract metadata from New Relic query comments (e.g., /* nr_apm_guid="ABC123", nr_service="order-service" */)
		nrApmGuid, clientName := helpers.ExtractNewRelicMetadata(*result.QueryText)

		// Normalize SQL and generate MD5 hash for cross-language query correlation
		normalizedSQL, sqlHash := helpers.NormalizeSqlAndHash(*result.QueryText)

		// Populate model fields with extracted metadata
		if nrApmGuid != "" {
			result.NrApmGuid = &nrApmGuid
		}
		if clientName != "" {
			result.ClientName = &clientName
		}
		if sqlHash != "" {
			result.NormalisedSqlHash = &sqlHash
		}

		// Replace QueryText with normalized version for privacy and consistency
		// This removes literals while preserving query structure
		result.QueryText = &normalizedSQL
	}

	planHandle := ""
	if result.PlanHandle != nil {
		planHandle = result.PlanHandle.String()
	}

	databaseName := ""
	if result.DatabaseName != nil {
		databaseName = *result.DatabaseName
	}

	queryText := ""
	if result.QueryText != nil {
		queryText = helpers.AnonymizeQueryText(*result.QueryText)
	}

	collectionTimestamp := ""
	if result.CollectionTimestamp != nil {
		collectionTimestamp = *result.CollectionTimestamp
	}

	lastExecutionTimestamp := ""
	if result.LastExecutionTimestamp != nil {
		lastExecutionTimestamp = *result.LastExecutionTimestamp
	}

	normalisedSqlHash := ""
	if result.NormalisedSqlHash != nil {
		normalisedSqlHash = *result.NormalisedSqlHash
	}

	nrApmGuid := ""
	if result.NrApmGuid != nil {
		nrApmGuid = *result.NrApmGuid
	}

	clientName := ""
	if result.ClientName != nil {
		clientName = *result.ClientName
	}

	// Emit a single metric with all query details (non-numeric attributes)
	s.mb.RecordSqlserverSlowqueryQueryDetailsDataPoint(
		timestamp,
		1,
		queryID,
		databaseName,
		planHandle,
		queryText,
		collectionTimestamp,
		lastExecutionTimestamp,
		normalisedSqlHash,
		nrApmGuid,
		clientName,
		"SqlServerSlowQueryDetails",
	)

	if result.AvgElapsedTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalAvgElapsedTimeMsDataPoint(
			timestamp,
			*result.AvgElapsedTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgElapsedTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgElapsedTimeMsDataPoint(
			timestamp,
			*result.IntervalAvgElapsedTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.ExecutionCount != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalExecutionCountDataPoint(
			timestamp,
			*result.ExecutionCount,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalExecutionCount != nil {
		s.mb.RecordSqlserverSlowqueryIntervalExecutionCountDataPoint(
			timestamp,
			*result.IntervalExecutionCount,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	// New historical metrics
	// Note: TotalWorkerTimeMS is *float64 from SQL (ms), cast to int64 for emission
	if result.TotalWorkerTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalWorkerTimeMsDataPoint(
			timestamp,
			int64(*result.TotalWorkerTimeMS),
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.TotalRows != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalRowsDataPoint(
			timestamp,
			*result.TotalRows,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.TotalLogicalReads != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalLogicalReadsDataPoint(
			timestamp,
			*result.TotalLogicalReads,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.TotalPhysicalReads != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalPhysicalReadsDataPoint(
			timestamp,
			*result.TotalPhysicalReads,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.TotalLogicalWrites != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalLogicalWritesDataPoint(
			timestamp,
			*result.TotalLogicalWrites,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.TotalWaitTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalWaitTimeMsDataPoint(
			timestamp,
			*result.TotalWaitTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	// New interval metrics
	if result.IntervalWorkerTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryIntervalWorkerTimeMsDataPoint(
			timestamp,
			*result.IntervalWorkerTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgWorkerTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgWorkerTimeMsDataPoint(
			timestamp,
			*result.IntervalAvgWorkerTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalRows != nil {
		s.mb.RecordSqlserverSlowqueryIntervalRowsDataPoint(
			timestamp,
			*result.IntervalRows,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgRows != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgRowsDataPoint(
			timestamp,
			*result.IntervalAvgRows,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalLogicalReads != nil {
		s.mb.RecordSqlserverSlowqueryIntervalLogicalReadsDataPoint(
			timestamp,
			*result.IntervalLogicalReads,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgLogicalReads != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgLogicalReadsDataPoint(
			timestamp,
			*result.IntervalAvgLogicalReads,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalPhysicalReads != nil {
		s.mb.RecordSqlserverSlowqueryIntervalPhysicalReadsDataPoint(
			timestamp,
			*result.IntervalPhysicalReads,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgPhysicalReads != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgPhysicalReadsDataPoint(
			timestamp,
			*result.IntervalAvgPhysicalReads,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalLogicalWrites != nil {
		s.mb.RecordSqlserverSlowqueryIntervalLogicalWritesDataPoint(
			timestamp,
			*result.IntervalLogicalWrites,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgLogicalWrites != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgLogicalWritesDataPoint(
			timestamp,
			*result.IntervalAvgLogicalWrites,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalWaitTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryIntervalWaitTimeMsDataPoint(
			timestamp,
			*result.IntervalWaitTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	if result.IntervalAvgWaitTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgWaitTimeMsDataPoint(
			timestamp,
			*result.IntervalAvgWaitTimeMS,
			queryID,
			databaseName,
			normalisedSqlHash,
			nrApmGuid,
			clientName,
		)
	}

	return nil
}

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

	return queryIDs
}

func (s *QueryPerformanceScraper) ExtractQueryDataFromSlowQueries(slowQueries []models.SlowQuery) ([]string, map[string]models.SlowQueryPlanData) {
	queryIDMap := make(map[string]bool)
	slowQueryPlanDataMap := make(map[string]models.SlowQueryPlanData)
	duplicatePlanHandles := 0

	for _, slowQuery := range slowQueries {
		if slowQuery.QueryID != nil && !slowQuery.QueryID.IsEmpty() {
			queryIDStr := slowQuery.QueryID.String()
			queryIDMap[queryIDStr] = true

			if slowQuery.PlanHandle != nil && !slowQuery.PlanHandle.IsEmpty() {
				if existingPlanData, exists := slowQueryPlanDataMap[queryIDStr]; exists {
					existingTime := ""
					newTime := ""
					if existingPlanData.LastExecutionTime != nil {
						existingTime = *existingPlanData.LastExecutionTime
					}
					if slowQuery.LastExecutionTimestamp != nil {
						newTime = *slowQuery.LastExecutionTimestamp
					}

					if newTime > existingTime {
						duplicatePlanHandles++
						slowQueryPlanDataMap[queryIDStr] = models.SlowQueryPlanData{
							QueryHash:         slowQuery.QueryID,
							PlanHandle:        slowQuery.PlanHandle,
							CreationTime:      slowQuery.CreationTime,
							LastExecutionTime: slowQuery.LastExecutionTimestamp,
							AvgElapsedTimeMs:  slowQuery.AvgElapsedTimeMS,
						}
					}
				} else {
					slowQueryPlanDataMap[queryIDStr] = models.SlowQueryPlanData{
						QueryHash:         slowQuery.QueryID,
						PlanHandle:        slowQuery.PlanHandle,
						CreationTime:      slowQuery.CreationTime,
						LastExecutionTime: slowQuery.LastExecutionTimestamp,
						AvgElapsedTimeMs:  slowQuery.AvgElapsedTimeMS,
					}
				}
			}
		}
	}

	// Convert map keys to slice
	queryIDs := make([]string, 0, len(queryIDMap))
	for queryID := range queryIDMap {
		queryIDs = append(queryIDs, queryID)
	}

	return queryIDs, slowQueryPlanDataMap
}
