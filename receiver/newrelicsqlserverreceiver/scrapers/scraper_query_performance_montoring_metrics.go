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
	}
}

// SetMetricsBuilder sets the metrics builder for this scraper
// This is called before each scrape operation to provide the current metrics builder
func (s *QueryPerformanceScraper) SetMetricsBuilder(mb *metadata.MetricsBuilder) {
	s.mb = mb
}

func (s *QueryPerformanceScraper) ScrapeSlowQueryMetrics(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold int, emitMetrics bool) ([]models.SlowQuery, error) {
	query := fmt.Sprintf(queries.SlowQuery, intervalSeconds, topN, elapsedTimeThreshold)

	var rawResults []models.SlowQuery
	if err := s.connection.Query(ctx, &rawResults, query); err != nil {
		return nil, fmt.Errorf("failed to execute slow query metrics query: %w", err)
	}
	var resultsWithIntervalMetrics []models.SlowQuery
	if s.intervalCalculator != nil {
		now := time.Now()
		resultsWithIntervalMetrics = make([]models.SlowQuery, 0, len(rawResults))
		for _, rawQuery := range rawResults {
			metrics := s.intervalCalculator.CalculateMetrics(&rawQuery, now)

			if metrics == nil || !metrics.HasNewExecutions {
				continue
			}

			if metrics.IntervalAvgElapsedTimeMs < float64(elapsedTimeThreshold) {
				continue
			}

			rawQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			rawQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

			resultsWithIntervalMetrics = append(resultsWithIntervalMetrics, rawQuery)
		}

		s.intervalCalculator.CleanupStaleEntries(now)
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
	} else {
		resultsToProcess = rawResults
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
		PlanHandle:         planData.PlanHandle,
		QueryID:            planData.QueryID,
		CreationTime:       planData.CreationTime,
		LastExecutionTime:  planData.LastExecutionTime,
		TotalElapsedTimeMs: planData.TotalElapsedTimeMs,
		ExecutionCount:     nil,
		AvgElapsedTimeMs:   nil,
		MinElapsedTimeMs:   nil,
		MaxElapsedTimeMs:   nil,
		LastElapsedTimeMs:  nil,
		AvgWorkerTimeMs:    nil,
		TotalWorkerTimeMs:  nil,
		AvgLogicalReads:    nil,
		AvgLogicalWrites:   nil,
		AvgRows:            nil,
		LastGrantKB:        nil,
		LastUsedGrantKB:    nil,
		LastSpills:         nil,
		MaxSpills:          nil,
		LastDOP:            nil,
	}
}

func (s *QueryPerformanceScraper) emitActiveQueryPlanMetrics(planResult models.PlanHandleResult, activeQuery models.ActiveRunningQuery, timestamp pcommon.Timestamp) {
	// Compute all attribute values once
	queryID := ""
	if planResult.QueryID != nil {
		queryID = planResult.QueryID.String()
	}

	planHandle := ""
	if planResult.PlanHandle != nil {
		planHandle = planResult.PlanHandle.String()
	}

	queryPlanHash := ""
	if planResult.QueryPlanHash != nil {
		queryPlanHash = planResult.QueryPlanHash.String()
	}

	sessionID := int64(0)
	if activeQuery.CurrentSessionID != nil {
		sessionID = *activeQuery.CurrentSessionID
	}

	requestID := int64(0)
	if activeQuery.RequestID != nil {
		requestID = *activeQuery.RequestID
	}

	requestStartTime := ""
	if activeQuery.RequestStartTime != nil {
		requestStartTime = *activeQuery.RequestStartTime
	}

	lastExecutionTime := ""
	if planResult.LastExecutionTime != nil {
		lastExecutionTime = *planResult.LastExecutionTime
	}

	creationTime := ""
	if planResult.CreationTime != nil {
		creationTime = *planResult.CreationTime
	}

	databaseName := ""
	if activeQuery.DatabaseName != nil {
		databaseName = *activeQuery.DatabaseName
	}

	schemaName := ""
	if activeQuery.SchemaName != nil {
		schemaName = *activeQuery.SchemaName
	}

	if planResult.AvgElapsedTimeMs != nil {
		s.mb.RecordSqlserverPlanAvgElapsedTimeMsDataPoint(
			timestamp,
			*planResult.AvgElapsedTimeMs,
			queryID,
			planHandle,
			queryPlanHash,
			sessionID,
			requestID,
			requestStartTime,
			lastExecutionTime,
			creationTime,
			databaseName,
			schemaName,
		)
	}

	if planResult.TotalElapsedTimeMs != nil {
		s.mb.RecordSqlserverPlanTotalElapsedTimeMsDataPoint(
			timestamp,
			*planResult.TotalElapsedTimeMs,
			queryID,
			planHandle,
			queryPlanHash,
			sessionID,
			requestID,
			requestStartTime,
			lastExecutionTime,
			creationTime,
			databaseName,
			schemaName,
		)
	}
}

func (s *QueryPerformanceScraper) emitExecutionPlanNodeMetrics(executionPlan models.ExecutionPlanAnalysis, activeQuery models.ActiveRunningQuery, timestamp pcommon.Timestamp) {
	// Compute all attribute values once
	sessionID := int64(0)
	if activeQuery.CurrentSessionID != nil {
		sessionID = *activeQuery.CurrentSessionID
	}

	requestID := int64(0)
	if activeQuery.RequestID != nil {
		requestID = *activeQuery.RequestID
	}

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
			node.AvgElapsedTimeMs,
			sessionID,
			requestID,
			requestStartTime,
			node.CollectionTimestamp,
			node.LastExecutionTime,
			"SqlExecutionPlan",
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

	planHandle := ""
	if result.PlanHandle != nil {
		planHandle = result.PlanHandle.String()
	}

	databaseName := ""
	if result.DatabaseName != nil {
		databaseName = *result.DatabaseName
	}

	schemaName := ""
	if result.SchemaName != nil {
		schemaName = *result.SchemaName
	}

	statementType := ""
	if result.StatementType != nil {
		statementType = *result.StatementType
	}

	queryText := ""
	if result.QueryText != nil {
		queryText = *result.QueryText
	}

	collectionTimestamp := ""
	if result.CollectionTimestamp != nil {
		collectionTimestamp = *result.CollectionTimestamp
	}

	lastExecutionTimestamp := ""
	if result.LastExecutionTimestamp != nil {
		lastExecutionTimestamp = *result.LastExecutionTimestamp
	}

	querySignature := ""
	if result.QueryText != nil {
		querySignature = helpers.ComputeQueryHash(*result.QueryText)
	}

	// Emit a single metric with all query details (non-numeric attributes)
	s.mb.RecordSqlserverSlowqueryQueryDetailsDataPoint(
		timestamp,
		1,
		queryID,
		databaseName,
		schemaName,
		planHandle,
		statementType,
		queryText,
		querySignature,
		collectionTimestamp,
		lastExecutionTimestamp,
		"SqlQueryDetails",
	)

	if result.AvgCPUTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalAvgCPUTimeMsDataPoint(
			timestamp,
			*result.AvgCPUTimeMS,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.AvgDiskReads != nil {
		s.mb.RecordSqlserverSlowqueryAvgDiskReadsDataPoint(
			timestamp,
			*result.AvgDiskReads,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.AvgDiskWrites != nil {
		s.mb.RecordSqlserverSlowqueryAvgDiskWritesDataPoint(
			timestamp,
			*result.AvgDiskWrites,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.AvgRowsProcessed != nil {
		s.mb.RecordSqlserverSlowqueryAvgRowsProcessedDataPoint(
			timestamp,
			*result.AvgRowsProcessed,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.AvgElapsedTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalAvgElapsedTimeMsDataPoint(
			timestamp,
			*result.AvgElapsedTimeMS,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.IntervalAvgElapsedTimeMS != nil {
		s.mb.RecordSqlserverSlowqueryIntervalAvgElapsedTimeMsDataPoint(
			timestamp,
			*result.IntervalAvgElapsedTimeMS,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.ExecutionCount != nil {
		s.mb.RecordSqlserverSlowqueryHistoricalExecutionCountDataPoint(
			timestamp,
			*result.ExecutionCount,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.IntervalExecutionCount != nil {
		s.mb.RecordSqlserverSlowqueryIntervalExecutionCountDataPoint(
			timestamp,
			*result.IntervalExecutionCount,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.MinElapsedTimeMs != nil {
		s.mb.RecordSqlserverSlowqueryMinElapsedTimeMsDataPoint(
			timestamp,
			*result.MinElapsedTimeMs,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.MaxElapsedTimeMs != nil {
		s.mb.RecordSqlserverSlowqueryMaxElapsedTimeMsDataPoint(
			timestamp,
			*result.MaxElapsedTimeMs,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.LastElapsedTimeMs != nil {
		s.mb.RecordSqlserverSlowqueryLastElapsedTimeMsDataPoint(
			timestamp,
			*result.LastElapsedTimeMs,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.LastGrantKB != nil {
		s.mb.RecordSqlserverSlowqueryLastGrantKbDataPoint(
			timestamp,
			*result.LastGrantKB,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.LastUsedGrantKB != nil {
		s.mb.RecordSqlserverSlowqueryLastUsedGrantKbDataPoint(
			timestamp,
			*result.LastUsedGrantKB,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.LastSpills != nil {
		s.mb.RecordSqlserverSlowqueryLastSpillsDataPoint(
			timestamp,
			*result.LastSpills,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.MaxSpills != nil {
		s.mb.RecordSqlserverSlowqueryMaxSpillsDataPoint(
			timestamp,
			*result.MaxSpills,
			queryID,
			databaseName,
			schemaName,
		)
	}

	if result.LastDOP != nil {
		s.mb.RecordSqlserverSlowqueryLastDopDataPoint(
			timestamp,
			*result.LastDOP,
			queryID,
			databaseName,
			schemaName,
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
							QueryID:            slowQuery.QueryID,
							PlanHandle:         slowQuery.PlanHandle,
							CreationTime:       slowQuery.CreationTime,
							LastExecutionTime:  slowQuery.LastExecutionTimestamp,
							TotalElapsedTimeMs: slowQuery.TotalElapsedTimeMS,
						}
					}
				} else {
					slowQueryPlanDataMap[queryIDStr] = models.SlowQueryPlanData{
						QueryID:            slowQuery.QueryID,
						PlanHandle:         slowQuery.PlanHandle,
						CreationTime:       slowQuery.CreationTime,
						LastExecutionTime:  slowQuery.LastExecutionTimestamp,
						TotalElapsedTimeMs: slowQuery.TotalElapsedTimeMS,
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
