// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/queries"
)

// SlowQueryScraper collects slow query metrics from MySQL performance_schema
// Following Oracle receiver pattern: simple architecture with interval calculator
type SlowQueryScraper struct {
	client             common.Client
	mb                 *metadata.MetricsBuilder
	logger             *zap.Logger
	intervalCalculator *MySQLIntervalCalculator

	// Configuration
	responseTimeThreshold int // Minimum avg elapsed time in milliseconds
	countThreshold        int // Top N queries to collect
	intervalSeconds       int // Time window for fetching queries
}

// NewSlowQueryScraper creates a new slow query scraper
func NewSlowQueryScraper(
	client common.Client,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	responseTimeThreshold int,
	countThreshold int,
	intervalSeconds int,
	enableIntervalCalculator bool,
	cacheTTLMinutes int,
) *SlowQueryScraper {
	var intervalCalc *MySQLIntervalCalculator

	// Initialize interval calculator if enabled
	if enableIntervalCalculator {
		cacheTTL := time.Duration(cacheTTLMinutes) * time.Minute
		intervalCalc = NewMySQLIntervalCalculator(logger, cacheTTL)
		logger.Info("MySQL interval-based delta calculator enabled",
			zap.Int("cache_ttl_minutes", cacheTTLMinutes))
	} else {
		logger.Info("MySQL interval-based delta calculator disabled - using cumulative averages")
	}

	return &SlowQueryScraper{
		client:                client,
		mb:                    mb,
		logger:                logger,
		intervalCalculator:    intervalCalc,
		responseTimeThreshold: responseTimeThreshold,
		countThreshold:        countThreshold,
		intervalSeconds:       intervalSeconds,
	}
}

// ScrapeMetrics collects slow query metrics
func (s *SlowQueryScraper) ScrapeMetrics(ctx context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Starting slow query metrics scrape")

	// Fetch slow queries from performance_schema
	slowQueries, err := s.fetchSlowQueries(ctx)
	if err != nil {
		errs.Add(err)
		return
	}

	s.logger.Debug("Fetched slow queries from database",
		zap.Int("raw_count", len(slowQueries)))

	// Apply interval-based delta calculation if enabled
	var queriesToProcess []models.SlowQuery
	if s.intervalCalculator != nil {
		queriesToProcess = s.applyIntervalCalculation(slowQueries)
	} else {
		// No interval calculator - use raw results
		queriesToProcess = slowQueries
	}

	s.logger.Debug("Processing slow queries",
		zap.Int("count", len(queriesToProcess)))

	// Record metrics for each query
	for _, query := range queriesToProcess {
		if !query.IsValidForMetrics() {
			continue
		}

		if err := s.recordMetrics(now, &query); err != nil {
			s.logger.Warn("Failed to record metrics for slow query",
				zap.String("query_id", query.GetQueryID()),
				zap.Error(err))
			errs.Add(err)
		}
	}

	s.logger.Debug("Slow query metrics scrape completed",
		zap.Int("queries_processed", len(queriesToProcess)))
}

// fetchSlowQueries fetches slow queries from performance_schema
func (s *SlowQueryScraper) fetchSlowQueries(ctx context.Context) ([]models.SlowQuery, error) {
	query := queries.GetSlowQueriesSQL(s.intervalSeconds)

	s.logger.Debug("Executing slow query SQL",
		zap.Int("interval_seconds", s.intervalSeconds))

	var slowQueries []models.SlowQuery
	rows, err := s.client.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute slow query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sq models.SlowQuery
		err := rows.Scan(
			&sq.CollectionTimestamp,
			&sq.QueryID,
			&sq.QueryText,
			&sq.DatabaseName,
			&sq.ExecutionCount,
			&sq.TotalElapsedTimeMS,
			&sq.AvgElapsedTimeMS,
			&sq.AvgCPUTimeMS,
			&sq.AvgLockTimeMS,
			&sq.AvgRowsExamined,
			&sq.AvgRowsSent,
			&sq.TotalErrors,
			&sq.FirstSeen,
			&sq.LastExecutionTimestamp,
		)
		if err != nil {
			s.logger.Warn("Failed to scan slow query row", zap.Error(err))
			continue
		}

		slowQueries = append(slowQueries, sq)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating slow query results: %w", err)
	}

	return slowQueries, nil
}

// applyIntervalCalculation applies delta calculation and filtering
func (s *SlowQueryScraper) applyIntervalCalculation(slowQueries []models.SlowQuery) []models.SlowQuery {
	now := time.Now()
	queriesToProcess := make([]models.SlowQuery, 0, len(slowQueries))

	// Calculate interval metrics for each query
	for _, slowQuery := range slowQueries {
		metrics := s.intervalCalculator.CalculateMetrics(&slowQuery, now)

		if metrics == nil {
			s.logger.Debug("Skipping query with nil metrics")
			continue
		}

		// Skip queries with no new executions
		if !metrics.HasNewExecutions {
			s.logger.Debug("Skipping query with no new executions",
				zap.String("query_id", slowQuery.GetQueryID()))
			continue
		}

		// Apply interval-based threshold filtering
		if metrics.IntervalAvgElapsedTimeMs < float64(s.responseTimeThreshold) {
			s.logger.Debug("Skipping query below threshold",
				zap.String("query_id", slowQuery.GetQueryID()),
				zap.Float64("interval_avg_ms", metrics.IntervalAvgElapsedTimeMs),
				zap.Int("threshold_ms", s.responseTimeThreshold))
			continue
		}

		// Store interval metrics in the query
		slowQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
		slowQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

		queriesToProcess = append(queriesToProcess, slowQuery)
	}

	// Cleanup stale entries periodically
	s.intervalCalculator.CleanupStaleEntries(now)

	// Log calculator statistics
	stats := s.intervalCalculator.GetCacheStats()
	s.logger.Debug("Interval calculator statistics", zap.Any("stats", stats))

	// Sort by interval average elapsed time (slowest first)
	if len(queriesToProcess) > 0 {
		sort.Slice(queriesToProcess, func(i, j int) bool {
			// Handle nil cases
			if queriesToProcess[i].IntervalAvgElapsedTimeMS == nil {
				return false
			}
			if queriesToProcess[j].IntervalAvgElapsedTimeMS == nil {
				return true
			}
			// Sort descending (highest first)
			return *queriesToProcess[i].IntervalAvgElapsedTimeMS > *queriesToProcess[j].IntervalAvgElapsedTimeMS
		})

		// Take top N queries
		if len(queriesToProcess) > s.countThreshold {
			s.logger.Debug("Applying top N selection",
				zap.Int("before_count", len(queriesToProcess)),
				zap.Int("top_n", s.countThreshold))
			queriesToProcess = queriesToProcess[:s.countThreshold]
		}
	}

	s.logger.Debug("Interval calculation completed",
		zap.Int("input_count", len(slowQueries)),
		zap.Int("output_count", len(queriesToProcess)))

	return queriesToProcess
}

// recordMetrics records metrics for a single slow query
func (s *SlowQueryScraper) recordMetrics(now pcommon.Timestamp, query *models.SlowQuery) error {
	if query == nil {
		return fmt.Errorf("query is nil")
	}

	// Get attribute values
	collectionTimestamp := query.GetCollectionTimestamp()
	dbName := query.GetDatabaseName()
	queryID := query.GetQueryID()
	queryText := query.GetQueryText()
	firstSeen := query.GetFirstSeen()
	lastExecTime := query.GetLastExecutionTimestamp()

	// Record execution count (cumulative)
	if query.ExecutionCount.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryExecutionCountDataPoint(
			now,
			query.ExecutionCount.Int64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record average CPU time
	if query.AvgCPUTimeMS.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgCPUTimeMsDataPoint(
			now,
			query.AvgCPUTimeMS.Float64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record average elapsed time (cumulative)
	if query.AvgElapsedTimeMS.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgElapsedTimeMsDataPoint(
			now,
			query.AvgElapsedTimeMS.Float64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record average lock time
	if query.AvgLockTimeMS.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgLockTimeMsDataPoint(
			now,
			query.AvgLockTimeMS.Float64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record average rows examined
	if query.AvgRowsExamined.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgRowsExaminedDataPoint(
			now,
			query.AvgRowsExamined.Float64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record average rows sent
	if query.AvgRowsSent.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgRowsSentDataPoint(
			now,
			query.AvgRowsSent.Float64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record interval metrics (delta) if available
	if query.IntervalAvgElapsedTimeMS != nil {
		s.mb.RecordNewrelicmysqlSlowqueryIntervalAvgElapsedTimeMsDataPoint(
			now,
			*query.IntervalAvgElapsedTimeMS,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	if query.IntervalExecutionCount != nil {
		s.mb.RecordNewrelicmysqlSlowqueryIntervalExecutionCountDataPoint(
			now,
			*query.IntervalExecutionCount,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	// Record query details (constant=1 with all attributes)
	s.mb.RecordNewrelicmysqlSlowqueryQueryDetailsDataPoint(
		now,
		1, // Constant value
		collectionTimestamp,
		dbName,
		queryID,
		queryText,
		firstSeen,
		lastExecTime,
	)

	// Record query errors
	if query.TotalErrors.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryQueryErrorsDataPoint(
			now,
			query.TotalErrors.Int64,
			collectionTimestamp,
			dbName,
			queryID,
		)
	}

	s.logger.Debug("Recorded metrics for slow query",
		zap.String("query_id", queryID),
		zap.String("database_name", dbName),
		zap.Int("query_text_length", len(queryText)))

	return nil
}
