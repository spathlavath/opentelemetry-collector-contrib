// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// SlowQueriesScraper scrapes slow query metrics from pg_stat_statements
type SlowQueriesScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
	pgVersion    int // PostgreSQL version number

	// Configuration
	sqlRowLimit              int  // SQL pre-filter: top N queries by historical average
	responseTimeThreshold    int  // Threshold in ms (applied AFTER delta calculation)
	countThreshold           int  // Top N queries after all filtering
	enableIntervalCalculator bool // Enable interval-based delta calculation

	// Interval calculator for delta-based filtering
	intervalCalculator *PgIntervalCalculator
}

// NewSlowQueriesScraper creates a new SlowQueriesScraper
func NewSlowQueriesScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
	pgVersion int,
	sqlRowLimit int,
	responseTimeThreshold int,
	countThreshold int,
	enableIntervalCalculator bool,
	cacheTTLMinutes int,
) *SlowQueriesScraper {
	scraper := &SlowQueriesScraper{
		client:                   client,
		mb:                       mb,
		logger:                   logger,
		instanceName:             instanceName,
		mbConfig:                 mbConfig,
		pgVersion:                pgVersion,
		sqlRowLimit:              sqlRowLimit,
		responseTimeThreshold:    responseTimeThreshold,
		countThreshold:           countThreshold,
		enableIntervalCalculator: enableIntervalCalculator,
	}

	// Initialize interval calculator if enabled
	if enableIntervalCalculator {
		cacheTTL := time.Duration(cacheTTLMinutes) * time.Minute
		scraper.intervalCalculator = NewPgIntervalCalculator(logger, cacheTTL)
	}

	return scraper
}

// ScrapeSlowQueries scrapes slow query metrics from pg_stat_statements
func (s *SlowQueriesScraper) ScrapeSlowQueries(ctx context.Context) []error {
	// Query slow queries with SQL row limit for pre-filtering
	slowQueries, err := s.client.QuerySlowQueries(ctx, s.pgVersion, s.sqlRowLimit)
	if err != nil {
		s.logger.Error("Failed to query slow queries", zap.Error(err))
		return []error{err}
	}

	s.logger.Debug("Fetched slow queries from database",
		zap.Int("query_count", len(slowQueries)))

	var queriesToProcess []models.PgSlowQueryMetric

	if s.enableIntervalCalculator && s.intervalCalculator != nil {
		// Apply interval-based delta calculation
		now := time.Now()
		queriesToProcess = make([]models.PgSlowQueryMetric, 0, len(slowQueries))

		for _, slowQuery := range slowQueries {
			// Calculate interval metrics
			metrics := s.intervalCalculator.CalculateMetrics(&slowQuery, now)

			if metrics == nil {
				s.logger.Debug("Skipping query with nil metrics")
				continue
			}

			// Skip queries with no new executions (THIS REPLACES SQL TIME FILTER!)
			if !metrics.HasNewExecutions {
				s.logger.Debug("Skipping query with no new executions",
					zap.String("query_id", slowQuery.QueryID))
				continue
			}

			// Apply interval-based threshold filtering (applied AFTER delta calculation)
			// First scrape: interval = historical (no baseline), filter still applies
			// Subsequent scrapes: interval = delta, filter on delta performance
			if metrics.IntervalAvgElapsedTimeMs < float64(s.responseTimeThreshold) {
				continue
			}

			// Store interval metrics in new fields (preserve historical values)
			// Historical values remain unchanged from DB
			slowQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			slowQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

			queriesToProcess = append(queriesToProcess, slowQuery)
		}

		// Cleanup stale entries periodically (TTL-based only)
		s.intervalCalculator.CleanupStaleEntries(now)

		// Log calculator statistics
		stats := s.intervalCalculator.GetCacheStats()
		s.logger.Debug("Interval calculator statistics", zap.Any("stats", stats))

		// Apply Go-level sorting and top-N selection AFTER delta calculation and filtering
		// This ensures we get the slowest queries based on interval (delta) averages, not historical
		if len(queriesToProcess) > 0 {
			// Sort by interval average elapsed time (delta) - slowest first
			sort.Slice(queriesToProcess, func(i, j int) bool {
				// Handle nil cases - queries without interval metrics go to the end
				if queriesToProcess[i].IntervalAvgElapsedTimeMS == nil {
					return false
				}
				if queriesToProcess[j].IntervalAvgElapsedTimeMS == nil {
					return true
				}
				// Sort descending (highest delta average first)
				return *queriesToProcess[i].IntervalAvgElapsedTimeMS > *queriesToProcess[j].IntervalAvgElapsedTimeMS
			})

			// Take top N queries after sorting
			if len(queriesToProcess) > s.countThreshold {
				queriesToProcess = queriesToProcess[:s.countThreshold]
			}
		}
	} else {
		// No interval calculator - use raw results
		queriesToProcess = slowQueries
	}

	// Emit metrics
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, metric := range queriesToProcess {
		// Validate before emitting
		if !metric.IsValidForMetrics() {
			s.logger.Warn("Skipping invalid slow query metric",
				zap.String("query_id", metric.QueryID))
			continue
		}

		s.recordMetricsForSlowQuery(now, metric)
	}

	s.logger.Debug("Slow queries scrape completed",
		zap.Int("queries_processed", len(queriesToProcess)))

	return nil
}

// recordMetricsForSlowQuery records all metrics for a single slow query
func (s *SlowQueriesScraper) recordMetricsForSlowQuery(now pcommon.Timestamp, metric models.PgSlowQueryMetric) {
	databaseName := getString(metric.DatabaseName)
	userName := getString(metric.UserName)
	queryID := metric.QueryID
	queryText := getString(metric.QueryText)

	// Format collection timestamp for metrics
	collectionTimestamp := metric.CollectionTimestamp.Format(time.RFC3339)

	// Record execution metrics (with timestamp, without query_text)
	s.mb.RecordPostgresqlSlowQueriesExecutionCountDataPoint(
		now,
		getInt64(metric.ExecutionCount),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesAvgElapsedTimeMsDataPoint(
		now,
		getFloat64(metric.AvgElapsedTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesMinElapsedTimeMsDataPoint(
		now,
		getFloat64(metric.MinElapsedTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesMaxElapsedTimeMsDataPoint(
		now,
		getFloat64(metric.MaxElapsedTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesStddevElapsedTimeMsDataPoint(
		now,
		getFloat64(metric.StddevElapsedTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesTotalElapsedTimeMsDataPoint(
		now,
		getFloat64(metric.TotalElapsedTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	// Record planning time metrics
	s.mb.RecordPostgresqlSlowQueriesAvgPlanTimeMsDataPoint(
		now,
		getFloat64(metric.AvgPlanTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	// Record CPU time metrics
	s.mb.RecordPostgresqlSlowQueriesAvgCPUTimeMsDataPoint(
		now,
		getFloat64(metric.AvgCPUTimeMs),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	// Record I/O metrics
	s.mb.RecordPostgresqlSlowQueriesAvgDiskReadsDataPoint(
		now,
		getFloat64(metric.AvgDiskReads),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesTotalDiskReadsDataPoint(
		now,
		getInt64(metric.TotalDiskReads),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesAvgBufferHitsDataPoint(
		now,
		getFloat64(metric.AvgBufferHits),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesTotalBufferHitsDataPoint(
		now,
		getInt64(metric.TotalBufferHits),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesAvgDiskWritesDataPoint(
		now,
		getFloat64(metric.AvgDiskWrites),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesTotalDiskWritesDataPoint(
		now,
		getInt64(metric.TotalDiskWrites),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	// Record row statistics
	s.mb.RecordPostgresqlSlowQueriesAvgRowsReturnedDataPoint(
		now,
		getFloat64(metric.AvgRowsReturned),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	s.mb.RecordPostgresqlSlowQueriesTotalRowsDataPoint(
		now,
		getInt64(metric.TotalRows),
		collectionTimestamp,
		databaseName,
		userName,
		queryID,
	)

	// Record interval-based average elapsed time if available (delta metric)
	// These are calculated by PgIntervalCalculator between scrapes
	if metric.IntervalAvgElapsedTimeMS != nil {
		s.mb.RecordPostgresqlSlowQueriesIntervalAvgElapsedTimeMsDataPoint(
			now,
			*metric.IntervalAvgElapsedTimeMS,
			collectionTimestamp,
			databaseName,
			userName,
			queryID,
		)
	}

	// Record interval execution count if available (delta metric)
	if metric.IntervalExecutionCount != nil {
		s.mb.RecordPostgresqlSlowQueriesIntervalExecutionCountDataPoint(
			now,
			*metric.IntervalExecutionCount,
			collectionTimestamp,
			databaseName,
			userName,
			queryID,
		)
	}

	// Record query details event (similar to Oracle's QueryDetails event)
	// This contains the full query text and other metadata
	s.mb.RecordPostgresqlSlowQueriesQueryDetailsDataPoint(
		now,
		1, // Event marker value
		"PostgreSQLQueryDetails",
		collectionTimestamp,
		databaseName,
		queryID,
		queryText,
		userName,
		s.instanceName,
	)
}
