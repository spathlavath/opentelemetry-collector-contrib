// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
)

// SlowQueryClient defines the interface for fetching slow queries from the database
type SlowQueryClient interface {
	GetSlowQueries(ctx context.Context, intervalSeconds int) ([]models.SlowQuery, error)
}

// SlowQueryScraperConfig holds configuration for slow query scraping
type SlowQueryScraperConfig struct {
	IntervalSeconds         int
	TopN                    int
	ElapsedTimeThreshold    int
	EnableIntervalCalc      bool
}

// SlowQueryScraper handles scraping of MySQL slow query metrics from performance_schema
// This implements interval-based delta calculation for accurate Top N selection
//
// Algorithm:
// 1. Fetch raw results from database (time-window filtered)
// 2. Apply interval-based delta calculation (compute metrics since last scrape)
// 3. Filter by threshold (using interval average, not historical average)
// 4. Sort by interval average elapsed time (slowest first)
// 5. Take Top N queries
// 6. Emit metrics
type SlowQueryScraper struct {
	logger             *zap.Logger
	mb                 *metadata.MetricsBuilder
	config             *SlowQueryScraperConfig
	intervalCalculator *MySQLIntervalCalculator
	client             SlowQueryClient
}

// NewSlowQueryScraper creates a new instance of SlowQueryScraper
func NewSlowQueryScraper(
	logger *zap.Logger,
	mb *metadata.MetricsBuilder,
	config *SlowQueryScraperConfig,
	client SlowQueryClient,
) *SlowQueryScraper {
	var intervalCalc *MySQLIntervalCalculator
	if config.EnableIntervalCalc {
		// Use default cache TTL of 60 minutes
		cacheTTL := 60 * time.Minute
		intervalCalc = NewMySQLIntervalCalculator(logger, cacheTTL)
		logger.Info("MySQL interval-based delta calculator enabled for slow queries",
			zap.Duration("cache_ttl", cacheTTL))
	} else {
		logger.Info("MySQL slow query monitoring enabled (using historical averages, delta calculation disabled)")
	}

	return &SlowQueryScraper{
		logger:             logger,
		mb:                 mb,
		config:             config,
		intervalCalculator: intervalCalc,
		client:             client,
	}
}

// Scrape collects slow query metrics using the exact pattern from SQL Server/Oracle receivers
// This implements interval-based delta calculation for accurate Top N selection
//
// Why this pattern works:
// - Database filtering on historical avg would miss queries that JUST became slow
// - Delta calculation shows current performance, not all-time average
// - Sorting AFTER delta ensures we get the truly slow queries RIGHT NOW
func (s *SlowQueryScraper) Scrape(ctx context.Context, now pcommon.Timestamp) error {
	// Step 1: Fetch raw results from database
	// Time window filter applied in SQL (e.g., LAST_SEEN >= NOW() - INTERVAL 60 SECOND)
	rawResults, err := s.client.GetSlowQueries(ctx, s.config.IntervalSeconds)
	if err != nil {
		return err
	}

	s.logger.Debug("Raw slow query metrics fetched",
		zap.Int("raw_result_count", len(rawResults)),
		zap.Int("interval_seconds", s.config.IntervalSeconds))

	// Step 2: Apply interval-based delta calculation if enabled
	// This is the SECRET SAUCE that makes Top N accurate!
	var resultsWithIntervalMetrics []models.SlowQuery
	if s.intervalCalculator != nil {
		currentTime := time.Now()

		// Pre-allocate slice to avoid reallocations
		resultsWithIntervalMetrics = make([]models.SlowQuery, 0, len(rawResults))

		// Calculate interval metrics for each query
		for _, rawQuery := range rawResults {
			// Compute delta: (current_total - previous_total) / (current_count - previous_count)
			metrics := s.intervalCalculator.CalculateMetrics(&rawQuery, currentTime)

			if metrics == nil {
				s.logger.Debug("Skipping query with nil metrics")
				continue
			}

			// Skip queries with no new executions since last scrape
			if !metrics.HasNewExecutions {
				s.logger.Debug("Skipping query with no new executions",
					zap.String("query_id", rawQuery.GetQueryID()),
					zap.Float64("time_since_last_exec_sec", metrics.TimeSinceLastExecSec))
				continue
			}

			// Apply interval-based threshold filtering
			// CRITICAL: This filters on INTERVAL average (delta), not historical average
			// Example: Historical avg = 5ms, but interval avg = 500ms → This query passes!
			if metrics.IntervalAvgElapsedTimeMs < float64(s.config.ElapsedTimeThreshold) {
				s.logger.Debug("Skipping query below interval threshold",
					zap.String("query_id", rawQuery.GetQueryID()),
					zap.Float64("interval_avg_ms", metrics.IntervalAvgElapsedTimeMs),
					zap.Float64("historical_avg_ms", metrics.HistoricalAvgElapsedTimeMs),
					zap.Int("threshold_ms", s.config.ElapsedTimeThreshold),
					zap.Bool("is_first_scrape", metrics.IsFirstScrape))
				continue
			}

			// Store interval metrics in the query struct
			// These fields are used for sorting and metric emission
			rawQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			rawQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount

			resultsWithIntervalMetrics = append(resultsWithIntervalMetrics, rawQuery)
		}

		// Cleanup stale entries (TTL-based)
		s.intervalCalculator.CleanupStaleEntries(currentTime)

		s.logger.Debug("Interval-based delta calculation applied",
			zap.Int("raw_count", len(rawResults)),
			zap.Int("processed_count", len(resultsWithIntervalMetrics)))

		// Log calculator statistics
		stats := s.intervalCalculator.GetCacheStats()
		s.logger.Debug("Interval calculator statistics", zap.Any("stats", stats))

		// Use interval-calculated results for next steps
		rawResults = resultsWithIntervalMetrics
	}

	// Step 3 & 4: Sort by interval average elapsed time (delta) - slowest first
	// This ensures Top N is based on CURRENT performance, not historical
	if s.intervalCalculator != nil && len(rawResults) > 0 {
		sort.Slice(rawResults, func(i, j int) bool {
			// Handle nil cases - queries without interval metrics go to the end
			if rawResults[i].IntervalAvgElapsedTimeMS == nil {
				return false
			}
			if rawResults[j].IntervalAvgElapsedTimeMS == nil {
				return true
			}
			// Sort descending (highest interval average first = slowest queries)
			return *rawResults[i].IntervalAvgElapsedTimeMS > *rawResults[j].IntervalAvgElapsedTimeMS
		})

		s.logger.Debug("Sorted queries by interval average elapsed time (delta)",
			zap.Int("total_count", len(rawResults)))

		// Step 5: Take Top N queries after sorting
		if len(rawResults) > s.config.TopN {
			s.logger.Debug("Applying Top N selection",
				zap.Int("before_count", len(rawResults)),
				zap.Int("top_n", s.config.TopN))
			rawResults = rawResults[:s.config.TopN]
		}
	}

	// Step 6: Emit metrics for the top N slow queries
	for _, query := range rawResults {
		if !query.IsValidForMetrics() {
			continue
		}

		if err := s.recordSlowQueryMetrics(now, &query); err != nil {
			s.logger.Warn("Failed to record slow query metrics",
				zap.String("query_id", query.GetQueryID()),
				zap.Error(err))
		}
	}

	s.logger.Info("Slow query metrics emitted",
		zap.Int("count", len(rawResults)),
		zap.Int("top_n", s.config.TopN),
		zap.Int("threshold_ms", s.config.ElapsedTimeThreshold))

	return nil
}

// recordSlowQueryMetrics emits all slow query metrics for a single query
//
// Metrics emitted:
// - Historical metrics: avg_elapsed_time, avg_cpu_time, avg_lock_time, execution_count
// - Interval (delta) metrics: interval_avg_elapsed_time, interval_execution_count
// - I/O metrics: avg_rows_examined, total_select_scan, total_tmp_disk_tables
// - Performance variance: min_elapsed_time, max_elapsed_time
func (s *SlowQueryScraper) recordSlowQueryMetrics(now pcommon.Timestamp, query *models.SlowQuery) error {
	queryID := query.GetQueryID()
	queryText := query.GetQueryText()
	dbName := query.GetDatabaseName()

	// Log key metrics for verification
	s.logger.Debug("Recording slow query metrics",
		zap.String("query_id", queryID),
		zap.String("database", dbName),
		zap.String("query_text_preview", truncateString(queryText, 100)),
		zap.Float64p("interval_avg_elapsed_ms", query.IntervalAvgElapsedTimeMS),
		zap.Int64p("interval_execution_count", query.IntervalExecutionCount),
		zap.Float64("historical_avg_elapsed_ms", getFloat64OrZero(query.AvgElapsedTimeMs)),
		zap.Int64("total_execution_count", getInt64OrZero(query.ExecutionCount)))

	// ===========================================
	// INTERVAL (DELTA) METRICS - MOST IMPORTANT!
	// ===========================================
	// These show current performance since last scrape

	if query.IntervalAvgElapsedTimeMS != nil {
		s.mb.RecordNewrelicmysqlSlowqueryIntervalAvgElapsedTimeDataPoint(
			now,
			*query.IntervalAvgElapsedTimeMS,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.IntervalExecutionCount != nil {
		s.mb.RecordNewrelicmysqlSlowqueryIntervalExecutionCountDataPoint(
			now,
			*query.IntervalExecutionCount,
			dbName,
			queryID,
			queryText,
		)
	}

	// ===========================================
	// HISTORICAL AVERAGE METRICS
	// ===========================================

	if query.AvgElapsedTimeMs.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgElapsedTimeDataPoint(
			now,
			query.AvgElapsedTimeMs.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.AvgLockTimeMs.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgLockTimeDataPoint(
			now,
			query.AvgLockTimeMs.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.AvgCPUTimeMs.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgCPUTimeDataPoint(
			now,
			query.AvgCPUTimeMs.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.ExecutionCount.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryExecutionCountDataPoint(
			now,
			query.ExecutionCount.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	// ===========================================
	// ROW METRICS
	// ===========================================

	if query.AvgRowsExamined.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgRowsExaminedDataPoint(
			now,
			query.AvgRowsExamined.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.AvgRowsSent.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgRowsSentDataPoint(
			now,
			query.AvgRowsSent.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.AvgRowsAffected.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryAvgRowsAffectedDataPoint(
			now,
			query.AvgRowsAffected.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	// ===========================================
	// PERFORMANCE VARIANCE METRICS
	// ===========================================

	if query.MinElapsedTimeMs.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryMinElapsedTimeDataPoint(
			now,
			query.MinElapsedTimeMs.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.MaxElapsedTimeMs.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryMaxElapsedTimeDataPoint(
			now,
			query.MaxElapsedTimeMs.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	// ===========================================
	// DISK I/O METRICS (PERFORMANCE INDICATORS)
	// ===========================================

	if query.TotalSelectScan.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalSelectScanDataPoint(
			now,
			query.TotalSelectScan.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.TotalSelectFullJoin.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalSelectFullJoinDataPoint(
			now,
			query.TotalSelectFullJoin.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.TotalNoIndexUsed.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalNoIndexUsedDataPoint(
			now,
			query.TotalNoIndexUsed.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.TotalNoGoodIndexUsed.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalNoGoodIndexUsedDataPoint(
			now,
			query.TotalNoGoodIndexUsed.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	// ===========================================
	// TEMPORARY TABLE METRICS (MEMORY PRESSURE)
	// ===========================================

	if query.TotalTmpTables.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalTmpTablesDataPoint(
			now,
			query.TotalTmpTables.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.TotalTmpDiskTables.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalTmpDiskTablesDataPoint(
			now,
			query.TotalTmpDiskTables.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	// ===========================================
	// ERROR AND WARNING METRICS
	// ===========================================

	if query.TotalLockTimeMS.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalLockTimeDataPoint(
			now,
			query.TotalLockTimeMS.Float64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.TotalErrors.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalErrorsDataPoint(
			now,
			query.TotalErrors.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	if query.TotalWarnings.Valid {
		s.mb.RecordNewrelicmysqlSlowqueryTotalWarningsDataPoint(
			now,
			query.TotalWarnings.Int64,
			dbName,
			queryID,
			queryText,
		)
	}

	return nil
}

// Helper functions for safe null handling

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func getFloat64OrZero(nf sql.NullFloat64) float64 {
	if nf.Valid {
		return nf.Float64
	}
	return 0
}

func getInt64OrZero(ni sql.NullInt64) int64 {
	if ni.Valid {
		return ni.Int64
	}
	return 0
}
