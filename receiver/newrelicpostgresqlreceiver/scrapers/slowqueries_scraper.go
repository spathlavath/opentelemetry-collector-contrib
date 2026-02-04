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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// SlowQueriesScraper contains the scraper for slow queries metrics using pg_stat_statements
type SlowQueriesScraper struct {
	client                               client.PostgreSQLClient
	mb                                   *metadata.MetricsBuilder
	logger                               *zap.Logger
	metricsBuilderConfig                 metadata.MetricsBuilderConfig
	queryMonitoringResponseTimeThreshold int
	queryMonitoringCountThreshold        int
	queryMonitoringIntervalSeconds       int                             // Time window for fetching queries (in seconds)
	intervalCalculator                   *PostgreSQLIntervalCalculator // Delta-based interval calculator
}

// NewSlowQueriesScraper creates a new slow queries scraper
func NewSlowQueriesScraper(
	sqlClient client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	metricsBuilderConfig metadata.MetricsBuilderConfig,
	responseTimeThreshold int,
	countThreshold int,
	intervalSeconds int,
	enableIntervalCalculator bool,
	intervalCalculatorCacheTTLMinutes int,
) *SlowQueriesScraper {
	var intervalCalc *PostgreSQLIntervalCalculator

	// Initialize interval-based calculator if enabled
	if enableIntervalCalculator {
		cacheTTL := time.Duration(intervalCalculatorCacheTTLMinutes) * time.Minute
		intervalCalc = NewPostgreSQLIntervalCalculator(logger, cacheTTL)
		logger.Info("PostgreSQL interval-based delta calculator enabled",
			zap.Duration("cache_ttl", cacheTTL))
	} else {
		logger.Info("PostgreSQL interval-based delta calculator disabled - using cumulative averages")
	}

	return &SlowQueriesScraper{
		client:                               sqlClient,
		mb:                                   mb,
		logger:                               logger,
		metricsBuilderConfig:                 metricsBuilderConfig,
		queryMonitoringResponseTimeThreshold: responseTimeThreshold,
		queryMonitoringCountThreshold:        countThreshold,
		queryMonitoringIntervalSeconds:       intervalSeconds,
		intervalCalculator:                   intervalCalc,
	}
}

// ScrapeSlowQueries scrapes slow queries from pg_stat_statements
func (s *SlowQueriesScraper) ScrapeSlowQueries(ctx context.Context) ([]string, []error) {
	var scrapeErrors []error
	var queryIDs []string

	// Fetch queries from the database
	// Note: intervalSeconds determines the expected collection interval
	// Delta calculation is done in Go layer (not in SQL WHERE clause)
	slowQueries, err := s.client.QuerySlowQueries(ctx, s.queryMonitoringIntervalSeconds, s.queryMonitoringResponseTimeThreshold, s.queryMonitoringCountThreshold)
	if err != nil {
		return nil, []error{err}
	}

	// Apply interval-based delta calculation if enabled
	var queriesToProcess []models.SlowQuery
	if s.intervalCalculator != nil {
		now := time.Now()

		// Pre-allocate slice capacity to avoid reallocations
		queriesToProcess = make([]models.SlowQuery, 0, len(slowQueries))

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
			// First scrape: interval = historical (no baseline), filter still applies
			// Subsequent scrapes: interval = delta, filter on delta performance
			if metrics.IntervalAvgElapsedTimeMs < float64(s.queryMonitoringResponseTimeThreshold) {
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
			if len(queriesToProcess) > s.queryMonitoringCountThreshold {
				queriesToProcess = queriesToProcess[:s.queryMonitoringCountThreshold]
			}
		}
	} else {
		// No interval calculator - use raw results
		queriesToProcess = slowQueries
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, slowQuery := range queriesToProcess {
		if !slowQuery.IsValidForMetrics() {
			continue
		}

		collectionTimestamp := slowQuery.GetCollectionTimestamp()
		dbName := slowQuery.GetDatabaseName()
		qID := slowQuery.GetQueryID()
		qText := slowQuery.GetQueryText()
		userName := slowQuery.GetUserName()

		if err := s.recordMetrics(now, &slowQuery, collectionTimestamp, dbName, qID, qText, userName); err != nil {
			s.logger.Warn("Failed to record metrics for slow query",
				zap.String("query_id", qID),
				zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if slowQuery.QueryID.Valid {
			queryIDs = append(queryIDs, slowQuery.QueryID.String)
		}
	}

	s.logger.Debug("Slow queries scrape completed",
		zap.Int("queries_processed", len(queriesToProcess)),
		zap.Int("errors", len(scrapeErrors)))

	return queryIDs, scrapeErrors
}

func (s *SlowQueriesScraper) recordMetrics(now pcommon.Timestamp, slowQuery *models.SlowQuery, collectionTimestamp, dbName, qID, qText, userName string) error {
	if slowQuery == nil {
		s.logger.Warn("Attempted to record metrics for nil slow query")
		return fmt.Errorf("slow query is nil")
	}

	// TODO: Add metric recording methods here once metadata.yaml is updated
	// These will be similar to Oracle's pattern:
	// - RecordNewrelicpostgresqlSlowQueriesExecutionCountDataPoint
	// - RecordNewrelicpostgresqlSlowQueriesAvgElapsedTimeDataPoint
	// - RecordNewrelicpostgresqlSlowQueriesAvgCPUTimeDataPoint
	// - RecordNewrelicpostgresqlSlowQueriesAvgDiskReadsDataPoint
	// - etc.

	s.logger.Debug("Slow query metrics recorded",
		zap.String("query_id", qID),
		zap.String("database", dbName),
		zap.String("user", userName))

	return nil
}
