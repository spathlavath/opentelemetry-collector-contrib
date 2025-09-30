// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// SlowQueriesScraper handles Oracle slow queries metrics
type SlowQueriesScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig
	enabled      bool
}

// NewSlowQueriesScraper creates a new slow queries scraper with default enabled set to true
func NewSlowQueriesScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *SlowQueriesScraper {
	return &SlowQueriesScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
		enabled:      true, // Default slow queries to true as requested
	}
}

// ScrapeSlowQueries collects Oracle slow queries information
func (s *SlowQueriesScraper) ScrapeSlowQueries(ctx context.Context) []error {
	var errors []error

	if !s.enabled {
		s.logger.Debug("Slow queries scraping is disabled")
		return errors
	}

	// Check if any slow queries metrics are enabled
	if !s.config.Metrics.NewrelicoracledbSlowQueriesAvgCpuTimeMs.Enabled &&
		!s.config.Metrics.NewrelicoracledbSlowQueriesAvgElapsedTimeMs.Enabled &&
		!s.config.Metrics.NewrelicoracledbSlowQueriesAvgDiskReads.Enabled &&
		!s.config.Metrics.NewrelicoracledbSlowQueriesExecutionCount.Enabled &&
		!s.config.Metrics.NewrelicoracledbSlowQueriesCount.Enabled {
		s.logger.Debug("All slow queries metrics are disabled")
		return errors
	}

	s.logger.Info("Scraping Oracle slow queries")
	now := pcommon.NewTimestampFromTime(time.Now())

	// Scrape slow queries metrics
	errors = append(errors, s.scrapeSlowQueriesMetrics(ctx, now)...)

	return errors
}

// scrapeSlowQueriesMetrics handles the slow queries metrics collection, similar to scrapeGlobalNameTablespaceMetrics
func (s *SlowQueriesScraper) scrapeSlowQueriesMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute slow queries query directly using the shared DB connection
	s.logger.Info("Executing slow queries query", zap.String("sql", queries.SlowQueries))

	rows, err := s.db.QueryContext(ctx, queries.SlowQueries)
	if err != nil {
		s.logger.Error("Error executing slow queries query", zap.Error(err))
		errors = append(errors, fmt.Errorf("error executing slow queries query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var databaseName, queryID, schemaName, statementType, queryText string
		var executionCount int64
		var avgCpuTimeMs, avgDiskReads, avgElapsedTimeMs float64

		err := rows.Scan(
			&databaseName,
			&queryID,
			&schemaName,
			&statementType,
			&executionCount,
			&queryText,
			&avgCpuTimeMs,
			&avgDiskReads,
			&avgElapsedTimeMs,
		)
		if err != nil {
			s.logger.Error("Error scanning slow query row", zap.Error(err))
			errors = append(errors, fmt.Errorf("error scanning slow query row: %w", err))
			continue
		}

		// Log the query_id and query_text as requested
		s.logger.Info("Slow query metrics collected",
			zap.String("query_id", queryID),
			zap.String("query_text", queryText),
			zap.String("database_name", databaseName),
			zap.String("schema_name", schemaName),
			zap.String("statement_type", statementType),
			zap.Int64("execution_count", executionCount),
			zap.Float64("avg_cpu_time_ms", avgCpuTimeMs),
			zap.Float64("avg_disk_reads", avgDiskReads),
			zap.Float64("avg_elapsed_time_ms", avgElapsedTimeMs),
			zap.String("instance", s.instanceName),
		)

		// Record slow queries metrics using the proper metadata builder methods
		// Following the pattern of scrapeGlobalNameTablespaceMetrics
		s.logger.Debug("Recording slow queries metrics",
			zap.String("query_id", queryID),
			zap.Float64("avg_cpu_time_ms", avgCpuTimeMs),
		)

		// Record metrics only if they are enabled
		if s.config.Metrics.NewrelicoracledbSlowQueriesCount.Enabled {
			s.mb.RecordNewrelicoracledbSlowQueriesCountDataPoint(now, 1, s.instanceName, s.instanceName)
		}
		if s.config.Metrics.NewrelicoracledbSlowQueriesAvgElapsedTimeMs.Enabled {
			s.mb.RecordNewrelicoracledbSlowQueriesAvgElapsedTimeMsDataPoint(now, avgElapsedTimeMs, s.instanceName, s.instanceName, queryID, databaseName, schemaName)
		}
		if s.config.Metrics.NewrelicoracledbSlowQueriesAvgCpuTimeMs.Enabled {
			s.mb.RecordNewrelicoracledbSlowQueriesAvgCpuTimeMsDataPoint(now, avgCpuTimeMs, s.instanceName, s.instanceName, queryID, databaseName, schemaName)
		}
		if s.config.Metrics.NewrelicoracledbSlowQueriesAvgDiskReads.Enabled {
			s.mb.RecordNewrelicoracledbSlowQueriesAvgDiskReadsDataPoint(now, avgDiskReads, s.instanceName, s.instanceName, queryID, databaseName, schemaName)
		}
		if s.config.Metrics.NewrelicoracledbSlowQueriesExecutionCount.Enabled {
			s.mb.RecordNewrelicoracledbSlowQueriesExecutionCountDataPoint(now, executionCount, s.instanceName, s.instanceName, queryID, databaseName, schemaName)
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		s.logger.Error("Error iterating slow queries rows", zap.Error(err))
		errors = append(errors, fmt.Errorf("error iterating slow queries rows: %w", err))
	}

	s.logger.Info("Collected Oracle slow queries metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// SetEnabled enables or disables slow queries collection
func (s *SlowQueriesScraper) SetEnabled(enabled bool) {
	s.enabled = enabled
}

// IsEnabled returns whether slow queries collection is enabled
func (s *SlowQueriesScraper) IsEnabled() bool {
	return s.enabled
}
