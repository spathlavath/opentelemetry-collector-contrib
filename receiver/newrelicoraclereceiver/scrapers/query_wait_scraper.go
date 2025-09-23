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

// QueryWaitScraper handles query wait metrics collection
type QueryWaitScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig
}

// QueryWaitMetric represents a single query wait metric record
type QueryWaitMetric struct {
	Timestamp       float64         `db:"timestamp"`
	QueryText       sql.NullString  `db:"query_text"`
	QueryID         sql.NullString  `db:"query_id"`
	Database        sql.NullString  `db:"database"`
	WaitEventName   sql.NullString  `db:"wait_event_name"`
	WaitCategory    sql.NullString  `db:"wait_category"`
	TotalWaitTimeMs sql.NullFloat64 `db:"total_wait_time_ms"`
}

// NewQueryWaitScraper creates a new query wait scraper
func NewQueryWaitScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *QueryWaitScraper {
	return &QueryWaitScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}
}

// ScrapeQueryWaitMetrics collects Oracle query wait metrics
func (s *QueryWaitScraper) ScrapeQueryWaitMetrics(ctx context.Context) []error {
	var errors []error

	// Check if the metric is enabled
	if !s.config.Metrics.NewrelicoracledbQueryWaitTime.Enabled {
		return errors
	}

	s.logger.Debug("Scraping Oracle query wait metrics")

	// Execute query wait metrics query
	s.logger.Debug("Executing query wait metrics query", zap.String("sql", queries.QueryWaitMetricsQuery))

	rows, err := s.db.QueryContext(ctx, queries.QueryWaitMetricsQuery)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing query wait metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	var recordCount int
	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var metric QueryWaitMetric

		err := rows.Scan(
			&metric.Timestamp,
			&metric.QueryText,
			&metric.QueryID,
			&metric.Database,
			&metric.WaitEventName,
			&metric.WaitCategory,
			&metric.TotalWaitTimeMs,
		)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning query wait metric row: %w", err))
			continue
		}

		// Prepare attribute values
		queryText := ""
		if metric.QueryText.Valid {
			queryText = metric.QueryText.String
		}

		queryID := ""
		if metric.QueryID.Valid {
			queryID = metric.QueryID.String
		}

		database := ""
		if metric.Database.Valid {
			database = metric.Database.String
		}

		waitEventName := ""
		if metric.WaitEventName.Valid {
			waitEventName = metric.WaitEventName.String
		}

		waitCategory := ""
		if metric.WaitCategory.Valid {
			waitCategory = metric.WaitCategory.String
		}

		waitTimeMs := 0.0
		if metric.TotalWaitTimeMs.Valid {
			waitTimeMs = metric.TotalWaitTimeMs.Float64
		}

		// Record the data point using the generated metadata builder
		s.mb.RecordNewrelicoracledbQueryWaitTimeDataPoint(
			now,
			waitTimeMs,
			queryText,
			queryID,
			database,
			waitEventName,
			waitCategory,
		)

		recordCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating over query wait metric rows: %w", err))
	}

	s.logger.Debug("Collected Oracle query wait metrics",
		zap.Int("record_count", recordCount),
		zap.String("instance", s.instanceName))

	return errors
}
