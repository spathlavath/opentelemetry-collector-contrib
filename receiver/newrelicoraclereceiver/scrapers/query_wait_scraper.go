// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
func (s *QueryWaitScraper) ScrapeQueryWaitMetrics(ctx context.Context) (pmetric.Metrics, []error) {
	var errors []error

	// Create a new metrics object for query wait metrics
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()

	// Add resource attributes
	resource := resourceMetrics.Resource()
	resource.Attributes().PutStr("newrelicoracledb.instance.name", s.instanceName)
	resource.Attributes().PutStr("host.name", s.instanceName)

	s.logger.Debug("Scraping Oracle query wait metrics")

	// Execute query wait metrics query
	s.logger.Debug("Executing query wait metrics query", zap.String("sql", queries.QueryWaitMetricsQuery))

	rows, err := s.db.QueryContext(ctx, queries.QueryWaitMetricsQuery)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing query wait metrics query: %w", err))
		return metrics, errors
	}
	defer rows.Close()

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scope := scopeMetrics.Scope()
	scope.SetName("newrelicoracledb")
	scope.SetVersion("1.0.0")

	var recordCount int
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

		// Create a metric point for each query wait record
		queryWaitMetric := scopeMetrics.Metrics().AppendEmpty()
		queryWaitMetric.SetName("newrelicoracledb.query.wait_time")
		queryWaitMetric.SetDescription("Oracle query wait time metrics")
		queryWaitMetric.SetUnit("ms")

		gauge := queryWaitMetric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()

		// Set timestamp - convert from Oracle epoch (seconds) to nanoseconds
		if metric.Timestamp > 0 {
			timestampNs := int64(metric.Timestamp * 1000000) // Convert milliseconds to nanoseconds
			dataPoint.SetTimestamp(pcommon.Timestamp(timestampNs))
		} else {
			dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		}

		// Set the value (total wait time in milliseconds)
		if metric.TotalWaitTimeMs.Valid {
			dataPoint.SetDoubleValue(metric.TotalWaitTimeMs.Float64)
		} else {
			dataPoint.SetDoubleValue(0)
		}

		// Add attributes to the data point
		attributes := dataPoint.Attributes()

		if metric.QueryText.Valid {
			attributes.PutStr("query_text", metric.QueryText.String)
		}

		if metric.QueryID.Valid {
			attributes.PutStr("query_id", metric.QueryID.String)
		}

		if metric.Database.Valid {
			attributes.PutStr("database", metric.Database.String)
		} else {
			attributes.PutStr("database", "")
		}

		if metric.WaitEventName.Valid {
			attributes.PutStr("wait_event_name", metric.WaitEventName.String)
		}

		if metric.WaitCategory.Valid {
			attributes.PutStr("wait_category", metric.WaitCategory.String)
		}

		recordCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating over query wait metric rows: %w", err))
	}

	s.logger.Debug("Collected Oracle query wait metrics",
		zap.Int("record_count", recordCount),
		zap.String("instance", s.instanceName))

	return metrics, errors
}
