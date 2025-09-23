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

// WaitScraper handles wait time Oracle metrics
type WaitScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig
}

// NewWaitScraper creates a new wait scraper
func NewWaitScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *WaitScraper {
	return &WaitScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}
}

// ScrapeWaitTime collects Oracle wait time metrics
func (s *WaitScraper) ScrapeWaitTime(ctx context.Context) []error {
	var errors []error
	s.logger.Debug("Scraping Oracle wait time metrics")
	now := pcommon.NewTimestampFromTime(time.Now())

	rows, err := s.db.QueryContext(ctx, queries.QueryWaitMetricsQuery)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing wait time query: %w", err))
		return errors
	}
	defer rows.Close()

	var rowCount int
	for rows.Next() {
		var databaseName, queryID, queryText, waitCategory, waitEventName string
		var collectionTimestamp time.Time
		var waitingTasksCount, totalWaitTimeMs int64

		if err := rows.Scan(
			&databaseName,
			&queryID,
			&queryText,
			&waitCategory,
			&waitEventName,
			&collectionTimestamp,
			&waitingTasksCount,
			&totalWaitTimeMs,
		); err != nil {
			errors = append(errors, fmt.Errorf("error scanning wait time row: %w", err))
			continue
		}

		// Assuming RecordNewrelicoracledbTotalWaitTimeDataPoint and RecordNewrelicoracledbWaitingTasksCountDataPoint exist
		// and have a signature that accepts these attributes.
		s.mb.RecordNewrelicoracledbTotalWaitTimeDataPoint(now, totalWaitTimeMs, s.instanceName, databaseName, queryID, queryText, waitCategory, waitEventName)
		// s.mb.RecordNewrelicoracledbWaitingTasksCountDataPoint(now, waitingTasksCount, s.instanceName, databaseName, queryID, queryText, waitCategory, waitEventName)
		rowCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating wait time rows: %w", err))
	}

	s.logger.Debug("Collected Oracle wait time metrics", zap.Int("rows", rowCount), zap.String("instance", s.instanceName))

	return errors
}
