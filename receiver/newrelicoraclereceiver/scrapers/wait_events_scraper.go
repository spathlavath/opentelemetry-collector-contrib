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
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

type WaitEventsScraper struct {
	db           *sql.DB
	logger       *zap.Logger
	config       *scraperhelper.ControllerConfig
	mb           *metadata.MetricsBuilder
	instanceName string
}

func NewWaitEventsScraper(db *sql.DB, logger *zap.Logger, config *scraperhelper.ControllerConfig, mb *metadata.MetricsBuilder, instanceName string) *WaitEventsScraper {
	return &WaitEventsScraper{
		db:           db,
		logger:       logger,
		config:       config,
		mb:           mb,
		instanceName: instanceName,
	}
}

func (s *WaitEventsScraper) Scrape(ctx context.Context) error {
	s.logger.Debug("Scraping Oracle wait events")

	now := pcommon.NewTimestampFromTime(time.Now())

	// Execute the wait metrics query
	rows, err := s.db.QueryContext(ctx, queries.QueryWaitMetricsQuery)
	if err != nil {
		s.logger.Error("Failed to execute wait events query", zap.Error(err))
		return fmt.Errorf("error executing wait events query: %w", err)
	}
	defer rows.Close()

	var rowCount int
	for rows.Next() {
		var (
			databaseName        string
			queryID             string
			queryText           string
			waitCategory        string
			waitEventName       string
			collectionTimestamp time.Time
			waitingTasksCount   int64
			totalWaitTimeMs     int64
		)

		// Scan the row data
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
			s.logger.Error("Failed to scan wait events row", zap.Error(err))
			continue
		}

		// Record the wait time metric with all attributes
		s.mb.RecordNewrelicoracledbTotalWaitTimeDataPoint(
			now,
			totalWaitTimeMs,
			s.instanceName, // newrelic.entity_name
			databaseName,   // database_name
			queryID,        // query_id
			queryText,      // query_text
			waitCategory,   // wait_category
			waitEventName,  // wait_event_name
		)

		rowCount++
		s.logger.Debug("Recorded wait event metric",
			zap.String("database", databaseName),
			zap.String("query_id", queryID),
			zap.String("wait_category", waitCategory),
			zap.String("wait_event", waitEventName),
			zap.Int64("wait_time_ms", totalWaitTimeMs),
		)
	}

	if err = rows.Err(); err != nil {
		s.logger.Error("Error iterating wait events rows", zap.Error(err))
		return fmt.Errorf("error iterating wait events rows: %w", err)
	}

	s.logger.Info("Completed wait events scraping",
		zap.Int("metrics_collected", rowCount),
		zap.String("instance", s.instanceName),
	)

	return nil
}
