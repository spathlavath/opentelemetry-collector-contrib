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

func (s *WaitEventsScraper) Scrape(ctx context.Context) []error {
	var errors []error
	s.logger.Info("Starting Oracle wait events scraping", zap.String("instance", s.instanceName))

	// Add early check to see if we can execute any query
	s.logger.Info("Testing database connection for wait events")
	testRow := s.db.QueryRowContext(ctx, "SELECT 1 FROM dual")
	var testVal int
	if err := testRow.Scan(&testVal); err != nil {
		s.logger.Error("Database connection test failed for wait events", zap.Error(err))
		errors = append(errors, fmt.Errorf("database connection test failed: %w", err))
		return errors
	}
	s.logger.Info("Database connection test successful for wait events")

	now := pcommon.NewTimestampFromTime(time.Now())

	// Execute the wait metrics query
	s.logger.Debug("Executing wait metrics query", zap.String("query", queries.QueryWaitMetricsQuery))
	rows, err := s.db.QueryContext(ctx, queries.QueryWaitMetricsQuery)
	if err != nil {
		s.logger.Error("Failed to execute wait events query", zap.Error(err))
		errors = append(errors, fmt.Errorf("error executing wait events query: %w", err))
		return errors
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
		errors = append(errors, fmt.Errorf("error iterating wait events rows: %w", err))
		return errors
	}

	s.logger.Info("Completed wait events scraping",
		zap.Int("metrics_collected", rowCount),
		zap.String("instance", s.instanceName),
	)

	return errors
}
