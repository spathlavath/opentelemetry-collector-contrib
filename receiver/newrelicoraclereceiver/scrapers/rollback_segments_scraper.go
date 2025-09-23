// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// RollbackSegmentsScraper is a scraper for rollback segment metrics
type RollbackSegmentsScraper struct {
	db           *sql.DB
	logger       *zap.Logger
	mb           *metadata.MetricsBuilder
	instanceName string
	config       Config
}

// NewRollbackSegmentsScraper creates a new RollbackSegmentsScraper
func NewRollbackSegmentsScraper(db *sql.DB, logger *zap.Logger, mb *metadata.MetricsBuilder, instanceName string, config Config) *RollbackSegmentsScraper {
	return &RollbackSegmentsScraper{
		db:           db,
		logger:       logger,
		mb:           mb,
		instanceName: instanceName,
		config:       config,
	}
}

// ScrapeRollbackSegments collects Oracle rollback segment metrics
// Follows the exact same pattern as nri-oracledb oracleRollbackSegments metricsGenerator
func (s *RollbackSegmentsScraper) ScrapeRollbackSegments(ctx context.Context) []error {
	var errors []error

	// Execute the RollbackSegmentsSQL query
	rows, err := s.db.QueryContext(ctx, queries.RollbackSegmentsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("failed to execute RollbackSegmentsSQL query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row returned by the query
	for rows.Next() {
		err := s.processRow(rows)
		if err != nil {
			errors = append(errors, fmt.Errorf("error processing row: %w", err))
		}
	}

	// Check for any errors that occurred during row iteration
	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating over rows: %w", err))
	}

	return errors
}

// processRow processes a single row from the RollbackSegmentsSQL query
// Matches the exact pattern used in nri-oracledb oracleRollbackSegments metricsGenerator
func (s *RollbackSegmentsScraper) processRow(rows *sql.Rows) error {
	var gets int64
	var waits int64
	var ratio float64
	var instID int64

	err := rows.Scan(&gets, &waits, &ratio, &instID)
	if err != nil {
		return fmt.Errorf("error scanning row: %w", err)
	}

	// Use timestamp at scan time like nri-oracledb
	now := pcommon.NewTimestampFromTime(time.Now())
	instanceIDStr := strconv.FormatInt(instID, 10)

	// Record rollback segment metrics if enabled
	if s.config.GetMetrics().NewrelicoracledbRollbackSegmentsGets.Enabled {
		s.mb.RecordNewrelicoracledbRollbackSegmentsGetsDataPoint(now, gets, s.instanceName, instanceIDStr)
	}

	if s.config.GetMetrics().NewrelicoracledbRollbackSegmentsWaits.Enabled {
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitsDataPoint(now, waits, s.instanceName, instanceIDStr)
	}

	if s.config.GetMetrics().NewrelicoracledbRollbackSegmentsRatioWait.Enabled {
		s.mb.RecordNewrelicoracledbRollbackSegmentsRatioWaitDataPoint(now, ratio, s.instanceName, instanceIDStr)
	}

	return nil
}
