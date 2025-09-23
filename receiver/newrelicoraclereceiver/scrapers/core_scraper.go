// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// CoreScraper handles Oracle core database metrics
type CoreScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig
}

// NewCoreScraper creates a new core scraper
func NewCoreScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *CoreScraper {
	return &CoreScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}
}

// ScrapeCoreMetrics collects Oracle core database metrics
func (s *CoreScraper) ScrapeCoreMetrics(ctx context.Context) []error {
	var errors []error

	s.logger.Debug("Scraping Oracle core database metrics")
	now := pcommon.NewTimestampFromTime(time.Now())

	// Scrape locked accounts metrics
	errors = append(errors, s.scrapeLockedAccountsMetrics(ctx, now)...)

	// Scrape redo log waits metrics
	errors = append(errors, s.scrapeRedoLogWaitsMetrics(ctx, now)...)

	return errors
}

// scrapeRedoLogWaitsMetrics handles the redo log waits metrics
func (s *CoreScraper) scrapeRedoLogWaitsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute redo log waits query
	s.logger.Debug("Executing redo log waits query", zap.String("sql", queries.RedoLogWaitsSQL))

	rows, err := s.db.QueryContext(ctx, queries.RedoLogWaitsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing redo log waits query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var totalWaits int64
		var instID interface{}
		var event string

		err := rows.Scan(&totalWaits, &instID, &event)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning redo log waits row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record redo log waits metrics based on event type
		s.logger.Debug("Redo log waits metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("total_waits", totalWaits),
			zap.String("event", event),
			zap.String("instance", s.instanceName),
		)

		// Map events to metrics based on the original New Relic implementation
		matched := false
		switch {
		case strings.Contains(event, "log file parallel write"):
			s.mb.RecordNewrelicoracledbRedoLogWaitsDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		case strings.Contains(event, "log file switch completion"):
			s.mb.RecordNewrelicoracledbRedoLogLogFileSwitchDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		case strings.Contains(event, "log file switch (check"):
			s.mb.RecordNewrelicoracledbRedoLogLogFileSwitchCheckpointIncompleteDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		case strings.Contains(event, "log file switch (arch"):
			s.mb.RecordNewrelicoracledbRedoLogLogFileSwitchArchivingNeededDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		case strings.Contains(event, "buffer busy waits"):
			s.mb.RecordNewrelicoracledbSgaBufferBusyWaitsDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		case strings.Contains(event, "free buffer waits"):
			s.mb.RecordNewrelicoracledbSgaFreeBufferWaitsDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		case strings.Contains(event, "free buffer inspected"):
			s.mb.RecordNewrelicoracledbSgaFreeBufferInspectedDataPoint(now, totalWaits, s.instanceName, instanceID)
			matched = true
		}

		if !matched {
			// Log unmatched events for debugging
			s.logger.Info("Unmatched system event found", 
				zap.String("event", event), 
				zap.Int64("total_waits", totalWaits),
				zap.String("instance_id", instanceID))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating redo log waits rows: %w", err))
	}

	s.logger.Debug("Collected Oracle redo log waits metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeLockedAccountsMetrics handles the locked accounts metrics
func (s *CoreScraper) scrapeLockedAccountsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	// Execute locked accounts query
	s.logger.Debug("Executing locked accounts query", zap.String("sql", queries.LockedAccountsSQL))

	rows, err := s.db.QueryContext(ctx, queries.LockedAccountsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing locked accounts query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process each row and record metrics
	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var lockedAccounts int64

		err := rows.Scan(&instID, &lockedAccounts)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning locked accounts row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Record locked accounts metrics
		s.logger.Info("Locked accounts metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("locked_accounts", lockedAccounts),
			zap.String("instance", s.instanceName),
		)

		// Record the locked accounts metric
		s.mb.RecordNewrelicoracledbLockedAccountsDataPoint(now, lockedAccounts, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating locked accounts rows: %w", err))
	}

	s.logger.Debug("Collected Oracle locked accounts metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// getInstanceIDString converts instance ID interface to string
func getInstanceIDString(instID interface{}) string {
	if instID == nil {
		return "unknown"
	}

	switch v := instID.(type) {
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
