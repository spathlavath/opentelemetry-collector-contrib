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

// RedoLogWaitsMapping represents the mapping between Oracle event names and OpenTelemetry metrics
// This follows the exact same pattern as nri-oracledb oracleRedoLogWaits metric group
type RedoLogWaitsMapping struct {
	OracleEventIdentifier string                                                                   // Event substring to match (like "log file parallel write")
	RecordFunc            func(*metadata.MetricsBuilder, pcommon.Timestamp, int64, string, string) // Function to record the metric
	Enabled               bool                                                                     // Whether this metric is enabled
}

// RedoLogWaitsScraper collects Oracle redo log and system event waits metrics
// Follows the exact same pattern as nri-oracledb oracleRedoLogWaits with strings.Contains matching
type RedoLogWaitsScraper struct {
	db             *sql.DB
	logger         *zap.Logger
	mb             *metadata.MetricsBuilder
	config         Config
	instanceName   string
	metricsMapping []RedoLogWaitsMapping
}

// Config interface to access metrics configuration
type Config interface {
	GetMetrics() metadata.MetricsConfig
}

// NewRedoLogWaitsScraper creates a new Oracle redo log waits scraper
// Follows the same initialization pattern as PDBSysMetricsScraper
func NewRedoLogWaitsScraper(
	db *sql.DB,
	logger *zap.Logger,
	mb *metadata.MetricsBuilder,
	config Config,
	instanceName string,
) *RedoLogWaitsScraper {
	scraper := &RedoLogWaitsScraper{
		db:           db,
		logger:       logger,
		mb:           mb,
		config:       config,
		instanceName: instanceName,
	}

	// Initialize metrics mapping based on nri-oracledb oracleRedoLogWaits
	scraper.initializeMetricsMapping()

	return scraper
}

// initializeMetricsMapping creates the exact mapping from nri-oracledb oracleRedoLogWaits
// Uses strings.Contains matching pattern for event identification
func (s *RedoLogWaitsScraper) initializeMetricsMapping() {
	s.metricsMapping = []RedoLogWaitsMapping{
		{
			// Map "log file sync" to log_file_sync metric - this is the primary redo log wait event
			OracleEventIdentifier: "log file sync",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSyncDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSync.Enabled,
		},
		{
			OracleEventIdentifier: "log file switch completion",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSwitchDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSwitch.Enabled,
		},
		{
			// Fixed: use correct event name with full text
			OracleEventIdentifier: "log file switch (checkpoint incomplete)",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSwitchCheckpointIncompleteDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSwitchCheckpointIncomplete.Enabled,
		},
		{
			// Note: "log file switch (archiving needed)" event doesn't exist in this Oracle instance
			// Keep the mapping but it won't match anything, which is fine
			OracleEventIdentifier: "log file switch (archiving needed)",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSwitchArchivingNeededDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSwitchArchivingNeeded.Enabled,
		},
		{
			OracleEventIdentifier: "buffer busy waits",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbSgaBufferBusyWaitsDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbSgaBufferBusyWaits.Enabled,
		},
		{
			// Fixed: Oracle uses spaces, not camelCase
			OracleEventIdentifier: "free buffer waits",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbSgaFreeBufferWaitsDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbSgaFreeBufferWaits.Enabled,
		},
		{
			OracleEventIdentifier: "free buffer inspected",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbSgaFreeBufferInspectedDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbSgaFreeBufferInspected.Enabled,
		},
	}
}

// ScrapeRedoLogWaits collects Oracle redo log and system event waits metrics
// Follows the exact same pattern as nri-oracledb oracleRedoLogWaits metricsGenerator
func (s *RedoLogWaitsScraper) ScrapeRedoLogWaits(ctx context.Context) []error {
	var errors []error

	s.logger.Debug("Scraping Oracle redo log waits metrics using gv$system_event")

	// Execute redo log waits query using the exact same query as nri-oracledb
	rows, err := s.db.QueryContext(ctx, queries.RedoLogWaitsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing redo log waits query: %w", err))
		return errors
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())
	processedCount := 0
	matchedCount := 0

	// Process rows using the same pattern as nri-oracledb oracleRedoLogWaits metricsGenerator
	for rows.Next() {
		var totalWaits int64
		var instID int64
		var event string

		err := rows.Scan(&totalWaits, &instID, &event)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning redo log waits row: %w", err))
			continue
		}

		processedCount++

		// Match the metric using strings.Contains as in nri-oracledb
		for _, mapping := range s.metricsMapping {
			if mapping.Enabled && strings.Contains(event, mapping.OracleEventIdentifier) {
				// Record the metric using the appropriate function
				mapping.RecordFunc(s.mb, now, totalWaits, s.instanceName, strconv.FormatInt(instID, 10))

				s.logger.Debug("Recorded redo log waits metric",
					zap.String("oracle_event", event),
					zap.String("event_identifier", mapping.OracleEventIdentifier),
					zap.Int64("total_waits", totalWaits),
					zap.Int64("instance_id", instID),
					zap.String("instance", s.instanceName))

				matchedCount++
				break // Stop after first match, as in nri-oracledb
			}
		}
	}

	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating redo log waits rows: %w", err))
		return errors
	}

	s.logger.Debug("Completed redo log waits collection",
		zap.Int("processed_rows", processedCount),
		zap.Int("matched_metrics", matchedCount),
		zap.String("instance", s.instanceName))

	return errors
}
