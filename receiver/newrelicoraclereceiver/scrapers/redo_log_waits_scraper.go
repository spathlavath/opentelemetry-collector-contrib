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
// Uses strings.Contains matching pattern for event identification - matches nri-oracledb exactly
func (s *RedoLogWaitsScraper) initializeMetricsMapping() {
	s.metricsMapping = []RedoLogWaitsMapping{
		{
			// nri-oracledb: "log file parallel write" -> "redoLog.waits"
			OracleEventIdentifier: "log file parallel write",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogWaitsDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogWaits.Enabled,
		},
		{
			// nri-oracledb: "log file switch completion" -> "redoLog.logFileSwitch"
			OracleEventIdentifier: "log file switch completion",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSwitchDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSwitch.Enabled,
		},
		{
			// nri-oracledb: "log file switch (check" -> "redoLog.logFileSwitchCheckpointIncomplete"
			OracleEventIdentifier: "log file switch (check",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSwitchCheckpointIncompleteDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSwitchCheckpointIncomplete.Enabled,
		},
		{
			// nri-oracledb: "log file switch (arch" -> "redoLog.logFileSwitchArchivingNeeded"
			OracleEventIdentifier: "log file switch (arch",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbRedoLogLogFileSwitchArchivingNeededDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbRedoLogLogFileSwitchArchivingNeeded.Enabled,
		},
		{
			// nri-oracledb: "buffer busy waits" -> "sga.bufferBusyWaits"
			OracleEventIdentifier: "buffer busy waits",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbSgaBufferBusyWaitsDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbSgaBufferBusyWaits.Enabled,
		},
		{
			// nri-oracledb: "freeBufferWaits" -> "sga.freeBufferWaits"
			OracleEventIdentifier: "freeBufferWaits",
			RecordFunc:            (*metadata.MetricsBuilder).RecordNewrelicoracledbSgaFreeBufferWaitsDataPoint,
			Enabled:               s.config.GetMetrics().NewrelicoracledbSgaFreeBufferWaits.Enabled,
		},
		{
			// nri-oracledb: "free buffer inspected" -> "sga.freeBufferInspected"
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

	// Execute the RedoLogWaitsSQL query
	rows, err := s.db.QueryContext(ctx, queries.RedoLogWaitsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("failed to execute RedoLogWaitsSQL query: %w", err))
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

// processRow processes a single row from the RedoLogWaitsSQL query
// Matches the exact pattern used in nri-oracledb oracleRedoLogWaits metricsGenerator
func (s *RedoLogWaitsScraper) processRow(rows *sql.Rows) error {
	var totalWaits int64
	var instID int64
	var event string

	err := rows.Scan(&totalWaits, &instID, &event)
	if err != nil {
		return fmt.Errorf("error scanning row: %w", err)
	}

	// Use timestamp at scan time like nri-oracledb
	now := pcommon.NewTimestampFromTime(time.Now())

	// Match the metric using strings.Contains as in nri-oracledb
	for _, mapping := range s.metricsMapping {
		if mapping.Enabled && strings.Contains(event, mapping.OracleEventIdentifier) {
			// Record the metric using the appropriate function
			mapping.RecordFunc(s.mb, now, totalWaits, s.instanceName, strconv.FormatInt(instID, 10))
			break // Stop after first match, as in nri-oracledb
		}
	}

	return nil
}
