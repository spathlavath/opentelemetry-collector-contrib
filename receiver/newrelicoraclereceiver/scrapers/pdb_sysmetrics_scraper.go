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

// PDBSysMetricsMapping represents the mapping between Oracle metric names and OpenTelemetry metric recording functions
type PDBSysMetricsMapping struct {
	OracleMetricName string
	RecordFunc       func(*metadata.MetricsBuilder, pcommon.Timestamp, float64, string, string)
	Enabled          bool
}

// PDBSysMetricsScraper handles Oracle PDB system metrics collection
// Following the exact same pattern as nri-oracledb oraclePDBSysMetrics
type PDBSysMetricsScraper struct {
	db           *sql.DB
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	config       metadata.MetricsBuilderConfig

	// Metrics mapping based on nri-oracledb oraclePDBSysMetrics
	metricsMapping []PDBSysMetricsMapping
	// metricMappingIndex provides O(1) lookup for metric mappings
	metricMappingIndex map[string]*PDBSysMetricsMapping
}

// NewPDBSysMetricsScraper creates a new PDB system metrics scraper
func NewPDBSysMetricsScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, config metadata.MetricsBuilderConfig) *PDBSysMetricsScraper {
	scraper := &PDBSysMetricsScraper{
		db:           db,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		config:       config,
	}

	// Initialize metrics mapping based on nri-oracledb oraclePDBSysMetrics
	scraper.initializeMetricsMapping()

	// Build index for O(1) metric lookup
	scraper.buildMetricIndex()

	return scraper
}

// initializeMetricsMapping creates the exact mapping from nri-oracledb oraclePDBSysMetrics
func (s *PDBSysMetricsScraper) initializeMetricsMapping() {
	s.metricsMapping = []PDBSysMetricsMapping{
		{
			OracleMetricName: "Active Parallel Sessions",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbSessionsActiveParallelDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbSessionsActiveParallel.Enabled,
		},
		{
			OracleMetricName: "Active Serial Sessions",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbSessionsActiveSerialDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbSessionsActiveSerial.Enabled,
		},
		{
			OracleMetricName: "Average Active Sessions",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbSessionsAverageActiveDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbSessionsAverageActive.Enabled,
		},
		{
			OracleMetricName: "CPU Usage Per Sec",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbCPUUsagePerSecondDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbCPUUsagePerSecond.Enabled,
		},
		{
			OracleMetricName: "CPU Usage Per Txn",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbCPUUsagePerTransactionDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbCPUUsagePerTransaction.Enabled,
		},
		{
			OracleMetricName: "Current Logons Count",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbSessionsCurrentLogonsDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbSessionsCurrentLogons.Enabled,
		},
		{
			OracleMetricName: "Current Open Cursors Count",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbSessionsCurrentOpenCursorsDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbSessionsCurrentOpenCursors.Enabled,
		},
		{
			OracleMetricName: "Database CPU Time Ratio",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbDatabaseCPUTimeRatioDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbDatabaseCPUTimeRatio.Enabled,
		},
		{
			OracleMetricName: "Database Wait Time Ratio",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbDatabaseWaitTimeRatioDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbDatabaseWaitTimeRatio.Enabled,
		},
		{
			OracleMetricName: "Executions Per Sec",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbExecutionsPerSecondDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbExecutionsPerSecond.Enabled,
		},
		{
			OracleMetricName: "Executions Per Txn",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbExecutionsPerTransactionDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbExecutionsPerTransaction.Enabled,
		},
		{
			OracleMetricName: "Network Traffic Volume Per Sec",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbNetworkTrafficVolumePerSecondDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbNetworkTrafficVolumePerSecond.Enabled,
		},
		{
			OracleMetricName: "Physical Read Total Bytes Per Sec",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbPhysicalReadsBytesPerSecondDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbPhysicalReadsBytesPerSecond.Enabled,
		},
		{
			OracleMetricName: "Session Count",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbSessionsCountDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbSessionsCount.Enabled,
		},
		{
			OracleMetricName: "SQL Service Response Time",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbResponseTimeSQLServiceDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbResponseTimeSQLService.Enabled,
		},
		{
			OracleMetricName: "User Transaction Per Sec",
			RecordFunc:       (*metadata.MetricsBuilder).RecordNewrelicoracledbPdbTransactionsPerSecondDataPoint,
			Enabled:          s.config.Metrics.NewrelicoracledbPdbTransactionsPerSecond.Enabled,
		},
	}
}

// buildMetricIndex creates a map for O(1) metric lookup optimization
func (s *PDBSysMetricsScraper) buildMetricIndex() {
	s.metricMappingIndex = make(map[string]*PDBSysMetricsMapping, len(s.metricsMapping))
	for i := range s.metricsMapping {
		mapping := &s.metricsMapping[i]
		if mapping.Enabled {
			s.metricMappingIndex[mapping.OracleMetricName] = mapping
		}
	}
}

// ScrapePDBSysMetrics collects Oracle PDB system metrics
// Follows the exact same pattern as nri-oracledb rowMetricsGenerator
func (s *PDBSysMetricsScraper) ScrapePDBSysMetrics(ctx context.Context) []error {
	var errors []error

	s.logger.Debug("Scraping Oracle PDB system metrics using gv$con_sysmetric")

	// Execute PDB system metrics query using the exact same query as nri-oracledb
	rows, err := s.db.QueryContext(ctx, queries.PDBSysMetricsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing PDB system metrics query: %w", err))
		return errors
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())
	processedCount := 0
	matchedCount := 0

	// Process rows using the same pattern as nri-oracledb rowMetricsGenerator
	for rows.Next() {
		var instID int64
		var metricName string
		var value float64

		err := rows.Scan(&instID, &metricName, &value)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning PDB metric row: %w", err))
			continue
		}

		processedCount++

		// Use O(1) lookup for metric mapping optimization
		if mapping, exists := s.metricMappingIndex[metricName]; exists {
			// Record the metric using the appropriate function
			mapping.RecordFunc(s.mb, now, value, s.instanceName, strconv.FormatInt(instID, 10))

			s.logger.Debug("Recorded PDB metric",
				zap.String("oracle_metric", metricName),
				zap.Float64("value", value),
				zap.Int64("instance_id", instID),
				zap.String("instance", s.instanceName))

			matchedCount++
		}
	}

	if err := rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating PDB metric rows: %w", err))
		return errors
	}

	s.logger.Debug("Completed PDB system metrics collection",
		zap.Int("processed_rows", processedCount),
		zap.Int("matched_metrics", matchedCount),
		zap.String("instance", s.instanceName))

	return errors
}
