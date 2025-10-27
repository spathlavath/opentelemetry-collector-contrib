// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// scrapeSysstatMetrics handles the sysstat metrics
func (s *CoreScraper) scrapeSysstatMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error
	metricCount := 0

	// Execute sysstat query
	s.logger.Debug("Executing sysstat query", zap.String("sql", queries.SysstatSQL))
	rows, err := s.db.QueryContext(ctx, queries.SysstatSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing sysstat query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process query results
	for rows.Next() {
		var instID interface{}
		var name string
		var value sql.NullInt64

		err := rows.Scan(&instID, &name, &value)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning sysstat metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Handle NULL values
		valueInt := int64(0)
		if value.Valid {
			valueInt = value.Int64
		}

		// Record appropriate metric based on the name
		switch name {
		case "redo buffer allocation retries":
			s.logger.Info("SGA log buffer redo allocation retries metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_log_buffer_redo_allocation_retries", valueInt),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaLogBufferRedoAllocationRetriesDataPoint(now, valueInt, s.instanceName, instanceID)

		case "redo entries":
			s.logger.Info("SGA log buffer redo entries metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_log_buffer_redo_entries", valueInt),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaLogBufferRedoEntriesDataPoint(now, valueInt, s.instanceName, instanceID)

		case "sorts (memory)":
			s.logger.Info("Sorts memory metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sorts_memory", valueInt),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSortsMemoryDataPoint(now, valueInt, s.instanceName, instanceID)

		case "sorts (disk)":
			s.logger.Info("Sorts disk metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sorts_disk", valueInt),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSortsDiskDataPoint(now, valueInt, s.instanceName, instanceID)

		default:
			s.logger.Warn("Unknown sysstat metric name", zap.String("name", name), zap.String("instance_id", instanceID))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating sysstat metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle sysstat metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeRollbackSegmentsMetrics handles the rollback segments metrics
func (s *CoreScraper) scrapeRollbackSegmentsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error
	metricCount := 0

	// Execute rollback segments query
	s.logger.Debug("Executing rollback segments query", zap.String("sql", queries.RollbackSegmentsSQL))
	rows, err := s.db.QueryContext(ctx, queries.RollbackSegmentsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing rollback segments query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process query results
	for rows.Next() {
		var gets sql.NullInt64
		var waits sql.NullInt64
		var ratio sql.NullFloat64
		var instID interface{}

		err := rows.Scan(&gets, &waits, &ratio, &instID)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning rollback segments metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Handle NULL values
		getsValue := int64(0)
		if gets.Valid {
			getsValue = gets.Int64
		}

		waitsValue := int64(0)
		if waits.Valid {
			waitsValue = waits.Int64
		}

		ratioValue := 0.0
		if ratio.Valid {
			ratioValue = ratio.Float64
		}

		// Record rollback segments metrics
		s.logger.Info("Rollback segments metrics collected",
			zap.String("instance_id", instanceID),
			zap.Int64("rollback_segments_gets", getsValue),
			zap.Int64("rollback_segments_waits", waitsValue),
			zap.Float64("rollback_segments_wait_ratio", ratioValue),
			zap.String("instance", s.instanceName),
		)

		// Record all rollback segments metrics
		s.mb.RecordNewrelicoracledbRollbackSegmentsGetsDataPoint(now, getsValue, s.instanceName, instanceID)
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitRatioDataPoint(now, ratioValue, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating rollback segments metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle rollback segments metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}

// scrapeRedoLogWaitsMetrics handles the redo log waits metrics
func (s *CoreScraper) scrapeRedoLogWaitsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error
	metricCount := 0

	// Execute redo log waits query
	s.logger.Debug("Executing redo log waits query", zap.String("sql", queries.RedoLogWaitsSQL))
	rows, err := s.db.QueryContext(ctx, queries.RedoLogWaitsSQL)
	if err != nil {
		errors = append(errors, fmt.Errorf("error executing redo log waits query: %w", err))
		return errors
	}
	defer rows.Close()

	// Process query results
	for rows.Next() {
		var totalWaits sql.NullInt64
		var instID interface{}
		var event string

		err := rows.Scan(&totalWaits, &instID, &event)
		if err != nil {
			errors = append(errors, fmt.Errorf("error scanning redo log waits metrics row: %w", err))
			continue
		}

		// Convert instance ID to string
		instanceID := getInstanceIDString(instID)

		// Handle NULL values
		waitsValue := int64(0)
		if totalWaits.Valid {
			waitsValue = totalWaits.Int64
		}

		// Record appropriate metric based on the event name
		switch {
		case strings.Contains(event, "log file parallel write"):
			s.logger.Info("Redo log parallel write waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("redo_log_parallel_write_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbRedoLogParallelWriteWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		case strings.Contains(event, "log file switch completion"):
			s.logger.Info("Redo log switch completion waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("redo_log_switch_completion_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbRedoLogSwitchCompletionWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		case strings.Contains(event, "log file switch (check"):
			s.logger.Info("Redo log switch checkpoint incomplete waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("redo_log_switch_checkpoint_incomplete_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbRedoLogSwitchCheckpointIncompleteWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		case strings.Contains(event, "log file switch (arch"):
			s.logger.Info("Redo log switch archiving needed waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("redo_log_switch_archiving_needed_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbRedoLogSwitchArchivingNeededWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		case strings.Contains(event, "buffer busy waits"):
			s.logger.Info("SGA buffer busy waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_buffer_busy_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaBufferBusyWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		case strings.Contains(event, "freeBufferWaits"):
			s.logger.Info("SGA free buffer waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_free_buffer_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaFreeBufferWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		case strings.Contains(event, "free buffer inspected"):
			s.logger.Info("SGA free buffer inspected waits metrics collected",
				zap.String("instance_id", instanceID),
				zap.Int64("sga_free_buffer_inspected_waits", waitsValue),
				zap.String("event", event),
				zap.String("instance", s.instanceName),
			)
			s.mb.RecordNewrelicoracledbSgaFreeBufferInspectedWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)

		default:
			s.logger.Warn("Unknown redo log waits event", zap.String("event", event), zap.String("instance_id", instanceID))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating redo log waits metrics rows: %w", err))
	}

	s.logger.Debug("Collected Oracle redo log waits metrics", zap.Int("metric_count", metricCount), zap.String("instance", s.instanceName))

	return errors
}
