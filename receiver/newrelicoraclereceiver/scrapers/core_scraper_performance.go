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

func (s *CoreScraper) scrapeSysstatMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SysstatSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing sysstat query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var instID interface{}
		var name string
		var value sql.NullInt64

		if err := rows.Scan(&instID, &name, &value); err != nil {
			errors = append(errors, fmt.Errorf("error scanning sysstat metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

		valueInt := int64(0)
		if value.Valid {
			valueInt = value.Int64
		}

		switch name {
		case "redo buffer allocation retries":
			s.mb.RecordNewrelicoracledbSgaLogBufferRedoAllocationRetriesDataPoint(now, valueInt, s.instanceName, instanceID)
		case "redo entries":
			s.mb.RecordNewrelicoracledbSgaLogBufferRedoEntriesDataPoint(now, valueInt, s.instanceName, instanceID)
		case "sorts (memory)":
			s.mb.RecordNewrelicoracledbSortsMemoryDataPoint(now, valueInt, s.instanceName, instanceID)
		case "sorts (disk)":
			s.mb.RecordNewrelicoracledbSortsDiskDataPoint(now, valueInt, s.instanceName, instanceID)
		default:
			s.logger.Debug("Unknown sysstat metric", zap.String("name", name))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating sysstat metrics rows: %w", err))
	}

	s.logger.Debug("Sysstat metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeRollbackSegmentsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.RollbackSegmentsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing rollback segments query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var gets sql.NullInt64
		var waits sql.NullInt64
		var ratio sql.NullFloat64
		var instID interface{}

		if err := rows.Scan(&gets, &waits, &ratio, &instID); err != nil {
			errors = append(errors, fmt.Errorf("error scanning rollback segments metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

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

		s.mb.RecordNewrelicoracledbRollbackSegmentsGetsDataPoint(now, getsValue, s.instanceName, instanceID)
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitRatioDataPoint(now, ratioValue, s.instanceName, instanceID)

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating rollback segments metrics rows: %w", err))
	}

	s.logger.Debug("Rollback segments metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}

func (s *CoreScraper) scrapeRedoLogWaitsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.RedoLogWaitsSQL)
	if err != nil {
		return []error{fmt.Errorf("error executing redo log waits query: %w", err)}
	}
	defer rows.Close()

	metricCount := 0
	for rows.Next() {
		var totalWaits sql.NullInt64
		var instID interface{}
		var event string

		if err := rows.Scan(&totalWaits, &instID, &event); err != nil {
			errors = append(errors, fmt.Errorf("error scanning redo log waits metrics row: %w", err))
			continue
		}

		instanceID := getInstanceIDString(instID)

		waitsValue := int64(0)
		if totalWaits.Valid {
			waitsValue = totalWaits.Int64
		}

		switch {
		case strings.Contains(event, "log file parallel write"):
			s.mb.RecordNewrelicoracledbRedoLogParallelWriteWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		case strings.Contains(event, "log file switch completion"):
			s.mb.RecordNewrelicoracledbRedoLogSwitchCompletionWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		case strings.Contains(event, "log file switch (check"):
			s.mb.RecordNewrelicoracledbRedoLogSwitchCheckpointIncompleteWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		case strings.Contains(event, "log file switch (arch"):
			s.mb.RecordNewrelicoracledbRedoLogSwitchArchivingNeededWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		case strings.Contains(event, "buffer busy waits"):
			s.mb.RecordNewrelicoracledbSgaBufferBusyWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		case strings.Contains(event, "freeBufferWaits"):
			s.mb.RecordNewrelicoracledbSgaFreeBufferWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		case strings.Contains(event, "free buffer inspected"):
			s.mb.RecordNewrelicoracledbSgaFreeBufferInspectedWaitsDataPoint(now, waitsValue, s.instanceName, instanceID)
		default:
			s.logger.Debug("Unknown redo log waits event", zap.String("event", event))
		}

		metricCount++
	}

	if err = rows.Err(); err != nil {
		errors = append(errors, fmt.Errorf("error iterating redo log waits metrics rows: %w", err))
	}

	s.logger.Debug("Redo log waits metrics scrape completed", zap.Int("metrics", metricCount))

	return errors
}
