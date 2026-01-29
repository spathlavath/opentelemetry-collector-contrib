// Copyright 2025 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func (s *CoreScraper) scrapeSysstatMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	metrics, err := s.client.QuerySysstatMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query sysstat metrics", zap.Error(err))
		return []error{err}
	}

	metricCount := 0
	for _, metric := range metrics {
		instanceID := getInstanceIDString(metric.InstID)

		valueInt := int64(0)
		if metric.Value.Valid {
			valueInt = metric.Value.Int64
		}

		switch metric.Name {
		case "redo buffer allocation retries":
			s.mb.RecordNewrelicoracledbSgaLogBufferRedoAllocationRetriesDataPoint(now, valueInt, instanceID)
		case "redo entries":
			s.mb.RecordNewrelicoracledbSgaLogBufferRedoEntriesDataPoint(now, valueInt, instanceID)
		case "sorts (memory)":
			s.mb.RecordNewrelicoracledbSortsMemoryDataPoint(now, valueInt, instanceID)
		case "sorts (disk)":
			s.mb.RecordNewrelicoracledbSortsDiskDataPoint(now, valueInt, instanceID)
		default:
			s.logger.Debug("Unknown sysstat metric", zap.String("name", metric.Name))
		}

		metricCount++
	}

	s.logger.Debug("Sysstat metrics scrape completed")

	return nil
}

func (s *CoreScraper) scrapeRollbackSegmentsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	metrics, err := s.client.QueryRollbackSegmentsMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query rollback segments metrics", zap.Error(err))
		return []error{err}
	}

	metricCount := 0
	for _, metric := range metrics {
		instanceID := getInstanceIDString(metric.InstID)

		getsValue := int64(0)
		if metric.Gets.Valid {
			getsValue = metric.Gets.Int64
		}

		waitsValue := int64(0)
		if metric.Waits.Valid {
			waitsValue = metric.Waits.Int64
		}

		ratioValue := 0.0
		if metric.Ratio.Valid {
			ratioValue = metric.Ratio.Float64
		}

		s.mb.RecordNewrelicoracledbRollbackSegmentsGetsDataPoint(now, getsValue, instanceID)
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitsDataPoint(now, waitsValue, instanceID)
		s.mb.RecordNewrelicoracledbRollbackSegmentsWaitRatioDataPoint(now, ratioValue, instanceID)

		metricCount++
	}

	s.logger.Debug("Rollback segments metrics scrape completed")

	return nil
}

// redoLogWaitEventRecorder defines a metric recorder for redo log wait events
type redoLogWaitEventRecorder struct {
	eventPattern string
	record       func(*CoreScraper, pcommon.Timestamp, int64, string)
}

// redoLogWaitEventRegistry contains all redo log wait event metric recorders
var redoLogWaitEventRegistry = []redoLogWaitEventRecorder{
	{
		eventPattern: "log file parallel write",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbRedoLogParallelWriteWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
	{
		eventPattern: "log file switch completion",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbRedoLogSwitchCompletionWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
	{
		eventPattern: "log file switch (check",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbRedoLogSwitchCheckpointIncompleteWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
	{
		eventPattern: "log file switch (arch",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbRedoLogSwitchArchivingNeededWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
	{
		eventPattern: "buffer busy waits",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbSgaBufferBusyWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
	{
		eventPattern: "freeBufferWaits",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbSgaFreeBufferWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
	{
		eventPattern: "free buffer inspected",
		record: func(s *CoreScraper, now pcommon.Timestamp, waitsValue int64, instanceID string) {
			s.mb.RecordNewrelicoracledbSgaFreeBufferInspectedWaitsDataPoint(now, waitsValue, instanceID)
		},
	},
}

// recordRedoLogWaitMetric records a redo log wait metric using the registry
func (s *CoreScraper) recordRedoLogWaitMetric(now pcommon.Timestamp, metric models.RedoLogWaitsMetric, instanceID string) {
	waitsValue := int64(0)
	if metric.TotalWaits.Valid {
		waitsValue = metric.TotalWaits.Int64
	}

	// Find and record matching metric
	for _, recorder := range redoLogWaitEventRegistry {
		if strings.Contains(metric.Event, recorder.eventPattern) {
			recorder.record(s, now, waitsValue, instanceID)
			return
		}
	}

	// Log unknown event
	s.logger.Debug("Unknown redo log waits event", zap.String("event", metric.Event))
}

func (s *CoreScraper) scrapeRedoLogWaitsMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	metrics, err := s.client.QueryRedoLogWaitsMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query redo log waits metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		instanceID := getInstanceIDString(metric.InstID)
		s.recordRedoLogWaitMetric(now, metric, instanceID)
	}

	s.logger.Debug("Redo log waits metrics scrape completed")

	return nil
}
