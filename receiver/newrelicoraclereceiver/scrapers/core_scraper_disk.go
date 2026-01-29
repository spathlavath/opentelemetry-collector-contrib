// Copyright 2025 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// diskIOMetricRecorder defines a metric recorder for disk I/O metrics
type diskIOMetricRecorder struct {
	isEnabled func(metadata.MetricsBuilderConfig) bool
	getValue  func(*models.DiskIOMetrics) int64
	record    func(*CoreScraper, pcommon.Timestamp, int64, string)
}

// diskIOMetricRegistry contains all disk I/O metric recorders
var diskIOMetricRegistry = []diskIOMetricRecorder{
	{
		isEnabled: func(cfg metadata.MetricsBuilderConfig) bool {
			return cfg.Metrics.NewrelicoracledbDiskReads.Enabled
		},
		getValue: func(m *models.DiskIOMetrics) int64 {
			return m.PhysicalReads
		},
		record: func(s *CoreScraper, now pcommon.Timestamp, value int64, instanceID string) {
			s.mb.RecordNewrelicoracledbDiskReadsDataPoint(now, value, instanceID)
		},
	},
	{
		isEnabled: func(cfg metadata.MetricsBuilderConfig) bool {
			return cfg.Metrics.NewrelicoracledbDiskWrites.Enabled
		},
		getValue: func(m *models.DiskIOMetrics) int64 {
			return m.PhysicalWrites
		},
		record: func(s *CoreScraper, now pcommon.Timestamp, value int64, instanceID string) {
			s.mb.RecordNewrelicoracledbDiskWritesDataPoint(now, value, instanceID)
		},
	},
	{
		isEnabled: func(cfg metadata.MetricsBuilderConfig) bool {
			return cfg.Metrics.NewrelicoracledbDiskBlocksRead.Enabled
		},
		getValue: func(m *models.DiskIOMetrics) int64 {
			return m.PhysicalBlockReads
		},
		record: func(s *CoreScraper, now pcommon.Timestamp, value int64, instanceID string) {
			s.mb.RecordNewrelicoracledbDiskBlocksReadDataPoint(now, value, instanceID)
		},
	},
	{
		isEnabled: func(cfg metadata.MetricsBuilderConfig) bool {
			return cfg.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled
		},
		getValue: func(m *models.DiskIOMetrics) int64 {
			return m.PhysicalBlockWrites
		},
		record: func(s *CoreScraper, now pcommon.Timestamp, value int64, instanceID string) {
			s.mb.RecordNewrelicoracledbDiskBlocksWrittenDataPoint(now, value, instanceID)
		},
	},
	{
		isEnabled: func(cfg metadata.MetricsBuilderConfig) bool {
			return cfg.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled
		},
		getValue: func(m *models.DiskIOMetrics) int64 {
			return m.ReadTime
		},
		record: func(s *CoreScraper, now pcommon.Timestamp, value int64, instanceID string) {
			s.mb.RecordNewrelicoracledbDiskReadTimeMillisecondsDataPoint(now, value, instanceID)
		},
	},
	{
		isEnabled: func(cfg metadata.MetricsBuilderConfig) bool {
			return cfg.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled
		},
		getValue: func(m *models.DiskIOMetrics) int64 {
			return m.WriteTime
		},
		record: func(s *CoreScraper, now pcommon.Timestamp, value int64, instanceID string) {
			s.mb.RecordNewrelicoracledbDiskWriteTimeMillisecondsDataPoint(now, value, instanceID)
		},
	},
}

// hasAnyDiskIOMetricEnabled checks if any disk I/O metric is enabled
func (s *CoreScraper) hasAnyDiskIOMetricEnabled() bool {
	for _, recorder := range diskIOMetricRegistry {
		if recorder.isEnabled(s.config) {
			return true
		}
	}
	return false
}

// recordDiskIOMetrics records all enabled disk I/O metrics for a single metric
func (s *CoreScraper) recordDiskIOMetrics(now pcommon.Timestamp, metric *models.DiskIOMetrics, instanceID string) {
	for _, recorder := range diskIOMetricRegistry {
		if recorder.isEnabled(s.config) {
			value := recorder.getValue(metric)
			recorder.record(s, now, value, instanceID)
		}
	}
}

func (s *CoreScraper) scrapeReadWriteMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	if !s.hasAnyDiskIOMetricEnabled() {
		return nil
	}

	metrics, err := s.client.QueryDiskIOMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query disk I/O metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		instanceID := getInstanceIDString(metric.InstID)
		s.recordDiskIOMetrics(now, &metric, instanceID)
	}

	s.logger.Debug("Disk I/O metrics scrape completed")

	return nil
}
