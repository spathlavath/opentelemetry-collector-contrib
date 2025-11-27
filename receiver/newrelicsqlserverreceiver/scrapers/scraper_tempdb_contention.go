// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// TempDBContentionScraper handles TempDB contention metrics collection
type TempDBContentionScraper struct {
	connection SQLConnectionInterface
	logger     *zap.Logger
}

// NewTempDBContentionScraper creates a new TempDB contention scraper
func NewTempDBContentionScraper(connection SQLConnectionInterface, logger *zap.Logger) *TempDBContentionScraper {
	return &TempDBContentionScraper{
		connection: connection,
		logger:     logger,
	}
}

// ScrapeTempDBContentionMetrics collects TempDB contention metrics
func (s *TempDBContentionScraper) ScrapeTempDBContentionMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	s.logger.Debug("Executing TempDB contention metrics collection")

	var results []models.TempDBContention
	if err := s.connection.Query(ctx, &results, queries.TempDBContentionQuery); err != nil {
		return fmt.Errorf("failed to execute TempDB contention query: %w", err)
	}

	s.logger.Debug("TempDB contention metrics fetched", zap.Int("result_count", len(results)))

	for i, result := range results {
		if err := s.processTempDBContentionMetrics(result, scopeMetrics, i); err != nil {
			s.logger.Error("Failed to process TempDB contention metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processTempDBContentionMetrics processes and emits metrics for TempDB contention
func (s *TempDBContentionScraper) processTempDBContentionMetrics(result models.TempDBContention, scopeMetrics pmetric.ScopeMetrics, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Metric 1: Current waiters for TempDB
	if result.CurrentWaiters != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.tempdb.current_waiters")
		metric.SetDescription("Number of tasks currently waiting on TempDB page latches")
		metric.SetUnit("tasks")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.CurrentWaiters)
		dp.SetTimestamp(timestamp)
		s.addTempDBAttributes(dp.Attributes(), result)
	}

	// Metric 2: Page latch waits
	if result.PagelatchWaits != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.tempdb.pagelatch_waits_ms")
		metric.SetDescription("Total page latch wait time since server start")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.PagelatchWaits)
		dp.SetTimestamp(timestamp)
		s.addTempDBAttributes(dp.Attributes(), result)
	}

	// Metric 3: Allocation waits
	if result.AllocationWaits != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.tempdb.allocation_waits_ms")
		metric.SetDescription("Total allocation-related wait time (GAM, SGAM, PFS)")
		metric.SetUnit("ms")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.AllocationWaits)
		dp.SetTimestamp(timestamp)
		s.addTempDBAttributes(dp.Attributes(), result)
	}

	// Metric 4: TempDB data file count
	if result.TempDBDataFileCount != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.tempdb.data_file_count")
		metric.SetDescription("Number of TempDB data files")
		metric.SetUnit("files")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.TempDBDataFileCount)
		dp.SetTimestamp(timestamp)
		s.addTempDBAttributes(dp.Attributes(), result)
	}

	// Metric 5: TempDB total size
	if result.TempDBTotalSizeMB != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.tempdb.total_size_mb")
		metric.SetDescription("Total size of TempDB data files")
		metric.SetUnit("MB")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(*result.TempDBTotalSizeMB)
		dp.SetTimestamp(timestamp)
		s.addTempDBAttributes(dp.Attributes(), result)
	}

	return nil
}

// addTempDBAttributes adds common attributes to TempDB metrics
func (s *TempDBContentionScraper) addTempDBAttributes(attrs pcommon.Map, result models.TempDBContention) {
	if result.SQLHostname != nil {
		attrs.PutStr("sql_hostname", *result.SQLHostname)
	}
	if result.TempDBHealthStatus != nil {
		attrs.PutStr("tempdb_health_status", *result.TempDBHealthStatus)
	}
	if result.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
	}
}
