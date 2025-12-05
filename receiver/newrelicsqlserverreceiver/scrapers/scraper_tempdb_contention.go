// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
)

// TempDBContentionScraper handles TempDB contention metrics collection
type TempDBContentionScraper struct {
	client        client.SQLServerClient
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
}

// NewTempDBContentionScraper creates a new TempDB contention scraper
func NewTempDBContentionScraper(sqlClient client.SQLServerClient, logger *zap.Logger, mb *metadata.MetricsBuilder) *TempDBContentionScraper {
	return &TempDBContentionScraper{
		client: sqlClient,
		logger: logger,
		mb:     mb,
	}
}

// ScrapeTempDBContentionMetrics collects TempDB contention metrics
func (s *TempDBContentionScraper) ScrapeTempDBContentionMetrics(ctx context.Context) error {
	s.logger.Debug("Executing TempDB contention metrics collection")

	results, err := s.client.QueryTempDBContention(ctx, s.engineEdition)
	if err != nil {
		return fmt.Errorf("failed to execute TempDB contention query: %w", err)
	}

	s.logger.Debug("TempDB contention metrics fetched", zap.Int("result_count", len(results)))

	for i, result := range results {
		if err := s.processTempDBContentionMetrics(result, i); err != nil {
			s.logger.Error("Failed to process TempDB contention metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processTempDBContentionMetrics processes and records metrics for TempDB contention using MetricsBuilder
func (s *TempDBContentionScraper) processTempDBContentionMetrics(result models.TempDBContention, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Record Metric 1: Current waiters for TempDB
	if result.CurrentWaiters != nil {
		s.mb.RecordSqlserverTempdbCurrentWaitersDataPoint(timestamp, *result.CurrentWaiters)
	}

	// Record Metric 2: Page latch waits
	if result.PagelatchWaits != nil {
		s.mb.RecordSqlserverTempdbPagelatchWaitsDataPoint(timestamp, *result.PagelatchWaits)
	}

	// Record Metric 3: Allocation waits
	if result.AllocationWaits != nil {
		s.mb.RecordSqlserverTempdbAllocationWaitsDataPoint(timestamp, *result.AllocationWaits)
	}

	// Record Metric 4: TempDB data file count
	if result.TempDBDataFileCount != nil {
		s.mb.RecordSqlserverTempdbDataFileCountDataPoint(timestamp, *result.TempDBDataFileCount)
	}

	// Record Metric 5: TempDB total size (convert MB to bytes for proper unit)
	if result.TempDBTotalSizeMB != nil {
		// Convert MB to bytes (metadata.yaml unit is MBy which expects bytes)
		sizeBytes := int64(*result.TempDBTotalSizeMB * 1024 * 1024)
		s.mb.RecordSqlserverTempdbTotalSizeDataPoint(timestamp, sizeBytes)
	}

	return nil
}
