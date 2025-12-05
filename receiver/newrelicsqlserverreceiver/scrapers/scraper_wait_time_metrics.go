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
)

// WaitTimeScraper handles SQL Server wait time metrics collection
type WaitTimeScraper struct {
	client        client.SQLServerClient
	logger        *zap.Logger
	startTime     pcommon.Timestamp
	engineEdition int
	mb            *metadata.MetricsBuilder
}

// NewWaitTimeScraper creates a new wait time scraper instance
func NewWaitTimeScraper(sqlClient client.SQLServerClient, logger *zap.Logger, engineEdition int, mb *metadata.MetricsBuilder) *WaitTimeScraper {
	return &WaitTimeScraper{
		client:        sqlClient,
		logger:        logger,
		startTime:     pcommon.NewTimestampFromTime(time.Now()),
		engineEdition: engineEdition,
		mb:            mb,
	}
}

// ScrapeWaitTimeMetrics collects wait time statistics from SQL Server
func (s *WaitTimeScraper) ScrapeWaitTimeMetrics(ctx context.Context) error {
	results, err := s.client.QueryWaitTimeMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute wait time metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute wait time metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from wait time metrics query")
		return fmt.Errorf("no results returned from wait time metrics query")
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, result := range results {
		if result.WaitType == nil {
			continue
		}

		if result.WaitTimeMs != nil {
			s.mb.RecordSqlserverWaitStatsWaitTimeDataPoint(now, int64(*result.WaitTimeMs), *result.WaitType)
		}

		if result.WaitingTasksCount != nil {
			s.mb.RecordSqlserverWaitStatsWaitingTasksCountDataPoint(now, int64(*result.WaitingTasksCount), *result.WaitType)
		}
	}

	return nil
}

// ScrapeLatchWaitTimeMetrics collects latch-specific wait time statistics from SQL Server
func (s *WaitTimeScraper) ScrapeLatchWaitTimeMetrics(ctx context.Context) error {
	results, err := s.client.QueryLatchWaitTimeMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute latch wait time query", zap.Error(err))
		return fmt.Errorf("failed to execute latch wait time query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from latch wait time query")
		return fmt.Errorf("no results returned from latch wait time query")
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, result := range results {
		if result.WaitType == nil {
			continue
		}

		if result.WaitTimeMs != nil {
			s.mb.RecordSqlserverWaitStatsLatchWaitTimeDataPoint(now, int64(*result.WaitTimeMs), *result.WaitType)
		}

		if result.WaitingTasksCount != nil {
			s.mb.RecordSqlserverWaitStatsLatchWaitingTasksCountDataPoint(now, int64(*result.WaitingTasksCount), *result.WaitType)
		}
	}

	return nil
}
