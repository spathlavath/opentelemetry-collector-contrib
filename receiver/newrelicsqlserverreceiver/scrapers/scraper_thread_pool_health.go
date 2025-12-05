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

// ThreadPoolHealthScraper handles thread pool health metrics collection
type ThreadPoolHealthScraper struct {
	client        client.SQLServerClient
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
}

// NewThreadPoolHealthScraper creates a new thread pool health scraper
func NewThreadPoolHealthScraper(sqlClient client.SQLServerClient, logger *zap.Logger, mb *metadata.MetricsBuilder) *ThreadPoolHealthScraper {
	return &ThreadPoolHealthScraper{
		client: sqlClient,
		logger: logger,
		mb:     mb,
	}
}

// ScrapeThreadPoolHealthMetrics collects thread pool health metrics
func (s *ThreadPoolHealthScraper) ScrapeThreadPoolHealthMetrics(ctx context.Context) error {
	s.logger.Debug("Executing thread pool health metrics collection")

	results, err := s.client.QueryThreadPoolHealth(ctx, s.engineEdition)
	if err != nil {
		return fmt.Errorf("failed to execute thread pool health query: %w", err)
	}

	s.logger.Debug("Thread pool health metrics fetched", zap.Int("result_count", len(results)))

	for i, result := range results {
		if err := s.processThreadPoolHealthMetrics(result, i); err != nil {
			s.logger.Error("Failed to process thread pool health metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processThreadPoolHealthMetrics processes and emits metrics for thread pool health
func (s *ThreadPoolHealthScraper) processThreadPoolHealthMetrics(result models.ThreadPoolHealth, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.CurrentlyWaitingForThreadpool != nil {
		s.mb.RecordSqlserverThreadpoolWaitingTasksDataPoint(timestamp, *result.CurrentlyWaitingForThreadpool)
	}

	if result.TotalWorkQueueCount != nil {
		s.mb.RecordSqlserverThreadpoolWorkQueueCountDataPoint(timestamp, *result.TotalWorkQueueCount)
	}

	if result.RunningWorkers != nil {
		s.mb.RecordSqlserverThreadpoolRunningWorkersDataPoint(timestamp, *result.RunningWorkers)
	}

	if result.MaxConfiguredWorkers != nil {
		s.mb.RecordSqlserverThreadpoolMaxWorkersDataPoint(timestamp, *result.MaxConfiguredWorkers)
	}

	if result.WorkerUtilizationPercent != nil {
		s.mb.RecordSqlserverThreadpoolUtilizationDataPoint(timestamp, *result.WorkerUtilizationPercent)
	}

	if result.TotalCurrentTasks != nil {
		s.mb.RecordSqlserverThreadpoolCurrentTasksDataPoint(timestamp, *result.TotalCurrentTasks)
	}

	if result.TotalRunnableTasks != nil {
		s.mb.RecordSqlserverThreadpoolRunnableTasksDataPoint(timestamp, *result.TotalRunnableTasks)
	}

	return nil
}

// addThreadPoolAttributes adds common attributes to thread pool metrics
func (s *ThreadPoolHealthScraper) addThreadPoolAttributes(attrs pcommon.Map, result models.ThreadPoolHealth) {
	if result.SQLHostname != nil {
		attrs.PutStr("sql_hostname", *result.SQLHostname)
	}
	if result.HealthStatus != nil {
		attrs.PutStr("health_status", *result.HealthStatus)
	}
	if result.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
	}
}
