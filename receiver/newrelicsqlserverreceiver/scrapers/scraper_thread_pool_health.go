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

// ThreadPoolHealthScraper handles thread pool health metrics collection
type ThreadPoolHealthScraper struct {
	connection SQLConnectionInterface
	logger     *zap.Logger
}

// NewThreadPoolHealthScraper creates a new thread pool health scraper
func NewThreadPoolHealthScraper(connection SQLConnectionInterface, logger *zap.Logger) *ThreadPoolHealthScraper {
	return &ThreadPoolHealthScraper{
		connection: connection,
		logger:     logger,
	}
}

// ScrapeThreadPoolHealthMetrics collects thread pool health metrics
func (s *ThreadPoolHealthScraper) ScrapeThreadPoolHealthMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	s.logger.Debug("Executing thread pool health metrics collection")

	var results []models.ThreadPoolHealth
	if err := s.connection.Query(ctx, &results, queries.ThreadPoolHealthQuery); err != nil {
		return fmt.Errorf("failed to execute thread pool health query: %w", err)
	}

	s.logger.Debug("Thread pool health metrics fetched", zap.Int("result_count", len(results)))

	for i, result := range results {
		if err := s.processThreadPoolHealthMetrics(result, scopeMetrics, i); err != nil {
			s.logger.Error("Failed to process thread pool health metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processThreadPoolHealthMetrics processes and emits metrics for thread pool health
func (s *ThreadPoolHealthScraper) processThreadPoolHealthMetrics(result models.ThreadPoolHealth, scopeMetrics pmetric.ScopeMetrics, index int) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Metric 1: Waiting for threadpool tasks
	if result.CurrentlyWaitingForThreadpool != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.waiting_tasks")
		metric.SetDescription("Number of tasks currently waiting for threadpool worker threads")
		metric.SetUnit("tasks")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.CurrentlyWaitingForThreadpool)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
	}

	// Metric 2: Work queue count
	if result.TotalWorkQueueCount != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.work_queue_count")
		metric.SetDescription("Total number of tasks in scheduler work queues")
		metric.SetUnit("tasks")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.TotalWorkQueueCount)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
	}

	// Metric 3: Running workers
	if result.RunningWorkers != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.running_workers")
		metric.SetDescription("Number of worker threads currently running")
		metric.SetUnit("threads")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.RunningWorkers)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
	}

	// Metric 4: Max configured workers
	if result.MaxConfiguredWorkers != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.max_workers")
		metric.SetDescription("Maximum configured worker threads")
		metric.SetUnit("threads")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.MaxConfiguredWorkers)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
	}

	// Metric 5: Worker utilization percentage
	if result.WorkerUtilizationPercent != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.utilization_percent")
		metric.SetDescription("Percentage of worker threads currently in use")
		metric.SetUnit("%")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(*result.WorkerUtilizationPercent)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
	}

	// Metric 6: Total current tasks
	if result.TotalCurrentTasks != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.current_tasks")
		metric.SetDescription("Total tasks currently assigned to schedulers")
		metric.SetUnit("tasks")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.TotalCurrentTasks)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
	}

	// Metric 7: Total runnable tasks
	if result.TotalRunnableTasks != nil {
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName("sqlserver.threadpool.runnable_tasks")
		metric.SetDescription("Tasks ready to run but waiting for CPU time")
		metric.SetUnit("tasks")
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.TotalRunnableTasks)
		dp.SetTimestamp(timestamp)
		s.addThreadPoolAttributes(dp.Attributes(), result)
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
