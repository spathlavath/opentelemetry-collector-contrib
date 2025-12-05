// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// InstanceConfig holds configuration for instance metrics
type InstanceConfig struct {
	EnableInstanceMemoryMetrics        bool
	EnableInstanceComprehensiveStats   bool
	EnableInstanceStats                bool
	EnableInstanceBufferPoolHitPercent bool
	EnableInstanceProcessCounts        bool
	EnableInstanceRunnableTasks        bool
	EnableInstanceDiskMetrics          bool
	EnableInstanceActiveConnections    bool
	EnableInstanceBufferPoolSize       bool
}

// SQLConnectionInterface defines the interface for database connections
type SQLConnectionInterface interface {
	Query(ctx context.Context, dest interface{}, query string) error
}

// InstanceScraper handles SQL Server instance-level metrics collection
type InstanceScraper struct {
	connection    SQLConnectionInterface
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
	config        InstanceConfig
}

// NewInstanceScraper creates a new instance scraper
func NewInstanceScraper(conn SQLConnectionInterface, logger *zap.Logger, engineEdition int, config InstanceConfig, mb *metadata.MetricsBuilder) *InstanceScraper {
	return &InstanceScraper{
		connection:    conn,
		logger:        logger,
		engineEdition: engineEdition,
		config:        config,
		mb:            mb,
	}
}

// getQueryForMetric retrieves the appropriate query for a metric based on engine edition with Default fallback
func (s *InstanceScraper) getQueryForMetric(metricName string) (string, bool) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, metricName, s.engineEdition)
	if found {
		s.logger.Debug("Using query for metric",
			zap.String("metric_name", metricName),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
	}
	return query, found
}

func (s *InstanceScraper) ScrapeInstanceMemoryMetrics(ctx context.Context) error {
	// Check if instance memory metrics are enabled
	if !s.config.EnableInstanceMemoryMetrics {
		s.logger.Debug("Instance memory metrics collection is disabled")
		return nil
	}

	s.logger.Debug("Scraping SQL Server instance memory metrics")

	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.memory_metrics", s.engineEdition) {
		s.logger.Debug("Instance memory metrics not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.memory_metrics")
	if !found {
		return fmt.Errorf("no memory query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing memory query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceMemoryDefinitionsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance memory query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute instance memory query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance memory query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from instance memory query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from instance memory query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]
	if result.TotalPhysicalMemory == nil && result.AvailablePhysicalMemory == nil && result.MemoryUtilization == nil {
		s.logger.Error("All memory metrics are null - invalid query result")
		return fmt.Errorf("all memory metrics are null in query result")
	}

	if err := s.processInstanceMemoryMetrics(result); err != nil {
		s.logger.Error("Failed to process instance memory metrics", zap.Error(err))
		return fmt.Errorf("failed to process instance memory metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped instance memory metrics")
	return nil
}

// ScrapeInstanceComprehensiveStats scrapes comprehensive instance statistics
func (s *InstanceScraper) ScrapeInstanceComprehensiveStats(ctx context.Context) error {
	// Check if instance comprehensive stats are enabled
	if !s.config.EnableInstanceComprehensiveStats {
		s.logger.Debug("Instance comprehensive stats collection is disabled")
		return nil
	}

	s.logger.Debug("Scraping SQL Server comprehensive instance statistics")

	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.InstanceQueries, "sqlserver.instance.comprehensive_stats", s.engineEdition) {
		s.logger.Debug("Instance comprehensive stats not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.instance.comprehensive_stats")
	if !found {
		return fmt.Errorf("no comprehensive stats query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing comprehensive stats query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.InstanceStatsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute comprehensive stats query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute comprehensive stats query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from comprehensive stats query - SQL Server may not be ready")
		return fmt.Errorf("no results returned from comprehensive stats query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from comprehensive stats query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]
	if err := s.processInstanceStatsMetrics(result); err != nil {
		s.logger.Error("Failed to process comprehensive stats metrics", zap.Error(err))
		return fmt.Errorf("failed to process comprehensive stats metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped comprehensive instance statistics")
	return nil
}

// processInstanceMemoryMetrics processes memory metrics using MetricsBuilder
func (s *InstanceScraper) processInstanceMemoryMetrics(result models.InstanceMemoryDefinitionsModel) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.TotalPhysicalMemory != nil {
		s.mb.RecordSqlserverInstanceMemoryTotalDataPoint(timestamp, int64(*result.TotalPhysicalMemory))
	}
	if result.AvailablePhysicalMemory != nil {
		s.mb.RecordSqlserverInstanceMemoryAvailableDataPoint(timestamp, int64(*result.AvailablePhysicalMemory))
	}
	if result.MemoryUtilization != nil {
		s.mb.RecordSqlserverInstanceMemoryUtilizationDataPoint(timestamp, *result.MemoryUtilization)
	}

	return nil
}

func (s *InstanceScraper) processInstanceStatsMetrics(result models.InstanceStatsModel) error {
	// Instance stats contains many metrics but most are rate counters that need to be calculated
	// For now, we'll just log that we got the results
	// TODO: Implement delta calculation for rate metrics
	s.logger.Debug("Instance stats metrics collected")
	return nil
}

func (s *InstanceScraper) processInstanceBufferPoolHitPercent(result models.BufferPoolHitPercentMetricsModel) error {
	if result.BufferPoolHitPercent != nil {
		timestamp := pcommon.NewTimestampFromTime(time.Now())
		s.mb.RecordSqlserverInstanceBufferPoolHitPercentDataPoint(timestamp, *result.BufferPoolHitPercent)
	}
	return nil
}

func (s *InstanceScraper) processInstanceProcessCounts(result models.InstanceProcessCountsModel) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Sum all process counts to get total
	var total int64
	if result.Preconnect != nil {
		total += *result.Preconnect
	}
	if result.Background != nil {
		total += *result.Background
	}
	if result.Dormant != nil {
		total += *result.Dormant
	}
	if result.Runnable != nil {
		total += *result.Runnable
	}
	if result.Suspended != nil {
		total += *result.Suspended
	}
	if result.Running != nil {
		total += *result.Running
	}
	if result.Blocked != nil {
		total += *result.Blocked
	}
	if result.Sleeping != nil {
		total += *result.Sleeping
	}

	if total > 0 {
		s.mb.RecordSqlserverInstanceProcessCountsDataPoint(timestamp, total)
	}

	return nil
}

func (s *InstanceScraper) processInstanceRunnableTasks(result models.RunnableTasksMetricsModel) error {
	if result.RunnableTasksCount != nil {
		timestamp := pcommon.NewTimestampFromTime(time.Now())
		s.mb.RecordSqlserverInstanceRunnableTasksDataPoint(timestamp, *result.RunnableTasksCount)
	}
	return nil
}

func (s *InstanceScraper) processInstanceDiskMetrics(result models.InstanceDiskMetricsModel) error {
	if result.TotalDiskSpace != nil {
		timestamp := pcommon.NewTimestampFromTime(time.Now())
		s.mb.RecordSqlserverInstanceDiskMetricsDataPoint(timestamp, *result.TotalDiskSpace)
	}
	return nil
}

func (s *InstanceScraper) processInstanceActiveConnections(result models.InstanceActiveConnectionsMetricsModel) error {
	if result.InstanceActiveConnections != nil {
		timestamp := pcommon.NewTimestampFromTime(time.Now())
		s.mb.RecordSqlserverInstanceActiveConnectionsDataPoint(timestamp, *result.InstanceActiveConnections)
	}
	return nil
}

func (s *InstanceScraper) processInstanceBufferPoolSize(result models.InstanceBufferMetricsModel) error {
	if result.BufferPoolSize != nil {
		timestamp := pcommon.NewTimestampFromTime(time.Now())
		s.mb.RecordSqlserverInstanceBufferPoolSizeDataPoint(timestamp, *result.BufferPoolSize)
	}
	return nil
}

func (s *InstanceScraper) processInstanceTargetMemoryMetrics(result models.InstanceTargetMemoryModel) error {
	// Note: There may not be a corresponding Record method for target memory
	// TODO: Verify if metadata.yaml has this metric defined
	s.logger.Debug("Instance target memory metrics collected")
	return nil
}

func (s *InstanceScraper) processInstancePerformanceRatiosMetrics(result models.InstancePerformanceRatiosModel) error {
	// Note: Performance ratio metrics may not be mapped yet
	// TODO: Verify if metadata.yaml has these metrics defined
	s.logger.Debug("Instance performance ratio metrics collected")
	return nil
}

func (s *InstanceScraper) processInstanceIndexMetrics(result models.InstanceIndexMetricsModel) error {
	// Note: Index metrics may not be mapped yet
	// TODO: Verify if metadata.yaml has these metrics defined
	s.logger.Debug("Instance index metrics collected")
	return nil
}

func (s *InstanceScraper) processInstanceLockMetrics(result models.InstanceLockMetricsModel) error {
	// Note: Lock metrics may not be mapped yet
	// TODO: Verify if metadata.yaml has these metrics defined
	s.logger.Debug("Instance lock metrics collected")
	return nil
}

// Scrape method implementations

func (s *InstanceScraper) ScrapeInstanceProcessCounts(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.process_counts")
	if !found {
		return fmt.Errorf("no instance process counts query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceProcessCountsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance process counts query", zap.Error(err))
		return fmt.Errorf("failed to execute instance process counts query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance process counts query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceProcessCounts(result); err != nil {
			s.logger.Error("Failed to process instance process counts", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceRunnableTasks(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.runnable_tasks")
	if !found {
		return fmt.Errorf("no instance runnable tasks query available for engine edition %d", s.engineEdition)
	}

	var results []models.RunnableTasksMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance runnable tasks query", zap.Error(err))
		return fmt.Errorf("failed to execute instance runnable tasks query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance runnable tasks query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceRunnableTasks(result); err != nil {
			s.logger.Error("Failed to process instance runnable tasks", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceActiveConnections(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.active_connections")
	if !found {
		return fmt.Errorf("no instance active connections query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceActiveConnectionsMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance active connections query", zap.Error(err))
		return fmt.Errorf("failed to execute instance active connections query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance active connections query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceActiveConnections(result); err != nil {
			s.logger.Error("Failed to process instance active connections", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceBufferPoolHitPercent(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.buffer_pool_hit_percent")
	if !found {
		return fmt.Errorf("no instance buffer pool hit percent query available for engine edition %d", s.engineEdition)
	}

	var results []models.BufferPoolHitPercentMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance buffer pool hit percent query", zap.Error(err))
		return fmt.Errorf("failed to execute instance buffer pool hit percent query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance buffer pool hit percent query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceBufferPoolHitPercent(result); err != nil {
			s.logger.Error("Failed to process instance buffer pool hit percent", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceDiskMetrics(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.disk_metrics")
	if !found {
		return fmt.Errorf("no instance disk metrics query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceDiskMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance disk metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute instance disk metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance disk metrics query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceDiskMetrics(result); err != nil {
			s.logger.Error("Failed to process instance disk metrics", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceBufferPoolSize(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.buffer_pool_size")
	if !found {
		return fmt.Errorf("no instance buffer pool size query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceBufferMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance buffer pool size query", zap.Error(err))
		return fmt.Errorf("failed to execute instance buffer pool size query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance buffer pool size query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceBufferPoolSize(result); err != nil {
			s.logger.Error("Failed to process instance buffer pool size", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceTargetMemoryMetrics(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.target_memory")
	if !found {
		return fmt.Errorf("no instance target memory query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceTargetMemoryModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance target memory query", zap.Error(err))
		return fmt.Errorf("failed to execute instance target memory query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance target memory query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceTargetMemoryMetrics(result); err != nil {
			s.logger.Error("Failed to process instance target memory", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstancePerformanceRatiosMetrics(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.performance_ratios")
	if !found {
		return fmt.Errorf("no instance performance ratios query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstancePerformanceRatiosModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance performance ratios query", zap.Error(err))
		return fmt.Errorf("failed to execute instance performance ratios query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance performance ratios query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstancePerformanceRatiosMetrics(result); err != nil {
			s.logger.Error("Failed to process instance performance ratios", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceIndexMetrics(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.index_metrics")
	if !found {
		return fmt.Errorf("no instance index metrics query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceIndexMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance index metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute instance index metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance index metrics query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceIndexMetrics(result); err != nil {
			s.logger.Error("Failed to process instance index metrics", zap.Error(err))
			return err
		}
	}

	return nil
}

func (s *InstanceScraper) ScrapeInstanceLockMetrics(ctx context.Context) error {
	query, found := s.getQueryForMetric("sqlserver.instance.lock_metrics")
	if !found {
		return fmt.Errorf("no instance lock metrics query available for engine edition %d", s.engineEdition)
	}

	var results []models.InstanceLockMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute instance lock metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute instance lock metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from instance lock metrics query")
		return nil
	}

	for _, result := range results {
		if err := s.processInstanceLockMetrics(result); err != nil {
			s.logger.Error("Failed to process instance lock metrics", zap.Error(err))
			return err
		}
	}

	return nil
}
