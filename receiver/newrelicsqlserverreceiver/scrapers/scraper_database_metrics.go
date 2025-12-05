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

// DatabaseScraper handles SQL Server database-level metrics collection
type DatabaseScraper struct {
	client        client.SQLServerClient
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
}

// NewDatabaseScraper creates a new database scraper
func NewDatabaseScraper(sqlClient client.SQLServerClient, logger *zap.Logger, engineEdition int, mb *metadata.MetricsBuilder) *DatabaseScraper {
	return &DatabaseScraper{
		client:        sqlClient,
		logger:        logger,
		mb:            mb,
		engineEdition: engineEdition,
	}
}

func (s *DatabaseScraper) ScrapeDatabaseBufferMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseBufferMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database buffer metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database buffer metrics query: %w", err)
	}

	s.logger.Debug("Buffer metrics query completed",
		zap.Int("result_count", len(results)),
		zap.Int("engine_edition", s.engineEdition))

	if len(results) == 0 {
		s.logger.Warn("No results returned from database buffer metrics query")
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	recordedCount := 0
	for _, result := range results {
		s.logger.Debug("Processing buffer metric result",
			zap.String("db_name", result.DatabaseName),
			zap.Int64p("buffer_pool_size", result.BufferPoolSizeBytes))

		if result.BufferPoolSizeBytes != nil {
			s.mb.RecordSqlserverDatabaseBufferpoolSizeDataPoint(now, *result.BufferPoolSizeBytes, result.DatabaseName)
			recordedCount++
			s.logger.Debug("Recorded buffer pool metric",
				zap.String("db_name", result.DatabaseName),
				zap.Int64("size_bytes", *result.BufferPoolSizeBytes))
		}
	}

	s.logger.Info("Buffer metrics recording completed",
		zap.Int("total_results", len(results)),
		zap.Int("recorded_count", recordedCount))

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseDiskMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseDiskMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database disk metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database disk metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database disk metrics query")
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	for _, result := range results {
		if result.MaxDiskSizeBytes != nil {
			s.mb.RecordSqlserverDatabaseDiskMaxSizeDataPoint(now, *result.MaxDiskSizeBytes)
		}
	}

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseIOMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseIOMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database IO metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database IO metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database IO metrics query")
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	for _, result := range results {
		if result.IOStallTimeMs != nil {
			s.mb.RecordSqlserverDatabaseIoStallTimeDataPoint(now, *result.IOStallTimeMs)
		}
	}

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseLogGrowthMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseLogGrowthMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database log growth metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database log growth metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database log growth metrics query")
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	for _, result := range results {
		if result.LogGrowthCount != nil {
			s.mb.RecordSqlserverDatabaseLogTransactionGrowthDataPoint(now, *result.LogGrowthCount)
		}
	}

	return nil
}

func (s *DatabaseScraper) ScrapeDatabasePageFileMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabasePageFileMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database page file metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database page file metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database page file metrics query")
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	for _, result := range results {
		if result.PageFileAvailableBytes != nil {
			s.mb.RecordSqlserverDatabasePagefileAvailableDataPoint(now, int64(*result.PageFileAvailableBytes))
		}
	}

	return nil
}

func (s *DatabaseScraper) ScrapeDatabasePageFileTotalMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabasePageFileTotalMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database page file total metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database page file total metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database page file total metrics query")
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	for _, result := range results {
		if result.PageFileTotalBytes != nil {
			s.mb.RecordSqlserverDatabasePagefileTotalDataPoint(now, int64(*result.PageFileTotalBytes))
		}
	}

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseMemoryMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseMemoryMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database memory metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database memory metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database memory metrics query")
		return nil
	}

	// Note: These are instance-level metrics, not per-database
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, result := range results {
		if result.TotalPhysicalMemoryBytes != nil {
			s.mb.RecordSqlserverInstanceMemoryTotalDataPoint(now, int64(*result.TotalPhysicalMemoryBytes))
		}
		if result.AvailablePhysicalMemoryBytes != nil {
			s.mb.RecordSqlserverInstanceMemoryAvailableDataPoint(now, int64(*result.AvailablePhysicalMemoryBytes))
		}
		if result.MemoryUtilizationPercent != nil {
			s.mb.RecordSqlserverInstanceMemoryUtilizationDataPoint(now, *result.MemoryUtilizationPercent)
		}
	}

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseSizeMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseSizeMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database size metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database size metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database size metrics query")
		return nil
	}

	s.logger.Debug("Database size metrics collected", zap.Int("result_count", len(results)))

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseTransactionLogMetrics(ctx context.Context) error {
	results, err := s.client.QueryDatabaseTransactionLogMetrics(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute database transaction log metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute database transaction log metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from database transaction log metrics query")
		return nil
	}

	s.logger.Debug("Database transaction log metrics collected", zap.Int("result_count", len(results)))

	return nil
}

func (s *DatabaseScraper) ScrapeDatabaseLogSpaceUsageMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping database log space usage metrics")

	// Note: Log space usage typically uses DBCC SQLPERF(LOGSPACE) or is captured through other metrics
	s.logger.Debug("DatabaseScraper.ScrapeDatabaseLogSpaceUsageMetrics not fully implemented")
	return nil
}
