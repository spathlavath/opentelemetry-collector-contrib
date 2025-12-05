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

// LockScraper handles SQL Server lock metrics collection
type LockScraper struct {
	client        client.SQLServerClient
	logger        *zap.Logger
	mb            *metadata.MetricsBuilder
	engineEdition int
}

// NewLockScraper creates a new lock scraper
func NewLockScraper(sqlClient client.SQLServerClient, logger *zap.Logger, engineEdition int, mb *metadata.MetricsBuilder) *LockScraper {
	return &LockScraper{
		client:        sqlClient,
		logger:        logger,
		mb:            mb,
		engineEdition: engineEdition,
	}
}

// ScrapeLockResourceMetrics collects lock resource metrics using engine-specific queries
func (s *LockScraper) ScrapeLockResourceMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server lock resource metrics")

	results, err := s.client.QueryLockResourceSummary(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute lock resource query",
			zap.Error(err),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute lock resource query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Debug("No lock resource results returned - no active locks")
		return nil
	}

	s.logger.Debug("Processing lock resource metrics results",
		zap.Int("result_count", len(results)))

	// Process each database's lock resource metrics
	for _, result := range results {
		if result.DatabaseName == nil {
			s.logger.Warn("Database name is null in lock resource results")
			continue
		}

		if err := s.processLockResourceMetrics(result); err != nil {
			s.logger.Error("Failed to process lock resource metrics",
				zap.Error(err),
				zap.String("database_name", *result.DatabaseName))
			continue
		}

		s.logger.Debug("Successfully processed lock resource metrics",
			zap.String("database_name", *result.DatabaseName),
			zap.Int64("total_active_locks", getInt64Value(result.TotalActiveLocks)))
	}

	s.logger.Debug("Successfully scraped lock resource metrics",
		zap.Int("database_count", len(results)))

	return nil
}

// ScrapeLockModeMetrics collects lock mode metrics using engine-specific queries
func (s *LockScraper) ScrapeLockModeMetrics(ctx context.Context) error {
	s.logger.Debug("Scraping SQL Server lock mode metrics")

	results, err := s.client.QueryLockModeSummary(ctx, s.engineEdition)
	if err != nil {
		s.logger.Error("Failed to execute lock mode query",
			zap.Error(err),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute lock mode query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Debug("No lock mode results returned - no active locks")
		return nil
	}

	s.logger.Debug("Processing lock mode metrics results",
		zap.Int("result_count", len(results)))

	// Process each database's lock mode metrics
	for _, result := range results {
		if result.DatabaseName == nil {
			s.logger.Warn("Database name is null in lock mode results")
			continue
		}

		if err := s.processLockModeMetrics(result); err != nil {
			s.logger.Error("Failed to process lock mode metrics",
				zap.Error(err),
				zap.String("database_name", *result.DatabaseName))
			continue
		}

		s.logger.Debug("Successfully processed lock mode metrics",
			zap.String("database_name", *result.DatabaseName),
			zap.Int64("total_active_locks", getInt64Value(result.TotalActiveLocks)))
	}

	s.logger.Debug("Successfully scraped lock mode metrics",
		zap.Int("database_count", len(results)))

	return nil
}

// processLockResourceMetrics processes lock resource metrics and creates OpenTelemetry metrics
func (s *LockScraper) processLockResourceMetrics(result models.LockResourceSummary) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.TotalActiveLocks != nil {
		s.mb.RecordSqlserverLockResourceTotalDataPoint(timestamp, *result.TotalActiveLocks)
	}
	if result.TableLocks != nil {
		s.mb.RecordSqlserverLockResourceTableDataPoint(timestamp, *result.TableLocks)
	}
	if result.PageLocks != nil {
		s.mb.RecordSqlserverLockResourcePageDataPoint(timestamp, *result.PageLocks)
	}
	if result.RowLocks != nil {
		s.mb.RecordSqlserverLockResourceRowDataPoint(timestamp, *result.RowLocks)
	}
	if result.KeyLocks != nil {
		s.mb.RecordSqlserverLockResourceKeyDataPoint(timestamp, *result.KeyLocks)
	}
	if result.ExtentLocks != nil {
		s.mb.RecordSqlserverLockResourceExtentDataPoint(timestamp, *result.ExtentLocks)
	}
	if result.DatabaseLocks != nil {
		s.mb.RecordSqlserverLockResourceDatabaseDataPoint(timestamp, *result.DatabaseLocks)
	}
	if result.FileLocks != nil {
		s.mb.RecordSqlserverLockResourceFileDataPoint(timestamp, *result.FileLocks)
	}
	if result.ApplicationLocks != nil {
		s.mb.RecordSqlserverLockResourceApplicationDataPoint(timestamp, *result.ApplicationLocks)
	}
	if result.MetadataLocks != nil {
		s.mb.RecordSqlserverLockResourceMetadataDataPoint(timestamp, *result.MetadataLocks)
	}
	if result.HobtLocks != nil {
		s.mb.RecordSqlserverLockResourceHobtDataPoint(timestamp, *result.HobtLocks)
	}
	if result.AllocationUnitLocks != nil {
		s.mb.RecordSqlserverLockResourceAllocationUnitDataPoint(timestamp, *result.AllocationUnitLocks)
	}

	return nil
}

// processLockModeMetrics processes lock mode metrics and creates OpenTelemetry metrics
func (s *LockScraper) processLockModeMetrics(result models.LockModeSummary) error {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if result.TotalActiveLocks != nil {
		s.mb.RecordSqlserverLockModeTotalDataPoint(timestamp, *result.TotalActiveLocks)
	}
	if result.SharedLocks != nil {
		s.mb.RecordSqlserverLockModeSharedDataPoint(timestamp, *result.SharedLocks)
	}
	if result.ExclusiveLocks != nil {
		s.mb.RecordSqlserverLockModeExclusiveDataPoint(timestamp, *result.ExclusiveLocks)
	}
	if result.UpdateLocks != nil {
		s.mb.RecordSqlserverLockModeUpdateDataPoint(timestamp, *result.UpdateLocks)
	}
	if result.IntentLocks != nil {
		s.mb.RecordSqlserverLockModeIntentDataPoint(timestamp, *result.IntentLocks)
	}
	if result.SchemaLocks != nil {
		s.mb.RecordSqlserverLockModeSchemaDataPoint(timestamp, *result.SchemaLocks)
	}
	if result.BulkUpdateLocks != nil {
		s.mb.RecordSqlserverLockModeBulkUpdateDataPoint(timestamp, *result.BulkUpdateLocks)
	}
	if result.SharedIntentExclusiveLocks != nil {
		s.mb.RecordSqlserverLockModeSharedIntentExclusiveDataPoint(timestamp, *result.SharedIntentExclusiveLocks)
	}

	return nil
}

// getInt64Value safely extracts int64 value from pointer, returns 0 if nil
func getInt64Value(ptr *int64) int64 {
	if ptr == nil {
		return 0
	}
	return *ptr
}
