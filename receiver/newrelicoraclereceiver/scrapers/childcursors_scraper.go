// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// ChildCursorsScraper handles scraping of child cursor metrics from V$SQL
type ChildCursorsScraper struct {
	client                 client.OracleClient
	mb                     *metadata.MetricsBuilder
	logger                 *zap.Logger
	metricsBuilderConfig   metadata.MetricsBuilderConfig
	collectDetailedMetrics bool // Flag to control collection of detailed metrics (cpu_time, disk_reads, buffer_gets, etc.)
}

// NewChildCursorsScraper creates a new child cursors scraper
func NewChildCursorsScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig, collectDetailedMetrics bool) *ChildCursorsScraper {
	if collectDetailedMetrics {
		logger.Info("Child cursors scraper initialized with DETAILED metrics collection enabled")
	} else {
		logger.Info("Child cursors scraper initialized with ESSENTIAL metrics only")
	}

	return &ChildCursorsScraper{
		client:                 oracleClient,
		mb:                     mb,
		logger:                 logger,
		metricsBuilderConfig:   metricsBuilderConfig,
		collectDetailedMetrics: collectDetailedMetrics,
	}
}

func (s *ChildCursorsScraper) ScrapeChildCursorsForIdentifiers(ctx context.Context, identifiers []models.SQLIdentifier, childLimit int) ([]models.SQLIdentifier, []error) {
	var errs []error
	s.logger.Debug("Starting child cursors scrape")
	now := pcommon.NewTimestampFromTime(time.Now())
	metricsEmitted := 0

	if len(identifiers) > 0 {
		for i := range identifiers {

			cursor, err := s.client.QuerySpecificChildCursor(ctx, identifiers[i].SQLID, identifiers[i].ChildNumber)
			if err != nil {
				s.logger.Warn("Failed to fetch specific child cursor from V$SQL",
					zap.String("sql_id", identifiers[i].SQLID),
					zap.Int64("child_number", identifiers[i].ChildNumber),
					zap.Error(err))
				errs = append(errs, err)
				continue
			}

			if cursor != nil && cursor.HasValidIdentifier() {
				s.recordChildCursorMetrics(now, cursor)
				metricsEmitted++

				planHashValue := fmt.Sprintf("%d", cursor.GetPlanHashValue())
				identifiers[i].PlanHash = planHashValue
			}
		}
	}

	s.logger.Info("Child cursors scrape completed")

	return identifiers, errs
}

// recordChildCursorMetrics records all metrics for a single child cursor
func (s *ChildCursorsScraper) recordChildCursorMetrics(now pcommon.Timestamp, cursor *models.ChildCursor) {
	collectionTimestamp := cursor.GetCollectionTimestamp()
	sqlID := cursor.GetSQLID()
	childNumber := cursor.GetChildNumber()
	planHashValue := fmt.Sprintf("%d", cursor.GetPlanHashValue())
	databaseName := cursor.GetDatabaseName()

	// Always record essential metrics
	s.recordEssentialChildCursorMetrics(now, cursor, collectionTimestamp, sqlID, childNumber, planHashValue, databaseName)

	// Record detailed metrics only when flag is enabled
	if s.collectDetailedMetrics {
		s.recordDetailedChildCursorMetrics(now, cursor, collectionTimestamp, sqlID, childNumber, planHashValue, databaseName)
	}
}

// recordEssentialChildCursorMetrics records essential child cursor metrics (always collected)
func (s *ChildCursorsScraper) recordEssentialChildCursorMetrics(now pcommon.Timestamp, cursor *models.ChildCursor, collectionTimestamp, sqlID string, childNumber int64, planHashValue, databaseName string) {
	// ESSENTIAL: elapsed time
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsElapsedTimeDataPoint(
			now,
			cursor.GetElapsedTime(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

	// ESSENTIAL: details with load times
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsDetails.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsDetailsDataPoint(
			now,
			1,
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
			cursor.GetFirstLoadTime(),
			cursor.GetLastLoadTime(),
		)
	}
}

// recordDetailedChildCursorMetrics records detailed child cursor metrics (only when detailed monitoring enabled)
func (s *ChildCursorsScraper) recordDetailedChildCursorMetrics(now pcommon.Timestamp, cursor *models.ChildCursor, collectionTimestamp, sqlID string, childNumber int64, planHashValue, databaseName string) {
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsCPUTimeDataPoint(
			now,
			cursor.GetCPUTime(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsUserIoWaitTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsUserIoWaitTimeDataPoint(
			now,
			cursor.GetUserIOWaitTime(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsExecutions.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsExecutionsDataPoint(
			now,
			cursor.GetExecutions(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsDiskReads.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsDiskReadsDataPoint(
			now,
			cursor.GetDiskReads(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsBufferGets.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsBufferGetsDataPoint(
			now,
			cursor.GetBufferGets(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsInvalidations.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsInvalidationsDataPoint(
			now,
			cursor.GetInvalidations(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			planHashValue,
		)
	}

}
