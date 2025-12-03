// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// ChildCursorsScraper handles scraping of child cursor metrics from V$SQL
type ChildCursorsScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewChildCursorsScraper creates a new child cursors scraper
func NewChildCursorsScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *ChildCursorsScraper {
	return &ChildCursorsScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

// ScrapeChildCursorsWithCache collects metrics for pre-fetched child cursors and additional new identifiers
// This optimized method avoids re-querying V$SQL for child cursors already fetched
// Parameters:
//   - cachedChildCursors: Child cursors already fetched from V$SQL (to avoid duplicate queries)
//   - newIdentifiers: New SQL identifiers found in wait events (need to be queried)
//   - childLimit: Limit for querying new identifiers
func (s *ChildCursorsScraper) ScrapeChildCursorsWithCache(ctx context.Context, cachedChildCursors []models.ChildCursor, newIdentifiers []models.SQLIdentifier, childLimit int) []error {
	var errs []error

	s.logger.Debug("Starting cached child cursors scrape",
		zap.Int("cached_cursors", len(cachedChildCursors)),
		zap.Int("new_identifiers", len(newIdentifiers)),
		zap.Int("child_limit", childLimit))

	now := pcommon.NewTimestampFromTime(time.Now())
	metricsEmitted := 0

	// STEP 1: Record metrics for cached child cursors (from V$SQL top N)
	for _, cursor := range cachedChildCursors {
		if !cursor.HasValidIdentifier() {
			continue
		}

		s.recordChildCursorMetrics(now, &cursor)
		metricsEmitted++
	}

	s.logger.Debug("Cached child cursor metrics emitted",
		zap.Int("metrics_emitted", metricsEmitted))

	// STEP 2: Query V$SQL ONLY for NEW child numbers found in wait events
	if len(newIdentifiers) > 0 {
		newMetricsEmitted := 0
		for _, identifier := range newIdentifiers {
			// Query this specific (SQL_ID, CHILD_NUMBER) pair directly
			cursor, err := s.client.QuerySpecificChildCursor(ctx, identifier.SQLID, identifier.ChildNumber)
			if err != nil {
				s.logger.Warn("Failed to fetch specific child cursor from V$SQL",
					zap.String("sql_id", identifier.SQLID),
					zap.Int64("child_number", identifier.ChildNumber),
					zap.Error(err))
				errs = append(errs, err)
				continue
			}

			// Record metrics if cursor was found
			if cursor != nil && cursor.HasValidIdentifier() {
				s.recordChildCursorMetrics(now, cursor)
				metricsEmitted++
				newMetricsEmitted++
			}
		}

		s.logger.Debug("New child cursor metrics emitted",
			zap.Int("new_metrics_emitted", newMetricsEmitted))
	}

	s.logger.Debug("Child cursors scrape with cache completed",
		zap.Int("total_metrics_emitted", metricsEmitted),
		zap.Int("errors", len(errs)))

	return errs
}

// recordChildCursorMetrics records all metrics for a single child cursor
func (s *ChildCursorsScraper) recordChildCursorMetrics(now pcommon.Timestamp, cursor *models.ChildCursor) {
	collectionTimestamp := cursor.GetCollectionTimestamp().Format("2006-01-02 15:04:05")
	sqlID := cursor.GetSQLID()
	childNumber := cursor.GetChildNumber()
	databaseName := cursor.GetDatabaseName()

	// Record CPU time
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsCPUTimeDataPoint(
			now,
			cursor.GetCPUTime(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record elapsed time
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsElapsedTimeDataPoint(
			now,
			cursor.GetElapsedTime(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record user I/O wait time
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsUserIoWaitTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsUserIoWaitTimeDataPoint(
			now,
			cursor.GetUserIOWaitTime(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record executions
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsExecutions.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsExecutionsDataPoint(
			now,
			cursor.GetExecutions(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record disk reads
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsDiskReads.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsDiskReadsDataPoint(
			now,
			cursor.GetDiskReads(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record buffer gets
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsBufferGets.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsBufferGetsDataPoint(
			now,
			cursor.GetBufferGets(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record invalidations
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsInvalidations.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsInvalidationsDataPoint(
			now,
			cursor.GetInvalidations(),
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
		)
	}

	// Record details with load times
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsDetails.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsDetailsDataPoint(
			now,
			1, // count of 1 for each child cursor
			collectionTimestamp,
			databaseName,
			sqlID,
			childNumber,
			cursor.GetFirstLoadTime(),
			cursor.GetLastLoadTime(),
		)
	}
}
