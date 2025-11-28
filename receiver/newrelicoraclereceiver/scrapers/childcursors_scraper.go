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

// ScrapeChildCursors collects metrics for child cursors for given SQL IDs
func (s *ChildCursorsScraper) ScrapeChildCursors(ctx context.Context, sqlIDs []string, childLimit int) []error {
	var errs []error

	if len(sqlIDs) == 0 {
		return errs
	}

	s.logger.Debug("Starting child cursors scrape",
		zap.Int("sql_ids", len(sqlIDs)),
		zap.Int("child_limit", childLimit))

	now := pcommon.NewTimestampFromTime(time.Now())
	metricsEmitted := 0

	for _, sqlID := range sqlIDs {
		childCursors, err := s.client.QueryChildCursors(ctx, sqlID, childLimit)
		if err != nil {
			s.logger.Warn("Failed to fetch child cursors for SQL_ID",
				zap.String("sql_id", sqlID),
				zap.Error(err))
			errs = append(errs, err)
			continue
		}

		if len(childCursors) == 0 {
			s.logger.Debug("No child cursors found for SQL_ID", zap.String("sql_id", sqlID))
			continue
		}

		for _, cursor := range childCursors {
			if !cursor.HasValidIdentifier() {
				continue
			}

			s.recordChildCursorMetrics(now, &cursor)
			metricsEmitted++
		}
	}

	s.logger.Debug("Child cursors scrape completed",
		zap.Int("metrics_emitted", metricsEmitted),
		zap.Int("errors", len(errs)))

	return errs
}

// recordChildCursorMetrics records all metrics for a single child cursor
func (s *ChildCursorsScraper) recordChildCursorMetrics(now pcommon.Timestamp, cursor *models.ChildCursor) {
	sqlID := cursor.GetSQLID()
	childNumber := cursor.GetChildNumber()
	databaseName := cursor.GetDatabaseName()

	// Record CPU time
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled {
		s.mb.RecordNewrelicoracledbChildCursorsCPUTimeDataPoint(
			now,
			cursor.GetCPUTime(),
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
			databaseName,
			sqlID,
			childNumber,
			cursor.GetFirstLoadTime(),
			cursor.GetLastLoadTime(),
		)
	}
}
