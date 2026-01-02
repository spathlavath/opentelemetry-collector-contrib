// Copyright The OpenTelemetry Authors
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
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewChildCursorsScraper creates a new child cursors scraper
func NewChildCursorsScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig) *ChildCursorsScraper {
	return &ChildCursorsScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

func (s *ChildCursorsScraper) ScrapeChildCursorsForIdentifiers(ctx context.Context, identifiers []models.SQLIdentifier, childLimit int) []error {
	var errs []error

	s.logger.Debug("Starting child cursors scrape", zap.Int("identifier_count", len(identifiers)))

	now := pcommon.NewTimestampFromTime(time.Now())
	metricsEmitted := 0

	if len(identifiers) > 0 {
		for _, identifier := range identifiers {
			s.logger.Debug("Querying child cursor",
				zap.String("sql_id", identifier.SQLID),
				zap.Int64("child_number", identifier.ChildNumber))

			cursor, err := s.client.QuerySpecificChildCursor(ctx, identifier.SQLID, identifier.ChildNumber)
			if err != nil {
				s.logger.Warn("Failed to fetch specific child cursor from V$SQL",
					zap.String("sql_id", identifier.SQLID),
					zap.Int64("child_number", identifier.ChildNumber),
					zap.Error(err))
				errs = append(errs, err)
				continue
			}

			if cursor != nil && cursor.HasValidIdentifier() {
				s.logger.Debug("Recording child cursor metrics",
					zap.String("sql_id", identifier.SQLID),
					zap.Int64("child_number", identifier.ChildNumber))
				s.recordChildCursorMetrics(now, cursor)
				metricsEmitted++
			} else {
				s.logger.Debug("Skipping child cursor with invalid identifier",
					zap.String("sql_id", identifier.SQLID),
					zap.Int64("child_number", identifier.ChildNumber))
			}
		}
	}

	s.logger.Info("Child cursors scrape completed",
		zap.Int("identifiers_processed", len(identifiers)),
		zap.Int("metrics_emitted", metricsEmitted),
		zap.Int("errors", len(errs)))

	return errs
}

// recordChildCursorMetrics records all metrics for a single child cursor
func (s *ChildCursorsScraper) recordChildCursorMetrics(now pcommon.Timestamp, cursor *models.ChildCursor) {
	collectionTimestamp := cursor.GetCollectionTimestamp().Format("2006-01-02 15:04:05")
	sqlID := cursor.GetSQLID()
	childNumber := cursor.GetChildNumber()
	planHashValue := fmt.Sprintf("%d", cursor.GetPlanHashValue())
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
			planHashValue,
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
			planHashValue,
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
			planHashValue,
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
			planHashValue,
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
			planHashValue,
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
			planHashValue,
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
			planHashValue,
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
			planHashValue,
			cursor.GetFirstLoadTime(),
			cursor.GetLastLoadTime(),
		)
	}
}
