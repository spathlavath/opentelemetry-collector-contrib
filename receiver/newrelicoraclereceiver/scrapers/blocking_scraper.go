// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// BlockingScraper collects Oracle blocking queries metrics
type BlockingScraper struct {
	db                            *sql.DB
	mb                            *metadata.MetricsBuilder
	logger                        *zap.Logger
	instanceName                  string
	metricsBuilderConfig          metadata.MetricsBuilderConfig
	queryMonitoringCountThreshold int
}

// NewBlockingScraper creates a new Blocking Queries Scraper instance
func NewBlockingScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig, countThreshold int) (*BlockingScraper, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if mb == nil {
		return nil, fmt.Errorf("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if instanceName == "" {
		return nil, fmt.Errorf("instance name cannot be empty")
	}

	return &BlockingScraper{
		db:                            db,
		mb:                            mb,
		logger:                        logger,
		instanceName:                  instanceName,
		metricsBuilderConfig:          metricsBuilderConfig,
		queryMonitoringCountThreshold: countThreshold,
	}, nil
}

// ScrapeBlockingQueries collects Oracle blocking queries metrics
func (s *BlockingScraper) ScrapeBlockingQueries(ctx context.Context) []error {
	var scrapeErrors []error

	blockingQueriesSQL := queries.GetBlockingQueriesSQL(s.queryMonitoringCountThreshold)
	rows, err := s.db.QueryContext(ctx, blockingQueriesSQL)
	if err != nil {
		s.logger.Error("Failed to execute blocking queries query", zap.Error(err))
		return []error{err}
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.logger.Warn("Failed to close blocking queries result set", zap.Error(closeErr))
		}
	}()

	now := pcommon.NewTimestampFromTime(time.Now())
	rowCount := 0

	for rows.Next() {
		rowCount++
		var blockingQuery models.BlockingQuery

		if err := s.scanBlockingQueryRow(rows, &blockingQuery); err != nil {
			s.logger.Error("Failed to scan blocking query row", zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		s.recordBlockingQueryMetric(now, &blockingQuery)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating through blocking queries rows", zap.Error(err))
		scrapeErrors = append(scrapeErrors, err)
	}

	if rowCount > 0 {
		s.logger.Debug("Completed blocking queries scrape", zap.Int("rows_processed", rowCount))
	}

	return scrapeErrors
}

func (s *BlockingScraper) scanBlockingQueryRow(rows *sql.Rows, blockingQuery *models.BlockingQuery) error {
	return rows.Scan(
		&blockingQuery.BlockedSID,
		&blockingQuery.BlockedSerial,
		&blockingQuery.BlockedUser,
		&blockingQuery.BlockedWaitSec,
		&blockingQuery.BlockedSQLID,
		&blockingQuery.BlockedQueryText,
		&blockingQuery.BlockingSID,
		&blockingQuery.BlockingSerial,
		&blockingQuery.BlockingUser,
		&blockingQuery.DatabaseName,
	)
}

func (s *BlockingScraper) recordBlockingQueryMetric(now pcommon.Timestamp, blockingQuery *models.BlockingQuery) {
	if !blockingQuery.BlockedWaitSec.Valid {
		return
	}

	s.mb.RecordNewrelicoracledbBlockingQueriesWaitTimeDataPoint(
		now,
		blockingQuery.BlockedWaitSec.Float64,
		s.instanceName,
		blockingQuery.GetBlockedUser(),
		blockingQuery.GetBlockingUser(),
		blockingQuery.GetBlockedSQLID(),
		formatInt64(blockingQuery.BlockedSID),
		formatInt64(blockingQuery.BlockingSID),
		formatInt64(blockingQuery.BlockedSerial),
		formatInt64(blockingQuery.BlockingSerial),
		commonutils.AnonymizeAndNormalize(blockingQuery.GetBlockedQueryText()),
		blockingQuery.GetDatabaseName(),
	)
}

func formatInt64(value sql.NullInt64) string {
	if value.Valid {
		return fmt.Sprintf("%d", value.Int64)
	}
	return ""
}
