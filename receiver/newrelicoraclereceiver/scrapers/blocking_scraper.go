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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// BlockingScraper collects Oracle blocking queries metrics
type BlockingScraper struct {
	client                        client.OracleClient
	mb                            *metadata.MetricsBuilder
	logger                        *zap.Logger
	instanceName                  string
	metricsBuilderConfig          metadata.MetricsBuilderConfig
	queryMonitoringCountThreshold int
}

// NewBlockingScraper creates a new Blocking Queries Scraper instance
func NewBlockingScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig, countThreshold int) (*BlockingScraper, error) {
	if oracleClient == nil {
		return nil, fmt.Errorf("client cannot be nil")
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
		client:                        oracleClient,
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

	blockingQueries, err := s.client.QueryBlockingQueries(ctx, s.queryMonitoringCountThreshold)
	if err != nil {
		s.logger.Error("Failed to query blocking queries", zap.Error(err))
		return []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, blockingQuery := range blockingQueries {
		s.recordBlockingQueryMetric(now, &blockingQuery)
	}

	if len(blockingQueries) > 0 {
		s.logger.Debug("Completed blocking queries scrape", zap.Int("rows_processed", len(blockingQueries)))
	}

	return scrapeErrors
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
		blockingQuery.GetQueryID(),
		formatInt64(blockingQuery.SessionID),
		formatInt64(blockingQuery.BlockingSID),
		formatInt64(blockingQuery.BlockedSerial),
		formatInt64(blockingQuery.BlockingSerial),
		blockingQuery.GetBlockedSQLExecStart(),
		blockingQuery.GetDatabaseName(),
	)
}

func formatInt64(value sql.NullInt64) string {
	if value.Valid {
		return fmt.Sprintf("%d", value.Int64)
	}
	return ""
}
