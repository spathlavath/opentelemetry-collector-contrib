// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// PgBouncerScraper scrapes PgBouncer connection pooler statistics
type PgBouncerScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
}

// NewPgBouncerScraper creates a new PgBouncerScraper
func NewPgBouncerScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
) *PgBouncerScraper {
	return &PgBouncerScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
	}
}

// ScrapePgBouncerStats scrapes PgBouncer connection pool statistics
func (s *PgBouncerScraper) ScrapePgBouncerStats(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryPgBouncerStats(ctx)
	if err != nil {
		s.logger.Debug("Failed to query PgBouncer stats (PgBouncer may not be available)", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordPgBouncerStatsMetrics(now, metric)
	}

	s.logger.Debug("PgBouncer stats scrape completed",
		zap.Int("databases", len(metrics)))

	return nil
}

// recordPgBouncerStatsMetrics records all PgBouncer statistics metrics for a single database
func (s *PgBouncerScraper) recordPgBouncerStatsMetrics(now pcommon.Timestamp, metric models.PgBouncerStatsMetric) {
	database := metric.Database

	// Record per-second rate metrics (these are averages from PgBouncer)
	s.mb.RecordPgbouncerStatsBytesInPerSecondDataPoint(now, getInt64(metric.AvgRecv), s.instanceName, database)
	s.mb.RecordPgbouncerStatsBytesOutPerSecondDataPoint(now, getInt64(metric.AvgSent), s.instanceName, database)
	s.mb.RecordPgbouncerStatsQueriesPerSecondDataPoint(now, getInt64(metric.AvgQueryCount), s.instanceName, database)
	s.mb.RecordPgbouncerStatsRequestsPerSecondDataPoint(now, getInt64(metric.AvgQueryCount), s.instanceName, database)
	s.mb.RecordPgbouncerStatsTransactionsPerSecondDataPoint(now, getInt64(metric.AvgXactCount), s.instanceName, database)

	// Calculate average bytes per request/transaction
	// avg_recv and avg_sent are per-second rates, so these are already averages
	s.mb.RecordPgbouncerStatsAvgBytesInDataPoint(now, getInt64(metric.AvgRecv), s.instanceName, database)
	s.mb.RecordPgbouncerStatsAvgBytesOutDataPoint(now, getInt64(metric.AvgSent), s.instanceName, database)

	// Record average metrics
	s.mb.RecordPgbouncerStatsAvgRequestsPerSecondDataPoint(now, getInt64(metric.AvgQueryCount), s.instanceName, database)
	s.mb.RecordPgbouncerStatsAvgTransactionCountDataPoint(now, getInt64(metric.AvgXactCount), s.instanceName, database)

	// Convert transaction time from microseconds to milliseconds
	avgXactTimeUs := getInt64(metric.AvgXactTime)
	avgXactTimeMs := float64(avgXactTimeUs) / 1000.0
	s.mb.RecordPgbouncerStatsAvgTransactionDurationMillisecondsDataPoint(now, avgXactTimeMs, s.instanceName, database)

	// Record server assignment counts (how many times server connections were assigned from pool)
	s.mb.RecordPgbouncerStatsTotalServerAssignmentCountDataPoint(now, getInt64(metric.TotalServerAssignmentCount), s.instanceName, database)
	s.mb.RecordPgbouncerStatsAvgServerAssignmentCountDataPoint(now, getInt64(metric.AvgServerAssignmentCount), s.instanceName, database)
}

// ScrapePgBouncerPools scrapes PgBouncer per-pool connection details
func (s *PgBouncerScraper) ScrapePgBouncerPools(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryPgBouncerPools(ctx)
	if err != nil {
		s.logger.Debug("Failed to query PgBouncer pools (PgBouncer may not be available)", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordPgBouncerPoolsMetrics(now, metric)
	}

	s.logger.Debug("PgBouncer pools scrape completed",
		zap.Int("pools", len(metrics)))

	return nil
}

// recordPgBouncerPoolsMetrics records all PgBouncer pool metrics for a single database/user combination
func (s *PgBouncerScraper) recordPgBouncerPoolsMetrics(now pcommon.Timestamp, metric models.PgBouncerPoolsMetric) {
	database := metric.Database
	user := metric.User
	poolMode := metric.PoolMode

	// Record client connection metrics
	s.mb.RecordPgbouncerPoolsClientConnectionsActiveDataPoint(now, getInt64(metric.ClActive), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsClientConnectionsWaitingDataPoint(now, getInt64(metric.ClWaiting), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsClientConnectionsActiveCancelReqDataPoint(now, getInt64(metric.ClActiveCancelReq), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsClientConnectionsWaitingCancelReqDataPoint(now, getInt64(metric.ClWaitingCancelReq), s.instanceName, database, user, poolMode)

	// Record server connection metrics
	s.mb.RecordPgbouncerPoolsServerConnectionsActiveDataPoint(now, getInt64(metric.SvActive), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsServerConnectionsIdleDataPoint(now, getInt64(metric.SvIdle), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsServerConnectionsUsedDataPoint(now, getInt64(metric.SvUsed), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsServerConnectionsTestedDataPoint(now, getInt64(metric.SvTested), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsServerConnectionsLoginDataPoint(now, getInt64(metric.SvLogin), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsServerConnectionsActiveCancelDataPoint(now, getInt64(metric.SvActiveCancel), s.instanceName, database, user, poolMode)
	s.mb.RecordPgbouncerPoolsServerConnectionsBeingCancelDataPoint(now, getInt64(metric.SvBeingCancel), s.instanceName, database, user, poolMode)

	// Convert maxwait from microseconds to milliseconds
	maxwaitUs := getInt64(metric.Maxwait)
	maxwaitMs := float64(maxwaitUs) / 1000.0
	s.mb.RecordPgbouncerPoolsMaxwaitInMillisecondsDataPoint(now, maxwaitMs, s.instanceName, database, user, poolMode)
}
