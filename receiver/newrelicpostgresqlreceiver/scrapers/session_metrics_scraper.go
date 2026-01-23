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

// SessionMetricsScraper scrapes session-level metrics from pg_stat_database (PostgreSQL 14+)
type SessionMetricsScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
}

// NewSessionMetricsScraper creates a new SessionMetricsScraper
func NewSessionMetricsScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
) *SessionMetricsScraper {
	return &SessionMetricsScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
	}
}

// ScrapeSessionMetrics scrapes all session-level metrics from pg_stat_database (PostgreSQL 14+)
func (s *SessionMetricsScraper) ScrapeSessionMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QuerySessionMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query session metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordMetricsForDatabase(now, metric)
	}

	s.logger.Debug("Session metrics scrape completed",
		zap.Int("databases", len(metrics)))

	return nil
}

// recordMetricsForDatabase records all session metrics for a single database
func (s *SessionMetricsScraper) recordMetricsForDatabase(now pcommon.Timestamp, metric models.PgStatDatabaseSessionMetric) {
	databaseName := metric.DatName

	// Record session metrics using helper functions to extract values
	s.mb.RecordPostgresqlSessionsSessionTimeDataPoint(now, getFloat64(metric.SessionTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsActiveTimeDataPoint(now, getFloat64(metric.ActiveTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsIdleInTransactionTimeDataPoint(now, getFloat64(metric.IdleInTransactionTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsCountDataPoint(now, getInt64(metric.SessionCount), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsAbandonedDataPoint(now, getInt64(metric.SessionsAbandoned), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsFatalDataPoint(now, getInt64(metric.SessionsFatal), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsKilledDataPoint(now, getInt64(metric.SessionsKilled), s.instanceName, databaseName)
}
