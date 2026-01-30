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

// ActivityScraper scrapes activity metrics from pg_stat_activity
type ActivityScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
}

// NewActivityScraper creates a new ActivityScraper
func NewActivityScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
) *ActivityScraper {
	return &ActivityScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
	}
}

// ScrapeActivityMetrics scrapes all activity metrics from pg_stat_activity
func (s *ActivityScraper) ScrapeActivityMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryActivityMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query activity metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordActivityMetrics(now, metric)
	}

	s.logger.Debug("Activity metrics scrape completed",
		zap.Int("activity_groups", len(metrics)))

	return nil
}

// recordActivityMetrics records all activity metrics for a single activity group
func (s *ActivityScraper) recordActivityMetrics(now pcommon.Timestamp, metric models.PgStatActivity) {
	// Get string values for attributes
	databaseName := getString(metric.DatName)
	userName := getString(metric.UserName)
	applicationName := getString(metric.ApplicationName)
	backendType := getString(metric.BackendType)

	// Record all activity metrics using helper functions to extract values
	s.mb.RecordPostgresqlActiveWaitingQueriesDataPoint(now, getInt64(metric.ActiveWaitingQueries), s.instanceName, databaseName, userName, applicationName, backendType)
	s.mb.RecordPostgresqlActivityXactStartAgeDataPoint(now, getFloat64(metric.XactStartAge), s.instanceName, databaseName, userName, applicationName, backendType)
	s.mb.RecordPostgresqlActivityBackendXidAgeDataPoint(now, getInt64(metric.BackendXIDAge), s.instanceName, databaseName, userName, applicationName, backendType)
	s.mb.RecordPostgresqlActivityBackendXminAgeDataPoint(now, getInt64(metric.BackendXminAge), s.instanceName, databaseName, userName, applicationName, backendType)
	s.mb.RecordPostgresqlTransactionsDurationMaxDataPoint(now, getFloat64(metric.MaxTransactionDuration), s.instanceName, databaseName, userName, applicationName, backendType)
	s.mb.RecordPostgresqlTransactionsDurationSumDataPoint(now, getFloat64(metric.SumTransactionDuration), s.instanceName, databaseName, userName, applicationName, backendType)
}

// ScrapeWaitEvents scrapes wait event metrics from pg_stat_activity
func (s *ActivityScraper) ScrapeWaitEvents(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryWaitEvents(ctx)
	if err != nil {
		s.logger.Error("Failed to query wait events", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordWaitEventMetrics(now, metric)
	}

	s.logger.Debug("Wait event metrics scrape completed",
		zap.Int("wait_event_groups", len(metrics)))

	return nil
}

// recordWaitEventMetrics records wait event metrics for a single wait event group
func (s *ActivityScraper) recordWaitEventMetrics(now pcommon.Timestamp, metric models.PgStatActivityWaitEvents) {
	// Get string values for attributes
	databaseName := getString(metric.DatName)
	userName := getString(metric.UserName)
	applicationName := getString(metric.ApplicationName)
	backendType := getString(metric.BackendType)
	waitEvent := getString(metric.WaitEvent)

	// Record wait event metric
	s.mb.RecordPostgresqlActivityWaitEventDataPoint(now, getInt64(metric.WaitEventCount), s.instanceName, databaseName, userName, applicationName, backendType, waitEvent)
}

// ScrapePgStatStatementsDealloc scrapes pg_stat_statements deallocation metric
// Requires pg_stat_statements extension to be installed and enabled
// Available in PostgreSQL 13+
func (s *ActivityScraper) ScrapePgStatStatementsDealloc(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryPgStatStatementsDealloc(ctx)
	if err != nil {
		s.logger.Error("Failed to query pg_stat_statements_info", zap.Error(err))
		return []error{err}
	}

	if metric != nil && metric.Dealloc.Valid {
		s.mb.RecordPostgresqlPgStatStatementsDeallocDataPoint(now, metric.Dealloc.Int64, s.instanceName)
		s.logger.Debug("pg_stat_statements dealloc metric scraped", zap.Int64("dealloc", metric.Dealloc.Int64))
	}

	return nil
}

// ScrapeSnapshot scrapes transaction snapshot metrics
// Available in PostgreSQL 13+
func (s *ActivityScraper) ScrapeSnapshot(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QuerySnapshot(ctx)
	if err != nil {
		s.logger.Error("Failed to query pg_snapshot", zap.Error(err))
		return []error{err}
	}

	if metric != nil {
		if metric.Xmin.Valid {
			s.mb.RecordPostgresqlSnapshotXminDataPoint(now, metric.Xmin.Int64, s.instanceName)
		}
		if metric.Xmax.Valid {
			s.mb.RecordPostgresqlSnapshotXmaxDataPoint(now, metric.Xmax.Int64, s.instanceName)
		}
		if metric.XipCount.Valid {
			s.mb.RecordPostgresqlSnapshotXipCountDataPoint(now, metric.XipCount.Int64, s.instanceName)
		}
		s.logger.Debug("Snapshot metrics scraped")
	}

	return nil
}
