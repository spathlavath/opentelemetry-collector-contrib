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
