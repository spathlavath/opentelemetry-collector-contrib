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

// ConflictMetricsScraper scrapes replication conflict metrics from pg_stat_database_conflicts
type ConflictMetricsScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
}

// NewConflictMetricsScraper creates a new ConflictMetricsScraper
func NewConflictMetricsScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
) *ConflictMetricsScraper {
	return &ConflictMetricsScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
	}
}

// ScrapeConflictMetrics scrapes all replication conflict metrics from pg_stat_database_conflicts
func (s *ConflictMetricsScraper) ScrapeConflictMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryConflictMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query conflict metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordMetricsForDatabase(now, metric)
	}

	s.logger.Debug("Conflict metrics scrape completed",
		zap.Int("databases", len(metrics)))

	return nil
}

// recordMetricsForDatabase records all conflict metrics for a single database
func (s *ConflictMetricsScraper) recordMetricsForDatabase(now pcommon.Timestamp, metric models.PgStatDatabaseConflictsMetric) {
	databaseName := metric.DatName

	// Record replication conflict metrics using helper functions to extract values
	s.mb.RecordPostgresqlConflictsBufferpinDataPoint(now, getInt64(metric.ConflBufferpin), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsDeadlockDataPoint(now, getInt64(metric.ConflDeadlock), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsLockDataPoint(now, getInt64(metric.ConflLock), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsSnapshotDataPoint(now, getInt64(metric.ConflSnapshot), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsTablespaceDataPoint(now, getInt64(metric.ConflTablespace), s.instanceName, databaseName)
}
