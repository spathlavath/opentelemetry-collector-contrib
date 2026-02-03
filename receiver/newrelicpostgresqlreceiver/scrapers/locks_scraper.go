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

// LocksScraper scrapes PostgreSQL lock statistics from pg_locks
type LocksScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
}

// NewLocksScraper creates a new LocksScraper
func NewLocksScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
) *LocksScraper {
	return &LocksScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
	}
}

// ScrapeLocks scrapes PostgreSQL lock statistics
func (s *LocksScraper) ScrapeLocks(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryLocks(ctx)
	if err != nil {
		s.logger.Error("Failed to query lock statistics", zap.Error(err))
		return []error{err}
	}

	if metric != nil {
		s.recordLocksMetrics(now, *metric)
	}

	s.logger.Debug("Lock statistics scrape completed")

	return nil
}

// recordLocksMetrics records all lock statistics metrics
func (s *LocksScraper) recordLocksMetrics(now pcommon.Timestamp, metric models.PgLocksMetric) {
	database := metric.Database

	// Record each lock type count
	s.mb.RecordPostgresqlLocksAccessShareDataPoint(now, getInt64(metric.AccessShareLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksRowShareDataPoint(now, getInt64(metric.RowShareLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksRowExclusiveDataPoint(now, getInt64(metric.RowExclusiveLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksShareUpdateExclusiveDataPoint(now, getInt64(metric.ShareUpdateExclusiveLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksShareDataPoint(now, getInt64(metric.ShareLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksShareRowExclusiveDataPoint(now, getInt64(metric.ShareRowExclusiveLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksExclusiveDataPoint(now, getInt64(metric.ExclusiveLock), s.instanceName, database)
	s.mb.RecordPostgresqlLocksAccessExclusiveDataPoint(now, getInt64(metric.AccessExclusiveLock), s.instanceName, database)
}
