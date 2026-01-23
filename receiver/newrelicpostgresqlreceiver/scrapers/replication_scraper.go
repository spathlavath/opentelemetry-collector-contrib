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

// ReplicationScraper scrapes replication metrics from pg_stat_replication
type ReplicationScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
	version      int // PostgreSQL version number
}

// NewReplicationScraper creates a new ReplicationScraper
func NewReplicationScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
	version int,
) *ReplicationScraper {
	return &ReplicationScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
		version:      version,
	}
}

// ScrapeReplicationMetrics scrapes all replication metrics from pg_stat_replication
func (s *ReplicationScraper) ScrapeReplicationMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// Replication metrics available in PostgreSQL 9.6+
	// Version-specific features:
	// - PostgreSQL 9.6: LSN delays only (no lag times)
	// - PostgreSQL 10+: LSN delays + lag times (write_lag, flush_lag, replay_lag)
	const PG96Version = 90600 // PostgreSQL 9.6.0

	if s.version < PG96Version {
		s.logger.Debug("Skipping replication metrics (PostgreSQL 9.6+ required)",
			zap.Int("version", s.version))
		return nil
	}

	metrics, err := s.client.QueryReplicationMetrics(ctx, s.version)
	if err != nil {
		s.logger.Error("Failed to query replication metrics", zap.Error(err))
		return []error{err}
	}

	// No error if no replication (empty result set)
	if len(metrics) == 0 {
		s.logger.Debug("No replication connections found (server may be standby or have no replicas)")
		return nil
	}

	for _, metric := range metrics {
		s.recordMetricsForReplica(now, metric)
	}

	s.logger.Debug("Replication metrics scrape completed",
		zap.Int("replicas", len(metrics)))

	return nil
}

// recordMetricsForReplica records replication metrics for a single standby server
func (s *ReplicationScraper) recordMetricsForReplica(now pcommon.Timestamp, metric models.PgStatReplicationMetric) {
	// Get string values for attributes
	applicationName := getString(metric.ApplicationName)
	state := getString(metric.State)
	syncState := getString(metric.SyncState)
	clientAddr := getString(metric.ClientAddr)

	// Record LSN delay metrics (bytes) - available in PostgreSQL 9.6+
	// Using helper functions to extract values (consistent with other scrapers)
	s.mb.RecordPostgresqlReplicationBackendXminAgeDataPoint(now, getInt64(metric.BackendXminAge), applicationName, clientAddr, state, syncState)
	s.mb.RecordPostgresqlReplicationSentLsnDelayDataPoint(now, getInt64(metric.SentLsnDelay), applicationName, clientAddr, state, syncState)
	s.mb.RecordPostgresqlReplicationWriteLsnDelayDataPoint(now, getInt64(metric.WriteLsnDelay), applicationName, clientAddr, state, syncState)
	s.mb.RecordPostgresqlReplicationFlushLsnDelayDataPoint(now, getInt64(metric.FlushLsnDelay), applicationName, clientAddr, state, syncState)
	s.mb.RecordPostgresqlReplicationReplayLsnDelayDataPoint(now, getInt64(metric.ReplayLsnDelay), applicationName, clientAddr, state, syncState)

	// Record WAL lag time metrics (seconds) - available in PostgreSQL 10+ only
	// For PostgreSQL 9.6, these will be NULL and getFloat64 will return 0.0
	s.mb.RecordPostgresqlReplicationWalWriteLagDataPoint(now, getFloat64(metric.WriteLag), applicationName, clientAddr, state, syncState)
	s.mb.RecordPostgresqlReplicationWalFlushLagDataPoint(now, getFloat64(metric.FlushLag), applicationName, clientAddr, state, syncState)
	s.mb.RecordPostgresqlReplicationWalReplayLagDataPoint(now, getFloat64(metric.ReplayLag), applicationName, clientAddr, state, syncState)
}
