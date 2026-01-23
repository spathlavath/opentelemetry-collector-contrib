// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// ReplicationScraper scrapes replication metrics from pg_stat_replication
type ReplicationScraper struct {
	client  client.PostgreSQLClient
	mb      *metadata.MetricsBuilder
	logger  *zap.Logger
	version int // PostgreSQL version number
}

// NewReplicationScraper creates a new ReplicationScraper
func NewReplicationScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	version int,
) *ReplicationScraper {
	return &ReplicationScraper{
		client:  client,
		mb:      mb,
		logger:  logger,
		version: version,
	}
}

// ScrapeReplicationMetrics scrapes all replication metrics from pg_stat_replication
func (s *ReplicationScraper) ScrapeReplicationMetrics(ctx context.Context, now pcommon.Timestamp) []error {
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
		s.logger.Warn("Failed to query replication metrics", zap.Error(err))
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

	return nil
}

// recordMetricsForReplica records replication metrics for a single standby server
func (s *ReplicationScraper) recordMetricsForReplica(now pcommon.Timestamp, metric models.PgStatReplicationMetric) {
	// Get string values for attributes
	applicationName := getStringValue(metric.ApplicationName)
	state := getStringValue(metric.State)
	syncState := getStringValue(metric.SyncState)
	clientAddr := getStringValue(metric.ClientAddr)

	// Record LSN delay metrics (bytes)
	if metric.BackendXminAge.Valid {
		s.mb.RecordPostgresqlReplicationBackendXminAgeDataPoint(
			now,
			metric.BackendXminAge.Int64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	if metric.SentLsnDelay.Valid {
		s.mb.RecordPostgresqlReplicationSentLsnDelayDataPoint(
			now,
			metric.SentLsnDelay.Int64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	if metric.WriteLsnDelay.Valid {
		s.mb.RecordPostgresqlReplicationWriteLsnDelayDataPoint(
			now,
			metric.WriteLsnDelay.Int64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	if metric.FlushLsnDelay.Valid {
		s.mb.RecordPostgresqlReplicationFlushLsnDelayDataPoint(
			now,
			metric.FlushLsnDelay.Int64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	if metric.ReplayLsnDelay.Valid {
		s.mb.RecordPostgresqlReplicationReplayLsnDelayDataPoint(
			now,
			metric.ReplayLsnDelay.Int64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	// Record WAL lag time metrics (seconds) - PostgreSQL 10+
	if metric.WriteLag.Valid {
		s.mb.RecordPostgresqlReplicationWalWriteLagDataPoint(
			now,
			metric.WriteLag.Float64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	if metric.FlushLag.Valid {
		s.mb.RecordPostgresqlReplicationWalFlushLagDataPoint(
			now,
			metric.FlushLag.Float64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}

	if metric.ReplayLag.Valid {
		s.mb.RecordPostgresqlReplicationWalReplayLagDataPoint(
			now,
			metric.ReplayLag.Float64,
			applicationName,
			clientAddr,
			state,
			syncState,
		)
	}
}

// getStringValue safely extracts string from sql.NullString
func getStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}
