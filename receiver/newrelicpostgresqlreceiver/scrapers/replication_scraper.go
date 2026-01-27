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

// ScrapeReplicationSlots scrapes all replication slot metrics from pg_replication_slots
func (s *ReplicationScraper) ScrapeReplicationSlots(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// Replication slot metrics available in PostgreSQL 9.4+
	const PG94Version = 90400 // PostgreSQL 9.4.0

	if s.version < PG94Version {
		s.logger.Debug("Skipping replication slot metrics (PostgreSQL 9.4+ required)",
			zap.Int("version", s.version))
		return nil
	}

	metrics, err := s.client.QueryReplicationSlots(ctx, s.version)
	if err != nil {
		s.logger.Error("Failed to query replication slot metrics", zap.Error(err))
		return []error{err}
	}

	// No error if no replication slots (empty result set)
	if len(metrics) == 0 {
		s.logger.Debug("No replication slots found (server may not have slots configured)")
		return nil
	}

	for _, metric := range metrics {
		s.recordMetricsForSlot(now, metric)
	}

	s.logger.Debug("Replication slot metrics scrape completed",
		zap.Int("slots", len(metrics)))

	return nil
}

// recordMetricsForSlot records replication slot metrics for a single slot
func (s *ReplicationScraper) recordMetricsForSlot(now pcommon.Timestamp, metric models.PgReplicationSlotMetric) {
	// Get string values for attributes
	slotName := getString(metric.SlotName)
	slotType := getString(metric.SlotType)
	plugin := getString(metric.Plugin)

	// Record slot metrics using helper functions
	s.mb.RecordPostgresqlReplicationSlotXminAgeDataPoint(now, getInt64(metric.XminAge), slotName, slotType, plugin)
	s.mb.RecordPostgresqlReplicationSlotCatalogXminAgeDataPoint(now, getInt64(metric.CatalogXminAge), slotName, slotType, plugin)
	s.mb.RecordPostgresqlReplicationSlotRestartDelayBytesDataPoint(now, getInt64(metric.RestartDelayBytes), slotName, slotType, plugin)
	s.mb.RecordPostgresqlReplicationSlotConfirmedFlushDelayBytesDataPoint(now, getInt64(metric.ConfirmedFlushDelayBytes), slotName, slotType, plugin)
}

// ScrapeReplicationSlotStats scrapes replication slot statistics from pg_stat_replication_slots
func (s *ReplicationScraper) ScrapeReplicationSlotStats(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// Replication slot stats available in PostgreSQL 14+
	const PG14Version = 140000 // PostgreSQL 14.0

	if s.version < PG14Version {
		s.logger.Debug("Skipping replication slot stats (PostgreSQL 14+ required)",
			zap.Int("version", s.version))
		return nil
	}

	metrics, err := s.client.QueryReplicationSlotStats(ctx)
	if err != nil {
		s.logger.Error("Failed to query replication slot stats", zap.Error(err))
		return []error{err}
	}

	// No error if no replication slot stats (empty result set)
	if len(metrics) == 0 {
		s.logger.Debug("No replication slot stats found (server may not have logical slots configured)")
		return nil
	}

	for _, metric := range metrics {
		s.recordMetricsForSlotStats(now, metric)
	}

	s.logger.Debug("Replication slot stats scrape completed",
		zap.Int("slots", len(metrics)))

	return nil
}

// recordMetricsForSlotStats records replication slot statistics for a single slot
func (s *ReplicationScraper) recordMetricsForSlotStats(now pcommon.Timestamp, metric models.PgStatReplicationSlotMetric) {
	// Get string values for attributes
	slotName := getString(metric.SlotName)
	slotType := getString(metric.SlotType)
	state := getString(metric.State)

	// Record slot stats metrics using helper functions
	s.mb.RecordPostgresqlReplicationSlotSpillTxnsDataPoint(now, getInt64(metric.SpillTxns), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotSpillCountDataPoint(now, getInt64(metric.SpillCount), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotSpillBytesDataPoint(now, getInt64(metric.SpillBytes), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotStreamTxnsDataPoint(now, getInt64(metric.StreamTxns), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotStreamCountDataPoint(now, getInt64(metric.StreamCount), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotStreamBytesDataPoint(now, getInt64(metric.StreamBytes), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotTotalTxnsDataPoint(now, getInt64(metric.TotalTxns), slotName, slotType, state)
	s.mb.RecordPostgresqlReplicationSlotTotalBytesDataPoint(now, getInt64(metric.TotalBytes), slotName, slotType, state)
}

// ScrapeReplicationDelay scrapes replication lag metrics from standby servers
func (s *ReplicationScraper) ScrapeReplicationDelay(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// Replication delay metrics available in PostgreSQL 9.6+
	const PG96Version = 90600 // PostgreSQL 9.6.0

	if s.version < PG96Version {
		s.logger.Debug("Skipping replication delay metrics (PostgreSQL 9.6+ required)",
			zap.Int("version", s.version))
		return nil
	}

	metric, err := s.client.QueryReplicationDelay(ctx, s.version)
	if err != nil {
		s.logger.Error("Failed to query replication delay", zap.Error(err))
		return []error{err}
	}

	// Record replication delay metrics
	// These return 0 on primary servers (not in recovery)
	// On standby servers, they show the lag from the primary
	s.recordReplicationDelayMetrics(now, metric)

	s.logger.Debug("Replication delay metrics scrape completed")

	return nil
}

// recordReplicationDelayMetrics records replication delay metrics for the standby server
func (s *ReplicationScraper) recordReplicationDelayMetrics(now pcommon.Timestamp, metric *models.PgReplicationDelayMetric) {
	// Record replication delay (time lag in seconds)
	s.mb.RecordPostgresqlReplicationDelayDataPoint(now, getFloat64(metric.ReplicationDelay), s.instanceName)

	// Record replication delay bytes (byte lag between receive and replay)
	s.mb.RecordPostgresqlReplicationDelayBytesDataPoint(now, getInt64(metric.ReplicationDelayBytes), s.instanceName)
}

// ScrapeWalReceiverMetrics scrapes WAL receiver metrics from pg_stat_wal_receiver
func (s *ReplicationScraper) ScrapeWalReceiverMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	// WAL receiver metrics available in PostgreSQL 9.6+
	const PG96Version = 90600 // PostgreSQL 9.6.0

	if s.version < PG96Version {
		s.logger.Debug("Skipping WAL receiver metrics (PostgreSQL 9.6+ required)",
			zap.Int("version", s.version))
		return nil
	}

	metric, err := s.client.QueryWalReceiverMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query WAL receiver metrics", zap.Error(err))
		return []error{err}
	}

	// nil metric means no WAL receiver found (normal on primary servers)
	if metric == nil {
		s.logger.Debug("No WAL receiver found (server is likely a primary)")
		return nil
	}

	// Record WAL receiver metrics
	s.recordWalReceiverMetrics(now, metric)

	s.logger.Debug("WAL receiver metrics scrape completed")

	return nil
}

// recordWalReceiverMetrics records WAL receiver metrics for the standby server
func (s *ReplicationScraper) recordWalReceiverMetrics(now pcommon.Timestamp, metric *models.PgStatWalReceiverMetric) {
	// Convert status to connected metric (1 if streaming, 0 otherwise)
	status := getString(metric.Status)
	connected := int64(0)
	if status == "streaming" {
		connected = 1
	}

	// Record WAL receiver metrics
	s.mb.RecordPostgresqlWalReceiverConnectedDataPoint(now, connected, s.instanceName)
	s.mb.RecordPostgresqlWalReceiverReceivedTimelineDataPoint(now, getInt64(metric.ReceivedTli), s.instanceName)
	s.mb.RecordPostgresqlWalReceiverLastMsgSendAgeDataPoint(now, getFloat64(metric.LastMsgSendAge), s.instanceName)
	s.mb.RecordPostgresqlWalReceiverLastMsgReceiptAgeDataPoint(now, getFloat64(metric.LastMsgReceiptAge), s.instanceName)
	s.mb.RecordPostgresqlWalReceiverLatestEndAgeDataPoint(now, getFloat64(metric.LatestEndAge), s.instanceName)
}
