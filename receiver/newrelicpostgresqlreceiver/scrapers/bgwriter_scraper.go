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

// BgwriterScraper scrapes background writer and checkpointer metrics from pg_stat_bgwriter
type BgwriterScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
	pgVersion    int // PostgreSQL version number for query selection
}

// NewBgwriterScraper creates a new BgwriterScraper
func NewBgwriterScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
	pgVersion int,
) *BgwriterScraper {
	return &BgwriterScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
		pgVersion:    pgVersion,
	}
}

// ScrapeBgwriterMetrics scrapes background writer and checkpointer metrics
func (s *BgwriterScraper) ScrapeBgwriterMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryBgwriterMetrics(ctx, s.pgVersion)
	if err != nil {
		s.logger.Error("Failed to query background writer metrics", zap.Error(err))
		return []error{err}
	}

	// Record all background writer and checkpointer metrics
	s.mb.RecordPostgresqlBgwriterBuffersCleanDataPoint(now, getInt64(metric.BuffersClean), s.instanceName)
	s.mb.RecordPostgresqlBgwriterMaxwrittenCleanDataPoint(now, getInt64(metric.MaxwrittenClean), s.instanceName)
	s.mb.RecordPostgresqlBgwriterBuffersAllocDataPoint(now, getInt64(metric.BuffersAlloc), s.instanceName)
	s.mb.RecordPostgresqlBgwriterCheckpointsTimedDataPoint(now, getInt64(metric.CheckpointsTimed), s.instanceName)
	s.mb.RecordPostgresqlBgwriterCheckpointsRequestedDataPoint(now, getInt64(metric.CheckpointsRequested), s.instanceName)
	s.mb.RecordPostgresqlBgwriterBuffersCheckpointDataPoint(now, getInt64(metric.BuffersCheckpoint), s.instanceName)
	s.mb.RecordPostgresqlBgwriterWriteTimeDataPoint(now, getFloat64(metric.CheckpointWriteTime), s.instanceName)
	s.mb.RecordPostgresqlBgwriterSyncTimeDataPoint(now, getFloat64(metric.CheckpointSyncTime), s.instanceName)
	s.mb.RecordPostgresqlBgwriterBuffersBackendDataPoint(now, getInt64(metric.BuffersBackend), s.instanceName)
	s.mb.RecordPostgresqlBgwriterBuffersBackendFsyncDataPoint(now, getInt64(metric.BuffersBackendFsync), s.instanceName)

	s.logger.Debug("Background writer metrics scrape completed")

	return nil
}

// ScrapeControlCheckpoint scrapes checkpoint control metrics from pg_control_checkpoint()
func (s *BgwriterScraper) ScrapeControlCheckpoint(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryControlCheckpoint(ctx)
	if err != nil {
		// pg_control_checkpoint() only works on primary servers, not standby servers in recovery
		// If we get "recovery is in progress" error, it's expected on standby servers
		if err.Error() == "failed to query pg_control_checkpoint: pq: recovery is in progress" {
			s.logger.Debug("Checkpoint control metrics not available (server is in recovery mode)")
			return nil
		}
		s.logger.Error("Failed to query checkpoint control metrics", zap.Error(err))
		return []error{err}
	}

	// Record all checkpoint control metrics
	s.mb.RecordPostgresqlControlTimelineIDDataPoint(now, getInt64(metric.TimelineID), s.instanceName)
	s.mb.RecordPostgresqlControlCheckpointDelayDataPoint(now, getFloat64(metric.CheckpointDelay), s.instanceName)
	s.mb.RecordPostgresqlControlCheckpointDelayBytesDataPoint(now, getInt64(metric.CheckpointDelayBytes), s.instanceName)
	s.mb.RecordPostgresqlControlRedoDelayBytesDataPoint(now, getInt64(metric.RedoDelayBytes), s.instanceName)

	s.logger.Debug("Checkpoint control metrics scrape completed")

	return nil
}

// ScrapeArchiverStats scrapes WAL archiver statistics from pg_stat_archiver
func (s *BgwriterScraper) ScrapeArchiverStats(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryArchiverStats(ctx)
	if err != nil {
		s.logger.Error("Failed to query archiver statistics", zap.Error(err))
		return []error{err}
	}

	// Record all archiver metrics
	s.mb.RecordPostgresqlArchiverArchivedCountDataPoint(now, getInt64(metric.ArchivedCount), s.instanceName)
	s.mb.RecordPostgresqlArchiverFailedCountDataPoint(now, getInt64(metric.FailedCount), s.instanceName)

	s.logger.Debug("Archiver statistics scrape completed")

	return nil
}

// ScrapeSLRUStats scrapes SLRU (Simple LRU) cache statistics from pg_stat_slru
func (s *BgwriterScraper) ScrapeSLRUStats(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QuerySLRUStats(ctx)
	if err != nil {
		s.logger.Error("Failed to query SLRU statistics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordSLRUMetricsForCache(now, metric)
	}

	s.logger.Debug("SLRU statistics scrape completed",
		zap.Int("slru_caches", len(metrics)))

	return nil
}

// recordSLRUMetricsForCache records all SLRU metrics for a single SLRU cache
func (s *BgwriterScraper) recordSLRUMetricsForCache(now pcommon.Timestamp, metric models.PgStatSLRUMetric) {
	slruName := metric.SLRUName

	// Record all SLRU metrics using helper functions to extract values
	s.mb.RecordPostgresqlSlruBlksZeroedDataPoint(now, getInt64(metric.BlksZeroed), s.instanceName, slruName)
	s.mb.RecordPostgresqlSlruBlksHitDataPoint(now, getInt64(metric.BlksHit), s.instanceName, slruName)
	s.mb.RecordPostgresqlSlruBlksReadDataPoint(now, getInt64(metric.BlksRead), s.instanceName, slruName)
	s.mb.RecordPostgresqlSlruBlksWrittenDataPoint(now, getInt64(metric.BlksWritten), s.instanceName, slruName)
	s.mb.RecordPostgresqlSlruBlksExistsDataPoint(now, getInt64(metric.BlksExists), s.instanceName, slruName)
	s.mb.RecordPostgresqlSlruFlushesDataPoint(now, getInt64(metric.Flushes), s.instanceName, slruName)
	s.mb.RecordPostgresqlSlruTruncatesDataPoint(now, getInt64(metric.Truncates), s.instanceName, slruName)
}

// ScrapeRecoveryPrefetch scrapes recovery prefetch statistics from pg_stat_recovery_prefetch
func (s *BgwriterScraper) ScrapeRecoveryPrefetch(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryRecoveryPrefetch(ctx)
	if err != nil {
		s.logger.Error("Failed to query recovery prefetch statistics", zap.Error(err))
		return []error{err}
	}

	// If no data returned, it means we're not on a standby server or prefetch is not active
	if metric == nil {
		s.logger.Debug("No recovery prefetch data available (not on standby or prefetch disabled)")
		return nil
	}

	// Record all recovery prefetch metrics
	s.mb.RecordPostgresqlRecoveryPrefetchPrefetchDataPoint(now, getInt64(metric.Prefetch), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchHitDataPoint(now, getInt64(metric.Hit), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchSkipInitDataPoint(now, getInt64(metric.SkipInit), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchSkipNewDataPoint(now, getInt64(metric.SkipNew), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchSkipFpwDataPoint(now, getInt64(metric.SkipFpw), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchSkipRepDataPoint(now, getInt64(metric.SkipRep), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchWalDistanceDataPoint(now, getInt64(metric.WalDistance), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchBlockDistanceDataPoint(now, getInt64(metric.BlockDistance), s.instanceName)
	s.mb.RecordPostgresqlRecoveryPrefetchIoDepthDataPoint(now, getInt64(metric.IoDepth), s.instanceName)

	s.logger.Debug("Recovery prefetch statistics scrape completed")

	return nil
}
