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
