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

// TableIOScraper scrapes table and index IO metrics from PostgreSQL
type TableIOScraper struct {
	client       client.PostgreSQLClient
	logger       *zap.Logger
	mb           *metadata.MetricsBuilder
	instanceName string
	pgVersion    int
}

// NewTableIOScraper creates a new TableIOScraper
func NewTableIOScraper(client client.PostgreSQLClient, logger *zap.Logger, mb *metadata.MetricsBuilder, instanceName string, pgVersion int) *TableIOScraper {
	return &TableIOScraper{
		client:       client,
		logger:       logger,
		mb:           mb,
		instanceName: instanceName,
		pgVersion:    pgVersion,
	}
}

// ScrapeUserTables scrapes per-table statistics from pg_stat_user_tables
func (s *TableIOScraper) ScrapeUserTables(ctx context.Context, schemas, tables []string) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryUserTables(ctx, schemas, tables)
	if err != nil {
		s.logger.Error("Failed to query user tables statistics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordUserTableMetrics(now, metric)
	}

	s.logger.Debug("User tables statistics scrape completed",
		zap.Int("table_count", len(metrics)))

	return nil
}

// recordUserTableMetrics records all metrics for a single table
func (s *TableIOScraper) recordUserTableMetrics(now pcommon.Timestamp, metric models.PgStatUserTablesMetric) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Record vacuum and analyze ages (gauge metrics)
	s.mb.RecordPostgresqlLastVacuumAgeDataPoint(now, getFloat64(metric.LastVacuumAge), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlLastAutovacuumAgeDataPoint(now, getFloat64(metric.LastAutovacuumAge), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlLastAnalyzeAgeDataPoint(now, getFloat64(metric.LastAnalyzeAge), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlLastAutoanalyzeAgeDataPoint(now, getFloat64(metric.LastAutoanalyzeAge), s.instanceName, metric.SchemaName, metric.TableName)

	// Record vacuum and analyze counts (cumulative counters)
	s.mb.RecordPostgresqlVacuumedDataPoint(now, getInt64(metric.VacuumCount), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlAutovacuumedDataPoint(now, getInt64(metric.AutovacuumCount), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlAnalyzedDataPoint(now, getInt64(metric.AnalyzeCount), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlAutoanalyzedDataPoint(now, getInt64(metric.AutoanalyzeCount), s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded metrics for table",
		zap.String("table", tableID))
}

// ScrapeIOUserTables scrapes per-table disk IO statistics from pg_statio_user_tables
func (s *TableIOScraper) ScrapeIOUserTables(ctx context.Context, schemas, tables []string) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryIOUserTables(ctx, schemas, tables)
	if err != nil {
		s.logger.Error("Failed to query user tables IO statistics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordIOUserTablesMetrics(now, metric)
	}

	s.logger.Debug("User tables IO statistics scrape completed",
		zap.Int("table_count", len(metrics)))

	return nil
}

// recordIOUserTablesMetrics records all IO metrics for a single table
func (s *TableIOScraper) recordIOUserTablesMetrics(now pcommon.Timestamp, metric models.PgStatIOUserTables) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Record heap block statistics (cumulative counters)
	s.mb.RecordPostgresqlHeapBlocksReadDataPoint(now, getInt64(metric.HeapBlksRead), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlHeapBlocksHitDataPoint(now, getInt64(metric.HeapBlksHit), s.instanceName, metric.SchemaName, metric.TableName)

	// Record index block statistics (cumulative counters)
	s.mb.RecordPostgresqlIndexBlocksReadDataPoint(now, getInt64(metric.IdxBlksRead), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlIndexBlocksHitDataPoint(now, getInt64(metric.IdxBlksHit), s.instanceName, metric.SchemaName, metric.TableName)

	// Record TOAST block statistics (cumulative counters)
	s.mb.RecordPostgresqlToastBlocksReadDataPoint(now, getInt64(metric.ToastBlksRead), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlToastBlocksHitDataPoint(now, getInt64(metric.ToastBlksHit), s.instanceName, metric.SchemaName, metric.TableName)

	// Record TOAST index block statistics (cumulative counters)
	s.mb.RecordPostgresqlToastIndexBlocksReadDataPoint(now, getInt64(metric.TidxBlksRead), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlToastIndexBlocksHitDataPoint(now, getInt64(metric.TidxBlksHit), s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded IO metrics for table",
		zap.String("table", tableID))
}

// ScrapeUserIndexes scrapes per-index statistics from pg_stat_user_indexes
func (s *TableIOScraper) ScrapeUserIndexes(ctx context.Context, schemas, tables []string) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryUserIndexes(ctx, schemas, tables)
	if err != nil {
		s.logger.Error("Failed to query user indexes statistics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordUserIndexesMetrics(now, metric)
	}

	s.logger.Debug("User indexes statistics scrape completed",
		zap.Int("index_count", len(metrics)))

	return nil
}

// recordUserIndexesMetrics records all index metrics for a single index
func (s *TableIOScraper) recordUserIndexesMetrics(now pcommon.Timestamp, metric models.PgStatUserIndexes) {
	// Create composite identifier for index
	indexID := metric.SchemaName + "." + metric.TableName + "." + metric.IndexName

	// Record index scan statistics (cumulative counters)
	s.mb.RecordPostgresqlIndexScansDataPoint(now, getInt64(metric.IdxScan), s.instanceName, metric.SchemaName, metric.TableName, metric.IndexName)
	s.mb.RecordPostgresqlIndexTuplesReadDataPoint(now, getInt64(metric.IdxTupRead), s.instanceName, metric.SchemaName, metric.TableName, metric.IndexName)
	s.mb.RecordPostgresqlIndexTuplesFetchedDataPoint(now, getInt64(metric.IdxTupFetch), s.instanceName, metric.SchemaName, metric.TableName, metric.IndexName)

	// Record index size (gauge) - calculated from pg_relation_size
	// We need to query this separately with the indexrelid
	if metric.IndexRelID.Valid {
		indexSize := s.getIndexSize(context.Background(), metric.IndexRelID.Int64)
		s.mb.RecordPostgresqlIndexSizeDataPoint(now, indexSize, s.instanceName, metric.SchemaName, metric.TableName, metric.IndexName)
	}

	s.logger.Debug("Recorded metrics for index",
		zap.String("index", indexID))
}

// getIndexSize retrieves the size of an index using pg_relation_size
// This is a simplified implementation that returns 0 for now
// TODO: Implement proper size calculation using client query method
func (s *TableIOScraper) getIndexSize(_ context.Context, indexRelID int64) int64 {
	// Placeholder: Will implement proper pg_relation_size query
	// query := "SELECT pg_relation_size($1)"
	s.logger.Debug("Index size calculation not yet implemented", zap.Int64("indexRelID", indexRelID))
	return 0
}
