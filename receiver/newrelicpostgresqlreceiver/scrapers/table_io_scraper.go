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

	// Record table row counts (gauge metrics)
	s.mb.RecordPostgresqlTableLiveRowsDataPoint(now, getInt64(metric.NLiveTup), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableDeadRowsDataPoint(now, getInt64(metric.NDeadTup), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableModifiedSinceAnalyzeDataPoint(now, getInt64(metric.NModSinceAnalyze), s.instanceName, metric.SchemaName, metric.TableName)

	// Record sequential scan statistics (cumulative counters)
	s.mb.RecordPostgresqlTableSequentialScansDataPoint(now, getInt64(metric.SeqScan), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableSequentialScanRowsFetchedDataPoint(now, getInt64(metric.SeqTupRead), s.instanceName, metric.SchemaName, metric.TableName)

	// Record index scan statistics (cumulative counters)
	s.mb.RecordPostgresqlTableIndexScansDataPoint(now, getInt64(metric.IdxScan), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableIndexScanRowsFetchedDataPoint(now, getInt64(metric.IdxTupFetch), s.instanceName, metric.SchemaName, metric.TableName)

	// Record row modification statistics (cumulative counters)
	s.mb.RecordPostgresqlTableRowsInsertedDataPoint(now, getInt64(metric.NTupIns), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableRowsUpdatedDataPoint(now, getInt64(metric.NTupUpd), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableRowsDeletedDataPoint(now, getInt64(metric.NTupDel), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableHotUpdatesDataPoint(now, getInt64(metric.NTupHotUpd), s.instanceName, metric.SchemaName, metric.TableName)

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
func (s *TableIOScraper) getIndexSize(ctx context.Context, indexRelID int64) int64 {
	size, err := s.client.QueryIndexSize(ctx, indexRelID)
	if err != nil {
		s.logger.Debug("Failed to query index size", zap.Int64("indexRelID", indexRelID), zap.Error(err))
		return 0
	}
	return size
}

// ScrapeToastTables scrapes TOAST table vacuum statistics
func (s *TableIOScraper) ScrapeToastTables(ctx context.Context, schemas, tables []string) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryToastTables(ctx, schemas, tables)
	if err != nil {
		s.logger.Error("Failed to query TOAST table statistics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordToastTablesMetrics(now, metric)
	}

	s.logger.Debug("TOAST table statistics scrape completed",
		zap.Int("toast_table_count", len(metrics)))

	return nil
}

// recordToastTablesMetrics records all TOAST table metrics for a single table
func (s *TableIOScraper) recordToastTablesMetrics(now pcommon.Timestamp, metric models.PgStatToastTables) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Record TOAST vacuum counts (cumulative counters)
	s.mb.RecordPostgresqlToastVacuumedDataPoint(now, getInt64(metric.ToastVacuumCount), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlToastAutovacuumedDataPoint(now, getInt64(metric.ToastAutovacuumCount), s.instanceName, metric.SchemaName, metric.TableName)

	// Record TOAST vacuum ages (gauges)
	s.mb.RecordPostgresqlToastLastVacuumAgeDataPoint(now, getFloat64(metric.ToastLastVacuumAge), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlToastLastAutovacuumAgeDataPoint(now, getFloat64(metric.ToastLastAutovacuumAge), s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded TOAST table metrics",
		zap.String("table", tableID))
}

// ScrapeTableSizes scrapes table size statistics from pg_class
func (s *TableIOScraper) ScrapeTableSizes(ctx context.Context, schemas, tables []string) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryTableSizes(ctx, schemas, tables)
	if err != nil {
		s.logger.Error("Failed to query table sizes", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordTableSizesMetrics(now, metric)
	}

	s.logger.Debug("Table sizes scrape completed",
		zap.Int("table_count", len(metrics)))

	return nil
}

// recordTableSizesMetrics records all table size metrics for a single table
func (s *TableIOScraper) recordTableSizesMetrics(now pcommon.Timestamp, metric models.PgClassSizes) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Record table sizes (gauges)
	s.mb.RecordPostgresqlRelationSizeDataPoint(now, getInt64(metric.RelationSize), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlToastSizeDataPoint(now, getInt64(metric.ToastSize), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableTotalSizeDataPoint(now, getInt64(metric.TotalSize), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlTableIndexesSizeDataPoint(now, getInt64(metric.IndexesSize), s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded table size metrics",
		zap.String("table", tableID))
}

func (s *TableIOScraper) ScrapeRelationStats(ctx context.Context, schemas, tables []string) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryRelationStats(ctx, schemas, tables)
	if err != nil {
		s.logger.Error("Failed to query relation statistics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordRelationStatsMetrics(now, metric)
	}

	s.logger.Debug("Relation statistics scrape completed",
		zap.Int("table_count", len(metrics)))

	return nil
}

// recordRelationStatsMetrics records all relation statistics metrics for a single table
func (s *TableIOScraper) recordRelationStatsMetrics(now pcommon.Timestamp, metric models.PgClassStats) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Record relation statistics (gauges)
	s.mb.RecordPostgresqlRelationPagesDataPoint(now, getInt64(metric.RelPages), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlRelationTuplesDataPoint(now, getFloat64(metric.RelTuples), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlRelationAllVisibleDataPoint(now, getInt64(metric.RelAllVisible), s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlRelationXminDataPoint(now, getInt64(metric.Xmin), s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded relation statistics metrics",
		zap.String("table", tableID))
}
