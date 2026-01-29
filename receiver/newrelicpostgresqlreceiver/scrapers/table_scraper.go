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

// TableScraper scrapes per-table metrics from various PostgreSQL system views
type TableScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
	pgVersion    int // PostgreSQL version number for query selection
}

// NewTableScraper creates a new TableScraper
func NewTableScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
	pgVersion int,
) *TableScraper {
	return &TableScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
		pgVersion:    pgVersion,
	}
}

// ScrapeUserTables scrapes per-table statistics from pg_stat_user_tables
func (s *TableScraper) ScrapeUserTables(ctx context.Context, schemas, tables []string) []error {
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
func (s *TableScraper) recordUserTableMetrics(now pcommon.Timestamp, metric models.PgStatUserTablesMetric) {
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

// ScrapeAnalyzeProgress scrapes ANALYZE operation progress from pg_stat_progress_analyze
// Only available in PostgreSQL 13+
func (s *TableScraper) ScrapeAnalyzeProgress(ctx context.Context) []error {
	// Skip if PostgreSQL version < 13
	if s.pgVersion < 130000 {
		s.logger.Debug("Skipping ANALYZE progress metrics (PostgreSQL 13+ required)",
			zap.Int("version", s.pgVersion))
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryAnalyzeProgress(ctx)
	if err != nil {
		s.logger.Error("Failed to query ANALYZE progress statistics", zap.Error(err))
		return []error{err}
	}

	// It's normal to have zero metrics (no ANALYZE operations running)
	if len(metrics) == 0 {
		s.logger.Debug("No ANALYZE operations currently running")
		return nil
	}

	for _, metric := range metrics {
		s.recordAnalyzeProgressMetrics(now, metric)
	}

	s.logger.Debug("ANALYZE progress statistics scrape completed",
		zap.Int("operation_count", len(metrics)))

	return nil
}

// recordAnalyzeProgressMetrics records all progress metrics for a single ANALYZE operation
func (s *TableScraper) recordAnalyzeProgressMetrics(now pcommon.Timestamp, metric models.PgStatProgressAnalyze) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Record sample block progress
	s.mb.RecordPostgresqlAnalyzeSampleBlksTotalDataPoint(now, getInt64(metric.SampleBlksTotal), metric.Database, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlAnalyzeSampleBlksScannedDataPoint(now, getInt64(metric.SampleBlksScanned), metric.Database, s.instanceName, metric.SchemaName, metric.TableName)

	// Record extended statistics progress
	s.mb.RecordPostgresqlAnalyzeExtStatsTotalDataPoint(now, getInt64(metric.ExtStatsTotal), metric.Database, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlAnalyzeExtStatsComputedDataPoint(now, getInt64(metric.ExtStatsComputed), metric.Database, s.instanceName, metric.SchemaName, metric.TableName)

	// Record child table progress (for partitioned tables)
	s.mb.RecordPostgresqlAnalyzeChildTablesTotalDataPoint(now, getInt64(metric.ChildTablesTotal), metric.Database, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlAnalyzeChildTablesDoneDataPoint(now, getInt64(metric.ChildTablesDone), metric.Database, s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded ANALYZE progress metrics",
		zap.String("table", tableID),
		zap.String("phase", metric.Phase.String))
}

// ScrapeClusterProgress scrapes CLUSTER/VACUUM FULL operation progress from pg_stat_progress_cluster
// Only available in PostgreSQL 12+
func (s *TableScraper) ScrapeClusterProgress(ctx context.Context) []error {
	// Skip if PostgreSQL version < 12
	if s.pgVersion < 120000 {
		s.logger.Debug("Skipping CLUSTER/VACUUM FULL progress metrics (PostgreSQL 12+ required)",
			zap.Int("version", s.pgVersion))
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryClusterProgress(ctx)
	if err != nil {
		s.logger.Error("Failed to query CLUSTER/VACUUM FULL progress statistics", zap.Error(err))
		return []error{err}
	}

	// It's normal to have zero metrics (no CLUSTER/VACUUM FULL operations running)
	if len(metrics) == 0 {
		s.logger.Debug("No CLUSTER/VACUUM FULL operations currently running")
		return nil
	}

	for _, metric := range metrics {
		s.recordClusterProgressMetrics(now, metric)
	}

	s.logger.Debug("CLUSTER/VACUUM FULL progress statistics scrape completed",
		zap.Int("operation_count", len(metrics)))

	return nil
}

// recordClusterProgressMetrics records all progress metrics for a single CLUSTER/VACUUM FULL operation
func (s *TableScraper) recordClusterProgressMetrics(now pcommon.Timestamp, metric models.PgStatProgressCluster) {
	// Create composite identifier for table
	tableID := metric.SchemaName + "." + metric.TableName

	// Get command type (CLUSTER or VACUUM FULL)
	command := metric.Command.String

	// Record heap block progress
	s.mb.RecordPostgresqlClusterVacuumHeapBlksTotalDataPoint(now, getInt64(metric.HeapBlksTotal), command, metric.Database, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlClusterVacuumHeapBlksScannedDataPoint(now, getInt64(metric.HeapBlksScanned), command, metric.Database, s.instanceName, metric.SchemaName, metric.TableName)

	// Record tuple progress
	s.mb.RecordPostgresqlClusterVacuumHeapTuplesScannedDataPoint(now, getInt64(metric.HeapTuplesScanned), command, metric.Database, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlClusterVacuumHeapTuplesWrittenDataPoint(now, getInt64(metric.HeapTuplesWritten), command, metric.Database, s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded CLUSTER/VACUUM FULL progress metrics",
		zap.String("table", tableID),
		zap.String("command", command),
		zap.String("phase", metric.Phase.String))
}

// ScrapeCreateIndexProgress scrapes CREATE INDEX operation progress from pg_stat_progress_create_index
// Only available in PostgreSQL 12+
func (s *TableScraper) ScrapeCreateIndexProgress(ctx context.Context) []error {
	// Skip if PostgreSQL version < 12
	if s.pgVersion < 120000 {
		s.logger.Debug("Skipping CREATE INDEX progress metrics (PostgreSQL 12+ required)",
			zap.Int("version", s.pgVersion))
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryCreateIndexProgress(ctx)
	if err != nil {
		s.logger.Error("Failed to query CREATE INDEX progress statistics", zap.Error(err))
		return []error{err}
	}

	// It's normal to have zero metrics (no CREATE INDEX operations running)
	if len(metrics) == 0 {
		s.logger.Debug("No CREATE INDEX operations currently running")
		return nil
	}

	for _, metric := range metrics {
		s.recordCreateIndexProgressMetrics(now, metric)
	}

	s.logger.Debug("CREATE INDEX progress statistics scrape completed",
		zap.Int("operation_count", len(metrics)))

	return nil
}

// recordCreateIndexProgressMetrics records all progress metrics for a single CREATE INDEX operation
func (s *TableScraper) recordCreateIndexProgressMetrics(now pcommon.Timestamp, metric models.PgStatProgressCreateIndex) {
	// Create composite identifier for table and index
	tableID := metric.SchemaName + "." + metric.TableName
	indexName := metric.IndexName.String

	// Record locker progress
	s.mb.RecordPostgresqlCreateIndexLockersTotalDataPoint(now, getInt64(metric.LockersTotal), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlCreateIndexLockersDoneDataPoint(now, getInt64(metric.LockersDone), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)

	// Record block progress
	s.mb.RecordPostgresqlCreateIndexBlocksTotalDataPoint(now, getInt64(metric.BlocksTotal), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlCreateIndexBlocksDoneDataPoint(now, getInt64(metric.BlocksDone), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)

	// Record tuple progress
	s.mb.RecordPostgresqlCreateIndexTuplesTotalDataPoint(now, getInt64(metric.TuplesTotal), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlCreateIndexTuplesDoneDataPoint(now, getInt64(metric.TuplesDone), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)

	// Record partition progress
	s.mb.RecordPostgresqlCreateIndexPartitionsTotalDataPoint(now, getInt64(metric.PartitionsTotal), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)
	s.mb.RecordPostgresqlCreateIndexPartitionsDoneDataPoint(now, getInt64(metric.PartitionsDone), metric.Database, indexName, s.instanceName, metric.SchemaName, metric.TableName)

	s.logger.Debug("Recorded CREATE INDEX progress metrics",
		zap.String("table", tableID),
		zap.String("index", indexName),
		zap.String("phase", metric.Phase.String))
}
