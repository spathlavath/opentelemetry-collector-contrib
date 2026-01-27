// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// Helper functions to extract values from sql.Null types
func getInt64(val sql.NullInt64) int64 {
	if val.Valid {
		return val.Int64
	}
	return 0
}

func getFloat64(val sql.NullFloat64) float64 {
	if val.Valid {
		return val.Float64
	}
	return 0.0
}

func getBool(val sql.NullBool) int64 {
	if val.Valid && val.Bool {
		return 1
	}
	return 0
}

func getString(val sql.NullString) string {
	if val.Valid {
		return val.String
	}
	return ""
}

// DatabaseMetricsScraper scrapes database-level metrics from pg_stat_database
type DatabaseMetricsScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
	supportsPG12 bool // Whether PostgreSQL version is 12 or higher
}

// NewDatabaseMetricsScraper creates a new DatabaseMetricsScraper
func NewDatabaseMetricsScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
	supportsPG12 bool,
) *DatabaseMetricsScraper {
	return &DatabaseMetricsScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
		supportsPG12: supportsPG12,
	}
}

// ScrapeDatabaseMetrics scrapes all database-level metrics from pg_stat_database
func (s *DatabaseMetricsScraper) ScrapeDatabaseMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryDatabaseMetrics(ctx, s.supportsPG12)
	if err != nil {
		s.logger.Error("Failed to query database metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordMetricsForDatabase(now, metric)
	}

	s.logger.Debug("Database metrics scrape completed",
		zap.Int("databases", len(metrics)))

	return nil
}

// recordMetricsForDatabase records all metrics for a single database
func (s *DatabaseMetricsScraper) recordMetricsForDatabase(now pcommon.Timestamp, metric models.PgStatDatabaseMetric) {
	databaseName := metric.DatName

	// Record all metrics using helper functions to extract values
	s.mb.RecordPostgresqlConnectionsDataPoint(now, getInt64(metric.NumBackends), s.instanceName, databaseName)
	s.mb.RecordPostgresqlCommitsDataPoint(now, getInt64(metric.XactCommit), s.instanceName, databaseName)
	s.mb.RecordPostgresqlRollbacksDataPoint(now, getInt64(metric.XactRollback), s.instanceName, databaseName)
	s.mb.RecordPostgresqlDiskReadDataPoint(now, getInt64(metric.BlksRead), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBufferHitDataPoint(now, getInt64(metric.BlksHit), s.instanceName, databaseName)
	s.mb.RecordPostgresqlRowsReturnedDataPoint(now, getInt64(metric.TupReturned), s.instanceName, databaseName)
	s.mb.RecordPostgresqlRowsFetchedDataPoint(now, getInt64(metric.TupFetched), s.instanceName, databaseName)
	s.mb.RecordPostgresqlRowsInsertedDataPoint(now, getInt64(metric.TupInserted), s.instanceName, databaseName)
	s.mb.RecordPostgresqlRowsUpdatedDataPoint(now, getInt64(metric.TupUpdated), s.instanceName, databaseName)
	s.mb.RecordPostgresqlRowsDeletedDataPoint(now, getInt64(metric.TupDeleted), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsDataPoint(now, getInt64(metric.Conflicts), s.instanceName, databaseName)
	s.mb.RecordPostgresqlDeadlocksDataPoint(now, getInt64(metric.Deadlocks), s.instanceName, databaseName)
	s.mb.RecordPostgresqlTempFilesDataPoint(now, getInt64(metric.TempFiles), s.instanceName, databaseName)
	s.mb.RecordPostgresqlTempBytesDataPoint(now, getInt64(metric.TempBytes), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBlkReadTimeDataPoint(now, getFloat64(metric.BlkReadTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBlkWriteTimeDataPoint(now, getFloat64(metric.BlkWriteTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBeforeXidWraparoundDataPoint(now, getInt64(metric.BeforeXIDWraparound), s.instanceName, databaseName)
	s.mb.RecordPostgresqlDatabaseSizeDataPoint(now, getInt64(metric.DatabaseSize), s.instanceName, databaseName)

	// Record checksum metrics if PostgreSQL 12+
	if s.supportsPG12 {
		s.mb.RecordPostgresqlChecksumsFailuresDataPoint(now, getInt64(metric.ChecksumFailures), s.instanceName, databaseName)
		s.mb.RecordPostgresqlChecksumsEnabledDataPoint(now, getBool(metric.ChecksumsEnabled), s.instanceName, databaseName)
	}
}

// ScrapeServerUptime scrapes the PostgreSQL server uptime metric
func (s *DatabaseMetricsScraper) ScrapeServerUptime(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryServerUptime(ctx)
	if err != nil {
		s.logger.Error("Failed to query server uptime", zap.Error(err))
		return []error{err}
	}

	// Record server uptime with instance name
	s.mb.RecordPostgresqlUptimeDataPoint(now, getFloat64(metric.Uptime), s.instanceName)

	s.logger.Debug("Server uptime scrape completed")

	return nil
}

// ScrapeDatabaseCount scrapes the database count metric
func (s *DatabaseMetricsScraper) ScrapeDatabaseCount(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryDatabaseCount(ctx)
	if err != nil {
		s.logger.Error("Failed to query database count", zap.Error(err))
		return []error{err}
	}

	// Record database count with instance name
	s.mb.RecordPostgresqlDbCountDataPoint(now, getInt64(metric.DatabaseCount), s.instanceName)

	s.logger.Debug("Database count scrape completed")

	return nil
}

// ScrapeRunningStatus scrapes the PostgreSQL server running status (health check)
func (s *DatabaseMetricsScraper) ScrapeRunningStatus(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metric, err := s.client.QueryRunningStatus(ctx)
	if err != nil {
		s.logger.Error("Failed to query running status", zap.Error(err))
		return []error{err}
	}

	// Record running status with instance name
	s.mb.RecordPostgresqlRunningDataPoint(now, getInt64(metric.Running), s.instanceName)

	s.logger.Debug("Running status scrape completed")

	return nil
}

// ScrapeSessionMetrics scrapes session-level metrics from pg_stat_database (PostgreSQL 14+)
func (s *DatabaseMetricsScraper) ScrapeSessionMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QuerySessionMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query session metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordSessionMetricsForDatabase(now, metric)
	}

	s.logger.Debug("Session metrics scrape completed",
		zap.Int("databases", len(metrics)))

	return nil
}

// recordSessionMetricsForDatabase records all session metrics for a single database
func (s *DatabaseMetricsScraper) recordSessionMetricsForDatabase(now pcommon.Timestamp, metric models.PgStatDatabaseSessionMetric) {
	databaseName := metric.DatName

	// Record session metrics using helper functions to extract values
	s.mb.RecordPostgresqlSessionsSessionTimeDataPoint(now, getFloat64(metric.SessionTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsActiveTimeDataPoint(now, getFloat64(metric.ActiveTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsIdleInTransactionTimeDataPoint(now, getFloat64(metric.IdleInTransactionTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsCountDataPoint(now, getInt64(metric.SessionCount), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsAbandonedDataPoint(now, getInt64(metric.SessionsAbandoned), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsFatalDataPoint(now, getInt64(metric.SessionsFatal), s.instanceName, databaseName)
	s.mb.RecordPostgresqlSessionsKilledDataPoint(now, getInt64(metric.SessionsKilled), s.instanceName, databaseName)
}

// ScrapeConflictMetrics scrapes replication conflict metrics from pg_stat_database_conflicts
func (s *DatabaseMetricsScraper) ScrapeConflictMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryConflictMetrics(ctx)
	if err != nil {
		s.logger.Error("Failed to query conflict metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range metrics {
		s.recordConflictMetricsForDatabase(now, metric)
	}

	s.logger.Debug("Conflict metrics scrape completed",
		zap.Int("databases", len(metrics)))

	return nil
}

// recordConflictMetricsForDatabase records all conflict metrics for a single database
func (s *DatabaseMetricsScraper) recordConflictMetricsForDatabase(now pcommon.Timestamp, metric models.PgStatDatabaseConflictsMetric) {
	databaseName := metric.DatName

	// Record replication conflict metrics using helper functions to extract values
	s.mb.RecordPostgresqlConflictsBufferpinDataPoint(now, getInt64(metric.ConflBufferpin), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsDeadlockDataPoint(now, getInt64(metric.ConflDeadlock), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsLockDataPoint(now, getInt64(metric.ConflLock), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsSnapshotDataPoint(now, getInt64(metric.ConflSnapshot), s.instanceName, databaseName)
	s.mb.RecordPostgresqlConflictsTablespaceDataPoint(now, getInt64(metric.ConflTablespace), s.instanceName, databaseName)
}
