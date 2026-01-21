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

// DatabaseMetricsScraper scrapes database-level metrics from pg_stat_database
type DatabaseMetricsScraper struct {
	client       client.PostgreSQLClient
	mb           *metadata.MetricsBuilder
	logger       *zap.Logger
	instanceName string
	mbConfig     metadata.MetricsBuilderConfig
}

// NewDatabaseMetricsScraper creates a new DatabaseMetricsScraper
func NewDatabaseMetricsScraper(
	client client.PostgreSQLClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	instanceName string,
	mbConfig metadata.MetricsBuilderConfig,
) *DatabaseMetricsScraper {
	return &DatabaseMetricsScraper{
		client:       client,
		mb:           mb,
		logger:       logger,
		instanceName: instanceName,
		mbConfig:     mbConfig,
	}
}

// ScrapeDatabaseMetrics scrapes all database-level metrics from pg_stat_database
func (s *DatabaseMetricsScraper) ScrapeDatabaseMetrics(ctx context.Context) []error {
	now := pcommon.NewTimestampFromTime(time.Now())

	metrics, err := s.client.QueryDatabaseMetrics(ctx)
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
	s.mb.RecordPostgresqlDeadlocksDataPoint(now, getInt64(metric.Deadlocks), s.instanceName, databaseName)
	s.mb.RecordPostgresqlTempFilesDataPoint(now, getInt64(metric.TempFiles), s.instanceName, databaseName)
	s.mb.RecordPostgresqlTempBytesDataPoint(now, getInt64(metric.TempBytes), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBlkReadTimeDataPoint(now, getFloat64(metric.BlkReadTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBlkWriteTimeDataPoint(now, getFloat64(metric.BlkWriteTime), s.instanceName, databaseName)
	s.mb.RecordPostgresqlBeforeXidWraparoundDataPoint(now, getInt64(metric.BeforeXIDWraparound), s.instanceName, databaseName)
	s.mb.RecordPostgresqlDatabaseSizeDataPoint(now, getInt64(metric.DatabaseSize), s.instanceName, databaseName)
}
