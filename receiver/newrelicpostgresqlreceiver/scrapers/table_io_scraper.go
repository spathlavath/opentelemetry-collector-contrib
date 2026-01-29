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
