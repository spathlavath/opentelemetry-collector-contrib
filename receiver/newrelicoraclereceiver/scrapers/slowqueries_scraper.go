package scrapers

import (
	"context"
	"time"

	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// SlowQueriesScraper contains the scraper for slow queries metrics
type SlowQueriesScraper struct {
	client                               client.OracleClient
	mb                                   *metadata.MetricsBuilder
	logger                               *zap.Logger
	instanceName                         string
	metricsBuilderConfig                 metadata.MetricsBuilderConfig
	queryMonitoringResponseTimeThreshold int
	queryMonitoringCountThreshold        int
}

func NewSlowQueriesScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig, responseTimeThreshold, countThreshold int) *SlowQueriesScraper {
	return &SlowQueriesScraper{
		client:                               oracleClient,
		mb:                                   mb,
		logger:                               logger,
		instanceName:                         instanceName,
		metricsBuilderConfig:                 metricsBuilderConfig,
		queryMonitoringResponseTimeThreshold: responseTimeThreshold,
		queryMonitoringCountThreshold:        countThreshold,
	}
}

func (s *SlowQueriesScraper) ScrapeSlowQueries(ctx context.Context) ([]string, []error) {
	var scrapeErrors []error
	var queryIDs []string

	slowQueries, err := s.client.QuerySlowQueries(ctx, s.queryMonitoringResponseTimeThreshold, s.queryMonitoringCountThreshold)
	if err != nil {
		return nil, []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, slowQuery := range slowQueries {
		if !slowQuery.IsValidForMetrics() {
			continue
		}

		dbName := slowQuery.GetDatabaseName()
		qID := slowQuery.GetQueryID()
		qText := commonutils.AnonymizeAndNormalize(slowQuery.GetQueryText())
		userName := slowQuery.GetUserName()
		schName := slowQuery.GetSchemaName()
		stmtType := slowQuery.GetStatementType()

		s.recordMetrics(now, &slowQuery, dbName, qID, qText, userName, schName, stmtType)

		if slowQuery.QueryID.Valid {
			queryIDs = append(queryIDs, slowQuery.QueryID.String)
		}
	}

	s.logger.Debug("Slow queries scrape completed",
		zap.Int("rows", len(slowQueries)),
		zap.Int("query_ids", len(queryIDs)),
		zap.Int("errors", len(scrapeErrors)))

	return queryIDs, scrapeErrors
}

func (s *SlowQueriesScraper) recordMetrics(now pcommon.Timestamp, slowQuery *models.SlowQuery, dbName, qID, qText, userName, schName, stmtType string) {
	if slowQuery.ExecutionCount.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesExecutionCountDataPoint(
			now,
			float64(slowQuery.ExecutionCount.Int64),
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.RowsProcessed.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesRowsProcessedDataPoint(
			now,
			slowQuery.RowsProcessed.Int64,
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.AvgCPUTimeMs.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesAvgCPUTimeDataPoint(
			now,
			slowQuery.AvgCPUTimeMs.Float64,
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.AvgDiskReads.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesAvgDiskReadsDataPoint(
			now,
			slowQuery.AvgDiskReads.Float64,
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.AvgDiskWrites.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesAvgDiskWritesDataPoint(
			now,
			slowQuery.AvgDiskWrites.Float64,
			dbName,
			qID,
			userName,
		)
	}

	s.mb.RecordNewrelicoracledbSlowQueriesAvgElapsedTimeDataPoint(
		now,
		slowQuery.AvgElapsedTimeMs.Float64,
		dbName,
		qID,
		userName,
	)

	if slowQuery.SharableMemoryBytes.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesSharableMemoryDataPoint(
			now,
			slowQuery.SharableMemoryBytes.Int64,
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.PersistentMemoryBytes.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesPersistentMemoryDataPoint(
			now,
			slowQuery.PersistentMemoryBytes.Int64,
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.RuntimeMemoryBytes.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesRuntimeMemoryDataPoint(
			now,
			slowQuery.RuntimeMemoryBytes.Int64,
			dbName,
			qID,
			userName,
		)
	}

	s.mb.RecordNewrelicoracledbSlowQueriesQueryDetailsDataPoint(
		now,
		1,
		dbName,
		qID,
		qText,
		schName,
		stmtType,
		userName,
	)
}
