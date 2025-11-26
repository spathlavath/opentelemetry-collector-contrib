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

		cdbName := slowQuery.GetCDBName()
		dbName := slowQuery.GetDatabaseName()
		qID := slowQuery.GetQueryID()
		qText := commonutils.AnonymizeAndNormalize(slowQuery.GetQueryText())
		userName := slowQuery.GetUserName()
		schName := slowQuery.GetSchemaName()
		lastActiveTime := slowQuery.GetLastActiveTime()

		s.recordMetrics(now, &slowQuery, cdbName, dbName, qID, qText, userName, schName, lastActiveTime)

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

func (s *SlowQueriesScraper) recordMetrics(now pcommon.Timestamp, slowQuery *models.SlowQuery, cdbName, dbName, qID, qText, userName, schName, lastActiveTime string) {
	if slowQuery.ExecutionCount.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesExecutionCountDataPoint(
			now,
			float64(slowQuery.ExecutionCount.Int64),
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

	if slowQuery.AvgRowsExamined.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesAvgRowsExaminedDataPoint(
			now,
			slowQuery.AvgRowsExamined.Float64,
			dbName,
			qID,
			userName,
		)
	}

	if slowQuery.AvgLockTimeMs.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesAvgLockTimeDataPoint(
			now,
			slowQuery.AvgLockTimeMs.Float64,
			dbName,
			qID,
			userName,
		)
	}

	// Only query_details metric includes cdb_name
	s.mb.RecordNewrelicoracledbSlowQueriesQueryDetailsDataPoint(
		now,
		1,
		cdbName,
		dbName,
		qID,
		qText,
		schName,
		userName,
		lastActiveTime,
	)
}

// GetSlowQueryIDs returns only the query IDs without emitting metrics
// This is used by the logs scraper to get query IDs for execution plans
// without duplicating the slow query metrics already emitted by the metrics scraper
func (s *SlowQueriesScraper) GetSlowQueryIDs(ctx context.Context) ([]string, []error) {
	var queryIDs []string

	slowQueries, err := s.client.QuerySlowQueries(ctx, s.queryMonitoringResponseTimeThreshold, s.queryMonitoringCountThreshold)
	if err != nil {
		return nil, []error{err}
	}

	for _, slowQuery := range slowQueries {
		if !slowQuery.IsValidForMetrics() {
			continue
		}

		if slowQuery.QueryID.Valid {
			queryIDs = append(queryIDs, slowQuery.QueryID.String)
		}
	}

	s.logger.Debug("Fetched slow query IDs (no metrics emitted)",
		zap.Int("rows", len(slowQueries)),
		zap.Int("query_ids", len(queryIDs)))

	return queryIDs, nil
}
