package scrapers

import (
	"context"
	"database/sql"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

type WaitEventsScraper struct {
	db                            *sql.DB
	mb                            *metadata.MetricsBuilder
	logger                        *zap.Logger
	instanceName                  string
	metricsBuilderConfig          metadata.MetricsBuilderConfig
	queryMonitoringCountThreshold int
}

func NewWaitEventsScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig, countThreshold int) *WaitEventsScraper {
	return &WaitEventsScraper{
		db:                            db,
		mb:                            mb,
		logger:                        logger,
		instanceName:                  instanceName,
		metricsBuilderConfig:          metricsBuilderConfig,
		queryMonitoringCountThreshold: countThreshold,
	}
}

func (s *WaitEventsScraper) ScrapeWaitEvents(ctx context.Context) []error {
	var scrapeErrors []error

	waitEventQueriesSQL := queries.GetWaitEventQueriesSQL(s.queryMonitoringCountThreshold)
	rows, err := s.db.QueryContext(ctx, waitEventQueriesSQL)
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	for rows.Next() {
		var waitEvent models.WaitEvent

		if err := rows.Scan(
			&waitEvent.DatabaseName,
			&waitEvent.QueryID,
			&waitEvent.WaitCategory,
			&waitEvent.WaitEventName,
			&waitEvent.CollectionTimestamp,
			&waitEvent.WaitingTasksCount,
			&waitEvent.TotalWaitTimeMs,
			&waitEvent.AvgWaitTimeMs,
		); err != nil {
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if !waitEvent.IsValidForMetrics() {
			continue
		}

		dbName := waitEvent.GetDatabaseName()
		qID := waitEvent.GetQueryID()
		waitCat := waitEvent.GetWaitCategory()
		waitEventName := waitEvent.GetWaitEventName()

		if waitEvent.HasValidWaitingTasksCount() {
			s.mb.RecordNewrelicoracledbWaitEventsWaitingTasksCountDataPoint(
				now,
				float64(waitEvent.GetWaitingTasksCount()),
				dbName,
				qID,
				waitEventName,
				waitCat,
			)
			metricCount++
		}

		s.mb.RecordNewrelicoracledbWaitEventsTotalWaitTimeMsDataPoint(
			now,
			waitEvent.GetTotalWaitTimeMs(),
			dbName,
			qID,
			waitEventName,
			waitCat,
		)
		metricCount++

		if waitEvent.HasValidAvgWaitTime() {
			s.mb.RecordNewrelicoracledbWaitEventsAvgWaitTimeMsDataPoint(
				now,
				waitEvent.GetAvgWaitTimeMs(),
				dbName,
				qID,
				waitEventName,
				waitCat,
			)
			metricCount++
		}
	}

	if err := rows.Err(); err != nil {
		scrapeErrors = append(scrapeErrors, err)
	}

	s.logger.Debug("Wait events scrape completed",
		zap.Int("metrics", metricCount),
		zap.Int("errors", len(scrapeErrors)))

	return scrapeErrors
}
