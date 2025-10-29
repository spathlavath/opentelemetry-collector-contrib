package scrapers

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

type WaitEventsScraper struct {
	client                        client.OracleClient
	mb                            *metadata.MetricsBuilder
	logger                        *zap.Logger
	instanceName                  string
	metricsBuilderConfig          metadata.MetricsBuilderConfig
	queryMonitoringCountThreshold int
}

func NewWaitEventsScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig, countThreshold int) *WaitEventsScraper {
	return &WaitEventsScraper{
		client:                        oracleClient,
		mb:                            mb,
		logger:                        logger,
		instanceName:                  instanceName,
		metricsBuilderConfig:          metricsBuilderConfig,
		queryMonitoringCountThreshold: countThreshold,
	}
}

func (s *WaitEventsScraper) ScrapeWaitEvents(ctx context.Context) []error {
	var scrapeErrors []error

	waitEvents, err := s.client.QueryWaitEvents(ctx, s.queryMonitoringCountThreshold)
	if err != nil {
		return []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	for _, waitEvent := range waitEvents {
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

	s.logger.Debug("Wait events scrape completed",
		zap.Int("events", len(waitEvents)),
		zap.Int("metrics", metricCount),
		zap.Int("errors", len(scrapeErrors)))

	return scrapeErrors
}
