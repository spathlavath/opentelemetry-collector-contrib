package scrapers

import (
	"context"
	"strconv"
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

		username := waitEvent.GetUsername()
		sid := strconv.FormatInt(waitEvent.GetSID(), 10)
		status := waitEvent.GetStatus()
		qID := waitEvent.GetQueryID()
		waitCat := waitEvent.GetWaitCategory()
		waitEventName := waitEvent.GetWaitEventName()
		program := waitEvent.GetProgram()
		machine := waitEvent.GetMachine()
		waitObjectOwner := waitEvent.GetObjectOwner()
		waitObjectName := waitEvent.GetObjectNameWaitedOn()
		waitObjectType := waitEvent.GetObjectTypeWaitedOn()
		sqlExecStart := waitEvent.GetSQLExecStart().Format("2006-01-02 15:04:05")
		sqlExecID := waitEvent.GetSQLExecID()
		rowWaitObjID := strconv.FormatInt(waitEvent.GetLockedObjectID(), 10)
		rowWaitFileID := strconv.FormatInt(waitEvent.GetLockedFileID(), 10)
		rowWaitBlockID := strconv.FormatInt(waitEvent.GetLockedBlockID(), 10)
		p1Text := waitEvent.GetP1Text()
		p1 := strconv.FormatInt(waitEvent.GetP1(), 10)
		p2Text := waitEvent.GetP2Text()
		p2 := strconv.FormatInt(waitEvent.GetP2(), 10)
		p3Text := waitEvent.GetP3Text()
		p3 := strconv.FormatInt(waitEvent.GetP3(), 10)

		if waitEvent.HasValidCurrentWaitSeconds() {
			collectionTimestamp := waitEvent.GetCollectionTimestamp().Format("2006-01-02 15:04:05")
			
			// Record current_wait_seconds metric
			s.mb.RecordNewrelicoracledbWaitEventsCurrentWaitSecondsDataPoint(
				now,
				float64(waitEvent.GetCurrentWaitSeconds()),
				collectionTimestamp,
				username,
				sid,
				status,
				qID,
				waitEventName,
				waitCat,
				program,
				machine,
				waitObjectOwner,
				waitObjectName,
				waitObjectType,
				sqlExecStart,
				sqlExecID,
				rowWaitObjID,
				rowWaitFileID,
				rowWaitBlockID,
				p1Text,
				p1,
				p2Text,
				p2,
				p3Text,
				p3,
			)
			
		// Record time_remaining metric with reduced attributes to avoid high cardinality
		if s.metricsBuilderConfig.Metrics.NewrelicoracledbWaitEventsTimeRemaining.Enabled {
			s.mb.RecordNewrelicoracledbWaitEventsTimeRemainingDataPoint(
				now,
				waitEvent.GetTimeRemainingSeconds(),
				collectionTimestamp,
				sid,
				qID,
				sqlExecID,
				sqlExecStart,
			)
		}
		
		metricCount++
		}
	}

	s.logger.Debug("Wait events scrape completed",
		zap.Int("events", len(waitEvents)),
		zap.Int("metrics", metricCount),
		zap.Int("errors", len(scrapeErrors)))

	return scrapeErrors
}
