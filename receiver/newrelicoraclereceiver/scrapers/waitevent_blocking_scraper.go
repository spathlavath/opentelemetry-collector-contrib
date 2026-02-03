package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// WaitEventBlockingScraper collects both Oracle wait events and blocking query metrics
type WaitEventBlockingScraper struct {
	client                        client.OracleClient
	mb                            *metadata.MetricsBuilder
	logger                        *zap.Logger
	metricsBuilderConfig          metadata.MetricsBuilderConfig
	queryMonitoringCountThreshold int
}

// NewWaitEventBlockingScraper creates a new combined Wait Events and Blocking Scraper instance
func NewWaitEventBlockingScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig, countThreshold int) (*WaitEventBlockingScraper, error) {
	if oracleClient == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if mb == nil {
		return nil, fmt.Errorf("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &WaitEventBlockingScraper{
		client:                        oracleClient,
		mb:                            mb,
		logger:                        logger,
		metricsBuilderConfig:          metricsBuilderConfig,
		queryMonitoringCountThreshold: countThreshold,
	}, nil
}

// fetchWaitEvents retrieves wait events with blocking information from the database
func (s *WaitEventBlockingScraper) fetchWaitEvents(ctx context.Context, slowQuerySQLIDs []string) ([]models.WaitEventWithBlocking, error) {
	waitEvents, err := s.client.QueryWaitEventsWithBlocking(ctx, s.queryMonitoringCountThreshold, slowQuerySQLIDs)
	if err != nil {
		s.logger.Error("Failed to query wait events", zap.Error(err))
		return nil, err
	}
	return waitEvents, nil
}

// ScrapeWaitEventsAndBlocking collects both wait events and blocking query metrics in a single query
func (s *WaitEventBlockingScraper) ScrapeWaitEventsAndBlocking(ctx context.Context, slowQuerySQLIDs []string) ([]models.SQLIdentifier, []error) {
	waitEvents, err := s.fetchWaitEvents(ctx, slowQuerySQLIDs)
	if err != nil {
		return nil, []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	s.emitWaitEventMetrics(now, waitEvents)
	sqlIdentifiers := s.extractSQLIdentifiers(waitEvents)

	s.logger.Debug("Wait events and blocking scrape completed")

	return sqlIdentifiers, nil
}

// recordWaitEventMetrics records wait event metrics for a session
func (s *WaitEventBlockingScraper) recordWaitEventMetrics(now pcommon.Timestamp, event *models.WaitEventWithBlocking) {
	if !event.HasValidCurrentWaitSeconds() {
		return
	}

	collectionTimestamp := commonutils.FormatTimestamp(event.GetCollectionTimestamp())
	dbName := event.GetDatabaseName()
	username := event.GetUsername()
	sid := commonutils.FormatInt64(event.GetSID())
	serial := event.GetSerial()
	status := event.GetStatus()
	state := event.GetState()
	queryID := event.GetQueryID()
	sqlChildNumber := event.GetSQLChildNumber()
	waitCategory := event.GetWaitCategory()
	waitEventName := event.GetWaitEventName()
	program := event.GetProgram()
	machine := event.GetMachine()
	waitObjectOwner := event.GetObjectOwner()
	waitObjectName := event.GetObjectNameWaitedOn()
	waitObjectType := event.GetObjectTypeWaitedOn()
	sqlExecStart := commonutils.FormatTimestamp(event.GetSQLExecStart())
	sqlExecID := event.GetSQLExecID()
	rowWaitObjID := commonutils.FormatInt64(event.GetLockedObjectID())
	rowWaitFileID := commonutils.FormatInt64(event.GetLockedFileID())
	rowWaitBlockID := commonutils.FormatInt64(event.GetLockedBlockID())

	s.mb.RecordNewrelicoracledbWaitEventsCurrentWaitTimeMsDataPoint(
		now,
		event.GetCurrentWaitMs(),
		collectionTimestamp,
		dbName,
		username,
		sid,
		serial,
		status,
		state,
		queryID,
		sqlChildNumber,
		waitEventName,
		waitCategory,
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
	)
}

// GetSQLIdentifiers retrieves unique SQL identifiers from wait events without emitting metrics
func (s *WaitEventBlockingScraper) GetSQLIdentifiers(ctx context.Context, slowQuerySQLIDs []string) ([]models.SQLIdentifier, []error) {
	waitEvents, err := s.fetchWaitEvents(ctx, slowQuerySQLIDs)
	if err != nil {
		return nil, []error{err}
	}

	sqlIdentifiers := s.extractSQLIdentifiers(waitEvents)

	s.logger.Debug("SQL identifiers collected without emitting metrics")

	return sqlIdentifiers, nil
}

// emitWaitEventMetrics emits metrics for wait events and blocking queries
func (s *WaitEventBlockingScraper) emitWaitEventMetrics(
	now pcommon.Timestamp,
	waitEvents []models.WaitEventWithBlocking,
) (int, int) {
	waitEventMetricCount := 0
	blockingMetricCount := 0

	for _, event := range waitEvents {
		if event.IsValidForMetrics() {
			s.recordWaitEventMetrics(now, &event)
			waitEventMetricCount++
		}

		if event.IsBlocked() {
			s.recordBlockingMetrics(now, &event)
			blockingMetricCount++
		}
	}

	return waitEventMetricCount, blockingMetricCount
}

// shouldIncludeIdentifier checks if an event has valid SQL identifier information
func (s *WaitEventBlockingScraper) shouldIncludeIdentifier(event *models.WaitEventWithBlocking) bool {
	return event.HasValidQueryID() && event.SQLChildNumber.Valid
}

// extractSQLIdentifiers extracts unique SQL identifiers from wait events
func (s *WaitEventBlockingScraper) extractSQLIdentifiers(
	waitEvents []models.WaitEventWithBlocking,
) []models.SQLIdentifier {
	identifiersMap := make(map[string]models.SQLIdentifier)

	for _, event := range waitEvents {
		if !event.IsValidForMetrics() || !s.shouldIncludeIdentifier(&event) {
			continue
		}

		sqlID := event.GetQueryID()
		childNumber := event.GetSQLChildNumber()
		key := commonutils.GenerateSQLIdentifierKey(sqlID, childNumber)

		if _, exists := identifiersMap[key]; !exists {
			timestamp := event.GetCollectionTimestamp()
			if timestamp.IsZero() {
				timestamp = time.Now()
			}

			identifiersMap[key] = models.SQLIdentifier{
				SQLID:       sqlID,
				ChildNumber: childNumber,
				Timestamp:   timestamp,
			}
		}
	}

	identifiers := make([]models.SQLIdentifier, 0, len(identifiersMap))
	for _, identifier := range identifiersMap {
		identifiers = append(identifiers, identifier)
	}

	return identifiers
}

// recordBlockingMetrics records blocking query metrics when a session is blocked
func (s *WaitEventBlockingScraper) recordBlockingMetrics(now pcommon.Timestamp, event *models.WaitEventWithBlocking) {
	blockedWaitMs := event.GetCurrentWaitMs()
	if blockedWaitMs <= 0 {
		return
	}

	collectionTimestamp := commonutils.FormatTimestamp(event.GetCollectionTimestamp())
	dbName := event.GetDatabaseName()
	blockedUser := event.GetUsername()
	queryID := event.GetQueryID()
	sessionID := commonutils.FormatInt64(event.GetSID())
	blockedSerial := event.GetSerial()
	state := event.GetState()
	sqlChildNumber := event.GetSQLChildNumber()
	sqlExecID := event.GetSQLExecID()
	sqlExecStart := commonutils.FormatTimestamp(event.GetSQLExecStart())
	waitEventName := event.GetWaitEventName()
	waitCategory := event.GetWaitCategory()
	waitObjectName := event.GetObjectNameWaitedOn()
	waitObjectOwner := event.GetObjectOwner()
	waitObjectType := event.GetObjectTypeWaitedOn()
	blockingSessionStatus := event.GetBlockingSessionStatus()
	immediateBlockerSID := commonutils.FormatInt64(event.GetImmediateBlockerSID())
	finalBlockingSessionStatus := event.GetFinalBlockingSessionStatus()
	finalBlockerSID := commonutils.FormatInt64(event.GetFinalBlockerSID())
	finalBlockerSerial := commonutils.FormatInt64(event.GetFinalBlockerSerial())
	finalBlockerUser := event.GetFinalBlockerUser()
	finalBlockerQueryID := event.GetFinalBlockerQueryID()

	s.mb.RecordNewrelicoracledbBlockingQueriesWaitTimeMsDataPoint(
		now,
		blockedWaitMs,
		collectionTimestamp,
		dbName,
		blockedUser,
		sessionID,
		blockedSerial,
		state,
		queryID,
		sqlChildNumber,
		sqlExecID,
		sqlExecStart,
		waitEventName,
		waitCategory,
		waitObjectName,
		waitObjectOwner,
		waitObjectType,
		blockingSessionStatus,
		immediateBlockerSID,
		finalBlockingSessionStatus,
		finalBlockerUser,
		finalBlockerSID,
		finalBlockerSerial,
		finalBlockerQueryID,
	)

	// Record the final blocker query details if we have a valid query ID and text
	finalBlockerQueryText := commonutils.AnonymizeAndNormalize(event.GetFinalBlockerQueryText())
	if finalBlockerQueryID != "" && finalBlockerQueryText != "" {
		s.mb.RecordNewrelicoracledbSlowQueriesQueryDetailsDataPoint(
			now,
			1,
			"OracleQueryDetails",
			collectionTimestamp,
			dbName,
			finalBlockerQueryID,
			finalBlockerQueryText,
			"", // schema_name not available in blocking event
			finalBlockerUser,
			sqlExecStart, // using blocked query's execution start as approximate last active time
		)
	}
}
