package scrapers

import (
	"context"
	"errors"
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
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
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
func (s *WaitEventBlockingScraper) ScrapeWaitEventsAndBlocking(ctx context.Context, slowQueryIdentifiers []models.SQLIdentifier) ([]models.SQLIdentifier, []error) {
	// Extract SQL IDs and create a map for metadata lookup
	sqlIDMap := make(map[string]models.SQLIdentifier)
	sqlIDs := make([]string, len(slowQueryIdentifiers))
	for i, identifier := range slowQueryIdentifiers {
		sqlIDs[i] = identifier.SQLID
		sqlIDMap[identifier.SQLID] = identifier
	}

	waitEvents, err := s.fetchWaitEvents(ctx, sqlIDs)
	if err != nil {
		return nil, []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	s.emitWaitEventMetrics(now, waitEvents, sqlIDMap)
	sqlIdentifiers := s.extractSQLIdentifiers(waitEvents, sqlIDMap)

	s.logger.Debug("Wait events and blocking scrape completed")

	return sqlIdentifiers, nil
}

// recordWaitEventMetrics records wait event metrics for a session (including blocking attributes if blocked)
func (s *WaitEventBlockingScraper) recordWaitEventMetrics(now pcommon.Timestamp, event *models.WaitEventWithBlocking, sqlIDMap map[string]models.SQLIdentifier) {
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

	// Get nr_apm_guid and normalised_sql_hash from sqlIDMap
	// These will be empty strings if not present in the map or if the metadata values were empty
	var nrServiceGuid, normalisedSQLHash string
	if metadata, exists := sqlIDMap[queryID]; exists {
		nrServiceGuid = metadata.NRServiceGuid
		normalisedSQLHash = metadata.NormalisedSQLHash
	}

	// Get blocking attributes (will be empty strings for non-blocked sessions)
	blockingSessionStatus := event.GetBlockingSessionStatus()
	immediateBlockerSID := commonutils.FormatInt64(event.GetImmediateBlockerSID())
	finalBlockingSessionStatus := event.GetFinalBlockingSessionStatus()
	finalBlockerUser := event.GetFinalBlockerUser()
	finalBlockerSID := commonutils.FormatInt64(event.GetFinalBlockerSID())
	finalBlockerSerial := commonutils.FormatInt64(event.GetFinalBlockerSerial())
	finalBlockerQueryID := event.GetFinalBlockerQueryID()
	finalBlockerQueryText := commonutils.NormalizeSql(event.GetFinalBlockerQueryText())

	// Extract metadata from final blocker query text (if blocked)
	var nrBlockingServiceGuid, normalisedBlockingSQLHash string
	rawFinalBlockerQueryText := event.GetFinalBlockerQueryText()
	if rawFinalBlockerQueryText != "" {
		nrBlockingServiceGuid = commonutils.ExtractNewRelicMetadata(rawFinalBlockerQueryText)
		_, normalisedBlockingSQLHash = commonutils.NormalizeSqlAndHash(rawFinalBlockerQueryText)
	}

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
		nrServiceGuid,
		normalisedSQLHash,
		blockingSessionStatus,
		immediateBlockerSID,
		finalBlockingSessionStatus,
		finalBlockerUser,
		finalBlockerSID,
		finalBlockerSerial,
		finalBlockerQueryID,
		finalBlockerQueryText,
		nrBlockingServiceGuid,
		normalisedBlockingSQLHash,
	)
}

// emitWaitEventMetrics emits metrics for wait events (including blocking attributes)
func (s *WaitEventBlockingScraper) emitWaitEventMetrics(
	now pcommon.Timestamp,
	waitEvents []models.WaitEventWithBlocking,
	sqlIDMap map[string]models.SQLIdentifier,
) int {
	waitEventMetricCount := 0

	for i := range waitEvents {
		event := &waitEvents[i]
		if event.IsValidForMetrics() {
			s.recordWaitEventMetrics(now, event, sqlIDMap)
			waitEventMetricCount++

			// Record final blocker query details if this is a blocked session with valid blocker info
			if event.IsBlocked() {
				s.recordFinalBlockerQueryDetails(now, event, sqlIDMap)
			}
		}
	}

	return waitEventMetricCount
}

// shouldIncludeIdentifier checks if an event has valid SQL identifier information
func (*WaitEventBlockingScraper) shouldIncludeIdentifier(event *models.WaitEventWithBlocking) bool {
	return event.HasValidQueryID() && event.SQLChildNumber.Valid
}

// extractSQLIdentifiers extracts unique SQL identifiers from wait events
func (s *WaitEventBlockingScraper) extractSQLIdentifiers(
	waitEvents []models.WaitEventWithBlocking,
	sqlIDMap map[string]models.SQLIdentifier,
) []models.SQLIdentifier {
	identifiersMap := make(map[string]models.SQLIdentifier)

	for i := range waitEvents {
		event := &waitEvents[i]
		if !event.IsValidForMetrics() || !s.shouldIncludeIdentifier(event) {
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

			// Get metadata from slow queries if available
			// These will be empty strings if not present
			var nrServiceGuid, normalisedSQLHash string
			if metadata, exists := sqlIDMap[sqlID]; exists {
				nrServiceGuid = metadata.NRServiceGuid
				normalisedSQLHash = metadata.NormalisedSQLHash
			}

			identifiersMap[key] = models.SQLIdentifier{
				SQLID:             sqlID,
				ChildNumber:       childNumber,
				Timestamp:         timestamp,
				NRServiceGuid:     nrServiceGuid,
				NormalisedSQLHash: normalisedSQLHash,
			}
		}
	}

	identifiers := make([]models.SQLIdentifier, 0, len(identifiersMap))
	for _, identifier := range identifiersMap {
		identifiers = append(identifiers, identifier)
	}

	return identifiers
}

// recordFinalBlockerQueryDetails records query details for the final blocking session
func (s *WaitEventBlockingScraper) recordFinalBlockerQueryDetails(now pcommon.Timestamp, event *models.WaitEventWithBlocking, sqlIDMap map[string]models.SQLIdentifier) {
	finalBlockerQueryID := event.GetFinalBlockerQueryID()
	rawFinalBlockerQueryText := event.GetFinalBlockerQueryText()
	finalBlockerQueryText := commonutils.NormalizeSql(rawFinalBlockerQueryText)

	// Only record if we have valid query ID and text
	if finalBlockerQueryID == "" || finalBlockerQueryText == "" {
		return
	}

	collectionTimestamp := commonutils.FormatTimestamp(event.GetCollectionTimestamp())
	dbName := event.GetDatabaseName()
	queryID := event.GetQueryID()

	// Get nrServiceGuid and normalised_sql_hash from sqlIDMap for the blocked query
	var nrServiceGuid, normalisedSQLHash string
	if metadata, exists := sqlIDMap[queryID]; exists {
		nrServiceGuid = metadata.NRServiceGuid
		normalisedSQLHash = metadata.NormalisedSQLHash
	}

	// Extract metadata from final blocker query text
	var nrBlockingServiceGuid, normalisedBlockingSQLHash string
	if rawFinalBlockerQueryText != "" {
		nrBlockingServiceGuid = commonutils.ExtractNewRelicMetadata(rawFinalBlockerQueryText)
		_, normalisedBlockingSQLHash = commonutils.NormalizeSqlAndHash(rawFinalBlockerQueryText)
	}

	s.mb.RecordNewrelicoracledbSlowQueriesQueryDetailsDataPoint(
		now,
		1,
		"OracleQueryDetails",
		collectionTimestamp,
		dbName,
		finalBlockerQueryID,
		finalBlockerQueryText,
		"",                        // schema_name
		"",                        // user_name
		"",                        // last_active_time
		normalisedSQLHash,         // normalised_sql_hash
		nrServiceGuid,             // nr_service_guid
		normalisedBlockingSQLHash, // normalised_blocking_sql_hash (same - this IS the blocking query)
		nrBlockingServiceGuid,     // nr_blocking_service_guid (same - this IS the blocking query)
	)
}
