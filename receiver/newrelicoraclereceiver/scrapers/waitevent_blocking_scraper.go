// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"errors"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
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

// fetchWaitEvents retrieves all active non-idle sessions in a single DB call.
// slowQueryIDs are the sql_ids from Phase 1; when non-empty the DB query prioritizes those
// sessions so they are never cut off by the FETCH FIRST row limit.
// Metadata (nrServiceGUID, normalisedSQLHash) is attached in Go: sessions whose sql_id
// matches a Phase 1 slow query use the pre-computed values from sqlIDMap; the rest are
// derived from the session's own query text.
func (s *WaitEventBlockingScraper) fetchWaitEvents(ctx context.Context, slowQueryIDs []string) ([]models.WaitEventWithBlocking, error) {
	events, err := s.client.QueryWaitEventsWithBlocking(ctx, s.queryMonitoringCountThreshold, slowQueryIDs)
	if err != nil {
		s.logger.Error("Failed to query active session wait events", zap.Error(err))
		return nil, err
	}
	s.logger.Debug("Fetched active session wait events", zap.Int("count", len(events)))
	return events, nil
}

// resolveQueryMetadata returns the nrServiceGUID and normalisedSQLHash for a query.
// It prefers metadata propagated from Phase 1 (sqlIDMap); falls back to extracting
// from rawQueryText when the SQL ID is not present in the map.
// precomputedHash is optional: pass a non-empty value to reuse an already-computed
// hash and avoid calling NormalizeSQLAndHash a second time.
func resolveQueryMetadata(queryID, rawQueryText, precomputedHash string, sqlIDMap map[string]models.SQLIdentifier) (nrServiceGUID, normalisedSQLHash string) {
	if md, exists := sqlIDMap[queryID]; exists {
		return md.NRServiceGUID, md.NormalisedSQLHash
	}
	if rawQueryText != "" {
		if precomputedHash != "" {
			normalisedSQLHash = precomputedHash
		} else {
			_, normalisedSQLHash = commonutils.NormalizeSQLAndHash(rawQueryText)
		}
		nrServiceGUID = commonutils.ExtractNewRelicMetadata(rawQueryText)
	}
	return nrServiceGUID, normalisedSQLHash
}

// ScrapeWaitEventsAndBlocking collects wait events and blocking query metrics.
// slowQueryIdentifiers from Phase 1 are used to attach pre-computed metadata (nrServiceGUID,
// normalisedSQLHash) to matching active sessions; all other sessions derive their own.
func (s *WaitEventBlockingScraper) ScrapeWaitEventsAndBlocking(ctx context.Context, slowQueryIdentifiers []models.SQLIdentifier) ([]models.SQLIdentifier, []error) {
	// Build metadata lookup map from Phase 1 slow-query identifiers.
	// Also collect the raw SQL IDs so the DB query can prioritize those sessions.
	sqlIDMap := make(map[string]models.SQLIdentifier, len(slowQueryIdentifiers))
	slowQueryIDs := make([]string, 0, len(slowQueryIdentifiers))
	for _, identifier := range slowQueryIdentifiers {
		sqlIDMap[identifier.SQLID] = identifier
		slowQueryIDs = append(slowQueryIDs, identifier.SQLID)
	}

	waitEvents, err := s.fetchWaitEvents(ctx, slowQueryIDs)
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

	collectionTimestamp := event.GetCollectionTimestamp()
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
	sqlExecStart := event.GetSQLExecStart()
	sqlExecID := event.GetSQLExecID()
	rowWaitObjID := commonutils.FormatInt64(event.GetLockedObjectID())
	rowWaitFileID := commonutils.FormatInt64(event.GetLockedFileID())
	rowWaitBlockID := commonutils.FormatInt64(event.GetLockedBlockID())

	// Normalise the active session's query text once; the result is shared by
	// both the metadata resolution below and the query-details emission at the end.
	rawQueryText := event.GetQueryText()
	var normalisedQueryText, queryHash string
	if rawQueryText != "" {
		normalisedQueryText, queryHash = commonutils.NormalizeSQLAndHash(rawQueryText)
	}

	// Prefer metadata propagated from Phase 1 (slow queries).
	// For sessions not in Phase 1, derive metadata from the event's own query text.
	// Pass queryHash so NormalizeSQLAndHash is not called a second time.
	nrServiceGUID, normalisedSQLHash := resolveQueryMetadata(queryID, rawQueryText, queryHash, sqlIDMap)

	// Get blocking attributes (will be empty strings for non-blocked sessions)
	blockingSessionStatus := event.GetBlockingSessionStatus()
	immediateBlockerSID := commonutils.FormatInt64(event.GetImmediateBlockerSID())
	finalBlockingSessionStatus := event.GetFinalBlockingSessionStatus()
	finalBlockerUser := event.GetFinalBlockerUser()
	finalBlockerSID := commonutils.FormatInt64(event.GetFinalBlockerSID())
	finalBlockerSerial := commonutils.FormatInt64(event.GetFinalBlockerSerial())
	finalBlockerQueryID := event.GetFinalBlockerQueryID()

	// Extract metadata from final blocker query text (if blocked)
	var nrBlockingServiceGUID, normalisedBlockingSQLHash string
	rawFinalBlockerQueryText := event.GetFinalBlockerQueryText()
	if rawFinalBlockerQueryText != "" {
		nrBlockingServiceGUID = commonutils.ExtractNewRelicMetadata(rawFinalBlockerQueryText)
		_, normalisedBlockingSQLHash = commonutils.NormalizeSQLAndHash(rawFinalBlockerQueryText)
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
		blockingSessionStatus,
		immediateBlockerSID,
		finalBlockingSessionStatus,
		finalBlockerUser,
		finalBlockerSID,
		finalBlockerSerial,
		finalBlockerQueryID,
		nrServiceGUID,
		normalisedSQLHash,
		nrBlockingServiceGUID,
		normalisedBlockingSQLHash,
	)

	// Emit query details for the active session's own query text.
	// schema_name and last_active_time are not available in V$SESSION;
	// blocking hash/guid are not applicable for the active (victim) session.
	if normalisedQueryText != "" {
		s.mb.RecordNewrelicoracledbSlowQueriesQueryDetailsDataPoint(
			now,
			1,
			"OracleQueryDetails",
			collectionTimestamp,
			dbName,
			queryID,
			normalisedQueryText,
			"",       // schema_name - not available in V$SESSION
			username, // user_name from V$SESSION
			"",       // last_active_time - not available in V$SESSION
			queryHash,
			nrServiceGUID,
			"", // normalised_blocking_sql_hash - not applicable
			"", // nr_blocking_service_guid - not applicable
		)
	}
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
			nrServiceGUID, normalisedSQLHash := resolveQueryMetadata(sqlID, event.GetQueryText(), "", sqlIDMap)
			identifiersMap[key] = models.SQLIdentifier{
				SQLID:             sqlID,
				ChildNumber:       childNumber,
				Timestamp:         time.Now(),
				NRServiceGUID:     nrServiceGUID,
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

	// Normalise once; the hash is a by-product with no extra cost.
	finalBlockerQueryText, normalisedBlockingSQLHash := commonutils.NormalizeSQLAndHash(rawFinalBlockerQueryText)

	// Only record if we have valid query ID and normalised text
	if finalBlockerQueryID == "" || finalBlockerQueryText == "" {
		return
	}

	collectionTimestamp := event.GetCollectionTimestamp()
	dbName := event.GetDatabaseName()
	queryID := event.GetQueryID()

	// Get nrServiceGUID and normalised_sql_hash from sqlIDMap for the blocked (victim) query
	nrServiceGUID, normalisedSQLHash := resolveQueryMetadata(queryID, "", "", sqlIDMap)

	nrBlockingServiceGUID := commonutils.ExtractNewRelicMetadata(rawFinalBlockerQueryText)

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
		nrServiceGUID,             // nr_service_guid
		normalisedBlockingSQLHash, // normalised_blocking_sql_hash (same - this IS the blocking query)
		nrBlockingServiceGUID,     // nr_blocking_service_guid (same - this IS the blocking query)
	)
}
