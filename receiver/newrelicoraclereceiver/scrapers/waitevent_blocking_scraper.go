package scrapers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// WaitEventBlockingScraper collects both Oracle wait events and blocking query metrics
// This replaces the separate BlockingScraper and WaitEventsScraper
type WaitEventBlockingScraper struct {
	client                        client.OracleClient
	mb                            *metadata.MetricsBuilder
	logger                        *zap.Logger
	instanceName                  string
	metricsBuilderConfig          metadata.MetricsBuilderConfig
	queryMonitoringCountThreshold int
}

// NewWaitEventBlockingScraper creates a new combined Wait Events and Blocking Scraper instance
func NewWaitEventBlockingScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig, countThreshold int) (*WaitEventBlockingScraper, error) {
	if oracleClient == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if mb == nil {
		return nil, fmt.Errorf("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if instanceName == "" {
		return nil, fmt.Errorf("instance name cannot be empty")
	}

	return &WaitEventBlockingScraper{
		client:                        oracleClient,
		mb:                            mb,
		logger:                        logger,
		instanceName:                  instanceName,
		metricsBuilderConfig:          metricsBuilderConfig,
		queryMonitoringCountThreshold: countThreshold,
	}, nil
}

// ScrapeWaitEventsAndBlocking collects both wait events and blocking query metrics in a single query
func (s *WaitEventBlockingScraper) ScrapeWaitEventsAndBlocking(ctx context.Context) []error {
	var scrapeErrors []error

	waitEvents, err := s.client.QueryWaitEventsWithBlocking(ctx, s.queryMonitoringCountThreshold)
	if err != nil {
		s.logger.Error("Failed to query wait events with blocking information", zap.Error(err))
		return []error{err}
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	waitEventMetricCount := 0
	blockingMetricCount := 0

	for _, event := range waitEvents {
		// Record wait event metrics
		if event.IsValidForMetrics() {
			s.recordWaitEventMetrics(now, &event)
			waitEventMetricCount++
		}

		// Record blocking metrics if this session is blocked
		if event.IsBlocked() {
			s.recordBlockingMetrics(now, &event)
			blockingMetricCount++
		}
	}

	s.logger.Debug("Wait events and blocking scrape completed",
		zap.Int("total_events", len(waitEvents)),
		zap.Int("wait_metrics", waitEventMetricCount),
		zap.Int("blocking_metrics", blockingMetricCount),
		zap.Int("errors", len(scrapeErrors)))

	return scrapeErrors
}

// recordWaitEventMetrics records wait event metrics for a session
func (s *WaitEventBlockingScraper) recordWaitEventMetrics(now pcommon.Timestamp, event *models.WaitEventWithBlocking) {
	if !event.HasValidCurrentWaitSeconds() {
		return
	}

	collectionTimestamp := event.GetCollectionTimestamp().Format("2006-01-02 15:04:05")
	dbName := event.GetDatabaseName()
	username := event.GetUsername()
	sid := strconv.FormatInt(event.GetSID(), 10)
	serial := event.GetSerial()
	status := event.GetStatus()
	qID := event.GetQueryID()
	sqlChildNumber := event.GetSQLChildNumber()
	waitCat := event.GetWaitCategory()
	waitEventName := event.GetWaitEventName()
	program := event.GetProgram()
	machine := event.GetMachine()
	waitObjectOwner := event.GetObjectOwner()
	waitObjectName := event.GetObjectNameWaitedOn()
	waitObjectType := event.GetObjectTypeWaitedOn()
	sqlExecStart := event.GetSQLExecStart().Format("2006-01-02 15:04:05")
	sqlExecID := event.GetSQLExecID()
	rowWaitObjID := strconv.FormatInt(event.GetLockedObjectID(), 10)
	rowWaitFileID := strconv.FormatInt(event.GetLockedFileID(), 10)
	rowWaitBlockID := strconv.FormatInt(event.GetLockedBlockID(), 10)
	p1Text := event.GetP1Text()
	p1 := strconv.FormatInt(event.GetP1(), 10)
	p2Text := event.GetP2Text()
	p2 := strconv.FormatInt(event.GetP2(), 10)
	p3Text := event.GetP3Text()
	p3 := strconv.FormatInt(event.GetP3(), 10)

	// Record current_wait_seconds metric
	s.mb.RecordNewrelicoracledbWaitEventsCurrentWaitSecondsDataPoint(
		now,
		float64(event.GetCurrentWaitSeconds()),
		collectionTimestamp,
		dbName,
		username,
		sid,
		serial,
		status,
		qID,
		sqlChildNumber,
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
			event.GetTimeRemainingSeconds(),
			collectionTimestamp,
			dbName,
			sid,
			qID,
			sqlChildNumber,
			sqlExecID,
			sqlExecStart,
		)
	}
}

// recordBlockingMetrics records blocking query metrics when a session is blocked
func (s *WaitEventBlockingScraper) recordBlockingMetrics(now pcommon.Timestamp, event *models.WaitEventWithBlocking) {
	// Calculate blocked wait time in seconds from current_wait_seconds
	// This represents how long the session has been blocked
	blockedWaitSec := float64(event.GetCurrentWaitSeconds())

	if blockedWaitSec <= 0 {
		return
	}

	collectionTimestamp := event.GetCollectionTimestamp().Format("2006-01-02 15:04:05")
	dbName := event.GetDatabaseName()
	blockedUser := event.GetUsername()
	queryID := event.GetQueryID()
	sessionID := strconv.FormatInt(event.GetSID(), 10)
	blockedSerial := event.GetSerial()
	sqlChildNumber := event.GetSQLChildNumber()
	sqlExecID := event.GetSQLExecID()
	sqlExecStart := event.GetSQLExecStart().Format("2006-01-02 15:04:05")

	// Blocking session information
	finalBlockerSID := strconv.FormatInt(event.GetFinalBlockerSID(), 10)
	finalBlockerSerial := strconv.FormatInt(event.GetFinalBlockerSerial(), 10)
	finalBlockerUser := event.GetFinalBlockerUser()
	finalBlockerQueryID := event.GetFinalBlockerQueryID()
	finalBlockerQueryText := commonutils.AnonymizeAndNormalize(event.GetFinalBlockerQueryText())

	s.mb.RecordNewrelicoracledbBlockingQueriesWaitTimeDataPoint(
		now,
		blockedWaitSec,
		collectionTimestamp,
		dbName,
		blockedUser,
		sessionID,
		blockedSerial,
		queryID,
		sqlChildNumber,
		sqlExecID,
		sqlExecStart,
		finalBlockerUser,
		finalBlockerSID,
		finalBlockerSerial,
		finalBlockerQueryID,
		finalBlockerQueryText,
	)
}


// getNewIdentifiersFromWaitEvents checks wait events for child numbers NOT in vsqlIdentifiers
func (s *WaitEventBlockingScraper) getNewIdentifiersFromWaitEvents(ctx context.Context, slowQueryIDs []string, vsqlIdentifiers []models.SQLIdentifier) ([]models.SQLIdentifier, error) {
	// Build a map of existing (SQL_ID, CHILD_NUMBER) from V$SQL for fast lookup
	existingMap := make(map[string]bool, len(vsqlIdentifiers))
	for _, id := range vsqlIdentifiers {
		key := fmt.Sprintf("%s#%d", id.SQLID, id.ChildNumber)
		existingMap[key] = true
	}

	// Build slow query map for filtering
	slowQueryMap := make(map[string]bool, len(slowQueryIDs))
	for _, sqlID := range slowQueryIDs {
		slowQueryMap[sqlID] = true
	}

	// Fetch all wait events with blocking information
	waitEvents, err := s.client.QueryWaitEventsWithBlocking(ctx, s.queryMonitoringCountThreshold)
	if err != nil {
		return nil, err
	}

	// Find NEW child numbers in wait events
	var newIdentifiers []models.SQLIdentifier
	newChildNumbersFound := 0

	for _, waitEvent := range waitEvents {
		if !waitEvent.HasValidQueryID() {
			continue
		}

		sqlID := waitEvent.GetQueryID()

		// Skip if not a slow query
		if !slowQueryMap[sqlID] {
			continue
		}

		childNum := waitEvent.GetSQLChildNumber()
		key := fmt.Sprintf("%s#%d", sqlID, childNum)

		// Check if this (SQL_ID, CHILD_NUMBER) is NEW (not in V$SQL results)
		if !existingMap[key] {
			newIdentifiers = append(newIdentifiers, models.SQLIdentifier{
				SQLID:       sqlID,
				ChildNumber: childNum,
			})
			existingMap[key] = true // Mark as added to avoid duplicates
			newChildNumbersFound++
		}
	}

	s.logger.Debug("Found new child numbers in wait events",
		zap.Int("new_child_numbers", newChildNumbersFound))

	return newIdentifiers, nil
}

// mergeIdentifiers combines identifiers from V$SQL and wait events, removing duplicates
func (s *WaitEventBlockingScraper) mergeIdentifiers(vsqlIdentifiers, waitEventIdentifiers []models.SQLIdentifier) []models.SQLIdentifier {
	// Start with V$SQL identifiers
	merged := make([]models.SQLIdentifier, 0, len(vsqlIdentifiers)+len(waitEventIdentifiers))
	merged = append(merged, vsqlIdentifiers...)

	// Add new identifiers from wait events
	merged = append(merged, waitEventIdentifiers...)

	return merged
}

// GetChildCursorsWithMetrics fetches child cursors with metrics and SQL identifiers for execution plans
// This optimized method returns BOTH:
// 1. Complete ChildCursor objects with metrics (to avoid re-querying V$SQL)
// 2. SQL identifiers for execution plan fetching
// Returns: (childCursorsFromVSQL, newChildCursorsFromWaitEvents, allSQLIdentifiers, error)
func (s *WaitEventBlockingScraper) GetChildCursorsWithMetrics(ctx context.Context, slowQueryIDs []string, childCursorsPerQuery int) ([]models.ChildCursor, []models.SQLIdentifier, []models.SQLIdentifier, error) {
	if len(slowQueryIDs) == 0 {
		return []models.ChildCursor{}, []models.SQLIdentifier{}, []models.SQLIdentifier{}, nil
	}

	s.logger.Debug("Fetching child cursors with metrics and SQL identifiers",
		zap.Int("slow_query_count", len(slowQueryIDs)),
		zap.Int("child_cursors_per_query", childCursorsPerQuery))

	// STEP 1: Fetch top N child cursors from V$SQL (with full metrics)
	childCursorsWithMetrics, vsqlIdentifiers, err := s.getChildCursorsFromVSQL(ctx, slowQueryIDs, childCursorsPerQuery)
	if err != nil {
		return nil, nil, nil, err
	}

	// STEP 2: Check wait events for any NEW child numbers not in V$SQL results
	waitEventIdentifiers, err := s.getNewIdentifiersFromWaitEvents(ctx, slowQueryIDs, vsqlIdentifiers)
	if err != nil {
		s.logger.Warn("Failed to fetch identifiers from wait events", zap.Error(err))
		// Continue with V$SQL results only
		mergedIdentifiers := vsqlIdentifiers
		return childCursorsWithMetrics, []models.SQLIdentifier{}, mergedIdentifiers, nil
	}

	// STEP 3: Merge identifiers (V$SQL + new from wait events)
	mergedIdentifiers := s.mergeIdentifiers(vsqlIdentifiers, waitEventIdentifiers)

	s.logger.Info("Child cursors and SQL identifiers prepared",
		zap.Int("child_cursors_from_vsql", len(childCursorsWithMetrics)),
		zap.Int("identifiers_from_vsql", len(vsqlIdentifiers)),
		zap.Int("new_identifiers_from_wait_events", len(waitEventIdentifiers)),
		zap.Int("total_identifiers", len(mergedIdentifiers)))

	return childCursorsWithMetrics, waitEventIdentifiers, mergedIdentifiers, nil
}

// getChildCursorsFromVSQL fetches child cursors WITH full metrics from V$SQL
// Returns: (childCursors, identifiers, error)
func (s *WaitEventBlockingScraper) getChildCursorsFromVSQL(ctx context.Context, slowQueryIDs []string, childCursorsPerQuery int) ([]models.ChildCursor, []models.SQLIdentifier, error) {
	var allChildCursors []models.ChildCursor
	var identifiers []models.SQLIdentifier

	for _, sqlID := range slowQueryIDs {
		childCursors, err := s.client.QueryChildCursors(ctx, sqlID, childCursorsPerQuery)
		if err != nil {
			s.logger.Warn("Failed to fetch child cursors for SQL_ID from V$SQL",
				zap.String("sql_id", sqlID),
				zap.Error(err))
			continue
		}

		// Skip if no child cursors returned
		if childCursors == nil || len(childCursors) == 0 {
			s.logger.Debug("No child cursors found for SQL_ID",
				zap.String("sql_id", sqlID))
			continue
		}

		// Collect both full child cursor objects AND identifiers
		for _, cursor := range childCursors {
			if cursor.HasValidIdentifier() {
				allChildCursors = append(allChildCursors, cursor)
				identifiers = append(identifiers, models.SQLIdentifier{
					SQLID:       cursor.GetSQLID(),
					ChildNumber: cursor.GetChildNumber(),
				})
			}
		}
	}

	s.logger.Debug("Fetched child cursors with metrics from V$SQL",
		zap.Int("slow_query_ids", len(slowQueryIDs)),
		zap.Int("total_child_cursors", len(allChildCursors)))

	return allChildCursors, identifiers, nil
}
