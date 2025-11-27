package scrapers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
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
		serial := waitEvent.GetSerial()
		status := waitEvent.GetStatus()
		qID := waitEvent.GetQueryID()
		sqlChildNumber := waitEvent.GetSQLChildNumber()
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
					waitEvent.GetTimeRemainingSeconds(),
					collectionTimestamp,
					sid,
					qID,
					sqlChildNumber,
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

// GetSQLIdentifiersForExecutionPlans gets SQL identifiers for execution plan fetching
// 1. Fetches top N child cursors per SQL_ID from V$SQL (most recent)
// 2. Checks wait events for any NEW child numbers not in V$SQL results
// 3. Merges both lists and returns unique (SQL_ID, CHILD_NUMBER) combinations
func (s *WaitEventsScraper) GetSQLIdentifiersForExecutionPlans(ctx context.Context, slowQueryIDs []string, childCursorsPerQuery int) ([]models.SQLIdentifier, error) {
	if len(slowQueryIDs) == 0 {
		return []models.SQLIdentifier{}, nil
	}

	s.logger.Debug("Fetching SQL identifiers for execution plans",
		zap.Int("slow_query_count", len(slowQueryIDs)),
		zap.Int("child_cursors_per_query", childCursorsPerQuery))

	// STEP 1: Fetch top N child cursors from V$SQL for each slow query
	vsqlIdentifiers, err := s.getIdentifiersFromVSQL(ctx, slowQueryIDs, childCursorsPerQuery)
	if err != nil {
		return nil, err
	}

	// STEP 2: Check wait events for any NEW child numbers
	waitEventIdentifiers, err := s.getNewIdentifiersFromWaitEvents(ctx, slowQueryIDs, vsqlIdentifiers)
	if err != nil {
		s.logger.Warn("Failed to fetch identifiers from wait events", zap.Error(err))
		// Continue with V$SQL results only
		return vsqlIdentifiers, nil
	}

	// STEP 3: Merge both lists (V$SQL + new wait event child numbers)
	mergedIdentifiers := s.mergeIdentifiers(vsqlIdentifiers, waitEventIdentifiers)

	s.logger.Info("SQL identifiers prepared for execution plans",
		zap.Int("from_vsql", len(vsqlIdentifiers)),
		zap.Int("new_from_wait_events", len(waitEventIdentifiers)),
		zap.Int("total_unique", len(mergedIdentifiers)))

	// Ensure we never return nil, return empty slice instead
	if mergedIdentifiers == nil {
		return []models.SQLIdentifier{}, nil
	}

	return mergedIdentifiers, nil
}

// getIdentifiersFromVSQL fetches top N child cursors for each SQL_ID from V$SQL
func (s *WaitEventsScraper) getIdentifiersFromVSQL(ctx context.Context, slowQueryIDs []string, childCursorsPerQuery int) ([]models.SQLIdentifier, error) {
	var identifiers []models.SQLIdentifier
	totalChildCursors := 0

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

		for _, cursor := range childCursors {
			if cursor.HasValidIdentifier() {
				identifiers = append(identifiers, models.SQLIdentifier{
					SQLID:       cursor.GetSQLID(),
					ChildNumber: cursor.GetChildNumber(),
				})
				totalChildCursors++
			}
		}
	}

	s.logger.Debug("Fetched SQL identifiers from V$SQL child cursors",
		zap.Int("slow_query_ids", len(slowQueryIDs)),
		zap.Int("total_child_cursors", totalChildCursors))

	return identifiers, nil
}

// getNewIdentifiersFromWaitEvents checks wait events for child numbers NOT in vsqlIdentifiers
func (s *WaitEventsScraper) getNewIdentifiersFromWaitEvents(ctx context.Context, slowQueryIDs []string, vsqlIdentifiers []models.SQLIdentifier) ([]models.SQLIdentifier, error) {
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

	// Fetch all wait events
	waitEvents, err := s.client.QueryWaitEvents(ctx, s.queryMonitoringCountThreshold)
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
func (s *WaitEventsScraper) mergeIdentifiers(vsqlIdentifiers, waitEventIdentifiers []models.SQLIdentifier) []models.SQLIdentifier {
	// Start with V$SQL identifiers
	merged := make([]models.SQLIdentifier, 0, len(vsqlIdentifiers)+len(waitEventIdentifiers))
	merged = append(merged, vsqlIdentifiers...)

	// Add new identifiers from wait events
	merged = append(merged, waitEventIdentifiers...)

	return merged
}
