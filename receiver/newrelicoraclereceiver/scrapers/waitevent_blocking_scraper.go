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
// Filters wait events to ONLY show sessions running the provided slow query SQL_IDs
// Returns unique SQL identifiers (sql_id, child_number) found in those filtered wait events
func (s *WaitEventBlockingScraper) ScrapeWaitEventsAndBlocking(ctx context.Context, slowQuerySQLIDs []string) ([]models.SQLIdentifier, []error) {
	var scrapeErrors []error

	waitEvents, err := s.client.QueryWaitEventsWithBlocking(ctx, s.queryMonitoringCountThreshold)
	if err != nil {
		s.logger.Error("Failed to query wait events with blocking information", zap.Error(err))
		return nil, []error{err}
	}

	// Create a map of slow query SQL_IDs for fast lookup
	slowQueryMap := make(map[string]bool, len(slowQuerySQLIDs))
	for _, sqlID := range slowQuerySQLIDs {
		slowQueryMap[sqlID] = true
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	waitEventMetricCount := 0
	blockingMetricCount := 0
	filteredOutCount := 0

	// Track unique (sql_id, child_number) combinations from wait events
	sqlIdentifiersMap := make(map[string]models.SQLIdentifier)

	for _, event := range waitEvents {
		eventSQLID := event.GetQueryID()

		// FILTER: Only process wait events for TOP N slow query SQL_IDs
		if len(slowQuerySQLIDs) > 0 && eventSQLID != "" && !slowQueryMap[eventSQLID] {
			filteredOutCount++
			continue
		}

		// Record wait event metrics
		if event.IsValidForMetrics() {
			s.recordWaitEventMetrics(now, &event)
			waitEventMetricCount++

			// Extract SQL_ID and child_number for child cursor fetching
			sqlID := eventSQLID
			childNumber := event.GetSQLChildNumber()
			hasValidQueryID := event.HasValidQueryID()
			hasValidChildNumber := event.SQLChildNumber.Valid

			s.logger.Debug("Wait event SQL info",
				zap.String("sql_id", sqlID),
				zap.Int64("child_number", childNumber),
				zap.Bool("has_valid_query_id", hasValidQueryID),
				zap.Bool("child_number_valid", hasValidChildNumber))

			// Only collect SQL identifiers if BOTH sql_id AND child_number are valid (not NULL)
			// Child number can be 0 (which is valid), but it must be explicitly set, not NULL
			if hasValidQueryID && hasValidChildNumber {
				key := fmt.Sprintf("%s#%d", sqlID, childNumber)

				// Store unique combination
				if _, exists := sqlIdentifiersMap[key]; !exists {
					sqlIdentifiersMap[key] = models.SQLIdentifier{
						SQLID:       sqlID,
						ChildNumber: childNumber,
					}
					s.logger.Info("Added SQL identifier for execution plan",
						zap.String("sql_id", sqlID),
						zap.Int64("child_number", childNumber))
				}
			} else {
				s.logger.Debug("Skipped SQL identifier - missing valid query ID or child number",
					zap.String("sql_id", sqlID),
					zap.Int64("child_number", childNumber),
					zap.Bool("has_valid_query_id", hasValidQueryID),
					zap.Bool("has_valid_child_number", hasValidChildNumber))
			}
		}

		// Record blocking metrics if this session is blocked
		if event.IsBlocked() {
			s.recordBlockingMetrics(now, &event)
			blockingMetricCount++
		}
	}

	// Convert map to slice
	sqlIdentifiers := make([]models.SQLIdentifier, 0, len(sqlIdentifiersMap))
	for _, identifier := range sqlIdentifiersMap {
		sqlIdentifiers = append(sqlIdentifiers, identifier)
	}

	s.logger.Debug("Wait events and blocking scrape completed",
		zap.Int("total_events", len(waitEvents)),
		zap.Int("filtered_out", filteredOutCount),
		zap.Int("wait_metrics", waitEventMetricCount),
		zap.Int("blocking_metrics", blockingMetricCount),
		zap.Int("unique_sql_identifiers", len(sqlIdentifiers)),
		zap.Int("slow_query_filter_count", len(slowQuerySQLIDs)),
		zap.Int("errors", len(scrapeErrors)))

	return sqlIdentifiers, scrapeErrors
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

	// Record current_wait_time_ms metric
	s.mb.RecordNewrelicoracledbWaitEventsCurrentWaitTimeMsDataPoint(
		now,
		event.GetCurrentWaitMs(),
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

	// Record time_remaining_ms metric with reduced attributes to avoid high cardinality
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbWaitEventsTimeRemainingMs.Enabled {
		s.mb.RecordNewrelicoracledbWaitEventsTimeRemainingMsDataPoint(
			now,
			event.GetTimeRemainingMs(),
			collectionTimestamp,
			dbName,
			sid,
			qID,
			sqlChildNumber,
			sqlExecID,
			sqlExecStart,
		)
	}

	// Record time_since_last_wait_ms metric to track ON CPU time
	if s.metricsBuilderConfig.Metrics.NewrelicoracledbWaitEventsTimeSinceLastWaitMs.Enabled {
		s.mb.RecordNewrelicoracledbWaitEventsTimeSinceLastWaitMsDataPoint(
			now,
			event.GetTimeSinceLastWaitMs(),
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
	// Get blocked wait time in milliseconds (ONLY populated when there's a blocker)
	// This is more specific than general wait_time_ms as it's NULL for non-blocking waits
	blockedWaitMs := event.GetBlockedTimeMs()

	if blockedWaitMs <= 0 {
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

	// Wait event information (helps identify the type of contention)
	waitEventName := event.GetWaitEventName()
	waitCat := event.GetWaitCategory()
	waitObjectName := event.GetObjectNameWaitedOn()
	waitObjectOwner := event.GetObjectOwner()
	waitObjectType := event.GetObjectTypeWaitedOn()

	// Blocking session information
	finalBlockerSID := strconv.FormatInt(event.GetFinalBlockerSID(), 10)
	finalBlockerSerial := strconv.FormatInt(event.GetFinalBlockerSerial(), 10)
	finalBlockerUser := event.GetFinalBlockerUser()
	finalBlockerQueryID := event.GetFinalBlockerQueryID()
	finalBlockerQueryText := commonutils.AnonymizeAndNormalize(event.GetFinalBlockerQueryText())

	s.mb.RecordNewrelicoracledbBlockingQueriesWaitTimeMsDataPoint(
		now,
		blockedWaitMs,
		collectionTimestamp,
		dbName,
		blockedUser,
		sessionID,
		blockedSerial,
		queryID,
		sqlChildNumber,
		sqlExecID,
		sqlExecStart,
		waitEventName,
		waitCat,
		waitObjectName,
		waitObjectOwner,
		waitObjectType,
		finalBlockerUser,
		finalBlockerSID,
		finalBlockerSerial,
		finalBlockerQueryID,
		finalBlockerQueryText,
	)
}
