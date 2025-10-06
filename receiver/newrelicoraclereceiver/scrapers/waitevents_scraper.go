package scrapers

import (
	"context"
	"database/sql"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// WaitEventsScraper contains the scraper for wait events metrics
type WaitEventsScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewWaitEventsScraper creates a new Wait Events Scraper instance
func NewWaitEventsScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *WaitEventsScraper {
	if db == nil {
		panic("database connection cannot be nil")
	}
	if mb == nil {
		panic("metrics builder cannot be nil")
	}
	if logger == nil {
		panic("logger cannot be nil")
	}
	if instanceName == "" {
		panic("instance name cannot be empty")
	}

	return &WaitEventsScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

// ScrapeWaitEvents collects Oracle wait events metrics
func (s *WaitEventsScraper) ScrapeWaitEvents(ctx context.Context) []error {
	s.logger.Debug("Begin Oracle wait events scrape")

	var scrapeErrors []error

	// Execute the wait events SQL
	rows, err := s.db.QueryContext(ctx, queries.WaitEventQueriesSQL)
	if err != nil {
		s.logger.Error("Failed to execute wait events query", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var databaseName sql.NullString
		var queryID sql.NullString
		var queryText sql.NullString
		var waitCategory sql.NullString
		var waitEventName sql.NullString
		var collectionTimestamp sql.NullTime
		var waitingTasksCount sql.NullInt64
		var totalWaitTimeMs sql.NullFloat64

		if err := rows.Scan(
			&databaseName,
			&queryID,
			&queryText,
			&waitCategory,
			&waitEventName,
			&collectionTimestamp,
			&waitingTasksCount,
			&totalWaitTimeMs,
		); err != nil {
			s.logger.Error("Failed to scan wait events row", zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		// Ensure we have valid values for key fields
		if !queryID.Valid || !waitEventName.Valid || !totalWaitTimeMs.Valid {
			s.logger.Debug("Skipping wait event with null key values",
				zap.String("query_id", queryID.String),
				zap.String("wait_event_name", waitEventName.String),
				zap.Float64("total_wait_time_ms", totalWaitTimeMs.Float64))
			continue
		}

		// Convert NullString/NullInt64/NullFloat64 to string values for attributes
		dbName := ""
		if databaseName.Valid {
			dbName = databaseName.String
		}

		qID := queryID.String
		// Store query text for potential future use (currently we store as info metric with attributes)
		_ = ""
		if queryText.Valid {
			_ = commonutils.AnonymizeAndNormalize(queryText.String)
		}

		waitCat := ""
		if waitCategory.Valid {
			waitCat = waitCategory.String
		}

		waitEvent := waitEventName.String

		s.logger.Debug("Processing wait event",
			zap.String("database_name", dbName),
			zap.String("query_id", qID),
			zap.String("wait_category", waitCat),
			zap.String("wait_event_name", waitEvent),
			zap.Float64("total_wait_time_ms", totalWaitTimeMs.Float64))

		// Record waiting tasks count if available
		if waitingTasksCount.Valid {
			s.mb.RecordNewrelicoracledbWaitEventsWaitingTasksCountDataPoint(
				now,
				float64(waitingTasksCount.Int64),
				s.instanceName,
				dbName,
				qID,
				waitCat,
				waitEvent,
			)
		}

		// Record total wait time
		s.mb.RecordNewrelicoracledbWaitEventsTotalWaitTimeDataPoint(
			now,
			totalWaitTimeMs.Float64,
			s.instanceName,
			dbName,
			qID,
			waitCat,
			waitEvent,
		)

		// Record wait event details
		s.mb.RecordNewrelicoracledbWaitEventsQueryDetailsDataPoint(
			now,
			1.0, // Use value 1 since this is an info metric
			s.instanceName,
			dbName,
			qID,
			waitCat,
			waitEvent,
		)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating over wait events rows", zap.Error(err))
		scrapeErrors = append(scrapeErrors, err)
	}

	s.logger.Debug("Completed Oracle wait events scrape")
	return scrapeErrors
}
