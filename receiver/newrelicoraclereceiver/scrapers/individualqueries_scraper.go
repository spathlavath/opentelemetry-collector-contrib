package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// IndividualQueriesScraper contains the scraper for individual queries metrics
type IndividualQueriesScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewIndividualQueriesScraper creates a new Individual Queries Scraper instance
func NewIndividualQueriesScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *IndividualQueriesScraper {
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

	return &IndividualQueriesScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

// ScrapeIndividualQueries collects Oracle individual queries metrics based on query IDs from slow queries
func (s *IndividualQueriesScraper) ScrapeIndividualQueries(ctx context.Context, queryIDs []string) []error {
	s.logger.Debug("Begin Oracle individual queries scrape", zap.Int("query_ids_count", len(queryIDs)))

	var scrapeErrors []error

	// If no query IDs provided, skip scraping
	if len(queryIDs) == 0 {
		s.logger.Debug("No query IDs provided for individual queries scraping")
		return scrapeErrors
	}

	// Build the SQL query with query ID placeholders
	placeholders := make([]string, len(queryIDs))
	args := make([]interface{}, len(queryIDs))
	for i, queryID := range queryIDs {
		placeholders[i] = "?"
		args[i] = queryID
	}

	sqlQuery := fmt.Sprintf(queries.IndividualQueriesSQL, strings.Join(placeholders, ","))

	s.logger.Debug("Executing individual queries SQL",
		zap.String("query", sqlQuery),
		zap.Strings("query_ids", queryIDs))

	// Execute the individual queries SQL
	rows, err := s.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		s.logger.Error("Failed to execute individual queries query", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var individualQuery models.IndividualQuery

		if err := rows.Scan(
			&individualQuery.SID,
			&individualQuery.Serial,
			&individualQuery.Username,
			&individualQuery.Status,
			&individualQuery.QueryID,
			&individualQuery.PlanHashValue,
			&individualQuery.ElapsedTimeMs,
			&individualQuery.CPUTimeMs,
			&individualQuery.OSUser,
			&individualQuery.Hostname,
			&individualQuery.QueryText,
		); err != nil {
			s.logger.Error("Failed to scan individual query row", zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		// Ensure we have valid values for key fields
		if !individualQuery.IsValidForMetrics() {
			s.logger.Debug("Skipping individual query with null key values",
				zap.String("query_id", individualQuery.GetQueryID()),
				zap.Int64("sid", individualQuery.GetSID()),
				zap.Float64("elapsed_time_ms", individualQuery.GetElapsedTimeMs()))
			continue
		}

		// Convert NullString/NullInt64/NullFloat64 to values for attributes
		qID := individualQuery.GetQueryID()
		username := individualQuery.GetUsername()
		status := individualQuery.GetStatus()
		osUser := individualQuery.GetOSUser()
		hostname := individualQuery.GetHostname()
		qText := commonutils.AnonymizeAndNormalize(individualQuery.GetQueryText())

		s.logger.Debug("Processing individual query",
			zap.String("query_id", qID),
			zap.Int64("sid", individualQuery.GetSID()),
			zap.String("username", username),
			zap.String("status", status),
			zap.Float64("elapsed_time_ms", individualQuery.GetElapsedTimeMs()))

		// Record elapsed time metric
		s.mb.RecordNewrelicoracledbIndividualQueriesElapsedTimeDataPoint(
			now,
			individualQuery.GetElapsedTimeMs(),
			s.instanceName,
			qID,
			fmt.Sprintf("%d", individualQuery.GetSID()),
			fmt.Sprintf("%d", individualQuery.GetSerial()),
			username,
			status,
			fmt.Sprintf("%d", individualQuery.GetPlanHashValue()),
			osUser,
			hostname,
		)

		// Record CPU time if available
		if individualQuery.CPUTimeMs.Valid {
			s.mb.RecordNewrelicoracledbIndividualQueriesCPUTimeDataPoint(
				now,
				individualQuery.GetCPUTimeMs(),
				s.instanceName,
				qID,
				fmt.Sprintf("%d", individualQuery.GetSID()),
				fmt.Sprintf("%d", individualQuery.GetSerial()),
				username,
				status,
				fmt.Sprintf("%d", individualQuery.GetPlanHashValue()),
				osUser,
				hostname,
			)
		}

		// Record query details (count metric with metadata as attributes)
		s.mb.RecordNewrelicoracledbIndividualQueriesQueryDetailsDataPoint(
			now,
			1,
			s.instanceName,
			qID,
			fmt.Sprintf("%d", individualQuery.GetSID()),
			fmt.Sprintf("%d", individualQuery.GetSerial()),
			username,
			status,
			fmt.Sprintf("%d", individualQuery.GetPlanHashValue()),
			osUser,
			hostname,
			qText,
		)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating over individual queries rows", zap.Error(err))
		scrapeErrors = append(scrapeErrors, err)
	}

	s.logger.Debug("Completed Oracle individual queries scrape")
	return scrapeErrors
}
