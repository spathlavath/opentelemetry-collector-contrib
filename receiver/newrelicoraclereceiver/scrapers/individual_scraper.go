package scrapers

import (
	"context"
	"database/sql"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

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

// ScrapeIndividualQueries collects Oracle individual queries metrics
func (s *IndividualQueriesScraper) ScrapeIndividualQueries(ctx context.Context) []error {
	s.logger.Debug("Begin Oracle individual queries scrape")

	var scrapeErrors []error

	// Execute the individual queries SQL
	rows, err := s.db.QueryContext(ctx, queries.IndividualQueriesSQL)
	if err != nil {
		s.logger.Error("Failed to execute individual queries query", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var individualQuery models.IndividualQuery

		if err := rows.Scan(
			&individualQuery.QueryID,
			&individualQuery.QueryText,
			&individualQuery.CPUTimeMs,
			&individualQuery.ElapsedTimeMs,
		); err != nil {
			s.logger.Error("Failed to scan individual query row", zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		// Ensure we have valid values for key fields
		if !individualQuery.IsValidForMetrics() {
			s.logger.Debug("Skipping individual query with null key values",
				zap.String("query_id", individualQuery.GetQueryID()),
				zap.Float64("elapsed_time_ms", individualQuery.ElapsedTimeMs.Float64))
			continue
		}

		// Convert NullString/NullFloat64 to string values for attributes
		qID := individualQuery.GetQueryID()

		s.logger.Debug("Processing individual query",
			zap.String("query_id", qID),
			zap.Float64("cpu_time_ms", individualQuery.CPUTimeMs.Float64),
			zap.Float64("elapsed_time_ms", individualQuery.ElapsedTimeMs.Float64))

		// Record CPU time if available
		if individualQuery.CPUTimeMs.Valid {
			s.mb.RecordNewrelicoracledbIndividualQueriesCPUTimeDataPoint(
				now,
				individualQuery.CPUTimeMs.Float64,
				s.instanceName,
				qID,
			)
		}

		// Record elapsed time
		s.mb.RecordNewrelicoracledbIndividualQueriesElapsedTimeDataPoint(
			now,
			individualQuery.ElapsedTimeMs.Float64,
			s.instanceName,
			qID,
		)

		// Record query details (count = 1 for each query)
		s.mb.RecordNewrelicoracledbIndividualQueriesQueryDetailsDataPoint(
			now,
			1, // Count of 1 for each query
			s.instanceName,
			qID,
		)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating over individual queries rows", zap.Error(err))
		scrapeErrors = append(scrapeErrors, err)
	}
	s.logger.Debug("Completed Oracle individual queries scrape")
	return scrapeErrors
}
