// Licensed to The New Relic under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. The New Relic licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package scrapers

import (
	"context"
	"database/sql"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// SlowQueriesScraper contains the scraper for slow queries metrics
type SlowQueriesScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewSlowQueriesScraper creates a new Slow Queries Scraper instance
func NewSlowQueriesScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *SlowQueriesScraper {
	return &SlowQueriesScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

// ScrapeSlowQueries collects Oracle slow queries metrics
func (s *SlowQueriesScraper) ScrapeSlowQueries(ctx context.Context) []error {
	s.logger.Debug("Begin Oracle slow queries scrape")

	var scrapeErrors []error

	// Execute the slow queries SQL
	rows, err := s.db.QueryContext(ctx, queries.SlowQueriesSQL)
	if err != nil {
		s.logger.Error("Failed to execute slow queries query", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	now := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var databaseName sql.NullString
		var queryID sql.NullString
		var schemaName sql.NullString
		var statementType sql.NullString
		var executionCount sql.NullInt64
		var queryText sql.NullString
		var avgCPUTimeMs sql.NullFloat64
		var avgDiskReads sql.NullFloat64
		var avgElapsedTimeMs sql.NullFloat64

		if err := rows.Scan(
			&databaseName,
			&queryID,
			&schemaName,
			&statementType,
			&executionCount,
			&queryText,
			&avgCPUTimeMs,
			&avgDiskReads,
			&avgElapsedTimeMs,
		); err != nil {
			s.logger.Error("Failed to scan slow query row", zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		// Ensure we have valid values for key fields
		if !queryID.Valid || !avgElapsedTimeMs.Valid {
			s.logger.Debug("Skipping slow query with null key values",
				zap.String("query_id", queryID.String),
				zap.Float64("avg_elapsed_time_ms", avgElapsedTimeMs.Float64))
			continue
		}

		// Convert NullString/NullInt64/NullFloat64 to string values for attributes
		dbName := ""
		if databaseName.Valid {
			dbName = databaseName.String
		}

		qID := queryID.String

		schName := ""
		if schemaName.Valid {
			schName = schemaName.String
		}

		stmtType := ""
		if statementType.Valid {
			stmtType = statementType.String
		}

		s.logger.Debug("Processing slow query",
			zap.String("database_name", dbName),
			zap.String("query_id", qID),
			zap.String("schema_name", schName),
			zap.String("statement_type", stmtType),
			zap.Float64("avg_elapsed_time_ms", avgElapsedTimeMs.Float64))

		// Record execution count if available
		if executionCount.Valid {
			s.mb.RecordNewrelicoracledbSlowQueriesExecutionCountDataPoint(
				now,
				float64(executionCount.Int64),
				s.instanceName,
				dbName,
				qID,
			)
		}

		// Record average CPU time if available
		if avgCPUTimeMs.Valid {
			s.mb.RecordNewrelicoracledbSlowQueriesAvgCPUTimeDataPoint(
				now,
				avgCPUTimeMs.Float64,
				s.instanceName,
				dbName,
				qID,
			)
		}

		// Record average disk reads if available
		if avgDiskReads.Valid {
			s.mb.RecordNewrelicoracledbSlowQueriesAvgDiskReadsDataPoint(
				now,
				avgDiskReads.Float64,
				s.instanceName,
				dbName,
				qID,
			)
		}

		// Record average elapsed time
			s.mb.RecordNewrelicoracledbSlowQueriesAvgElapsedTimeDataPoint(
				now,
				avgElapsedTimeMs.Float64,
				s.instanceName,
				dbName,
				qID,
			)

		// Record query text
		s.mb.RecordNewrelicoracledbSlowQueriesQueryTextDataPoint(
			now,
			queryText.String,
			s.instanceName,
			dbName,
			qID,
		)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating over slow queries rows", zap.Error(err))
		scrapeErrors = append(scrapeErrors, err)
	}

	s.logger.Debug("Completed Oracle slow queries scrape")
	return scrapeErrors
}
