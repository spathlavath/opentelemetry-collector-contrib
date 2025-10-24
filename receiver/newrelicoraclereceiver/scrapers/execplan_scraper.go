// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// ExecPlanScraper contains the scraper for execution plans
type ExecPlanScraper struct {
	db                   *sql.DB
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewExecPlanScraper creates a new Execution Plan Scraper instance
func NewExecPlanScraper(db *sql.DB, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) (*ExecPlanScraper, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if instanceName == "" {
		return nil, fmt.Errorf("instance name cannot be empty")
	}

	return &ExecPlanScraper{
		db:                   db,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}, nil
}

// ScrapeExecPlans collects execution plans for the provided query IDs
func (s *ExecPlanScraper) ScrapeExecPlans(ctx context.Context, queryIDs []string) ([]models.ExecPlan, []error) {
	s.logger.Debug("Begin Oracle execution plans scrape", zap.Int("query_ids_count", len(queryIDs)))

	var scrapeErrors []error
	var execPlans []models.ExecPlan

	if len(queryIDs) == 0 {
		s.logger.Debug("No query IDs provided for execution plan scraping")
		return execPlans, scrapeErrors
	}

	// Iterate through each query ID and fetch its execution plan
	for _, queryID := range queryIDs {
		if queryID == "" {
			s.logger.Debug("Skipping empty query ID")
			continue
		}

		s.logger.Debug("Fetching execution plan", zap.String("query_id", queryID))

		execPlanSQL := queries.GetExecPlanSQL(queryID)
		rows, err := s.db.QueryContext(ctx, execPlanSQL)
		if err != nil {
			s.logger.Error("Failed to execute execution plan query",
				zap.String("query_id", queryID),
				zap.Error(err))
			scrapeErrors = append(scrapeErrors, fmt.Errorf("failed to get execution plan for query_id %s: %w", queryID, err))
			continue
		}

		// Collect all plan output rows for this query
		var planOutputLines []string
		for rows.Next() {
			var execPlan models.ExecPlan
			if err := rows.Scan(&execPlan.QueryID, &execPlan.PlanTableOutput); err != nil {
				s.logger.Error("Failed to scan execution plan row",
					zap.String("query_id", queryID),
					zap.Error(err))
				scrapeErrors = append(scrapeErrors, err)
				continue
			}

			if execPlan.PlanTableOutput.Valid && execPlan.PlanTableOutput.String != "" {
				planOutputLines = append(planOutputLines, execPlan.PlanTableOutput.String)
			}
		}

		if err := rows.Close(); err != nil {
			s.logger.Warn("Failed to close execution plan result set",
				zap.String("query_id", queryID),
				zap.Error(err))
		}

		if err := rows.Err(); err != nil {
			s.logger.Error("Error iterating over execution plan rows",
				zap.String("query_id", queryID),
				zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		// Combine all plan output lines into a single execution plan
		if len(planOutputLines) > 0 {
			combinedPlan := strings.Join(planOutputLines, "\n")
			execPlan := models.ExecPlan{
				QueryID: sql.NullString{
					String: queryID,
					Valid:  true,
				},
				PlanTableOutput: sql.NullString{
					String: combinedPlan,
					Valid:  true,
				},
			}
			execPlans = append(execPlans, execPlan)

			s.logger.Debug("Successfully retrieved execution plan",
				zap.String("query_id", queryID),
				zap.Int("plan_lines", len(planOutputLines)))
		} else {
			s.logger.Debug("No execution plan found", zap.String("query_id", queryID))
		}
	}

	s.logger.Debug("Completed Oracle execution plans scrape",
		zap.Int("query_ids_requested", len(queryIDs)),
		zap.Int("plans_collected", len(execPlans)),
		zap.Int("errors_encountered", len(scrapeErrors)))

	return execPlans, scrapeErrors
}
