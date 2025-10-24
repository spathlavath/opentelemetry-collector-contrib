// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// ExecutionPlanScraper handles scraping execution plans for SQL IDs obtained from slow queries
type ExecutionPlanScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewExecutionPlanScraper creates a new ExecutionPlanScraper instance
func NewExecutionPlanScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

// ScrapeExecutionPlans fetches execution plans for the provided SQL IDs using DBMS_XPLAN.DISPLAY_CURSOR
func (s *ExecutionPlanScraper) ScrapeExecutionPlans(ctx context.Context, sqlIDs []string) []error {
	var errs []error

	// Skip if no SQL IDs provided
	if len(sqlIDs) == 0 {
		s.logger.Debug("No SQL IDs provided for execution plan scraping")
		return errs
	}

	s.logger.Debug("Starting execution plan scraping using DBMS_XPLAN.DISPLAY_CURSOR",
		zap.Int("sql_ids_count", len(sqlIDs)),
		zap.Strings("sql_ids", sqlIDs))

	// Process each SQL ID individually for better error handling and timeout management
	successCount := 0
	for i, sqlID := range sqlIDs {
		// Add timeout for each individual query to prevent hanging
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		err := s.processSingleSQLID(queryCtx, sqlID, i+1)
		cancel()

		if err != nil {
			s.logger.Warn("Failed to process execution plan for SQL ID",
				zap.String("sql_id", sqlID),
				zap.Int("sql_id_index", i+1),
				zap.Error(err))
			errs = append(errs, err)
		} else {
			successCount++
		}

		// Check for context cancellation between SQL IDs
		select {
		case <-ctx.Done():
			s.logger.Debug("Context cancelled during execution plan processing")
			errs = append(errs, fmt.Errorf("execution plan processing cancelled: %w", ctx.Err()))
			break
		default:
		}
	}

	s.logger.Info("Execution plan scraping completed",
		zap.Int("total_sql_ids", len(sqlIDs)),
		zap.Int("successful_plans", successCount),
		zap.Int("failed_plans", len(errs)))

	return errs
}

// processSingleSQLID processes execution plan for a single SQL ID
func (s *ExecutionPlanScraper) processSingleSQLID(ctx context.Context, sqlID string, index int) error {
	startTime := time.Now()

	// Get the execution plan query for this SQL ID
	query := queries.GetExecutionPlanQuery(sqlID)

	s.logger.Debug("Executing execution plan query for SQL ID",
		zap.String("sql_id", sqlID),
		zap.Int("sql_id_index", index))

	// Execute the query
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute execution plan query for SQL ID %s: %w", sqlID, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.logger.Warn("Failed to close execution plan query rows",
				zap.String("sql_id", sqlID),
				zap.Error(closeErr))
		}
	}()

	// Process results
	planCount := 0
	for rows.Next() {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("execution plan processing cancelled for SQL ID %s: %w", sqlID, ctx.Err())
		default:
		}

		var plan models.ExecutionPlan

		// Scan the row into execution plan model
		err := rows.Scan(
			&plan.DatabaseName,
			&plan.QueryID,
			&plan.PlanHashValue,
			&plan.ExecutionPlanText, // Using text instead of XML for DBMS_XPLAN output
		)
		if err != nil {
			return fmt.Errorf("failed to scan execution plan row for SQL ID %s: %w", sqlID, err)
		}

		// Validate the execution plan
		if !plan.IsValidForMetrics() {
			s.logger.Debug("Skipping invalid execution plan",
				zap.String("sql_id", sqlID),
				zap.String("query_id", plan.GetQueryID()),
				zap.Int64("plan_hash_value", plan.GetPlanHashValue()))
			continue
		}

		planCount++
		s.logger.Debug("Processed execution plan",
			zap.Int("plan_number", planCount),
			zap.String("database_name", plan.GetDatabaseName()),
			zap.String("query_id", plan.GetQueryID()),
			zap.Int64("plan_hash_value", plan.GetPlanHashValue()),
			zap.Int("plan_text_length", len(plan.GetExecutionPlanText())))

		// Build and emit metrics for the execution plan
		s.buildExecutionPlanMetrics(&plan)
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during execution plan rows iteration for SQL ID %s: %w", sqlID, err)
	}

	s.logger.Debug("Successfully processed execution plan for SQL ID",
		zap.String("sql_id", sqlID),
		zap.Int("plans_found", planCount),
		zap.Duration("processing_time", time.Since(startTime)))

	if planCount == 0 {
		s.logger.Debug("No execution plans found for SQL ID", zap.String("sql_id", sqlID))
	}

	return nil
}

// buildExecutionPlanMetrics creates metrics from execution plan data
func (s *ExecutionPlanScraper) buildExecutionPlanMetrics(plan *models.ExecutionPlan) {
	// Only build metrics if execution plan metrics are enabled
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled {
		s.logger.Debug("Execution plan metrics disabled, skipping metric recording")
		return
	}

	s.logger.Debug("Building execution plan metrics",
		zap.String("query_id", plan.GetQueryID()),
		zap.Int64("plan_hash_value", plan.GetPlanHashValue()))

	// Record execution plan info metric with DBMS_XPLAN text output
	s.mb.RecordNewrelicoracledbExecutionPlanInfoDataPoint(
		pcommon.NewTimestampFromTime(time.Now()), // Current timestamp
		1,                                        // Set to 1 to indicate presence of execution plan
		plan.GetDatabaseName(),
		plan.GetQueryID(),
		fmt.Sprintf("%d", plan.GetPlanHashValue()),
		plan.GetExecutionPlanText(), // This contains the DBMS_XPLAN.DISPLAY_CURSOR output
	)
}
