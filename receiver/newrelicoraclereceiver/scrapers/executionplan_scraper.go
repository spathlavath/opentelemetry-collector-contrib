// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

type ExecutionPlanScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

func NewExecutionPlanScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

func (s *ExecutionPlanScraper) ScrapeExecutionPlans(ctx context.Context, sqlIDs []string) []error {
	var errs []error

	if len(sqlIDs) == 0 {
		return errs
	}

	successCount := 0
	for i, sqlID := range sqlIDs {
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

		select {
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("execution plan processing cancelled: %w", ctx.Err()))
			break
		default:
		}
	}

	s.logger.Debug("Scraped execution plans",
		zap.Int("total", len(sqlIDs)),
		zap.Int("success", successCount),
		zap.Int("failed", len(errs)))

	return errs
}

func (s *ExecutionPlanScraper) processSingleSQLID(ctx context.Context, sqlID string, index int) error {
	query := queries.GetExecutionPlanQuery(sqlID)

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

	var executionPlan models.ExecutionPlan
	var planLines []string
	rowCount := 0

	for rows.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("execution plan processing cancelled for SQL ID %s: %w", sqlID, ctx.Err())
		default:
		}

		var databaseName, queryID, planLine sql.NullString
		var planHashValue sql.NullInt64

		err := rows.Scan(
			&databaseName,
			&queryID,
			&planHashValue,
			&planLine,
		)
		if err != nil {
			return fmt.Errorf("failed to scan execution plan row for SQL ID %s: %w", sqlID, err)
		}

		rowCount++

		if rowCount == 1 {
			executionPlan.DatabaseName = databaseName
			executionPlan.QueryID = queryID
			executionPlan.PlanHashValue = planHashValue
		}

		if planLine.Valid && planLine.String != "" {
			planLines = append(planLines, planLine.String)
		}
	}

	if len(planLines) > 0 {
		completePlanText := strings.Join(planLines, "\n")

		trimmedPlanText := s.trimExecutionPlanText(completePlanText)

		executionPlan.ExecutionPlanText = sql.NullString{
			String: trimmedPlanText,
			Valid:  true,
		}

		if !executionPlan.IsValidForMetrics() {
			s.logger.Debug("Skipping invalid execution plan",
				zap.String("sql_id", sqlID),
				zap.String("query_id", executionPlan.GetQueryID()),
				zap.Int64("plan_hash_value", executionPlan.GetPlanHashValue()))
		} else {
			s.buildExecutionPlanMetrics(&executionPlan)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during execution plan rows iteration for SQL ID %s: %w", sqlID, err)
	}

	s.logger.Debug("Processed execution plan", zap.String("sql_id", sqlID), zap.Int("lines", len(planLines)))

	return nil
}

func (s *ExecutionPlanScraper) buildExecutionPlanMetrics(plan *models.ExecutionPlan) {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled {
		return
	}

	s.mb.RecordNewrelicoracledbExecutionPlanInfoDataPoint(
		pcommon.NewTimestampFromTime(time.Now()),
		1,
		plan.GetDatabaseName(),
		plan.GetQueryID(),
		fmt.Sprintf("%d", plan.GetPlanHashValue()),
		plan.GetExecutionPlanText(),
	)
}

func (s *ExecutionPlanScraper) trimExecutionPlanText(planText string) string {
	planHashIndex := strings.Index(planText, "Plan hash value:")
	if planHashIndex == -1 {
		return planText
	}

	trimmedText := planText[planHashIndex:]

	nextSQLIndex := strings.Index(trimmedText, "\nSQL_ID")
	if nextSQLIndex > 0 {
		trimmedText = trimmedText[:nextSQLIndex]
	}

	return strings.TrimSpace(trimmedText)
}
