// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

type ExecutionPlanScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

func NewExecutionPlanScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		client:               oracleClient,
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
	executionPlans, err := s.client.QueryExecutionPlans(ctx, sqlID)
	if err != nil {
		return fmt.Errorf("failed to query execution plan for SQL ID %s: %w", sqlID, err)
	}

	s.logger.Debug("Retrieved execution plans",
		zap.String("sql_id", sqlID),
		zap.Int("count", len(executionPlans)))

	for _, executionPlan := range executionPlans {
		trimmedPlanText := s.trimExecutionPlanText(executionPlan.ExecutionPlanText.String)

		plan := executionPlan
		plan.ExecutionPlanText.String = trimmedPlanText

		if !plan.IsValidForMetrics() {
			s.logger.Debug("Skipping invalid execution plan",
				zap.String("sql_id", sqlID),
				zap.String("query_id", plan.GetQueryID()),
				zap.Int64("plan_hash_value", plan.GetPlanHashValue()))
		} else {
			s.buildExecutionPlanMetrics(&plan)
		}
	}

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
