// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"encoding/json"
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

	// Format SQL IDs for IN clause: 'id1', 'id2', 'id3'
	quotedIDs := make([]string, len(sqlIDs))
	for i, id := range sqlIDs {
		quotedIDs[i] = fmt.Sprintf("'%s'", id)
	}
	sqlIDsParam := strings.Join(quotedIDs, ", ")

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	executionPlans, err := s.client.QueryExecutionPlans(queryCtx, sqlIDsParam)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to query execution plans: %w", err))
		return errs
	}

	s.logger.Info("Retrieved execution plans from database",
		zap.Int("count", len(executionPlans)),
		zap.Strings("sql_ids", sqlIDs))

	// Log each execution plan summary
	for i, ep := range executionPlans {
		s.logger.Debug("Execution plan details",
			zap.Int("index", i),
			zap.String("sql_id", ep.SQLID),
			zap.Int64("child_number", ep.ChildNumber),
			zap.Int64("plan_hash_value", ep.PlanHashValue),
			zap.Bool("has_plan_tree", ep.PlanTree != nil))
	}

	successCount := 0
	for _, executionPlan := range executionPlans {
		if executionPlan.SQLID == "" || executionPlan.PlanTree == nil {
			s.logger.Debug("Skipping invalid execution plan",
				zap.String("sql_id", executionPlan.SQLID))
			continue
		}

		if err := s.buildExecutionPlanMetrics(&executionPlan); err != nil {
			s.logger.Warn("Failed to build metrics for execution plan",
				zap.String("sql_id", executionPlan.SQLID),
				zap.Error(err))
			errs = append(errs, err)
		} else {
			successCount++
		}
	}

	s.logger.Debug("Scraped execution plans",
		zap.Int("total", len(executionPlans)),
		zap.Int("success", successCount),
		zap.Int("failed", len(errs)))

	return errs
}

func (s *ExecutionPlanScraper) buildExecutionPlanMetrics(plan *models.ExecutionPlan) error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled {
		return nil
	}

	// Convert execution plan to JSON
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("failed to marshal execution plan to JSON: %w", err)
	}

	s.logger.Info("Execution plan JSON generated",
		zap.String("sql_id", plan.SQLID),
		zap.Int64("plan_hash_value", plan.PlanHashValue),
		zap.String("json", string(planJSON)))

	s.mb.RecordNewrelicoracledbExecutionPlanInfoDataPoint(
		pcommon.NewTimestampFromTime(time.Now()),
		1,
		s.instanceName,
		plan.SQLID,
		fmt.Sprintf("%d", plan.PlanHashValue),
		string(planJSON),
	)

	return nil
}
