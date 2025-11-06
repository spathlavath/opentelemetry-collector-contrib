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
	client            client.OracleClient
	lb                *metadata.LogsBuilder
	logger            *zap.Logger
	instanceName      string
	logsBuilderConfig metadata.LogsBuilderConfig
}

func NewExecutionPlanScraper(oracleClient client.OracleClient, lb *metadata.LogsBuilder, logger *zap.Logger, instanceName string, logsBuilderConfig metadata.LogsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		client:            oracleClient,
		lb:                lb,
		logger:            logger,
		instanceName:      instanceName,
		logsBuilderConfig: logsBuilderConfig,
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

	planRows, err := s.client.QueryExecutionPlanRows(queryCtx, sqlIDsParam)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to query execution plans: %w", err))
		return errs
	}

	s.logger.Debug("Retrieved execution plan rows",
		zap.Int("count", len(planRows)))

	successCount := 0
	for _, row := range planRows {
		if !row.SQLID.Valid || row.SQLID.String == "" {
			continue
		}

		if err := s.buildExecutionPlanLogs(&row); err != nil {
			s.logger.Warn("Failed to build logs for execution plan row",
				zap.String("sql_id", row.SQLID.String),
				zap.Error(err))
			errs = append(errs, err)
		} else {
			successCount++
		}
	}

	s.logger.Debug("Scraped execution plan rows",
		zap.Int("total", len(planRows)),
		zap.Int("success", successCount),
		zap.Int("failed", len(errs)))

	return errs
}

// buildExecutionPlanLogs converts an execution plan to a log event with hierarchical JSON structure.
// The execution plan is sent as a plain JSON string with the hierarchical tree structure preserved.
// This allows for direct querying in New Relic without needing to decode or decompress.
// The hierarchical structure makes it easier to understand the query execution flow compared to flat arrays.
func (s *ExecutionPlanScraper) buildExecutionPlanLogs(plan *models.ExecutionPlan) error {
	if !s.logsBuilderConfig.Events.NewrelicoracledbExecutionPlan.Enabled {
		return nil
	}

	// Convert execution plan to JSON with hierarchical structure
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("failed to marshal execution plan to JSON: %w", err)
	}

	// Send as plain JSON string - this preserves the hierarchical tree structure
	// and makes it directly queryable in New Relic
	executionPlanStr := string(planJSON)

	// Note: query_text should be provided by the caller (e.g., QPM scraper)
	// For now, we use the SQL_ID as the query_id
	s.lb.RecordNewrelicoracledbExecutionPlanEvent(
		context.Background(),
		pcommon.NewTimestampFromTime(time.Now()),
		plan.SQLID,                            // query_id
		fmt.Sprintf("%d", plan.PlanHashValue), // plan_hash_value
		"",                                    // query_text (empty for now, should be provided by caller)
		executionPlanStr,                      // execution_plan_json as hierarchical JSON string
	)

	return nil
}
