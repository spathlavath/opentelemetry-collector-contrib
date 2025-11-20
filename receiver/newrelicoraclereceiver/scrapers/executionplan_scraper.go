// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
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

	for _, id := range sqlIDs {
		// Create a separate timeout for each SQL ID query
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		planRows, err := s.client.QueryExecutionPlanRows(queryCtx, id)
		cancel() // Clean up immediately after query
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to query execution plans for SQL_ID %s: %w", id, err))
			continue // Skip this SQL ID but continue processing others
		}
		s.logger.Debug("Retrieved execution plan rows",
			zap.String("sql_id", id),
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
			zap.String("sql_id", id),
			zap.Int("total", len(planRows)),
			zap.Int("success", successCount),
			zap.Int("failed", len(errs)))
	}

	return errs
}

// buildExecutionPlanLogs converts an execution plan row to a log event with individual attributes.
// Each field from V$SQL_PLAN is sent as a separate attribute for direct querying in New Relic.
func (s *ExecutionPlanScraper) buildExecutionPlanLogs(row *models.ExecutionPlanRow) error {
	if !s.logsBuilderConfig.Events.NewrelicoracledbExecutionPlan.Enabled {
		return nil
	}

	// Extract values with defaults for null fields
	queryID := ""
	if row.SQLID.Valid {
		queryID = row.SQLID.String
	}

	planHashValue := ""
	if row.PlanHashValue.Valid {
		planHashValue = fmt.Sprintf("%d", row.PlanHashValue.Int64)
	}

	queryText := "" // Empty for now, should be provided by caller

	childNumber := int64(-1)
	if row.ChildNumber.Valid {
		childNumber = row.ChildNumber.Int64
	}

	planID := int64(-1)
	if row.ID.Valid {
		planID = row.ID.Int64
	}

	parentID := int64(-1)
	if row.ParentID.Valid {
		parentID = row.ParentID.Int64
	}

	depth := int64(-1)
	if row.Depth.Valid {
		depth = row.Depth.Int64
	}

	operation := ""
	if row.Operation.Valid {
		operation = row.Operation.String
	}

	options := ""
	if row.Options.Valid {
		options = row.Options.String
	}

	objectOwner := ""
	if row.ObjectOwner.Valid {
		objectOwner = row.ObjectOwner.String
	}

	objectName := ""
	if row.ObjectName.Valid {
		objectName = row.ObjectName.String
	}

	position := int64(-1)
	if row.Position.Valid {
		position = row.Position.Int64
	}

	cost := int64(-1)
	if row.Cost.Valid && row.Cost.String != "" {
		cost = s.parseIntSafe(row.Cost.String)
	}

	cardinality := int64(-1)
	if row.Cardinality.Valid && row.Cardinality.String != "" {
		cardinality = s.parseIntSafe(row.Cardinality.String)
	}

	bytes := int64(-1)
	if row.Bytes.Valid && row.Bytes.String != "" {
		bytes = s.parseIntSafe(row.Bytes.String)
	}

	cpuCost := int64(-1)
	if row.CPUCost.Valid && row.CPUCost.String != "" {
		cpuCost = s.parseIntSafe(row.CPUCost.String)
	}

	ioCost := int64(-1)
	if row.IOCost.Valid && row.IOCost.String != "" {
		ioCost = s.parseIntSafe(row.IOCost.String)
	}

	timestamp := ""
	if row.Timestamp.Valid {
		timestamp = row.Timestamp.String
	}

	tempSpace := int64(-1)
	if row.TempSpace.Valid && row.TempSpace.String != "" {
		tempSpace = s.parseIntSafe(row.TempSpace.String)
	}

	accessPredicates := ""
	if row.AccessPredicates.Valid {
		accessPredicates = commonutils.AnonymizeAndNormalize(row.AccessPredicates.String)
	}

	projection := ""
	if row.Projection.Valid {
		projection = row.Projection.String
	}

	timeVal := int64(-1)
	if row.Time.Valid && row.Time.String != "" {
		timeVal = s.parseIntSafe(row.Time.String)
	}

	filterPredicates := ""
	if row.FilterPredicates.Valid {
		filterPredicates = commonutils.AnonymizeAndNormalize(row.FilterPredicates.String)
	}

	// Record the event with all attributes
	s.lb.RecordNewrelicoracledbExecutionPlanEvent(
		context.Background(),
		pcommon.NewTimestampFromTime(time.Now()),
		queryID,
		planHashValue,
		queryText,
		childNumber,
		planID,
		parentID,
		depth,
		operation,
		options,
		objectOwner,
		objectName,
		position,
		cost,
		cardinality,
		bytes,
		cpuCost,
		ioCost,
		timestamp,
		tempSpace,
		accessPredicates,
		projection,
		timeVal,
		filterPredicates,
	)

	return nil
}

// parseIntSafe safely parses a string to int64, handling overflow cases.
// If the value exceeds int64 max, it returns the max int64 value.
// If parsing fails, it returns -1.
func (s *ExecutionPlanScraper) parseIntSafe(value string) int64 {
	if value == "" {
		return -1
	}

	// Try to parse as int64
	var result int64
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		// If parsing fails, it's likely a very large number that exceeds int64 max
		// Oracle can return values that exceed int64 max (e.g., 18446744073709551615)
		// In such cases, we'll use int64 max value to indicate an extremely high cost
		s.logger.Debug("Failed to parse large numeric value, using int64 max",
			zap.String("value", value),
			zap.Error(err))
		return math.MaxInt64
	}

	return result
}
