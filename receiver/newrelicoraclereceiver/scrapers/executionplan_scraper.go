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
	if row.Cost.Valid {
		cost = row.Cost.Int64
	}

	cardinality := int64(-1)
	if row.Cardinality.Valid {
		cardinality = row.Cardinality.Int64
	}

	bytes := int64(-1)
	if row.Bytes.Valid {
		bytes = row.Bytes.Int64
	}

	cpuCost := int64(-1)
	if row.CPUCost.Valid {
		// Parse CPU cost from string to handle large values
		cpuCost = s.parseCPUCost(row.CPUCost.String)
	}

	ioCost := int64(-1)
	if row.IOCost.Valid {
		ioCost = row.IOCost.Int64
	}

	timestamp := ""
	if row.Timestamp.Valid {
		timestamp = row.Timestamp.String
	}

	tempSpace := int64(-1)
	if row.TempSpace.Valid {
		tempSpace = row.TempSpace.Int64
	}

	accessPredicates := ""
	if row.AccessPredicates.Valid {
		accessPredicates = row.AccessPredicates.String
	}

	projection := ""
	if row.Projection.Valid {
		projection = row.Projection.String
	}

	timeVal := int64(-1)
	if row.Time.Valid {
		timeVal = row.Time.Int64
	}

	filterPredicates := ""
	if row.FilterPredicates.Valid {
		filterPredicates = row.FilterPredicates.String
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

// parseCPUCost parses CPU cost from string to int64, handling overflow cases
// Oracle CPU_COST can be very large (up to NUMBER(38)) and may exceed int64 max
// Returns -1 for invalid/overflow values or Oracle's sentinel value (2^64-1)
func (s *ExecutionPlanScraper) parseCPUCost(cpuCostStr string) int64 {
	// Handle empty string
	if cpuCostStr == "" {
		return -1
	}

	// Try to parse as int64
	var cpuCost int64
	_, err := fmt.Sscanf(cpuCostStr, "%d", &cpuCost)
	if err != nil {
		// If parsing fails or value is out of range, check if it's the sentinel value
		// Oracle uses 18446744073709551615 (2^64-1) to indicate undefined/max cost
		if cpuCostStr == "18446744073709551615" {
			s.logger.Debug("CPU cost is at maximum value (undefined)", zap.String("value", cpuCostStr))
			return -1
		}
		s.logger.Warn("Failed to parse CPU cost, using -1", zap.String("value", cpuCostStr), zap.Error(err))
		return -1
	}

	return cpuCost
}
