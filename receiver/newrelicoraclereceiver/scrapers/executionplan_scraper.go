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
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

func NewExecutionPlanScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

func (s *ExecutionPlanScraper) ScrapeExecutionPlans(ctx context.Context, sqlIdentifiers []models.SQLIdentifier) []error {
	var errs []error

	if len(sqlIdentifiers) == 0 {
		return errs
	}

	// Single timeout context for all queries to prevent excessive total runtime
	// If processing many SQL identifiers, this ensures we don't exceed a reasonable total time
	totalTimeout := 30 * time.Second // Adjust based on your needs
	queryCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for _, identifier := range sqlIdentifiers {
		// Check if context is already cancelled/timed out before proceeding
		select {
		case <-queryCtx.Done():
			errs = append(errs, fmt.Errorf("context cancelled/timed out, stopping execution plan scraping"))
			return errs
		default:
			// Continue processing
		}

		// Query execution plan for specific SQL_ID and CHILD_NUMBER
		s.logger.Debug("Querying execution plan",
			zap.String("sql_id", identifier.SQLID),
			zap.Int64("child_number", identifier.ChildNumber))

		planRows, err := s.client.QueryExecutionPlanForChild(queryCtx, identifier.SQLID, identifier.ChildNumber)
		if err != nil {
			s.logger.Warn("Failed to query execution plan",
				zap.String("sql_id", identifier.SQLID),
				zap.Int64("child_number", identifier.ChildNumber),
				zap.Error(err))
			errs = append(errs, fmt.Errorf("failed to query execution plan for SQL_ID %s, CHILD_NUMBER %d: %w", identifier.SQLID, identifier.ChildNumber, err))
			continue // Skip this identifier but continue processing others
		}

		s.logger.Debug("Retrieved execution plan rows",
			zap.String("sql_id", identifier.SQLID),
			zap.Int64("child_number", identifier.ChildNumber),
			zap.Int("row_count", len(planRows)))

		successCount := 0
		for _, row := range planRows {
			if !row.SQLID.Valid || row.SQLID.String == "" {
				s.logger.Debug("Skipping row with invalid SQL_ID")
				continue
			}

			// Pass the timestamp from the identifier (when the query was captured)
			if err := s.buildExecutionPlanMetrics(&row, identifier.Timestamp); err != nil {
				s.logger.Warn("Failed to build metrics for execution plan row",
					zap.String("sql_id", row.SQLID.String),
					zap.Error(err))
				errs = append(errs, err)
			} else {
				successCount++
			}
		}

		s.logger.Info("Scraped execution plan rows for SQL_ID",
			zap.String("sql_id", identifier.SQLID),
			zap.Int64("child_number", identifier.ChildNumber),
			zap.Int("total_rows", len(planRows)),
			zap.Int("successful_metrics", successCount))
	}

	return errs
}

// buildExecutionPlanMetrics converts an execution plan row to a metric data point with all attributes.
func (s *ExecutionPlanScraper) buildExecutionPlanMetrics(row *models.ExecutionPlanRow, queryTimestamp time.Time) error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlan.Enabled {
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

	planGeneratedTimestamp := ""
	if row.Timestamp.Valid {
		planGeneratedTimestamp = row.Timestamp.String
	}

	// Convert queryTimestamp to string for the timestamp attribute
	queryTimestampStr := queryTimestamp.Format(time.RFC3339)

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

	s.mb.RecordNewrelicoracledbExecutionPlanDataPoint(
		pcommon.NewTimestampFromTime(queryTimestamp),
		int64(1), // Value of 1 to indicate this execution plan step exists
		queryID,
		planHashValue,
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
		queryTimestampStr,
		planGeneratedTimestamp,
		tempSpace,
		accessPredicates,
		projection,
		timeVal,
		filterPredicates,
	)

	return nil
}

// parseIntSafe safely parses a string to int64, handling overflow cases.

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
