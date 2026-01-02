// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
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

	// Timeout for all execution plan queries - allow more time for multiple queries
	// Each query can take a few seconds, so budget accordingly
	totalTimeout := 2 * time.Minute // Allow 2 minutes total for all execution plans
	queryCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	s.logger.Info("Starting execution plan scraping",
		zap.Int("total_identifiers", len(sqlIdentifiers)),
		zap.Duration("timeout", totalTimeout))

	for i, identifier := range sqlIdentifiers {
		// Check if context is already cancelled/timed out before proceeding
		select {
		case <-queryCtx.Done():
			s.logger.Warn("Context timeout reached during execution plan scraping",
				zap.Int("processed", i),
				zap.Int("total", len(sqlIdentifiers)))
			errs = append(errs, fmt.Errorf("context cancelled/timed out after processing %d of %d identifiers", i, len(sqlIdentifiers)))
			return errs
		default:
			// Continue processing
		}

		// Query execution plan for specific SQL_ID and CHILD_NUMBER
		s.logger.Debug("Querying execution plan",
			zap.Int("progress", i+1),
			zap.Int("total", len(sqlIdentifiers)),
			zap.String("sql_id", identifier.SQLID),
			zap.Int64("child_number", identifier.ChildNumber))

		planRows, err := s.client.QueryExecutionPlanForChild(queryCtx, identifier.SQLID, identifier.ChildNumber)
		if err != nil {
			s.logger.Error("Failed to query execution plan",
				zap.String("sql_id", identifier.SQLID),
				zap.Int64("child_number", identifier.ChildNumber),
				zap.Error(err))
			errs = append(errs, fmt.Errorf("failed to query execution plan for SQL_ID %s, CHILD_NUMBER %d: %w", identifier.SQLID, identifier.ChildNumber, err))
			continue // Skip this identifier but continue processing others
		}
		s.logger.Debug("Retrieved execution plan rows",
			zap.String("sql_id", identifier.SQLID),
			zap.Int("row_count", len(planRows)))

		successCount := 0
		for _, row := range planRows {
			if !row.SQLID.Valid || row.SQLID.String == "" {
				continue
			}

			// Pass the timestamp from the identifier (when the query was captured)
			if err := s.recordExecutionPlanMetrics(&row, identifier.Timestamp); err != nil {
				s.logger.Warn("Failed to record metrics for execution plan row",
					zap.String("sql_id", row.SQLID.String),
					zap.Error(err))
				errs = append(errs, err)
			} else {
				successCount++
			}
		}

		s.logger.Debug("Scraped execution plan rows",
			zap.String("sql_id", identifier.SQLID),
			zap.Int("success_count", successCount))
	}

	s.logger.Info("Execution plan scraping completed",
		zap.Int("sql_identifiers_processed", len(sqlIdentifiers)),
		zap.Int("errors", len(errs)))

	return errs
}

// recordExecutionPlanMetrics converts an execution plan row to metrics with individual attributes.
func (s *ExecutionPlanScraper) recordExecutionPlanMetrics(row *models.ExecutionPlanRow, queryTimestamp time.Time) error {
	queryID := ""
	if row.SQLID.Valid {
		queryID = row.SQLID.String
	}

	planGeneratedTimestamp := ""
	if row.Timestamp.Valid {
		planGeneratedTimestamp = row.Timestamp.String
	}

	tempSpace := int64(-1)
	if row.TempSpace.Valid && row.TempSpace.String != "" {
		tempSpace = commonutils.ParseIntSafe(row.TempSpace.String, s.logger)
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
	if row.Time.Valid && row.Time.String != "" {
		timeVal = commonutils.ParseIntSafe(row.Time.String, s.logger)
	}

	filterPredicates := ""
	if row.FilterPredicates.Valid {
		filterPredicates = row.FilterPredicates.String
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

	planHashValue := ""
	if row.PlanHashValue.Valid {
		planHashValue = fmt.Sprintf("%d", row.PlanHashValue.Int64)
	}

	cost := int64(-1)
	if row.Cost.Valid && row.Cost.String != "" {
		cost = commonutils.ParseIntSafe(row.Cost.String, s.logger)
	}

	cardinality := int64(-1)
	if row.Cardinality.Valid && row.Cardinality.String != "" {
		cardinality = commonutils.ParseIntSafe(row.Cardinality.String, s.logger)
	}

	bytes := int64(-1)
	if row.Bytes.Valid && row.Bytes.String != "" {
		bytes = commonutils.ParseIntSafe(row.Bytes.String, s.logger)
	}

	cpuCost := int64(-1)
	if row.CPUCost.Valid && row.CPUCost.String != "" {
		cpuCost = commonutils.ParseIntSafe(row.CPUCost.String, s.logger)
	}

	ioCost := int64(-1)
	if row.IOCost.Valid && row.IOCost.String != "" {
		ioCost = commonutils.ParseIntSafe(row.IOCost.String, s.logger)
	}

	// Convert query timestamp to string for attribute
	queryTimestampStr := queryTimestamp.Format(time.RFC3339)

	// Use cpuCost as the metric value; if not available, use 0
	metricValue := int64(0)
	if cpuCost >= 0 {
		metricValue = cpuCost
	}

	s.mb.RecordNewrelicoracledbExecutionPlanDataPoint(
		pcommon.NewTimestampFromTime(queryTimestamp),
		metricValue,
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

	s.logger.Debug("Execution plan collected")

	return nil
}
