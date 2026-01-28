// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// planHashCacheEntry stores when a plan hash value was last scraped
type planHashCacheEntry struct {
	lastScraped time.Time
}

type ExecutionPlanScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
	cache                map[string]*planHashCacheEntry // key: plan_hash_value
	cacheMutex           sync.RWMutex
	cacheTTL             time.Duration
}

func NewExecutionPlanScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
		cache:                make(map[string]*planHashCacheEntry),
		cacheTTL:             5 * time.Minute, // 5-minute window
	}
}

func (s *ExecutionPlanScraper) ScrapeExecutionPlans(ctx context.Context, sqlIdentifiers []models.SQLIdentifier) []error {
	var errs []error
	if len(sqlIdentifiers) == 0 {
		return errs
	}
	now := time.Now()

	// Clean up expired cache entries first
	s.cleanupCache(now)

	planHashIdentifier := make(map[string]models.SQLIdentifier)
	s.cacheMutex.RLock()
	for _, identifier := range sqlIdentifiers {
		if _, exists := s.cache[identifier.PlanHash]; exists {
			s.logger.Debug("skipping execution plan scrape for cached plan hash",
				zap.String("plan_hash", identifier.PlanHash),
				zap.String("sql_id", identifier.SQLID))
			continue
		}
		planHashIdentifier[identifier.PlanHash] = identifier
	}
	s.cacheMutex.RUnlock()
	if len(planHashIdentifier) == 0 {
		s.logger.Debug("All plan hash values are cached, skipping execution plan scraping")
		return errs
	}
	totalTimeout := 30 * time.Second // Adjust based on your needs
	queryCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for planHash, identifier := range planHashIdentifier {
		select {
		case <-queryCtx.Done():
			errs = append(errs, fmt.Errorf("context cancelled/timed out, stopping execution plan scraping"))
			return errs
		default:
			// Continue processing
		}
		planRows, err := s.client.QueryExecutionPlanForChild(queryCtx, identifier.SQLID, identifier.ChildNumber)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to query execution plan for SQL_ID %s, CHILD_NUMBER %d: %w", identifier.SQLID, identifier.ChildNumber, err))
			continue
		}
		for _, row := range planRows {
			if !row.SQLID.Valid || row.SQLID.String == "" {
				s.logger.Debug("Skipping row with invalid SQL_ID")
				continue
			}
			if err := s.buildExecutionPlanMetrics(&row, identifier.Timestamp); err != nil {
				s.logger.Warn("Failed to build metrics for execution plan row",
					zap.String("sql_id", row.SQLID.String),
					zap.Error(err))
				errs = append(errs, err)
			}
		}

		// Add plan hash value to cache after successful scraping
		s.cacheMutex.Lock()
		s.cache[planHash] = &planHashCacheEntry{
			lastScraped: now,
		}
		s.cacheMutex.Unlock()

		s.logger.Info("Scraped execution plan",
			zap.String("plan_hash", planHash),
			zap.String("sql_id", identifier.SQLID))
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
		"OracleExecutionPlanTest",
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
func (s *ExecutionPlanScraper) parseIntSafe(value string) int64 {
	if value == "" {
		return -1
	}
	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		s.logger.Debug("Failed to parse numeric value",
			zap.String("value", value),
			zap.Error(err))
		return -1
	}
	return result
}

// cleanupCache removes stale entries from the cache (older than TTL)
func (s *ExecutionPlanScraper) cleanupCache(now time.Time) {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	var keysToDelete []string
	for key, entry := range s.cache {
		if now.Sub(entry.lastScraped) >= s.cacheTTL {
			keysToDelete = append(keysToDelete, key)
		}
	}

	for _, key := range keysToDelete {
		delete(s.cache, key)
	}

	if len(keysToDelete) > 0 {
		s.logger.Debug("Cleaned up stale cache entries",
			zap.Int("removed_entries", len(keysToDelete)),
			zap.Int("remaining_entries", len(s.cache)))
	}
}
