// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
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
	cache                map[string]*planHashCacheEntry // key: sql_id_childnumber
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
		cacheTTL:             24 * time.Hour, // 24-hour window
	}
}

func (s *ExecutionPlanScraper) ScrapeExecutionPlans(ctx context.Context, sqlIdentifiers []models.SQLIdentifier) []error {
	var errs []error
	if len(sqlIdentifiers) == 0 {
		return errs
	}
	now := time.Now()
	s.cleanupCache(now)

	totalTimeout := 30 * time.Second // Adjust based on your needs
	queryCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for _, identifier := range sqlIdentifiers {
		select {
		case <-queryCtx.Done():
			errs = append(errs, errors.New("context cancelled/timed out, stopping execution plan scraping"))
			return errs
		default:
			// Continue processing
		}

		if identifier.PlanHash != "" {
			cacheKey := fmt.Sprintf("%s_%d", identifier.PlanHash, identifier.ChildNumber)
			s.cacheMutex.RLock()
			_, exists := s.cache[cacheKey]
			s.cacheMutex.RUnlock()
			if exists {
				s.logger.Debug("skipping execution plan scrape for cached SQL_ID",
					zap.String("sql_id", identifier.SQLID),
					zap.Int64("child_number", identifier.ChildNumber),
					zap.String("plan_hash", identifier.PlanHash))
				continue
			}
		}

		planRows, err := s.client.QueryExecutionPlanForChild(queryCtx, identifier.SQLID, identifier.ChildNumber)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to query execution plan for SQL_ID %s, CHILD_NUMBER %d: %w", identifier.SQLID, identifier.ChildNumber, err))
			continue
		}
		if len(planRows) == 0 {
			continue
		}
		for i := range planRows {
			row := &planRows[i]
			if !row.SQLID.Valid || row.SQLID.String == "" {
				s.logger.Debug("Skipping row with invalid SQL_ID")
				continue
			}
			s.buildExecutionPlanMetrics(row, identifier.Timestamp)
		}

		// Validate plan hash and child number before caching
		if !planRows[0].PlanHashValue.Valid || !planRows[0].ChildNumber.Valid {
			s.logger.Debug("Cannot cache - missing plan hash or child number in result",
				zap.String("sql_id", identifier.SQLID))
			continue
		}

		// Use string format to match the cache lookup format
		cacheKey := fmt.Sprintf("%s_%d", strconv.FormatInt(planRows[0].PlanHashValue.Int64, 10), planRows[0].ChildNumber.Int64)

		// Add SQL_ID to cache after successful scraping
		s.cacheMutex.Lock()
		s.cache[cacheKey] = &planHashCacheEntry{
			lastScraped: now,
		}
		s.cacheMutex.Unlock()
	}

	return errs
}

// buildExecutionPlanMetrics converts an execution plan row to a metric data point with all attributes.
func (s *ExecutionPlanScraper) buildExecutionPlanMetrics(row *models.ExecutionPlanRow, queryTimestamp time.Time) {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlan.Enabled {
		return
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
	if row.PlanGeneratedTimestamp.Valid {
		planGeneratedTimestamp = row.PlanGeneratedTimestamp.String
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

	s.mb.RecordNewrelicoracledbExecutionPlanDataPoint(
		pcommon.NewTimestampFromTime(queryTimestamp),
		int64(1), // Value of 1 to indicate this execution plan step exists
		"OracleExecutionPlan",
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
		planGeneratedTimestamp,
		tempSpace,
		accessPredicates,
		projection,
		timeVal,
		filterPredicates,
	)
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
