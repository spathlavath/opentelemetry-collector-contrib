// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// sqlIdentifierCacheEntry stores when a SQL identifier was last scraped
type sqlIdentifierCacheEntry struct {
	lastScraped time.Time
}

type ExecutionPlanScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
	cache                map[string]*sqlIdentifierCacheEntry // key: "sql_id:child_number"
	cacheMutex           sync.RWMutex
	cacheTTL             time.Duration
}

func NewExecutionPlanScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig) *ExecutionPlanScraper {
	return &ExecutionPlanScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
		cache:                make(map[string]*sqlIdentifierCacheEntry),
		cacheTTL:             10 * time.Minute, // 10-minute window
	}
}

func (s *ExecutionPlanScraper) ScrapeExecutionPlans(ctx context.Context, sqlIdentifiers []models.SQLIdentifier) []error {
	var errs []error

	if len(sqlIdentifiers) == 0 {
		return errs
	}

	now := time.Now()

	// Filter out cached SQL identifiers (already scraped within TTL window)
	var newIdentifiers []models.SQLIdentifier
	var skippedCount int

	s.cacheMutex.RLock()
	for _, identifier := range sqlIdentifiers {
		cacheKey := s.getCacheKey(identifier.SQLID, identifier.ChildNumber)
		if entry, exists := s.cache[cacheKey]; exists {
			// Check if entry is still within TTL window
			if now.Sub(entry.lastScraped) < s.cacheTTL {
				skippedCount++
				s.logger.Debug("Skipping cached SQL identifier",
					zap.String("sql_id", identifier.SQLID),
					zap.Int64("child_number", identifier.ChildNumber),
					zap.Duration("age", now.Sub(entry.lastScraped)))
				continue
			}
		}
		newIdentifiers = append(newIdentifiers, identifier)
	}
	s.cacheMutex.RUnlock()

	s.logger.Info("Execution plan cache filtering",
		zap.Int("total_identifiers", len(sqlIdentifiers)),
		zap.Int("cached_skipped", skippedCount),
		zap.Int("new_to_scrape", len(newIdentifiers)))

	if len(newIdentifiers) == 0 {
		s.logger.Debug("All SQL identifiers are cached, skipping execution plan scraping")
		return errs
	}

	// Single timeout context for all queries to prevent excessive total runtime
	// If processing many SQL identifiers, this ensures we don't exceed a reasonable total time
	totalTimeout := 30 * time.Second // Adjust based on your needs
	queryCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	for _, identifier := range newIdentifiers {
		// Check if context is already cancelled/timed out before proceeding
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
			continue // Skip this identifier but continue processing others
		}

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

		// Add to cache after successful scraping
		cacheKey := s.getCacheKey(identifier.SQLID, identifier.ChildNumber)
		s.cacheMutex.Lock()
		s.cache[cacheKey] = &sqlIdentifierCacheEntry{
			lastScraped: now,
		}
		s.cacheMutex.Unlock()

		s.logger.Info("Scraped execution plan rows for SQL_ID")
	}

	// Clean up stale cache entries (older than TTL)
	s.cleanupCache(now)

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

// getCacheKey generates a unique cache key for a SQL identifier
func (s *ExecutionPlanScraper) getCacheKey(sqlID string, childNumber int64) string {
	return fmt.Sprintf("%s:%d", sqlID, childNumber)
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
