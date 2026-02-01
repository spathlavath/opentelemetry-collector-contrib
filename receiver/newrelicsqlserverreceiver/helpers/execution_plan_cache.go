// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ExecutionPlanCache provides deduplication caching for execution plans
// to prevent sending the same plan repeatedly within a TTL window.
// Cache key is composite: (query_hash, plan_handle) to handle cases where
// one query has multiple execution plans.
type ExecutionPlanCache struct {
	cache  map[string]time.Time // key = "query_hash|plan_handle", value = last sent timestamp
	ttl    time.Duration
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewExecutionPlanCache creates a new execution plan cache with hardcoded 24 hour TTL
// TTL is hardcoded because execution plans rarely change and we want to minimize database load
func NewExecutionPlanCache(logger *zap.Logger) *ExecutionPlanCache {
	ttl := 24 * time.Hour // Hardcoded: 24 hours
	return &ExecutionPlanCache{
		cache:  make(map[string]time.Time),
		ttl:    ttl,
		logger: logger,
	}
}

// ShouldEmit checks if an execution plan should be emitted based on cache state.
// Returns true if the plan should be emitted (first time or TTL expired).
// Returns false if the plan was recently sent and is still within TTL.
func (c *ExecutionPlanCache) ShouldEmit(queryHash, planHandle string) bool {
	if queryHash == "" || planHandle == "" {
		c.logger.Debug("Empty query_hash or plan_handle, skipping cache check")
		return true // Always emit if we don't have proper identifiers
	}

	cacheKey := c.buildCacheKey(queryHash, planHandle)

	// Use write lock for entire operation to prevent race conditions
	// where multiple goroutines could fetch the same plan simultaneously
	c.mu.Lock()
	defer c.mu.Unlock()

	lastSent, exists := c.cache[cacheKey]

	if exists {
		age := time.Since(lastSent)
		if age < c.ttl {
			// Still within TTL - skip emission
			c.logger.Info(" Execution plan found in cache, skipping fetch/emit (saving DB query)",
				zap.String("query_hash", queryHash),
				zap.String("plan_handle", planHandle),
				zap.Duration("age", age),
				zap.Duration("ttl", c.ttl))
			return false
		}
		// TTL expired - will emit and update timestamp
		c.logger.Info(" Execution plan TTL expired, will fetch and emit",
			zap.String("query_hash", queryHash),
			zap.String("plan_handle", planHandle),
			zap.Duration("age", age),
			zap.Duration("ttl", c.ttl))
	} else {
		// First time seeing this plan
		c.logger.Info("First time seeing this execution plan, will fetch and emit",
			zap.String("query_hash", queryHash),
			zap.String("plan_handle", planHandle))
	}

	// First time or expired - mark as sent and return true
	c.cache[cacheKey] = time.Now()
	return true
}

// CleanupStaleEntries removes expired entries from the cache to prevent unbounded growth
func (c *ExecutionPlanCache) CleanupStaleEntries() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, timestamp := range c.cache {
		if now.Sub(timestamp) > c.ttl {
			delete(c.cache, key)
			removed++
		}
	}

	if removed > 0 {
		c.logger.Debug("Cleaned up stale execution plan cache entries",
			zap.Int("removed_count", removed),
			zap.Int("remaining_count", len(c.cache)))
	}
}

// GetCacheStats returns statistics about the cache for debugging/monitoring
func (c *ExecutionPlanCache) GetCacheStats() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]int{
		"total_entries": len(c.cache),
	}
}

// buildCacheKey creates a composite cache key from query_hash and plan_handle
func (c *ExecutionPlanCache) buildCacheKey(queryHash, planHandle string) string {
	return fmt.Sprintf("%s|%s", queryHash, planHandle)
}
