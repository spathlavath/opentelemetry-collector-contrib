// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// ExecutionPlanCache provides deduplication caching for execution plans
// to prevent sending the same plan repeatedly within a TTL window.
// Cache key is plan_handle, which uniquely identifies a cached execution plan in SQL Server.
// Multiple queries may share the same execution plan (same plan_handle), so using only
// plan_handle as the key eliminates duplicate fetches when queries share plans.
type ExecutionPlanCache struct {
	cache  map[string]time.Time // key = "plan_handle", value = last sent timestamp
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
func (c *ExecutionPlanCache) ShouldEmit(planHandle string) bool {
	if planHandle == "" {
		c.logger.Debug("Empty plan_handle, skipping cache check")
		return true // Always emit if we don't have proper identifiers
	}

	// Use write lock for entire operation to prevent race conditions
	// where multiple goroutines could fetch the same plan simultaneously
	c.mu.Lock()
	defer c.mu.Unlock()

	lastSent, exists := c.cache[planHandle]

	now := time.Now()

	if exists {
		age := time.Since(lastSent)

		if age < c.ttl {
			// Still within TTL - skip emission
			c.logger.Debug("Execution plan cache hit, skipping fetch",
				zap.String("plan_handle", planHandle),
				zap.Duration("age", age))
			return false
		}
		// TTL expired - will emit and update timestamp
		c.logger.Debug("Execution plan cache expired, will re-fetch",
			zap.String("plan_handle", planHandle),
			zap.Duration("age", age))
	}

	// First time or expired - mark as sent and return true
	c.cache[planHandle] = now

	return true
}

// CleanupStaleEntries removes expired entries from the cache to prevent unbounded growth
func (c *ExecutionPlanCache) CleanupStaleEntries() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, timestamp := range c.cache {
		age := now.Sub(timestamp)
		if age > c.ttl {
			delete(c.cache, key)
			removed++
		}
	}

	if removed > 0 {
		c.logger.Debug("Removed expired execution plan cache entries",
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
