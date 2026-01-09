// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
)

// PlanHandleCache is a thread-safe cache for storing plan handles AND active queries from the metric pipeline
// to be used by the log pipeline, eliminating duplicate database queries
type PlanHandleCache struct {
	mu             sync.RWMutex
	planDataMap    map[string]models.SlowQueryPlanData // Key: query_hash (as string), Value: plan data
	activeQueries  []models.ActiveRunningQuery         // Active queries found in metric pipeline
	lastUpdateTime time.Time
	queryIDs       []string // Ordered list of query IDs for easy iteration
}

// NewPlanHandleCache creates a new thread-safe plan handle cache
func NewPlanHandleCache() *PlanHandleCache {
	return &PlanHandleCache{
		planDataMap:   make(map[string]models.SlowQueryPlanData),
		activeQueries: make([]models.ActiveRunningQuery, 0),
		queryIDs:      make([]string, 0),
	}
}

// Update replaces the entire cache with new data from the metric pipeline
// This is called after each successful metric scrape
func (c *PlanHandleCache) Update(queryIDs []string, planDataMap map[string]models.SlowQueryPlanData, activeQueries []models.ActiveRunningQuery) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Replace the entire cache with fresh data
	c.planDataMap = planDataMap
	c.queryIDs = queryIDs
	c.activeQueries = activeQueries
	c.lastUpdateTime = time.Now()
}

// GetAll returns a copy of all cached plan data and active queries
// This is called by the log pipeline to retrieve plan handles and active queries
func (c *PlanHandleCache) GetAll() ([]string, map[string]models.SlowQueryPlanData, []models.ActiveRunningQuery) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return copies to prevent external modification
	queryIDsCopy := make([]string, len(c.queryIDs))
	copy(queryIDsCopy, c.queryIDs)

	planDataMapCopy := make(map[string]models.SlowQueryPlanData, len(c.planDataMap))
	for k, v := range c.planDataMap {
		planDataMapCopy[k] = v
	}

	activeQueriesCopy := make([]models.ActiveRunningQuery, len(c.activeQueries))
	copy(activeQueriesCopy, c.activeQueries)

	return queryIDsCopy, planDataMapCopy, activeQueriesCopy
}

// GetActiveQueries returns a copy of cached active queries only
func (c *PlanHandleCache) GetActiveQueries() []models.ActiveRunningQuery {
	c.mu.RLock()
	defer c.mu.RUnlock()

	activeQueriesCopy := make([]models.ActiveRunningQuery, len(c.activeQueries))
	copy(activeQueriesCopy, c.activeQueries)
	return activeQueriesCopy
}

// Get returns plan data for a specific query ID
func (c *PlanHandleCache) Get(queryID string) (models.SlowQueryPlanData, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	planData, exists := c.planDataMap[queryID]
	return planData, exists
}

// GetLastUpdateTime returns the timestamp of the last cache update
func (c *PlanHandleCache) GetLastUpdateTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastUpdateTime
}

// Size returns the number of cached plan handles
func (c *PlanHandleCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.planDataMap)
}

// Clear removes all entries from the cache
func (c *PlanHandleCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.planDataMap = make(map[string]models.SlowQueryPlanData)
	c.queryIDs = make([]string, 0)
	c.lastUpdateTime = time.Time{}
}
