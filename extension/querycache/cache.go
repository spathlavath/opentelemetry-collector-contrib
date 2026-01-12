// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package querycache // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache"

import (
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
)

// QueryPerformanceCache is a thread-safe cache for storing query performance data
// from the metric pipeline to be used by the log pipeline, eliminating duplicate database queries
type QueryPerformanceCache struct {
	mu                   sync.RWMutex
	slowQueryIDs         []string                               // Ordered list of query IDs
	slowQueryPlanDataMap map[string]models.SlowQueryPlanData    // Query hash -> plan data
	activeQueries        []models.ActiveRunningQuery            // Active queries found in metric pipeline
	lastUpdateTime       time.Time
}

// NewQueryPerformanceCache creates a new thread-safe query performance cache
func NewQueryPerformanceCache() *QueryPerformanceCache {
	return &QueryPerformanceCache{
		slowQueryIDs:         make([]string, 0),
		slowQueryPlanDataMap: make(map[string]models.SlowQueryPlanData),
		activeQueries:        make([]models.ActiveRunningQuery, 0),
	}
}

// Update replaces the entire cache with new data from the metric pipeline
// This is called after each successful metric scrape
func (c *QueryPerformanceCache) Update(
	slowQueryIDs []string,
	slowQueryPlanDataMap map[string]models.SlowQueryPlanData,
	activeQueries []models.ActiveRunningQuery,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Replace the entire cache with fresh data
	c.slowQueryIDs = slowQueryIDs
	c.slowQueryPlanDataMap = slowQueryPlanDataMap
	c.activeQueries = activeQueries
	c.lastUpdateTime = time.Now()
}

// GetAll returns a copy of all cached data
// This is called by the log pipeline to retrieve query performance data
func (c *QueryPerformanceCache) GetAll() ([]string, map[string]models.SlowQueryPlanData, []models.ActiveRunningQuery) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Return copies to prevent external modification
	slowQueryIDsCopy := make([]string, len(c.slowQueryIDs))
	copy(slowQueryIDsCopy, c.slowQueryIDs)

	slowQueryPlanDataMapCopy := make(map[string]models.SlowQueryPlanData, len(c.slowQueryPlanDataMap))
	for k, v := range c.slowQueryPlanDataMap {
		slowQueryPlanDataMapCopy[k] = v
	}

	activeQueriesCopy := make([]models.ActiveRunningQuery, len(c.activeQueries))
	copy(activeQueriesCopy, c.activeQueries)

	return slowQueryIDsCopy, slowQueryPlanDataMapCopy, activeQueriesCopy
}

// GetLastUpdateTime returns the timestamp of the last cache update
func (c *QueryPerformanceCache) GetLastUpdateTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastUpdateTime
}

// Size returns the number of cached slow queries
func (c *QueryPerformanceCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.slowQueryPlanDataMap)
}

// Clear removes all entries from the cache
func (c *QueryPerformanceCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.slowQueryIDs = make([]string, 0)
	c.slowQueryPlanDataMap = make(map[string]models.SlowQueryPlanData)
	c.activeQueries = make([]models.ActiveRunningQuery, 0)
	c.lastUpdateTime = time.Time{}
}
