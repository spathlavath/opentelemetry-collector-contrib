// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"sync"

	"go.uber.org/zap"
)

// APMMetadata stores APM correlation metadata extracted from active query comments
type APMMetadata struct {
	NrServiceGuid     string // New Relic APM service GUID
	NormalizedSqlHash string // MD5 hash of normalized SQL
}

// APMMetadataCache provides thread-safe caching of APM metadata keyed by query_hash.
// This cache is SCOPED TO A SINGLE SCRAPE CYCLE and is recreated fresh for each scrape.
//
// This ensures:
// 1. No stale metadata persists across scrapes
// 2. Cache reflects only the current scrape's active queries
// 3. Simple lifecycle management (create at scrape start, discard at scrape end)
//
// Flow within a single scrape:
// 1. Active queries with APM comments (/* nr_apm_guid="...", nr_service="..." */) are captured
// 2. APM metadata is extracted and cached by query_hash
// 3. Slow queries are enriched with cached metadata using their query_hash
// 4. Both slow and active query metrics have consistent APM correlation attributes
// 5. Cache is discarded after scrape completes
type APMMetadataCache struct {
	cache  map[string]*APMMetadata // key = query_hash (hex string)
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewAPMMetadataCache creates a new APM metadata cache for a single scrape cycle
// This cache is recreated fresh for each scrape to ensure no stale data
func NewAPMMetadataCache(logger *zap.Logger) *APMMetadataCache {
	logger.Info("🆕 CACHE CREATE: New APM metadata cache created for scrape cycle")

	return &APMMetadataCache{
		cache:  make(map[string]*APMMetadata),
		logger: logger,
	}
}

// Set stores APM metadata for a query_hash (thread-safe)
// If the entry already exists, it updates the metadata with latest values
func (c *APMMetadataCache) Set(queryHash, nrServiceGuid, normalizedSqlHash string) {
	if queryHash == "" {
		c.logger.Debug("❌ CACHE SET: Empty query_hash, skipping")
		return
	}

	// Only cache if we have at least one piece of APM metadata
	if nrServiceGuid == "" && normalizedSqlHash == "" {
		c.logger.Debug("❌ CACHE SET: No APM metadata to cache",
			zap.String("query_hash", queryHash))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if this is an update or new entry
	_, exists := c.cache[queryHash]

	metadata := &APMMetadata{
		NrServiceGuid:     nrServiceGuid,
		NormalizedSqlHash: normalizedSqlHash,
	}

	c.cache[queryHash] = metadata

	if exists {
		c.logger.Info("🔄 CACHE SET: Updated existing entry",
			zap.String("query_hash", queryHash),
			zap.String("nr_service_guid", nrServiceGuid),
			zap.String("normalized_sql_hash", normalizedSqlHash))
	} else {
		c.logger.Info("✅ CACHE SET: Added new entry",
			zap.String("query_hash", queryHash),
			zap.String("nr_service_guid", nrServiceGuid),
			zap.String("normalized_sql_hash", normalizedSqlHash),
			zap.Int("total_cache_entries", len(c.cache)))
	}
}

// Get retrieves APM metadata for a query_hash (thread-safe)
// Returns (metadata, true) if found in current scrape, or (nil, false) if not found
func (c *APMMetadataCache) Get(queryHash string) (*APMMetadata, bool) {
	if queryHash == "" {
		c.logger.Debug("❌ CACHE GET: Empty query_hash")
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	metadata, exists := c.cache[queryHash]
	if !exists {
		c.logger.Debug("❌ CACHE GET: Entry not found",
			zap.String("query_hash", queryHash),
			zap.Int("total_cache_entries", len(c.cache)))
		return nil, false
	}

	c.logger.Info("✅ CACHE GET: Retrieved metadata from cache",
		zap.String("query_hash", queryHash),
		zap.String("nr_service_guid", metadata.NrServiceGuid),
		zap.String("normalized_sql_hash", metadata.NormalizedSqlHash))

	return metadata, true
}

// Clear removes all entries from the cache
// This is called at the end of each scrape cycle to ensure fresh data next time
func (c *APMMetadataCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	entryCount := len(c.cache)
	c.cache = make(map[string]*APMMetadata)

	if entryCount > 0 {
		c.logger.Info("🗑️  CACHE CLEAR: Cleared cache at end of scrape cycle",
			zap.Int("cleared_entries", entryCount))
	} else {
		c.logger.Debug("CACHE CLEAR: Cache was already empty")
	}
}

// GetCacheStats returns statistics about the cache for debugging/monitoring
func (c *APMMetadataCache) GetCacheStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := map[string]interface{}{
		"total_entries": len(c.cache),
	}

	// Count entries with each type of metadata
	withGuid := 0
	withHash := 0

	for _, metadata := range c.cache {
		if metadata.NrServiceGuid != "" {
			withGuid++
		}
		if metadata.NormalizedSqlHash != "" {
			withHash++
		}
	}

	stats["with_nr_service_guid"] = withGuid
	stats["with_normalized_hash"] = withHash

	return stats
}
