// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// APMMetadata stores APM correlation metadata extracted from active query comments
type APMMetadata struct {
	NrServiceGuid     string    // New Relic APM service GUID
	ClientName        string    // Client/service name (from nr_service)
	NormalisedSqlHash string    // MD5 hash of normalized SQL
	LastSeen          time.Time // Last time this metadata was observed
}

// APMMetadataCache provides thread-safe caching of APM metadata keyed by query_hash.
// This enables enriching slow query metrics (from sys.dm_exec_query_stats) with APM
// correlation data captured from active running queries (from sys.dm_exec_requests).
//
// Flow:
// 1. Active queries with APM comments (/* nr_apm_guid="...", nr_service="..." */) are captured
// 2. APM metadata is extracted and cached by query_hash
// 3. Slow queries are enriched with cached metadata using their query_hash
// 4. Both slow and active query metrics have consistent APM correlation attributes
type APMMetadataCache struct {
	cache  map[string]*APMMetadata // key = query_hash (hex string)
	ttl    time.Duration
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewAPMMetadataCache creates a new APM metadata cache with configurable TTL
// Default TTL: 1 hour (metadata remains valid as long as the query plan is cached)
func NewAPMMetadataCache(ttlMinutes int, logger *zap.Logger) *APMMetadataCache {
	ttl := time.Duration(ttlMinutes) * time.Minute
	logger.Info("Creating APM metadata cache",
		zap.Int("ttl_minutes", ttlMinutes),
		zap.Duration("ttl", ttl))

	return &APMMetadataCache{
		cache:  make(map[string]*APMMetadata),
		ttl:    ttl,
		logger: logger,
	}
}

// Set stores APM metadata for a query_hash (thread-safe)
// If the entry already exists, it updates the metadata and refreshes the timestamp
func (c *APMMetadataCache) Set(queryHash, nrServiceGuid, clientName, normalisedSqlHash string) {
	if queryHash == "" {
		c.logger.Debug("Empty query_hash, skipping APM metadata cache set")
		return
	}

	// Only cache if we have at least one piece of APM metadata
	if nrServiceGuid == "" && clientName == "" && normalisedSqlHash == "" {
		c.logger.Debug("No APM metadata to cache",
			zap.String("query_hash", queryHash))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	metadata := &APMMetadata{
		NrServiceGuid:     nrServiceGuid,
		ClientName:        clientName,
		NormalisedSqlHash: normalisedSqlHash,
		LastSeen:          time.Now(),
	}

	c.cache[queryHash] = metadata

	c.logger.Info("Cached APM metadata for query_hash",
		zap.String("query_hash", queryHash),
		zap.String("nr_service_guid", nrServiceGuid),
		zap.String("client_name", clientName),
		zap.String("normalised_sql_hash", normalisedSqlHash))
}

// Get retrieves APM metadata for a query_hash (thread-safe)
// Returns (metadata, true) if found and not expired, or (nil, false) if not found/expired
func (c *APMMetadataCache) Get(queryHash string) (*APMMetadata, bool) {
	if queryHash == "" {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	metadata, exists := c.cache[queryHash]
	if !exists {
		return nil, false
	}

	// Check if expired
	age := time.Since(metadata.LastSeen)
	if age > c.ttl {
		c.logger.Debug("APM metadata expired",
			zap.String("query_hash", queryHash),
			zap.Duration("age", age),
			zap.Duration("ttl", c.ttl))
		return nil, false
	}

	c.logger.Debug("Retrieved APM metadata from cache",
		zap.String("query_hash", queryHash),
		zap.String("nr_service_guid", metadata.NrServiceGuid),
		zap.String("client_name", metadata.ClientName),
		zap.String("normalised_sql_hash", metadata.NormalisedSqlHash),
		zap.Duration("age", age))

	return metadata, true
}

// CleanupStaleEntries removes expired entries from the cache to prevent unbounded growth
func (c *APMMetadataCache) CleanupStaleEntries() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, metadata := range c.cache {
		if now.Sub(metadata.LastSeen) > c.ttl {
			delete(c.cache, key)
			removed++
		}
	}

	if removed > 0 {
		c.logger.Info("Cleaned up stale APM metadata cache entries",
			zap.Int("removed_count", removed),
			zap.Int("remaining_count", len(c.cache)))
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
	withClientName := 0
	withHash := 0

	for _, metadata := range c.cache {
		if metadata.NrServiceGuid != "" {
			withGuid++
		}
		if metadata.ClientName != "" {
			withClientName++
		}
		if metadata.NormalisedSqlHash != "" {
			withHash++
		}
	}

	stats["with_nr_service_guid"] = withGuid
	stats["with_client_name"] = withClientName
	stats["with_normalised_hash"] = withHash

	return stats
}
