// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestAPMMetadataCache_SetAndGet(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	queryHash := "0x1234567890abcdef"
	nrApmGuid := "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw"
	normalizedHash := "abc123def456"

	// Set metadata
	cache.Set(queryHash, nrApmGuid, normalizedHash)

	// Get metadata
	metadata, found := cache.Get(queryHash)
	assert.True(t, found, "Metadata should be found")
	assert.NotNil(t, metadata, "Metadata should not be nil")
	assert.Equal(t, nrApmGuid, metadata.NrServiceGuid)
	assert.Equal(t, normalizedHash, metadata.NormalisedSqlHash)
}

func TestAPMMetadataCache_GetNonExistent(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	// Try to get non-existent metadata
	metadata, found := cache.Get("0xnonexistent")
	assert.False(t, found, "Metadata should not be found")
	assert.Nil(t, metadata, "Metadata should be nil")
}

func TestAPMMetadataCache_SetEmptyQueryHash(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	// Try to set with empty query hash
	cache.Set("", "guid123", "hash456")

	// Should not be stored
	stats := cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"], "Cache should be empty")
}

func TestAPMMetadataCache_SetEmptyMetadata(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	// Try to set with all empty metadata
	cache.Set("", "", "")

	// Should not be stored
	stats := cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"], "Cache should be empty")
}

func TestAPMMetadataCache_SetPartialMetadata(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	queryHash := "0x1234567890abcdef"

	// Set with only GUID
	cache.Set(queryHash, "guid123", "")
	metadata, found := cache.Get(queryHash)
	assert.True(t, found)
	assert.Equal(t, "guid123", metadata.NrServiceGuid)
	assert.Equal(t, "", metadata.NormalisedSqlHash)

	// Update with only hash
	cache.Set(queryHash, "", "hash456")
	metadata, found = cache.Get(queryHash)
	assert.True(t, found)
	assert.Equal(t, "", metadata.NrServiceGuid) // Overwritten with empty
	assert.Equal(t, "hash456", metadata.NormalisedSqlHash)

	// Set with all fields
	cache.Set(queryHash, "guid456", "hash789")
	metadata, found = cache.Get(queryHash)
	assert.True(t, found)
	assert.Equal(t, "guid456", metadata.NrServiceGuid)
	assert.Equal(t, "hash789", metadata.NormalisedSqlHash)
}

func TestAPMMetadataCache_Clear(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	// Add multiple entries
	cache.Set("0x1", "guid1", "hash1")
	cache.Set("0x2", "guid2", "hash2")
	cache.Set("0x3", "guid3", "hash3")

	stats := cache.GetCacheStats()
	assert.Equal(t, 3, stats["total_entries"], "Should have 3 entries")

	// Clear cache (simulates end of scrape cycle)
	cache.Clear()

	stats = cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"], "All entries should be cleared")

	// Verify entries are gone
	_, found := cache.Get("0x1")
	assert.False(t, found, "Entry should not be found after clear")
}

func TestAPMMetadataCache_GetCacheStats(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	// Empty cache
	stats := cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"])
	assert.Equal(t, 0, stats["with_nr_service_guid"])
	assert.Equal(t, 0, stats["with_normalized_hash"])

	// Add entries with different metadata combinations
	cache.Set("0x1", "guid1", "hash1") // Both fields
	cache.Set("0x2", "guid2", "")      // Only GUID
	cache.Set("0x3", "", "hash3")      // Only hash
	cache.Set("0x4", "guid4", "hash4") // Both fields

	stats = cache.GetCacheStats()
	assert.Equal(t, 4, stats["total_entries"])
	assert.Equal(t, 3, stats["with_nr_service_guid"]) // 0x1, 0x2, 0x4
	assert.Equal(t, 3, stats["with_normalized_hash"]) // 0x1, 0x3, 0x4
}

func TestAPMMetadataCache_ConcurrentAccess(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(logger)

	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(id int) {
			queryHash := "0x" + string(rune('0'+id))
			cache.Set(queryHash, "guid", "hash")
			done <- true
		}(i)
	}

	// Wait for all writes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func(id int) {
			queryHash := "0x" + string(rune('0'+id))
			_, _ = cache.Get(queryHash)
			done <- true
		}(i)
	}

	// Wait for all reads
	for i := 0; i < 10; i++ {
		<-done
	}

	stats := cache.GetCacheStats()
	assert.GreaterOrEqual(t, stats["total_entries"].(int), 1, "Should have at least 1 entry")
}
