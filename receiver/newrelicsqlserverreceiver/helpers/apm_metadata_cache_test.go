// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helpers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestAPMMetadataCache_SetAndGet(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	queryHash := "0x1234567890abcdef"
	nrApmGuid := "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw"
	clientName := "order-service"
	normalisedHash := "abc123def456"

	// Set metadata
	cache.Set(queryHash, nrApmGuid, clientName, normalisedHash)

	// Get metadata
	metadata, found := cache.Get(queryHash)
	assert.True(t, found, "Metadata should be found")
	assert.NotNil(t, metadata, "Metadata should not be nil")
	assert.Equal(t, nrApmGuid, metadata.NrServiceGuid)
	assert.Equal(t, clientName, metadata.ClientName)
	assert.Equal(t, normalisedHash, metadata.NormalisedSqlHash)
}

func TestAPMMetadataCache_GetNonExistent(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	// Try to get non-existent metadata
	metadata, found := cache.Get("0xnonexistent")
	assert.False(t, found, "Metadata should not be found")
	assert.Nil(t, metadata, "Metadata should be nil")
}

func TestAPMMetadataCache_SetEmptyQueryHash(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	// Try to set with empty query hash
	cache.Set("", "guid", "service", "hash")

	// Should not be stored
	stats := cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"], "Cache should be empty")
}

func TestAPMMetadataCache_SetEmptyMetadata(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	// Try to set with all empty metadata
	cache.Set("0x123", "", "", "")

	// Should not be stored
	stats := cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"], "Cache should be empty")
}

func TestAPMMetadataCache_SetPartialMetadata(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	queryHash := "0x1234567890abcdef"

	// Set with only GUID
	cache.Set(queryHash, "guid123", "", "")
	metadata, found := cache.Get(queryHash)
	assert.True(t, found)
	assert.Equal(t, "guid123", metadata.NrServiceGuid)
	assert.Equal(t, "", metadata.ClientName)
	assert.Equal(t, "", metadata.NormalisedSqlHash)

	// Update with client name
	cache.Set(queryHash, "", "service123", "")
	metadata, found = cache.Get(queryHash)
	assert.True(t, found)
	assert.Equal(t, "", metadata.NrServiceGuid) // Overwritten with empty
	assert.Equal(t, "service123", metadata.ClientName)

	// Set with all fields
	cache.Set(queryHash, "guid456", "service456", "hash456")
	metadata, found = cache.Get(queryHash)
	assert.True(t, found)
	assert.Equal(t, "guid456", metadata.NrServiceGuid)
	assert.Equal(t, "service456", metadata.ClientName)
	assert.Equal(t, "hash456", metadata.NormalisedSqlHash)
}

func TestAPMMetadataCache_Expiration(t *testing.T) {
	logger := zap.NewNop()
	// Create cache with 1 second TTL
	cache := NewAPMMetadataCache(0, logger) // 0 minutes = immediate expiration
	cache.ttl = 1 * time.Second

	queryHash := "0x1234567890abcdef"
	cache.Set(queryHash, "guid", "service", "hash")

	// Should be found immediately
	metadata, found := cache.Get(queryHash)
	assert.True(t, found)
	assert.NotNil(t, metadata)

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Should not be found after expiration
	metadata, found = cache.Get(queryHash)
	assert.False(t, found, "Metadata should be expired")
	assert.Nil(t, metadata, "Metadata should be nil after expiration")
}

func TestAPMMetadataCache_CleanupStaleEntries(t *testing.T) {
	logger := zap.NewNop()
	// Create cache with 1 second TTL
	cache := NewAPMMetadataCache(0, logger)
	cache.ttl = 1 * time.Second

	// Add multiple entries
	cache.Set("0x123", "guid1", "service1", "hash1")
	cache.Set("0x456", "guid2", "service2", "hash2")
	cache.Set("0x789", "guid3", "service3", "hash3")

	stats := cache.GetCacheStats()
	assert.Equal(t, 3, stats["total_entries"], "Should have 3 entries")

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Cleanup
	cache.CleanupStaleEntries()

	stats = cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"], "All entries should be cleaned up")
}

func TestAPMMetadataCache_GetCacheStats(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	// Empty cache
	stats := cache.GetCacheStats()
	assert.Equal(t, 0, stats["total_entries"])
	assert.Equal(t, 0, stats["with_nr_service_guid"])
	assert.Equal(t, 0, stats["with_client_name"])
	assert.Equal(t, 0, stats["with_normalised_hash"])

	// Add entries with different metadata combinations
	cache.Set("0x1", "guid1", "service1", "hash1") // All fields
	cache.Set("0x2", "guid2", "", "hash2")         // No client name
	cache.Set("0x3", "", "service3", "")           // Only client name
	cache.Set("0x4", "", "", "hash4")              // Only hash

	stats = cache.GetCacheStats()
	assert.Equal(t, 4, stats["total_entries"])
	assert.Equal(t, 2, stats["with_nr_service_guid"]) // 0x1, 0x2
	assert.Equal(t, 2, stats["with_client_name"])     // 0x1, 0x3
	assert.Equal(t, 3, stats["with_normalised_hash"]) // 0x1, 0x2, 0x4
}

func TestAPMMetadataCache_ConcurrentAccess(t *testing.T) {
	logger := zap.NewNop()
	cache := NewAPMMetadataCache(60, logger)

	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(id int) {
			queryHash := "0x" + string(rune('0'+id))
			cache.Set(queryHash, "guid", "service", "hash")
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
