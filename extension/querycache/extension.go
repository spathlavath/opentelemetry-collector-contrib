// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package querycache // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache"

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// Extension is an OTel Extension that provides shared cache storage
// for query performance data between metrics and logs pipelines
type Extension struct {
	component.StartFunc
	component.ShutdownFunc

	mu       sync.RWMutex
	caches   map[component.ID]*QueryPerformanceCache // receiverID -> cache
	settings component.TelemetrySettings
}

// NewExtension creates a new query cache extension
func NewExtension(settings component.TelemetrySettings) *Extension {
	return &Extension{
		caches:   make(map[component.ID]*QueryPerformanceCache),
		settings: settings,
	}
}

// Start initializes the extension
func (e *Extension) Start(ctx context.Context, host component.Host) error {
	e.settings.Logger.Info("Query cache extension started")
	return nil
}

// Shutdown cleans up the extension
func (e *Extension) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Clear all caches
	for id := range e.caches {
		delete(e.caches, id)
	}

	e.settings.Logger.Info("Query cache extension shutdown, all caches cleared")
	return nil
}

// GetOrCreateCache retrieves or creates a cache for a specific receiver ID
func (e *Extension) GetOrCreateCache(receiverID component.ID) *QueryPerformanceCache {
	e.mu.Lock()
	defer e.mu.Unlock()

	cache, exists := e.caches[receiverID]
	if !exists {
		cache = NewQueryPerformanceCache()
		e.caches[receiverID] = cache
		e.settings.Logger.Info("Created new cache for receiver",
			zap.String("receiver_id", receiverID.String()))
	}

	return cache
}

// GetCache retrieves a cache for a specific receiver ID (returns nil if not exists)
func (e *Extension) GetCache(receiverID component.ID) *QueryPerformanceCache {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.caches[receiverID]
}

// RemoveCache removes a cache for a specific receiver ID
func (e *Extension) RemoveCache(receiverID component.ID) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.caches, receiverID)
	e.settings.Logger.Info("Removed cache for receiver",
		zap.String("receiver_id", receiverID.String()))
}
