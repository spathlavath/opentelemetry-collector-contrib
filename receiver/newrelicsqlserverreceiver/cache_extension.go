// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
)

// QueryCacheExtension is an OTel Extension that provides shared cache storage
// for query performance data between metrics and logs pipelines
type QueryCacheExtension struct {
	component.StartFunc
	component.ShutdownFunc

	mu       sync.RWMutex
	caches   map[component.ID]*helpers.QueryPerformanceCache // receiverID -> cache
	settings component.TelemetrySettings
}

// NewQueryCacheExtension creates a new query cache extension
func NewQueryCacheExtension(settings component.TelemetrySettings) *QueryCacheExtension {
	return &QueryCacheExtension{
		caches:   make(map[component.ID]*helpers.QueryPerformanceCache),
		settings: settings,
	}
}

// Start initializes the extension
func (e *QueryCacheExtension) Start(ctx context.Context, host component.Host) error {
	e.settings.Logger.Info("Query cache extension started")
	return nil
}

// Shutdown cleans up the extension
func (e *QueryCacheExtension) Shutdown(ctx context.Context) error {
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
func (e *QueryCacheExtension) GetOrCreateCache(receiverID component.ID) *helpers.QueryPerformanceCache {
	e.mu.Lock()
	defer e.mu.Unlock()

	cache, exists := e.caches[receiverID]
	if !exists {
		cache = helpers.NewQueryPerformanceCache()
		e.caches[receiverID] = cache
		e.settings.Logger.Info("Created new cache for receiver",
			zap.String("receiver_id", receiverID.String()))
	}

	return cache
}

// GetCache retrieves a cache for a specific receiver ID (returns nil if not exists)
func (e *QueryCacheExtension) GetCache(receiverID component.ID) *helpers.QueryPerformanceCache {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.caches[receiverID]
}

// RemoveCache removes a cache for a specific receiver ID
func (e *QueryCacheExtension) RemoveCache(receiverID component.ID) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.caches, receiverID)
	e.settings.Logger.Info("Removed cache for receiver",
		zap.String("receiver_id", receiverID.String()))
}

// Extension factory components

type queryCacheExtensionConfig struct{}

func createDefaultExtensionConfig() component.Config {
	return &queryCacheExtensionConfig{}
}

func createExtension(
	_ context.Context,
	params extension.Settings,
	_ component.Config,
) (extension.Extension, error) {
	return NewQueryCacheExtension(params.TelemetrySettings), nil
}

// NewExtensionFactory creates a factory for the query cache extension
func NewExtensionFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType("querycache"),
		createDefaultExtensionConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}
