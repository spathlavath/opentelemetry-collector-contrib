// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
)

// sharedScrapers stores scraper instances to be shared between metrics and logs receivers
// Key: receiver.Settings.ID (unique per configured receiver instance)
var sharedScrapers = make(map[component.ID]*sqlServerScraper)
var scrapersMu sync.Mutex

// NewFactory creates a factory for SQL Server receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	cfg := scraperhelper.NewDefaultControllerConfig()
	cfg.CollectionInterval = 10 * time.Second

	return &Config{
		ControllerConfig:     cfg,
		Hostname:             "localhost",
		Port:                 "1433",
		Username:             "",
		Password:             "",
		MaxConcurrentWorkers: 5,
		Timeout:              30 * time.Second,
	}
}

func createMetricsReceiver(
	_ context.Context,
	params receiver.Settings,
	rConf component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cfg := rConf.(*Config)

	// Get or create shared scraper instance
	scrapersMu.Lock()
	ns, exists := sharedScrapers[params.ID]
	if !exists {
		ns = newSqlServerScraper(params, cfg)
		sharedScrapers[params.ID] = ns
	}
	scrapersMu.Unlock()

	s, err := scraper.NewMetrics(
		ns.scrape,
		scraper.WithStart(ns.Start),
		scraper.WithShutdown(ns.Shutdown))
	if err != nil {
		return nil, err
	}

	return scraperhelper.NewMetricsController(
		&cfg.ControllerConfig, params, consumer,
		scraperhelper.AddScraper(metadata.Type, s),
	)
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	rConf component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	cfg := rConf.(*Config)

	// Get or create shared scraper instance (SAME instance as metrics receiver!)
	scrapersMu.Lock()
	ns, exists := sharedScrapers[params.ID]
	if !exists {
		ns = newSqlServerScraper(params, cfg)
		sharedScrapers[params.ID] = ns
	}
	scrapersMu.Unlock()

	f := scraper.NewFactory(metadata.Type, nil,
		scraper.WithLogs(func(context.Context, scraper.Settings, component.Config) (scraper.Logs, error) {
			return ns, nil
		}, component.StabilityLevelDevelopment))

	opt := scraperhelper.AddFactoryWithConfig(f, nil)

	return scraperhelper.NewLogsController(
		&cfg.ControllerConfig,
		params,
		consumer,
		opt,
	)
}
