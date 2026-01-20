// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
)

// NewFactory creates a factory for SQL Server receiver.
//
// This receiver collects all metrics in a single pipeline including:
// - Database and instance metrics
// - Slow query metrics
// - Active query metrics
// - Query plan statistics as metrics
// - All other performance metrics
//
// Example configuration:
//
//	receivers:
//	  newrelicsqlserver:
//	    hostname: localhost
//	    port: "1433"
//	    enable_query_monitoring: true
//	    enable_active_running_queries: true
//
//	service:
//	  pipelines:
//	    metrics:
//	      receivers: [newrelicsqlserver]
//	      exporters: [...]
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability))
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

	ns := newSqlServerScraper(params, cfg)
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
