// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoracledbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoracledbreceiver"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoracledbreceiver/internal/metadata"
)

const (
	// The value of "type" key in configuration.
	typeStr = "newrelicoracledb"
	// The stability level of the receiver.
	stability = component.StabilityLevelAlpha

	defaultInterval = 60 * time.Second
)

var ErrNoConnectionString = errors.New("connection string is required")

// NewFactory creates a factory for newrelicoracledb receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, stability),
	)
}

func createDefaultConfig() component.Config {
	cfg := scraperhelper.NewDefaultControllerConfig()
	cfg.CollectionInterval = defaultInterval

	return &Config{
		ControllerConfig:     cfg,
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
		ExtendedConfig: ExtendedConfig{
			ExtendedMetrics:       false,
			MaxOpenConnections:    10,
			DisableConnectionPool: false,
			CustomMetricsQuery:    "",
			CustomMetricsConfig:   "",
			IsSysDBA:              false,
			IsSysOper:             false,
			SysMetricsSource:      "",         // Default: CDB access (empty means default)
			SkipMetricsGroups:     []string{}, // Default: collect all metric groups
		},
		TopQueryCollection: TopQueryCollection{
			MaxQuerySampleCount: 1000,
			TopQueryCount:       100,
		},
		QuerySample: QuerySample{
			MaxRowsPerQuery: 10000,
		},
		TablespaceConfig: TablespaceConfig{
			IncludeTablespaces: []string{},
			ExcludeTablespaces: []string{},
		},
	}
}

func createMetricsReceiver(_ context.Context, settings receiver.Settings, rConf component.Config, consumer consumer.Metrics) (receiver.Metrics, error) {
	cfg, ok := rConf.(*Config)
	if !ok {
		return nil, errors.New("invalid configuration type")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	oracledbScraper, err := newScraper(cfg, settings)
	if err != nil {
		return nil, err
	}

	f := scraper.NewFactory(component.MustNewType(typeStr), nil,
		scraper.WithMetrics(func(context.Context, scraper.Settings, component.Config) (scraper.Metrics, error) {
			return oracledbScraper, nil
		}, component.StabilityLevelAlpha))
	opt := scraperhelper.AddFactoryWithConfig(f, nil)

	return scraperhelper.NewMetricsController(
		&cfg.ControllerConfig,
		settings,
		consumer,
		opt,
	)
}
