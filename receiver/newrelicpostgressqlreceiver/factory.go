// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgressqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver/internal/metadata"
)

// NewFactory creates a factory for PostgreSQL receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiverFunc(func(dataSourceName string) (*sql.DB, error) {
			return sql.Open("postgres", dataSourceName)
		}), metadata.MetricsStability))
}

func createDefaultConfig() component.Config {
	cfg := scraperhelper.NewDefaultControllerConfig()
	cfg.CollectionInterval = 10 * time.Second
	cfg.Timeout = 30 * time.Second

	return &Config{
		ControllerConfig:     cfg,
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
		Hostname:             "localhost",
		Port:                 "5432",
		Username:             "postgres",
		Password:             "",
		Database:             "postgres",
		Timeout:              30 * time.Second,
		SSLMode:              "disable",
	}
}

type sqlOpenerFunc func(dataSourceName string) (*sql.DB, error)

func createMetricsReceiverFunc(sqlOpenerFunc sqlOpenerFunc) receiver.CreateMetricsFunc {
	return func(
		_ context.Context,
		settings receiver.Settings,
		cfg component.Config,
		consumer consumer.Metrics,
	) (receiver.Metrics, error) {
		sqlCfg := cfg.(*Config)

		// Validate configuration
		if err := sqlCfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid configuration: %w", err)
		}

		metricsBuilder := metadata.NewMetricsBuilder(sqlCfg.MetricsBuilderConfig, settings)

		instanceName := getInstanceName(*sqlCfg)
		hostName := getHostName(*sqlCfg)

		mp, err := newScraper(
			metricsBuilder,
			sqlCfg.MetricsBuilderConfig,
			sqlCfg.ControllerConfig,
			sqlCfg,
			settings.Logger,
			func() (*sql.DB, error) {
				db, err := sqlOpenerFunc(getDataSource(*sqlCfg))
				if err != nil {
					return nil, err
				}

				// Configure connection pool settings
				db.SetMaxOpenConns(10)
				db.SetMaxIdleConns(2)
				db.SetConnMaxLifetime(10 * time.Minute)
				db.SetConnMaxIdleTime(30 * time.Second)

				return db, nil
			},
			instanceName,
			hostName,
		)
		if err != nil {
			return nil, err
		}

		opt := scraperhelper.AddScraper(metadata.Type, mp)

		return scraperhelper.NewMetricsController(
			&sqlCfg.ControllerConfig,
			settings,
			consumer,
			opt,
		)
	}
}

func getDataSource(cfg Config) string {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		cfg.Hostname,
		cfg.Port,
		cfg.Username,
		cfg.Database,
		cfg.SSLMode,
	)

	if cfg.Password != "" {
		connStr += fmt.Sprintf(" password=%s", cfg.Password)
	}

	if cfg.SSLCert != "" {
		connStr += fmt.Sprintf(" sslcert=%s", cfg.SSLCert)
	}

	if cfg.SSLKey != "" {
		connStr += fmt.Sprintf(" sslkey=%s", cfg.SSLKey)
	}

	if cfg.SSLRootCert != "" {
		connStr += fmt.Sprintf(" sslrootcert=%s", cfg.SSLRootCert)
	}

	if cfg.Timeout > 0 {
		connStr += fmt.Sprintf(" connect_timeout=%d", int(cfg.Timeout.Seconds()))
	}

	return connStr
}

func getInstanceName(cfg Config) string {
	return fmt.Sprintf("%s:%s/%s", cfg.Hostname, cfg.Port, cfg.Database)
}

func getHostName(cfg Config) string {
	return fmt.Sprintf("%s:%s", cfg.Hostname, cfg.Port)
}
