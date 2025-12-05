// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver"

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/azuread"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/internal/metadata"
)

// NewFactory creates a factory for SQL Server receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiverFunc(func(cfg *Config) (*sqlx.DB, error) {
			return openSQLConnection(cfg)
		}), metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiverFunc(func(cfg *Config) (*sqlx.DB, error) {
			return openSQLConnection(cfg)
		}), metadata.LogsStability))
}

func createDefaultConfig() component.Config {
	cfg := scraperhelper.NewDefaultControllerConfig()
	cfg.CollectionInterval = 10 * time.Second
	cfg.Timeout = 30 * time.Second

	config := &Config{
		ControllerConfig:     cfg,
		Hostname:             "localhost",
		Port:                 "1433",
		Username:             "",
		Password:             "",
		EnableBufferMetrics:  true,
		MaxConcurrentWorkers: 5,
		Timeout:              30 * time.Second,
	}

	// Apply defaults
	config.SetDefaults()

	return config
}

type sqlOpenerFunc func(cfg *Config) (*sqlx.DB, error)

func createMetricsReceiverFunc(sqlOpenerFunc sqlOpenerFunc) receiver.CreateMetricsFunc {
	return func(
		_ context.Context,
		settings receiver.Settings,
		cfg component.Config,
		consumer consumer.Metrics,
	) (receiver.Metrics, error) {
		sqlCfg := cfg.(*Config)

		// Ensure defaults are set and configuration is valid
		sqlCfg.SetDefaults()
		if err := sqlCfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid configuration: %w", err)
		}

		instanceName := getInstanceName(*sqlCfg)
		hostName := getHostName(*sqlCfg)

		mp, err := newScraper(sqlCfg.ControllerConfig, sqlCfg, settings, func() (*sqlx.DB, error) {
			db, err := sqlOpenerFunc(sqlCfg)
			if err != nil {
				return nil, err
			}

			// Configure connection pool settings
			if sqlCfg.MaxOpenConnections > 0 {
				db.SetMaxOpenConns(sqlCfg.MaxOpenConnections)
			} else {
				db.SetMaxOpenConns(10) // Default
			}

			// Set connection timeouts to ensure proper cancellation
			db.SetMaxIdleConns(2)
			db.SetConnMaxLifetime(10 * time.Minute)
			db.SetConnMaxIdleTime(30 * time.Second)

			return db, nil
		}, instanceName, hostName)
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

func createLogsReceiverFunc(sqlOpenerFunc sqlOpenerFunc) receiver.CreateLogsFunc {
	return func(
		_ context.Context,
		settings receiver.Settings,
		cfg component.Config,
		consumer consumer.Logs,
	) (receiver.Logs, error) {
		sqlCfg := cfg.(*Config)

		// Ensure defaults are set and configuration is valid
		sqlCfg.SetDefaults()
		if err := sqlCfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid configuration: %w", err)
		}

		instanceName := getInstanceName(*sqlCfg)
		hostName := getHostName(*sqlCfg)

		lp, err := newLogsScraper(sqlCfg.ControllerConfig, sqlCfg, settings, func() (*sqlx.DB, error) {
			db, err := sqlOpenerFunc(sqlCfg)
			if err != nil {
				return nil, err
			}

			// Configure connection pool settings
			if sqlCfg.MaxOpenConnections > 0 {
				db.SetMaxOpenConns(sqlCfg.MaxOpenConnections)
			} else {
				db.SetMaxOpenConns(10) // Default
			}

			// Set connection timeouts
			db.SetMaxIdleConns(2)
			db.SetConnMaxLifetime(10 * time.Minute)
			db.SetConnMaxIdleTime(30 * time.Second)

			return db, nil
		}, instanceName, hostName)
		if err != nil {
			return nil, err
		}

		f := scraper.NewFactory(metadata.Type, nil,
			scraper.WithLogs(func(context.Context, scraper.Settings, component.Config) (scraper.Logs, error) {
				return lp, nil
			}, metadata.LogsStability))
		opt := scraperhelper.AddFactoryWithConfig(f, nil)

		return scraperhelper.NewLogsController(
			&sqlCfg.ControllerConfig,
			settings,
			consumer,
			opt,
		)
	}
}

// openSQLConnection opens a SQL Server connection using the appropriate authentication method
func openSQLConnection(cfg *Config) (*sqlx.DB, error) {
	if cfg.IsAzureADAuth() {
		connectionURL := cfg.CreateAzureADConnectionURL("")
		return sqlx.Connect(azuread.DriverName, connectionURL)
	}
	connectionURL := cfg.CreateConnectionURL("")
	return sqlx.Connect("mssql", connectionURL)
}

// getInstanceName extracts the instance name from configuration
func getInstanceName(cfg Config) string {
	if cfg.Instance != "" {
		return fmt.Sprintf("%s\\%s", cfg.Hostname, cfg.Instance)
	}
	return fmt.Sprintf("%s:%s", cfg.Hostname, cfg.Port)
}

// getHostName extracts the host name from configuration
func getHostName(cfg Config) string {
	return fmt.Sprintf("%s:%s", cfg.Hostname, cfg.Port)
}
