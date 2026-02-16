// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/scrapers"
)

func TestNewNewRelicMySQLScraper(t *testing.T) {
	settings := receiver.Settings{
		ID:                component.NewIDWithName(component.MustNewType("newrelicmysql"), "test"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	cfg := &Config{
		Username: "testuser",
		AddrConfig: confignet.AddrConfig{
			Endpoint:  "localhost:3306",
			Transport: confignet.TransportTypeTCP,
		},
		TLS: configtls.ClientConfig{
			Insecure: true,
		},
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newNewRelicMySQLScraper(settings, cfg)
	assert.NotNil(t, scraper)
	assert.NotNil(t, scraper.logger)
	assert.NotNil(t, scraper.config)
	assert.NotNil(t, scraper.mb)
	assert.Nil(t, scraper.sqlclient)
	assert.Nil(t, scraper.scrapers)
}

func TestNewRelicMySQLScraper_Scrape_NoClient(t *testing.T) {
	settings := receiver.Settings{
		ID:                component.NewIDWithName(component.MustNewType("newrelicmysql"), "test"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	cfg := &Config{
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newNewRelicMySQLScraper(settings, cfg)
	ctx := context.Background()

	metrics, err := scraper.scrape(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to database client")
	// Metrics will still be empty pmetric structure even with error
	assert.NotNil(t, metrics)
}

func TestNewRelicMySQLScraper_Start(t *testing.T) {
	t.Run("start fails with invalid config", func(t *testing.T) {
		settings := receiver.Settings{
			ID:                component.NewIDWithName(component.MustNewType("newrelicmysql"), "test"),
			TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		}
		cfg := &Config{
			Username: "",
			AddrConfig: confignet.AddrConfig{
				Endpoint:  "", // Empty endpoint will cause error
				Transport: confignet.TransportTypeTCP,
			},
			MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
		}

		scraper := newNewRelicMySQLScraper(settings, cfg)
		ctx := context.Background()

		err := scraper.start(ctx, componenttest.NewNopHost())
		assert.Error(t, err)
	})
}

func TestNewRelicMySQLScraper_Scrape_WithClient(t *testing.T) {
	settings := receiver.Settings{
		ID:                component.NewIDWithName(component.MustNewType("newrelicmysql"), "test"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	cfg := &Config{
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	t.Run("scrape with working client", func(t *testing.T) {
		scraper := newNewRelicMySQLScraper(settings, cfg)

		mockClient := common.NewMockClient()
		mockClient.GetVersionFunc = func() (string, error) {
			return "8.0.30", nil
		}
		mockClient.GetGlobalStatsFunc = func() (map[string]string, error) {
			return map[string]string{
				"Connections":       "1000",
				"Queries":           "5000",
				"Uptime":            "3600",
				"Threads_connected": "10",
			}, nil
		}
		mockClient.GetGlobalVariablesFunc = func() (map[string]string, error) {
			return map[string]string{
				"max_connections": "100",
			}, nil
		}
		mockClient.GetReplicationStatusFunc = func() (map[string]string, error) {
			return map[string]string{}, nil
		}
		scraper.sqlclient = mockClient

		// Manually initialize scrapers since we're bypassing start()
		coreScraper, err := scrapers.NewCoreScraper(mockClient, scraper.mb, scraper.logger)
		assert.NoError(t, err)
		replicationScraper, err := scrapers.NewReplicationScraper(mockClient, scraper.mb, scraper.logger)
		assert.NoError(t, err)
		scraper.scrapers = []scrapers.Scraper{coreScraper, replicationScraper}

		ctx := context.Background()
		metrics, err := scraper.scrape(ctx)

		assert.NoError(t, err)
		assert.NotNil(t, metrics)
		assert.Greater(t, metrics.ResourceMetrics().Len(), 0)
	})

	t.Run("scrape handles scraper errors gracefully", func(t *testing.T) {
		scraper := newNewRelicMySQLScraper(settings, cfg)

		mockClient := common.NewMockClient()
		mockClient.GetVersionFunc = func() (string, error) {
			return "", errors.New("version error")
		}
		mockClient.GetGlobalStatsFunc = func() (map[string]string, error) {
			return map[string]string{}, nil
		}
		mockClient.GetGlobalVariablesFunc = func() (map[string]string, error) {
			return map[string]string{}, nil
		}
		mockClient.GetReplicationStatusFunc = func() (map[string]string, error) {
			return map[string]string{}, nil
		}
		scraper.sqlclient = mockClient

		coreScraper, err := scrapers.NewCoreScraper(mockClient, scraper.mb, scraper.logger)
		assert.NoError(t, err)
		replicationScraper, err := scrapers.NewReplicationScraper(mockClient, scraper.mb, scraper.logger)
		assert.NoError(t, err)
		scraper.scrapers = []scrapers.Scraper{coreScraper, replicationScraper}

		ctx := context.Background()
		metrics, err := scraper.scrape(ctx)

		// Even with errors in scrapers, scrape should return metrics (partial success)
		assert.NoError(t, err)
		assert.NotNil(t, metrics)
	})
}

func TestNewRelicMySQLScraper_Shutdown(t *testing.T) {
	settings := receiver.Settings{
		ID:                component.NewIDWithName(component.MustNewType("newrelicmysql"), "test"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}
	cfg := &Config{
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	t.Run("shutdown with nil client", func(t *testing.T) {
		scraper := newNewRelicMySQLScraper(settings, cfg)
		ctx := context.Background()

		err := scraper.shutdown(ctx)
		assert.NoError(t, err)
	})

	t.Run("shutdown with mock client", func(t *testing.T) {
		scraper := newNewRelicMySQLScraper(settings, cfg)

		closeCalled := false
		mockClient := common.NewMockClient()
		mockClient.CloseFunc = func() error {
			closeCalled = true
			return nil
		}
		scraper.sqlclient = mockClient

		ctx := context.Background()
		err := scraper.shutdown(ctx)

		assert.NoError(t, err)
		assert.True(t, closeCalled)
	})

	t.Run("shutdown with client close error", func(t *testing.T) {
		scraper := newNewRelicMySQLScraper(settings, cfg)

		mockClient := common.NewMockClient()
		mockClient.CloseFunc = func() error {
			return errors.New("close error")
		}
		scraper.sqlclient = mockClient

		ctx := context.Background()
		err := scraper.shutdown(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "close error")
	})
}
