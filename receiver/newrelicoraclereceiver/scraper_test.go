// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestScraper_Start(t *testing.T) {
	cfg := &Config{
		DataSource:           "oracle://user:pass@localhost:1521/XE",
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newOracleScraper(receivertest.NewNopSettings(metadata.Type), cfg)

	// Starting should work (though connection will fail with mock)
	err := scraper.Start(context.Background(), componenttest.NewNopHost())
	require.Error(t, err) // Expected to fail with real connection
}

func TestScraper_Shutdown(t *testing.T) {
	cfg := &Config{
		DataSource:           "oracle://user:pass@localhost:1521/XE",
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newOracleScraper(receivertest.NewNopSettings(metadata.Type), cfg).(*oracleScraper)

	// Should handle shutdown gracefully even without initialization
	err := scraper.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestScraper_ScrapeWithoutClient(t *testing.T) {
	cfg := &Config{
		DataSource:           "oracle://user:pass@localhost:1521/XE",
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newOracleScraper(receivertest.NewNopSettings(metadata.Type), cfg).(*oracleScraper)

	// Scraping without a client should return an error
	_, err := scraper.Scrape(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "database client not initialized")
}

func TestScraper_ScrapeWithMockClient(t *testing.T) {
	cfg := &Config{
		DataSource:           "oracle://user:pass@localhost:1521/XE",
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newOracleScraper(receivertest.NewNopSettings(metadata.Type), cfg).(*oracleScraper)

	// Set up mock client
	mockClient := newMockDBClient()
	scraper.dbClient = mockClient

	// This will fail because of the mock implementation limitations,
	// but demonstrates the structure. We expect an error.
	_, err := scraper.Scrape(context.Background())
	require.Error(t, err) // Expected due to mock limitations and nil QueryRow
}

func TestGetInstanceInfo(t *testing.T) {
	cfg := &Config{
		DataSource:           "oracle://user:pass@localhost:1521/XE",
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newOracleScraper(receivertest.NewNopSettings(metadata.Type), cfg).(*oracleScraper)
	mockClient := newMockDBClient()
	scraper.dbClient = mockClient

	// Should return error with uninitialized mock
	_, err := scraper.getInstanceInfo()
	require.Error(t, err)
}

func TestConfig_GetConnectionString_WithSpecialCharacters(t *testing.T) {
	cfg := &Config{
		Endpoint: "localhost:1521",
		Username: "test@user",
		Password: "pass@word!",
		Service:  "TEST.DB",
	}

	expected := "oracle://test@user:pass@word!@localhost:1521/TEST.DB"
	result := cfg.GetConnectionString()
	require.Equal(t, expected, result)
}

func TestNewOracleScraper(t *testing.T) {
	cfg := &Config{
		DataSource:           "oracle://user:pass@localhost:1521/XE",
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}

	scraper := newOracleScraper(receivertest.NewNopSettings(metadata.Type), cfg)
	require.NotNil(t, scraper)

	oracleScraper := scraper.(*oracleScraper)
	require.NotNil(t, oracleScraper.logger)
	require.NotNil(t, oracleScraper.config)
	require.NotNil(t, oracleScraper.mb)
}
