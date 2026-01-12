// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestNewSessionScraper(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSessionScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.config)
}

func TestSessionScraper_NilDatabase(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSessionScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.client)
}

func TestSessionScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()

	scraper := NewSessionScraper(mockClient, nil, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestSessionScraper_NilLogger(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper := NewSessionScraper(mockClient, mb, nil, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestSessionScraper_EmptyInstanceName(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
}

func TestSessionScraper_Config(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSessionScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, config, scraper.config)
}

func TestSessionScraper_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper1 := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
	scraper2 := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	// Verify that two separate instances are created (different memory addresses)
	assert.NotSame(t, scraper1, scraper2)
}

func TestSessionScraper_MetricEnabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSessionsCount.Enabled = true
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewSessionScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.True(t, scraper.config.Metrics.NewrelicoracledbSessionsCount.Enabled)
}

func TestSessionScraper_MetricDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSessionsCount.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewSessionScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.False(t, scraper.config.Metrics.NewrelicoracledbSessionsCount.Enabled)
}

func TestSessionScraper_DifferentConfigs(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	logger := zap.NewNop()

	config1 := metadata.DefaultMetricsBuilderConfig()
	config1.Metrics.NewrelicoracledbSessionsCount.Enabled = true
	mb1 := metadata.NewMetricsBuilder(config1, settings)
	scraper1 := NewSessionScraper(mockClient, mb1, logger, config1)

	config2 := metadata.DefaultMetricsBuilderConfig()
	config2.Metrics.NewrelicoracledbSessionsCount.Enabled = false
	mb2 := metadata.NewMetricsBuilder(config2, settings)
	scraper2 := NewSessionScraper(mockClient, mb2, logger, config2)

	assert.True(t, scraper1.config.Metrics.NewrelicoracledbSessionsCount.Enabled)
	assert.False(t, scraper2.config.Metrics.NewrelicoracledbSessionsCount.Enabled)
}
