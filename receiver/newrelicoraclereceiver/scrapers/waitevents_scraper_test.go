// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestNewWaitEventsScraper(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	instanceName := "test-instance"
	config := metadata.DefaultMetricsBuilderConfig()
	countThreshold := 10

	scraper := NewWaitEventsScraper(db, mb, logger, instanceName, config, countThreshold)

	assert.NotNil(t, scraper)
	assert.Equal(t, db, scraper.db)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.Equal(t, countThreshold, scraper.queryMonitoringCountThreshold)
}

func TestWaitEventsScraper_NilDatabase(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(nil, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.db)
}

func TestWaitEventsScraper_NilMetricsBuilder(t *testing.T) {
	db := &sql.DB{}
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(db, nil, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestWaitEventsScraper_NilLogger(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper := NewWaitEventsScraper(db, mb, nil, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestWaitEventsScraper_EmptyInstanceName(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(db, mb, logger, "", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Equal(t, "", scraper.instanceName)
}

func TestWaitEventsScraper_ZeroCountThreshold(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 0)

	assert.NotNil(t, scraper)
	assert.Equal(t, 0, scraper.queryMonitoringCountThreshold)
}

func TestWaitEventsScraper_NegativeCountThreshold(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), -5)

	assert.NotNil(t, scraper)
	assert.Equal(t, -5, scraper.queryMonitoringCountThreshold)
}

func TestWaitEventsScraper_PositiveCountThreshold(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 100)

	assert.NotNil(t, scraper)
	assert.Equal(t, 100, scraper.queryMonitoringCountThreshold)
}

func TestWaitEventsScraper_MetricsBuilderConfig(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewWaitEventsScraper(db, mb, logger, "test-instance", config, 10)

	assert.NotNil(t, scraper)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
}

func TestWaitEventsScraper_MultipleInstances(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper1 := NewWaitEventsScraper(db, mb, logger, "instance-1", metadata.DefaultMetricsBuilderConfig(), 10)
	scraper2 := NewWaitEventsScraper(db, mb, logger, "instance-2", metadata.DefaultMetricsBuilderConfig(), 20)

	assert.NotEqual(t, scraper1, scraper2)
	assert.Equal(t, "instance-1", scraper1.instanceName)
	assert.Equal(t, "instance-2", scraper2.instanceName)
	assert.Equal(t, 10, scraper1.queryMonitoringCountThreshold)
	assert.Equal(t, 20, scraper2.queryMonitoringCountThreshold)
}

func TestWaitEventsScraper_DifferentThresholds(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	testCases := []struct {
		name      string
		threshold int
	}{
		{"threshold_1", 1},
		{"threshold_5", 5},
		{"threshold_10", 10},
		{"threshold_50", 50},
		{"threshold_100", 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scraper := NewWaitEventsScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), tc.threshold)
			assert.NotNil(t, scraper)
			assert.Equal(t, tc.threshold, scraper.queryMonitoringCountThreshold)
		})
	}
}
