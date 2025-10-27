// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestNewSlowQueriesScraper(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	instanceName := "test-instance"
	config := metadata.DefaultMetricsBuilderConfig()
	responseTimeThreshold := 1000
	countThreshold := 100

	scraper := NewSlowQueriesScraper(nil, mb, logger, instanceName, config, responseTimeThreshold, countThreshold)

	assert.NotNil(t, scraper)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.Equal(t, responseTimeThreshold, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, countThreshold, scraper.queryMonitoringCountThreshold)
	assert.Nil(t, scraper.db)
}

func TestNewSlowQueriesScraper_DefaultThresholds(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 0, 0)

	assert.NotNil(t, scraper)
	assert.Equal(t, 0, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 0, scraper.queryMonitoringCountThreshold)
}

func TestNewSlowQueriesScraper_CustomThresholds(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	testCases := []struct {
		name                  string
		responseTimeThreshold int
		countThreshold        int
	}{
		{"small_thresholds", 100, 10},
		{"medium_thresholds", 1000, 100},
		{"large_thresholds", 10000, 1000},
		{"mixed_thresholds", 500, 5000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), tc.responseTimeThreshold, tc.countThreshold)
			assert.Equal(t, tc.responseTimeThreshold, scraper.queryMonitoringResponseTimeThreshold)
			assert.Equal(t, tc.countThreshold, scraper.queryMonitoringCountThreshold)
		})
	}
}

func TestNewSlowQueriesScraper_NilParameters(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	t.Run("nil_db", func(t *testing.T) {
		scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, 1000, 100)
		assert.NotNil(t, scraper)
		assert.Nil(t, scraper.db)
	})

	t.Run("nil_mb", func(t *testing.T) {
		scraper := NewSlowQueriesScraper(nil, nil, logger, "test-instance", config, 1000, 100)
		assert.NotNil(t, scraper)
		assert.Nil(t, scraper.mb)
	})

	t.Run("nil_logger", func(t *testing.T) {
		scraper := NewSlowQueriesScraper(nil, mb, nil, "test-instance", config, 1000, 100)
		assert.NotNil(t, scraper)
		assert.Nil(t, scraper.logger)
	})
}

func TestNewSlowQueriesScraper_EmptyInstanceName(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSlowQueriesScraper(nil, mb, logger, "", config, 1000, 100)
	assert.NotNil(t, scraper)
	assert.Equal(t, "", scraper.instanceName)
}

func TestNewSlowQueriesScraper_MultipleInstances(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb1 := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	mb2 := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper1 := NewSlowQueriesScraper(nil, mb1, logger, "instance-1", config, 1000, 100)
	scraper2 := NewSlowQueriesScraper(nil, mb2, logger, "instance-2", config, 2000, 200)

	assert.Equal(t, "instance-1", scraper1.instanceName)
	assert.Equal(t, "instance-2", scraper2.instanceName)
	assert.Equal(t, 1000, scraper1.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 2000, scraper2.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 100, scraper1.queryMonitoringCountThreshold)
	assert.Equal(t, 200, scraper2.queryMonitoringCountThreshold)
	assert.NotEqual(t, scraper1.mb, scraper2.mb)
}

func TestNewSlowQueriesScraper_NegativeThresholds(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, -100, -50)
	assert.NotNil(t, scraper)
	assert.Equal(t, -100, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, -50, scraper.queryMonitoringCountThreshold)
}

func TestSlowQueriesScraper_ConfigurationPersistence(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, 1500, 150)

	assert.Equal(t, 1500, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 150, scraper.queryMonitoringCountThreshold)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
}

func TestSlowQueriesScraper_MetricsBuilderConfig(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	t.Run("default_config", func(t *testing.T) {
		config := metadata.DefaultMetricsBuilderConfig()
		scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, 1000, 100)
		assert.NotNil(t, scraper.metricsBuilderConfig)
	})

	t.Run("custom_config", func(t *testing.T) {
		customConfig := metadata.DefaultMetricsBuilderConfig()
		scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", customConfig, 1000, 100)
		assert.Equal(t, customConfig, scraper.metricsBuilderConfig)
	})
}

func TestSlowQueriesScraper_LongInstanceName(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	longInstanceName := "this-is-a-very-long-instance-name-that-exceeds-normal-length-expectations-for-testing-purposes"
	scraper := NewSlowQueriesScraper(nil, mb, logger, longInstanceName, config, 1000, 100)

	assert.Equal(t, longInstanceName, scraper.instanceName)
}

func TestSlowQueriesScraper_ZeroThresholds(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, 0, 0)

	assert.Equal(t, 0, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, 0, scraper.queryMonitoringCountThreshold)
}

func TestSlowQueriesScraper_MaxIntThresholds(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	maxInt := int(^uint(0) >> 1)
	scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, maxInt, maxInt)

	assert.Equal(t, maxInt, scraper.queryMonitoringResponseTimeThreshold)
	assert.Equal(t, maxInt, scraper.queryMonitoringCountThreshold)
}

func TestSlowQueriesScraper_StructFieldAccess(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSlowQueriesScraper(nil, mb, logger, "test-instance", config, 1000, 100)

	assert.NotNil(t, scraper)
	assert.NotPanics(t, func() {
		_ = scraper.db
		_ = scraper.mb
		_ = scraper.logger
		_ = scraper.instanceName
		_ = scraper.metricsBuilderConfig
		_ = scraper.queryMonitoringResponseTimeThreshold
		_ = scraper.queryMonitoringCountThreshold
	})
}
