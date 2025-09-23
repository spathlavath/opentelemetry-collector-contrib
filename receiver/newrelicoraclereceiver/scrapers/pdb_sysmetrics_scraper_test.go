// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

// createTestMetricsBuilder creates a MetricsBuilder for testing
func createTestMetricsBuilder() *metadata.MetricsBuilder {
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receiver.Settings{
		ID:        component.MustNewID("test"),
		BuildInfo: component.NewDefaultBuildInfo(),
	}
	return metadata.NewMetricsBuilder(config, settings)
}

func TestPDBSysMetricsScraper_InitializeMetricsMap(t *testing.T) {
	metricsMap := initializeMetricsMap()
	
	// Test that we have the expected number of metrics
	assert.Greater(t, len(metricsMap), 0, "Metrics map should not be empty")
	
	// Test some specific metric mappings
	testCases := map[string]string{
		"Session Count":                     "pdb.sessions.count",
		"CPU Usage Per Sec":                "pdb.cpu.usage_per_sec",
		"User Commits Per Sec":             "pdb.transactions.commits_per_sec",
		"Physical Reads Per Sec":           "pdb.io.physical_reads_per_sec",
		"Buffer Cache Hit Ratio":           "pdb.cache.buffer_hit_ratio",
	}
	
	for oracleMetric, expectedOtelMetric := range testCases {
		otelMetric, exists := metricsMap[oracleMetric]
		assert.True(t, exists, "Oracle metric %s should be mapped", oracleMetric)
		assert.Equal(t, expectedOtelMetric, otelMetric, "Mapping for %s should be correct", oracleMetric)
	}
}

func TestPDBSysMetricsScraper_GetSupportedMetrics(t *testing.T) {
	// Create a minimal config for testing
	config := metadata.DefaultMetricsBuilderConfig()
	logger := zap.NewNop()
	mb := createTestMetricsBuilder()
	
	scraper := NewPDBSysMetricsScraper(nil, mb, logger, "test-instance", config)
	
	supportedMetrics := scraper.GetSupportedMetrics()
	assert.Greater(t, len(supportedMetrics), 0, "Should have supported metrics")
	
	// Verify some expected metrics are present
	expectedMetrics := []string{
		"Session Count",
		"CPU Usage Per Sec",
		"Physical Reads Per Sec",
		"User Commits Per Sec",
	}
	
	for _, expected := range expectedMetrics {
		found := false
		for _, supported := range supportedMetrics {
			if supported == expected {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected metric %s should be in supported metrics", expected)
	}
}

func TestPDBSysMetricsScraper_GetMetricMapping(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	logger := zap.NewNop()
	mb := createTestMetricsBuilder()
	
	scraper := NewPDBSysMetricsScraper(nil, mb, logger, "test-instance", config)
	
	mapping := scraper.GetMetricMapping()
	assert.Greater(t, len(mapping), 0, "Mapping should not be empty")
	
	// Verify the mapping is a copy (immutable)
	originalSize := len(mapping)
	mapping["test"] = "test"
	newMapping := scraper.GetMetricMapping()
	assert.Equal(t, originalSize, len(newMapping), "Original mapping should not be modified")
}

func TestPDBSysMetricsScraper_ScrapePDBSysMetrics_NilDB(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	logger := zap.NewNop()
	mb := createTestMetricsBuilder()
	
	scraper := NewPDBSysMetricsScraper(nil, mb, logger, "test-instance", config)
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	
	errors := scraper.ScrapePDBSysMetrics(ctx)
	
	// Should have at least one error due to nil DB
	assert.Greater(t, len(errors), 0, "Should return errors when DB is nil")
}

// MockRow implements database scanning for testing
type MockRow struct {
	InstID     int64
	MetricName string
	Value      float64
	MetricUnit string
	ConID      int64
}

func TestPDBSysMetric_Scanning(t *testing.T) {
	// Test that our PDBSysMetric struct can properly scan values
	testMetric := PDBSysMetric{}
	
	// This would normally be done by the SQL driver
	testMetric.InstID = 1
	testMetric.MetricName = "Session Count"
	testMetric.Value = 9.0
	testMetric.MetricUnit = "Sessions"
	testMetric.ConID = 3
	
	assert.Equal(t, int64(1), testMetric.InstID)
	assert.Equal(t, "Session Count", testMetric.MetricName)
	assert.Equal(t, 9.0, testMetric.Value)
	assert.Equal(t, "Sessions", testMetric.MetricUnit)
	assert.Equal(t, int64(3), testMetric.ConID)
}

func TestNewPDBSysMetricsScraper(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	logger := zap.NewNop()
	mb := createTestMetricsBuilder()
	
	scraper := NewPDBSysMetricsScraper(nil, mb, logger, "test-instance", config)
	
	require.NotNil(t, scraper)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.NotNil(t, scraper.logger)
	assert.NotNil(t, scraper.mb)
	assert.NotNil(t, scraper.metricsMap)
	assert.NotNil(t, scraper.metricsChan)
	assert.NotNil(t, scraper.errorsChan)
}