// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

var testTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func TestNewConnectionScraper(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	instanceName := "test-instance"
	metricsConfig := metadata.DefaultMetricsBuilderConfig()

	scraper := NewConnectionScraper(nil, mb, logger, instanceName, metricsConfig)

	assert.NotNil(t, scraper)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, metricsConfig, scraper.metricsBuilderConfig)
}

func TestConnectionScraperFormatInt64(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewConnectionScraper(nil, mb, zap.NewNop(), "test-instance", metadata.DefaultMetricsBuilderConfig())

	tests := []struct {
		name     string
		input    sql.NullInt64
		expected string
	}{
		{
			name:     "valid value",
			input:    sql.NullInt64{Int64: 123, Valid: true},
			expected: "123",
		},
		{
			name:     "invalid null value",
			input:    sql.NullInt64{Valid: false},
			expected: "",
		},
		{
			name:     "zero value",
			input:    sql.NullInt64{Int64: 0, Valid: true},
			expected: "0",
		},
		{
			name:     "negative value",
			input:    sql.NullInt64{Int64: -456, Valid: true},
			expected: "-456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scraper.formatInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConnectionScraperFormatString(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewConnectionScraper(nil, mb, zap.NewNop(), "test-instance", metadata.DefaultMetricsBuilderConfig())

	tests := []struct {
		name     string
		input    sql.NullString
		expected string
	}{
		{
			name:     "valid string",
			input:    sql.NullString{String: "test", Valid: true},
			expected: "test",
		},
		{
			name:     "invalid null string",
			input:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty string",
			input:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
		{
			name:     "string with spaces",
			input:    sql.NullString{String: "  test  ", Valid: true},
			expected: "  test  ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scraper.formatString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScrapeSingleValue(t *testing.T) {
	ctx := context.Background()
	timestamp := pcommon.NewTimestampFromTime(testTime)

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewConnectionScraper(nil, mb, zap.NewNop(), "test-instance", metadata.DefaultMetricsBuilderConfig())

	// Test with nil DB - should fail gracefully
	err := scraper.scrapeSingleValue(ctx, "SELECT 1", "total_sessions", timestamp)
	assert.Error(t, err)
}

func TestScrapeConnectionMetrics_NilDB(t *testing.T) {
	ctx := context.Background()

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewConnectionScraper(nil, mb, zap.NewNop(), "test-instance", metadata.DefaultMetricsBuilderConfig())

	// Should handle nil DB gracefully
	errs := scraper.ScrapeConnectionMetrics(ctx)
	assert.NotEmpty(t, errs, "Expected errors when DB is nil")
}

func TestScraperStructFields(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	instanceName := "prod-instance"
	metricsConfig := metadata.DefaultMetricsBuilderConfig()

	scraper := NewConnectionScraper(nil, mb, logger, instanceName, metricsConfig)

	// Verify all fields are properly set
	assert.Nil(t, scraper.db)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, metricsConfig, scraper.metricsBuilderConfig)
}

func TestScrapeMethodsReturnErrorSlices(t *testing.T) {
	ctx := context.Background()
	timestamp := pcommon.NewTimestampFromTime(testTime)

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewConnectionScraper(nil, mb, zap.NewNop(), "test-instance", metadata.DefaultMetricsBuilderConfig())

	// All scrape methods should return error slices (not panic)
	t.Run("scrapeCoreConnectionCounts", func(t *testing.T) {
		errs := scraper.scrapeCoreConnectionCounts(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeSessionBreakdown", func(t *testing.T) {
		errs := scraper.scrapeSessionBreakdown(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeLogonStats", func(t *testing.T) {
		errs := scraper.scrapeLogonStats(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeSessionResourceConsumption", func(t *testing.T) {
		errs := scraper.scrapeSessionResourceConsumption(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeWaitEvents", func(t *testing.T) {
		errs := scraper.scrapeWaitEvents(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeBlockingSessions", func(t *testing.T) {
		errs := scraper.scrapeBlockingSessions(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeWaitEventSummary", func(t *testing.T) {
		errs := scraper.scrapeWaitEventSummary(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeConnectionPoolMetrics", func(t *testing.T) {
		errs := scraper.scrapeConnectionPoolMetrics(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeSessionLimits", func(t *testing.T) {
		errs := scraper.scrapeSessionLimits(ctx, timestamp)
		assert.NotNil(t, errs)
	})

	t.Run("scrapeConnectionQuality", func(t *testing.T) {
		errs := scraper.scrapeConnectionQuality(ctx, timestamp)
		assert.NotNil(t, errs)
	})
}
