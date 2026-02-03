// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

type mockExtraStatusClient struct {
	stats     map[string]string
	statsErr  error
	variables map[string]string
	varsErr   error
}

func (m *mockExtraStatusClient) Connect() error {
	return nil
}

func (m *mockExtraStatusClient) GetGlobalStats() (map[string]string, error) {
	return m.stats, m.statsErr
}

func (m *mockExtraStatusClient) GetGlobalVariables() (map[string]string, error) {
	return m.variables, m.varsErr
}

func (m *mockExtraStatusClient) GetReplicationStatus() (map[string]string, error) {
	return nil, nil
}

func (m *mockExtraStatusClient) GetMasterStatus() (map[string]string, error) {
	return nil, nil
}

func (m *mockExtraStatusClient) GetGroupReplicationStats() (map[string]string, error) {
	return nil, nil
}

func (m *mockExtraStatusClient) GetVersion() (string, error) {
	return "8.0.0", nil
}

func (m *mockExtraStatusClient) Close() error {
	return nil
}

func TestNewExtraStatusScraper(t *testing.T) {
	tests := []struct {
		name        string
		client      *mockExtraStatusClient
		mb          *metadata.MetricsBuilder
		logger      *zap.Logger
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_inputs",
			client:      &mockExtraStatusClient{},
			mb:          metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:      zap.NewNop(),
			expectError: false,
		},
		{
			name:        "nil_client",
			client:      nil,
			mb:          metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:      zap.NewNop(),
			expectError: true,
			errorMsg:    "client cannot be nil",
		},
		{
			name:        "nil_metrics_builder",
			client:      &mockExtraStatusClient{},
			mb:          nil,
			logger:      zap.NewNop(),
			expectError: true,
			errorMsg:    "metrics builder cannot be nil",
		},
		{
			name:        "nil_logger",
			client:      &mockExtraStatusClient{},
			mb:          metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType)),
			logger:      nil,
			expectError: true,
			errorMsg:    "logger cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var client common.Client
			if tt.client != nil {
				client = tt.client
			}
			scraper, err := NewExtraStatusScraper(client, tt.mb, tt.logger)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, scraper)
			} else {
				require.NoError(t, err)
				require.NotNil(t, scraper)
			}
		})
	}
}

func TestExtraStatusScraper_ScrapeMetrics_Success(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{
			// Binlog cache metrics
			"Binlog_cache_disk_use": "10",
			"Binlog_cache_use":      "100",

			// Handler metrics
			"Handler_commit":        "500",
			"Handler_delete":        "20",
			"Handler_prepare":       "30",
			"Handler_read_first":    "40",
			"Handler_read_key":      "1000",
			"Handler_read_next":     "2000",
			"Handler_read_prev":     "50",
			"Handler_read_rnd":      "60",
			"Handler_read_rnd_next": "3000",
			"Handler_rollback":      "5",
			"Handler_update":        "200",
			"Handler_write":         "150",

			// Table operations
			"Opened_tables": "100",

			// Query cache metrics
			"Qcache_total_blocks":     "500",
			"Qcache_free_blocks":      "100",
			"Qcache_free_memory":      "1048576",
			"Qcache_not_cached":       "50",
			"Qcache_queries_in_cache": "200",

			// SELECT metrics
			"Select_full_join":       "5",
			"Select_full_range_join": "10",
			"Select_range":           "100",
			"Select_range_check":     "2",
			"Select_scan":            "500",

			// Sort metrics
			"Sort_merge_passes": "10",
			"Sort_range":        "50",
			"Sort_rows":         "1000",
			"Sort_scan":         "200",

			// Table locks
			"Table_locks_immediate": "5000",

			// Thread metrics
			"Threads_cached":  "8",
			"Threads_created": "20",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.Nil(t, errs.Combine(), "Expected no errors")
}

func TestExtraStatusScraper_ScrapeMetrics_ClientError(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		statsErr: errors.New("database connection error"),
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.NotNil(t, errs.Combine(), "Expected partial scrape error")
}

func TestExtraStatusScraper_ScrapeMetrics_MissingMetrics(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{
			// Only include a few metrics
			"Handler_commit": "500",
			"Threads_cached": "8",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	// Should not error even if some metrics are missing
	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors even with missing metrics")
}

func TestExtraStatusScraper_ScrapeMetrics_InvalidNumericValues(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{
			"Handler_commit":   "not_a_number",
			"Threads_cached":   "invalid",
			"Binlog_cache_use": "also_invalid",
			"Handler_delete":   "12.34", // Will fail parseInt64
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	// Should handle invalid values gracefully (parseInt64 returns 0 and logs)
	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors, invalid values handled gracefully")
}

func TestExtraStatusScraper_ScrapeMetrics_EmptyStats(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors with empty stats")
}

func TestExtraStatusScraper_ScrapeMetrics_ZeroValues(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{
			"Binlog_cache_disk_use": "0",
			"Handler_commit":        "0",
			"Threads_cached":        "0",
			"Sort_rows":             "0",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors with zero values")
}

func TestExtraStatusScraper_ScrapeMetrics_LargeValues(t *testing.T) {
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{
			"Handler_commit":        "9223372036854775807", // Max int64
			"Handler_read_rnd_next": "1000000000000",
			"Threads_created":       "999999999",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors with large values")
}

func TestExtraStatusScraper_ScrapeMetrics_AllExtraStatusMetrics(t *testing.T) {
	// Test that all 36 extra_status_metrics are handled
	mockClient := &mockExtraStatusClient{
		stats: map[string]string{
			// Binlog (2)
			"Binlog_cache_disk_use": "1",
			"Binlog_cache_use":      "2",

			// Handler (12)
			"Handler_commit":        "3",
			"Handler_delete":        "4",
			"Handler_prepare":       "5",
			"Handler_read_first":    "6",
			"Handler_read_key":      "7",
			"Handler_read_next":     "8",
			"Handler_read_prev":     "9",
			"Handler_read_rnd":      "10",
			"Handler_read_rnd_next": "11",
			"Handler_rollback":      "12",
			"Handler_update":        "13",
			"Handler_write":         "14",

			// Table (1)
			"Opened_tables": "15",

			// Qcache (5)
			"Qcache_total_blocks":     "16",
			"Qcache_free_blocks":      "17",
			"Qcache_free_memory":      "18",
			"Qcache_not_cached":       "19",
			"Qcache_queries_in_cache": "20",

			// Select (5)
			"Select_full_join":       "21",
			"Select_full_range_join": "22",
			"Select_range":           "23",
			"Select_range_check":     "24",
			"Select_scan":            "25",

			// Sort (4)
			"Sort_merge_passes": "26",
			"Sort_range":        "27",
			"Sort_rows":         "28",
			"Sort_scan":         "29",

			// Table locks (1 - records 2 metrics)
			"Table_locks_immediate": "30",

			// Threads (2)
			"Threads_cached":  "31",
			"Threads_created": "32",
		},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewExtraStatusScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.False(t, errs.Combine() != nil, "Expected no errors when all metrics present")
}
