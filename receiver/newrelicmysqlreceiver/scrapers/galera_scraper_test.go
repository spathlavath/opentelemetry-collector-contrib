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

type mockGaleraClient struct {
	stats     map[string]string
	statsErr  error
	variables map[string]string
	varsErr   error
}

func (m *mockGaleraClient) Connect() error {
	return nil
}

func (m *mockGaleraClient) GetGlobalStats() (map[string]string, error) {
	return m.stats, m.statsErr
}

func (m *mockGaleraClient) GetGlobalVariables() (map[string]string, error) {
	return m.variables, m.varsErr
}

func (m *mockGaleraClient) GetReplicationStatus() (map[string]string, error) {
	return nil, nil
}

func (m *mockGaleraClient) GetVersion() (string, error) {
	return "8.0.0", nil
}

func (m *mockGaleraClient) Close() error {
	return nil
}

func TestNewGaleraScraper(t *testing.T) {
	tests := []struct {
		name        string
		client      *mockGaleraClient
		mb          *metadata.MetricsBuilder
		logger      *zap.Logger
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid_inputs",
			client:      &mockGaleraClient{},
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
			client:      &mockGaleraClient{},
			mb:          nil,
			logger:      zap.NewNop(),
			expectError: true,
			errorMsg:    "metrics builder cannot be nil",
		},
		{
			name:        "nil_logger",
			client:      &mockGaleraClient{},
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

			scraper, err := NewGaleraScraper(client, tt.mb, tt.logger)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, scraper)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, scraper)
			}
		})
	}
}

func TestGaleraScraperScrapeMetrics_Success(t *testing.T) {
	mockClient := &mockGaleraClient{
		stats: map[string]string{
			"wsrep_cert_deps_distance":     "1.5",
			"wsrep_cluster_size":           "3",
			"wsrep_flow_control_paused":    "0.0123",
			"wsrep_flow_control_paused_ns": "1000000000",
			"wsrep_flow_control_recv":      "10",
			"wsrep_flow_control_sent":      "5",
			"wsrep_local_cert_failures":    "2",
			"wsrep_local_recv_queue":       "15",
			"wsrep_local_recv_queue_avg":   "12.5",
			"wsrep_local_send_queue":       "8",
			"wsrep_local_send_queue_avg":   "7.2",
			"wsrep_local_state":            "4",
			"wsrep_received":               "1000",
			"wsrep_received_bytes":         "500000",
			"wsrep_replicated_bytes":       "450000",
		},
	}

	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()

	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewGaleraScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.Nil(t, errs.Combine(), "Expected no scrape errors")

	metrics := mb.Emit()
	assert.Greater(t, metrics.MetricCount(), 0, "Expected metrics to be collected")
}

func TestGaleraScraperScrapeMetrics_ClientError(t *testing.T) {
	mockClient := &mockGaleraClient{
		statsErr: errors.New("database connection failed"),
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewGaleraScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.NotNil(t, errs.Combine(), "Expected partial scrape error")
}

func TestGaleraScraperScrapeMetrics_EmptyStats(t *testing.T) {
	mockClient := &mockGaleraClient{
		stats: map[string]string{},
	}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewGaleraScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.Nil(t, errs.Combine(), "Expected no errors for empty stats")
}

func TestGaleraScraperScrapeMetrics_InvalidValues(t *testing.T) {
	mockClient := &mockGaleraClient{
		stats: map[string]string{
			"wsrep_cert_deps_distance": "invalid_float",
			"wsrep_cluster_size":       "invalid_int",
		},
	}

	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	metricsBuilderConfig.Metrics.NewrelicmysqlGaleraWsrepCertDepsDistance.Enabled = true
	metricsBuilderConfig.Metrics.NewrelicmysqlGaleraWsrepClusterSize.Enabled = true

	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewGaleraScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	// Should complete without errors, but invalid values will be logged and set to 0
	assert.Nil(t, errs.Combine(), "Expected no errors for invalid values (they should be logged and set to 0)")
}

func TestGaleraScraperScrapeMetrics_PartialData(t *testing.T) {
	mockClient := &mockGaleraClient{
		stats: map[string]string{
			"wsrep_cluster_size": "3",
			"wsrep_local_state":  "4",
			// Only some Galera metrics present
		},
	}

	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	metricsBuilderConfig.Metrics.NewrelicmysqlGaleraWsrepClusterSize.Enabled = true
	metricsBuilderConfig.Metrics.NewrelicmysqlGaleraWsrepLocalState.Enabled = true

	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewGaleraScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.Nil(t, errs.Combine(), "Expected no errors for partial data")

	metrics := mb.Emit()
	assert.Greater(t, metrics.MetricCount(), 0, "Expected some metrics to be collected")
}

func TestGaleraScraperScrapeMetrics_ZeroValues(t *testing.T) {
	mockClient := &mockGaleraClient{
		stats: map[string]string{
			"wsrep_cert_deps_distance":     "0.0",
			"wsrep_cluster_size":           "0",
			"wsrep_flow_control_paused":    "0.0",
			"wsrep_flow_control_paused_ns": "0",
			"wsrep_flow_control_recv":      "0",
			"wsrep_flow_control_sent":      "0",
			"wsrep_local_cert_failures":    "0",
			"wsrep_local_recv_queue":       "0",
			"wsrep_local_recv_queue_avg":   "0.0",
			"wsrep_local_send_queue":       "0",
			"wsrep_local_send_queue_avg":   "0.0",
			"wsrep_local_state":            "0",
			"wsrep_received":               "0",
			"wsrep_received_bytes":         "0",
			"wsrep_replicated_bytes":       "0",
		},
	}

	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	metricsBuilderConfig.Metrics.NewrelicmysqlGaleraWsrepClusterSize.Enabled = true
	metricsBuilderConfig.Metrics.NewrelicmysqlGaleraWsrepLocalState.Enabled = true

	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, receivertest.NewNopSettings(receivertest.NopType))
	logger := zap.NewNop()

	scraper, err := NewGaleraScraper(mockClient, mb, logger)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	scraper.ScrapeMetrics(ctx, now, errs)

	assert.Nil(t, errs.Combine(), "Expected no errors for zero values")

	metrics := mb.Emit()
	assert.Greater(t, metrics.MetricCount(), 0, "Expected metrics with zero values to be collected")
}
