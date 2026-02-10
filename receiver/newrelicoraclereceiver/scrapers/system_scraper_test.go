// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewSystemScraper(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSystemScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
}

func TestSystemScraper_NilDatabase(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.client)
}

func TestSystemScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()

	scraper := NewSystemScraper(mockClient, nil, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestSystemScraper_NilLogger(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper := NewSystemScraper(mockClient, mb, nil, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestSystemScraper_EmptyInstanceName(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
}

func TestSystemScraper_MetricsBuilderConfig(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewSystemScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
}

func TestSystemScraper_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper1 := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
	scraper2 := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotEqual(t, scraper1, scraper2)
}

func TestSystemScraper_RecordMetric_BufferCacheMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Buffer Cache Hit Ratio", 95.5, "1")
	})
}

func TestSystemScraper_RecordMetric_TransactionMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "User Transaction Per Sec", 100.0, "1")
		scraper.recordMetric(0, "User Commits Per Sec", 50.0, "1")
		scraper.recordMetric(0, "User Rollbacks Per Sec", 5.0, "1")
	})
}

func TestSystemScraper_RecordMetric_IOMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Physical Reads Per Sec", 1000.0, "1")
		scraper.recordMetric(0, "Physical Writes Per Sec", 500.0, "1")
		scraper.recordMetric(0, "Logical Reads Per Sec", 5000.0, "1")
	})
}

func TestSystemScraper_RecordMetric_ParseMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Total Parse Count Per Sec", 200.0, "1")
		scraper.recordMetric(0, "Hard Parse Count Per Sec", 10.0, "1")
		scraper.recordMetric(0, "Soft Parse Ratio", 95.0, "1")
	})
}

func TestSystemScraper_RecordMetric_CPUMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "CPU Usage Per Sec", 75.5, "1")
		scraper.recordMetric(0, "Host CPU Utilization (%)", 60.0, "1")
		scraper.recordMetric(0, "Background CPU Usage Per Sec", 10.0, "1")
	})
}

func TestSystemScraper_RecordMetric_SessionMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Current Logons Count", 50.0, "1")
		scraper.recordMetric(0, "Session Count", 100.0, "1")
		scraper.recordMetric(0, "Average Active Sessions", 25.0, "1")
	})
}

func TestSystemScraper_RecordMetric_UnknownMetric(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Unknown Metric Name", 42.0, "1")
	})
}

// Tests for ScrapeSystemMetrics

func TestScrapeSystemMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "Buffer Cache Hit Ratio", Value: 95.5},
			{InstanceID: "1", MetricName: "CPU Usage Per Sec", Value: 75.0},
			{InstanceID: "2", MetricName: "User Transaction Per Sec", Value: 100.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("database connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection failed")
}

func TestScrapeSystemMetrics_EmptyResults(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "Buffer Cache Hit Ratio", Value: 95.5},
			{InstanceID: "2", MetricName: "Buffer Cache Hit Ratio", Value: 93.0},
			{InstanceID: "3", MetricName: "Buffer Cache Hit Ratio", Value: 97.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_MixedMetrics(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "Buffer Cache Hit Ratio", Value: 95.5},
			{InstanceID: "1", MetricName: "CPU Usage Per Sec", Value: 75.0},
			{InstanceID: "1", MetricName: "User Transaction Per Sec", Value: 100.0},
			{InstanceID: "1", MetricName: "Physical Reads Per Sec", Value: 1000.0},
			{InstanceID: "1", MetricName: "Logical Reads Per Sec", Value: 5000.0},
			{InstanceID: "1", MetricName: "Total Parse Count Per Sec", Value: 200.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_UnknownMetrics(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "Unknown Metric 1", Value: 10.0},
			{InstanceID: "1", MetricName: "Buffer Cache Hit Ratio", Value: 95.5},
			{InstanceID: "1", MetricName: "Unknown Metric 2", Value: 20.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_ZeroValues(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "User Rollbacks Per Sec", Value: 0.0},
			{InstanceID: "1", MetricName: "Hard Parse Count Per Sec", Value: 0.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_LargeValues(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "Logical Reads Per Sec", Value: 999999.99},
			{InstanceID: "1", MetricName: "Physical Reads Per Sec", Value: 888888.88},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeSystemMetrics_ContextCanceled(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: context.Canceled,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeSystemMetrics(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSystemMetrics_MultipleSuccessfulCalls(t *testing.T) {
	mockClient := &client.MockClient{
		SystemMetricsList: []models.SystemMetric{
			{InstanceID: "1", MetricName: "CPU Usage Per Sec", Value: 50.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()

	// First call
	errs1 := scraper.ScrapeSystemMetrics(ctx)
	assert.Nil(t, errs1)

	// Second call with different values
	mockClient.SystemMetricsList = []models.SystemMetric{
		{InstanceID: "1", MetricName: "CPU Usage Per Sec", Value: 75.0},
	}
	errs2 := scraper.ScrapeSystemMetrics(ctx)
	assert.Nil(t, errs2)
}

func TestScrapeSystemMetrics_ErrorThenSuccess(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("temporary error"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()

	// First call fails
	errs1 := scraper.ScrapeSystemMetrics(ctx)
	assert.NotNil(t, errs1)
	assert.Len(t, errs1, 1)

	// Second call succeeds
	mockClient.QueryErr = nil
	mockClient.SystemMetricsList = []models.SystemMetric{
		{InstanceID: "1", MetricName: "CPU Usage Per Sec", Value: 50.0},
	}
	errs2 := scraper.ScrapeSystemMetrics(ctx)
	assert.Nil(t, errs2)
}

// Tests for recordMetric

func TestSystemScraper_RecordMetric_KnownMetric(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Buffer Cache Hit Ratio", 95.5, "1")
	})
}

func TestSystemScraper_RecordMetric_UnknownMetricLogged(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "NonExistent Metric", 999.0, "1")
	})
}

func TestSystemScraper_RecordMetric_MultipleMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "Buffer Cache Hit Ratio", 95.5, "1")
		scraper.recordMetric(0, "CPU Usage Per Sec", 75.0, "1")
		scraper.recordMetric(0, "User Transaction Per Sec", 100.0, "1")
	})
}

func TestSystemScraper_RecordMetric_DifferentInstances(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSystemScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.NotPanics(t, func() {
		scraper.recordMetric(0, "CPU Usage Per Sec", 50.0, "1")
		scraper.recordMetric(0, "CPU Usage Per Sec", 75.0, "2")
		scraper.recordMetric(0, "CPU Usage Per Sec", 60.0, "3")
	})
}
