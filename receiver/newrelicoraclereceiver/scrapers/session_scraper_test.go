// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
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

// Tests for ScrapeSessionCount

func TestScrapeSessionCount_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SessionCount: &models.SessionCount{Count: 150},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.Nil(t, errs)
	assert.Len(t, errs, 0)
}

func TestScrapeSessionCount_MetricDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSessionsCount.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, config)

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.Nil(t, errs)
	assert.Len(t, errs, 0)
}

func TestScrapeSessionCount_NoRows(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: sql.ErrNoRows,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.Nil(t, errs)
	assert.Len(t, errs, 0)
}

func TestScrapeSessionCount_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("database connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection failed")
}

func TestScrapeSessionCount_ZeroCount(t *testing.T) {
	mockClient := &client.MockClient{
		SessionCount: &models.SessionCount{Count: 0},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.Nil(t, errs)
	assert.Len(t, errs, 0)
}

func TestScrapeSessionCount_LargeCount(t *testing.T) {
	mockClient := &client.MockClient{
		SessionCount: &models.SessionCount{Count: 9999},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.Nil(t, errs)
	assert.Len(t, errs, 0)
}

func TestScrapeSessionCount_NilCount(t *testing.T) {
	mockClient := &client.MockClient{
		SessionCount: nil,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.Nil(t, errs)
	assert.Len(t, errs, 0)
}

func TestScrapeSessionCount_ContextCanceled(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: context.Canceled,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapeSessionCount(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSessionCount_MultipleSuccessfulCalls(t *testing.T) {
	mockClient := &client.MockClient{
		SessionCount: &models.SessionCount{Count: 100},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()

	// First call
	errs1 := scraper.ScrapeSessionCount(ctx)
	assert.Nil(t, errs1)

	// Second call with different count
	mockClient.SessionCount = &models.SessionCount{Count: 200}
	errs2 := scraper.ScrapeSessionCount(ctx)
	assert.Nil(t, errs2)
}

func TestScrapeSessionCount_ErrorThenSuccess(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("temporary error"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewSessionScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()

	// First call fails
	errs1 := scraper.ScrapeSessionCount(ctx)
	assert.NotNil(t, errs1)
	assert.Len(t, errs1, 1)

	// Second call succeeds
	mockClient.QueryErr = nil
	mockClient.SessionCount = &models.SessionCount{Count: 150}
	errs2 := scraper.ScrapeSessionCount(ctx)
	assert.Nil(t, errs2)
}
