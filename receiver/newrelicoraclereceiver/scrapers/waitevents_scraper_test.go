// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewWaitEventsScraper(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(mockClient, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.Equal(t, 10, scraper.queryMonitoringCountThreshold)
}

func TestNewWaitEventsScraper_NilClient(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(nil, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.client)
}

func TestNewWaitEventsScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(mockClient, nil, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestNewWaitEventsScraper_NilLogger(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper := NewWaitEventsScraper(mockClient, mb, nil, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestNewWaitEventsScraper_EmptyInstanceName(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewWaitEventsScraper(mockClient, mb, logger, "", metadata.DefaultMetricsBuilderConfig(), 10)

	assert.NotNil(t, scraper)
	assert.Equal(t, "", scraper.instanceName)
}

func TestWaitEventsScraper_ScrapeWithValidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.WaitEvents = []models.WaitEvent{
		{
			QueryID:             sql.NullString{String: "wait_sql_1", Valid: true},
			WaitCategory:        sql.NullString{String: "User I/O", Valid: true},
			WaitEventName:       sql.NullString{String: "db file sequential read", Valid: true},
			WaitingTasksCount:   sql.NullInt64{Int64: 1500, Valid: true},
			TotalWaitTimeMs:     sql.NullFloat64{Float64: 45.7, Valid: true},
			AvgWaitTimeMs:       sql.NullFloat64{Float64: 0.03, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			CollectionTimestamp: sql.NullTime{Time: time.Now(), Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewWaitEventsScraper(mockClient, mb, zap.NewNop(), "test", config, 10)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Empty(t, errs)

	metrics := mb.Emit()
	require.Greater(t, metrics.ResourceMetrics().Len(), 0)
}

func TestWaitEventsScraper_ScrapeWithEmptyResults(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.WaitEvents = []models.WaitEvent{}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewWaitEventsScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig(), 10)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Empty(t, errs)
}

func TestWaitEventsScraper_ScrapeWithQueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("ORA-00942: table or view does not exist")

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewWaitEventsScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig(), 10)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "ORA-00942")
}

func TestWaitEventsScraper_ScrapeWithMultipleEvents(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.WaitEvents = []models.WaitEvent{
		{
			QueryID:           sql.NullString{String: "wait_sql_1", Valid: true},
			WaitCategory:      sql.NullString{String: "User I/O", Valid: true},
			WaitEventName:     sql.NullString{String: "db file sequential read", Valid: true},
			WaitingTasksCount: sql.NullInt64{Int64: 1500, Valid: true},
			TotalWaitTimeMs:   sql.NullFloat64{Float64: 25.0, Valid: true},
		},
		{
			QueryID:           sql.NullString{String: "wait_sql_2", Valid: true},
			WaitCategory:      sql.NullString{String: "Commit", Valid: true},
			WaitEventName:     sql.NullString{String: "log file sync", Valid: true},
			WaitingTasksCount: sql.NullInt64{Int64: 800, Valid: true},
			TotalWaitTimeMs:   sql.NullFloat64{Float64: 15.0, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewWaitEventsScraper(mockClient, mb, zap.NewNop(), "test", config, 10)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Empty(t, errs)
}

func TestWaitEventsScraper_ScrapeWithInvalidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.WaitEvents = []models.WaitEvent{
		{
			QueryID:           sql.NullString{Valid: false}, // Invalid
			WaitCategory:      sql.NullString{String: "User I/O", Valid: true},
			WaitEventName:     sql.NullString{String: "db file sequential read", Valid: true},
			WaitingTasksCount: sql.NullInt64{Int64: 1500, Valid: true},
			TotalWaitTimeMs:   sql.NullFloat64{Float64: 25.0, Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper := NewWaitEventsScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig(), 10)

	errs := scraper.ScrapeWaitEvents(context.Background())

	assert.Empty(t, errs)
}
