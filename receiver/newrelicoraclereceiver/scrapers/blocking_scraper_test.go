// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewBlockingScraper(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper, err := NewBlockingScraper(mockClient, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	require.NoError(t, err)
	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, "test-instance", scraper.instanceName)
	assert.Equal(t, 10, scraper.queryMonitoringCountThreshold)
}

func TestNewBlockingScraper_NilClient(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper, err := NewBlockingScraper(nil, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	require.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "client cannot be nil")
}

func TestNewBlockingScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()

	scraper, err := NewBlockingScraper(mockClient, nil, logger, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	require.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "metrics builder cannot be nil")
}

func TestNewBlockingScraper_NilLogger(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper, err := NewBlockingScraper(mockClient, mb, nil, "test-instance", metadata.DefaultMetricsBuilderConfig(), 10)

	require.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

func TestNewBlockingScraper_EmptyInstanceName(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper, err := NewBlockingScraper(mockClient, mb, logger, "", metadata.DefaultMetricsBuilderConfig(), 10)

	require.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "instance name cannot be empty")
}

func TestBlockingScraper_ScrapeWithValidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.BlockingQueries = []models.BlockingQuery{
		{
			SessionID:        sql.NullInt64{Int64: 100, Valid: true},
			BlockedSerial:    sql.NullInt64{Int64: 5001, Valid: true},
			BlockedUser:      sql.NullString{String: "USER1", Valid: true},
			BlockedWaitSec:   sql.NullFloat64{Float64: 125.5, Valid: true},
			QueryID:          sql.NullString{String: "blocked_sql_1", Valid: true},
			BlockingQueryText: sql.NullString{String: "UPDATE users SET status = 1", Valid: true},
			BlockingSID:      sql.NullInt64{Int64: 200, Valid: true},
			BlockingSerial:   sql.NullInt64{Int64: 6001, Valid: true},
			BlockingUser:     sql.NullString{String: "USER2", Valid: true},
			DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbBlockingQueriesWaitTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper, err := NewBlockingScraper(mockClient, mb, zap.NewNop(), "test", config, 10)
	require.NoError(t, err)

	errs := scraper.ScrapeBlockingQueries(context.Background())

	assert.Empty(t, errs)

	metrics := mb.Emit()
	require.Greater(t, metrics.ResourceMetrics().Len(), 0)
}

func TestBlockingScraper_ScrapeWithEmptyResults(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.BlockingQueries = []models.BlockingQuery{}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper, err := NewBlockingScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig(), 10)
	require.NoError(t, err)

	errs := scraper.ScrapeBlockingQueries(context.Background())

	assert.Empty(t, errs)
}

func TestBlockingScraper_ScrapeWithQueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("ORA-01017: invalid username/password")

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper, err := NewBlockingScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig(), 10)
	require.NoError(t, err)

	errs := scraper.ScrapeBlockingQueries(context.Background())

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "ORA-01017")
}

func TestBlockingScraper_ScrapeWithMultipleBlocking(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.BlockingQueries = []models.BlockingQuery{
		{
			SessionID:      sql.NullInt64{Int64: 100, Valid: true},
			BlockedUser:    sql.NullString{String: "USER1", Valid: true},
			BlockedWaitSec: sql.NullFloat64{Float64: 50.0, Valid: true},
			BlockingSID:    sql.NullInt64{Int64: 200, Valid: true},
			BlockingUser:   sql.NullString{String: "USER2", Valid: true},
			DatabaseName:   sql.NullString{String: "TESTDB", Valid: true},
		},
		{
			SessionID:      sql.NullInt64{Int64: 150, Valid: true},
			BlockedUser:    sql.NullString{String: "USER3", Valid: true},
			BlockedWaitSec: sql.NullFloat64{Float64: 75.5, Valid: true},
			BlockingSID:    sql.NullInt64{Int64: 250, Valid: true},
			BlockingUser:   sql.NullString{String: "USER4", Valid: true},
			DatabaseName:   sql.NullString{String: "TESTDB", Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbBlockingQueriesWaitTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper, err := NewBlockingScraper(mockClient, mb, zap.NewNop(), "test", config, 10)
	require.NoError(t, err)

	errs := scraper.ScrapeBlockingQueries(context.Background())

	assert.Empty(t, errs)
}

func TestBlockingScraper_ScrapeWithInvalidWaitTime(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.BlockingQueries = []models.BlockingQuery{
		{
			SessionID:      sql.NullInt64{Int64: 100, Valid: true},
			BlockedUser:    sql.NullString{String: "USER1", Valid: true},
			BlockedWaitSec: sql.NullFloat64{Valid: false},
			BlockingSID:    sql.NullInt64{Int64: 200, Valid: true},
			BlockingUser:   sql.NullString{String: "USER2", Valid: true},
		},
	}

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	scraper, err := NewBlockingScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig(), 10)
	require.NoError(t, err)

	errs := scraper.ScrapeBlockingQueries(context.Background())

	assert.Empty(t, errs)
}
