// Copyright 2025 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func testTS() pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(time.Now())
}

// Test scrapeSysstatMetrics

func TestScrapeSysstatMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SysstatMetricsList: []models.SysstatMetric{
			{InstID: "1", Name: "redo buffer allocation retries", Value: sql.NullInt64{Int64: 100, Valid: true}},
			{InstID: "1", Name: "redo entries", Value: sql.NullInt64{Int64: 5000, Valid: true}},
			{InstID: "1", Name: "sorts (memory)", Value: sql.NullInt64{Int64: 1000, Valid: true}},
			{InstID: "1", Name: "sorts (disk)", Value: sql.NullInt64{Int64: 50, Valid: true}},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeSysstatMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("query failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Len(t, errs, 1)
}

func TestScrapeSysstatMetrics_NullValues(t *testing.T) {
	mockClient := &client.MockClient{
		SysstatMetricsList: []models.SysstatMetric{
			{InstID: "1", Name: "redo entries", Value: sql.NullInt64{Valid: false}},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Empty(t, errs)
}

// Test scrapeRollbackSegmentsMetrics

func TestScrapeRollbackSegmentsMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		RollbackSegmentsMetricsList: []models.RollbackSegmentsMetric{
			{
				Gets:   sql.NullInt64{Int64: 1000, Valid: true},
				Waits:  sql.NullInt64{Int64: 10, Valid: true},
				Ratio:  sql.NullFloat64{Float64: 0.01, Valid: true},
				InstID: "1",
			},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("query failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, testTS())
	require.Len(t, errs, 1)
}

// Test scrapeRedoLogWaitsMetrics

func TestScrapeRedoLogWaitsMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		RedoLogWaitsMetricsList: []models.RedoLogWaitsMetric{
			{TotalWaits: sql.NullInt64{Int64: 100, Valid: true}, InstID: "1", Event: "log file parallel write"},
			{TotalWaits: sql.NullInt64{Int64: 50, Valid: true}, InstID: "1", Event: "log file switch completion"},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("query failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, testTS())
	require.Len(t, errs, 1)
}
func TestScrapeRedoLogWaitsMetrics_AllEventTypes(t *testing.T) {
	mockClient := &client.MockClient{
		RedoLogWaitsMetricsList: []models.RedoLogWaitsMetric{
			{TotalWaits: sql.NullInt64{Int64: 100, Valid: true}, InstID: "1", Event: "log file parallel write"},
			{TotalWaits: sql.NullInt64{Int64: 50, Valid: true}, InstID: "1", Event: "log file switch completion"},
			{TotalWaits: sql.NullInt64{Int64: 30, Valid: true}, InstID: "1", Event: "log file switch (checkpoint incomplete)"},
			{TotalWaits: sql.NullInt64{Int64: 20, Valid: true}, InstID: "1", Event: "log file switch (archiving needed)"},
			{TotalWaits: sql.NullInt64{Int64: 40, Valid: true}, InstID: "1", Event: "buffer busy waits"},
			{TotalWaits: sql.NullInt64{Int64: 10, Valid: true}, InstID: "1", Event: "freeBufferWaits"},
			{TotalWaits: sql.NullInt64{Int64: 5, Valid: true}, InstID: "1", Event: "free buffer inspected"},
			{TotalWaits: sql.NullInt64{Int64: 15, Valid: true}, InstID: "1", Event: "unknown event type"},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_NullValues(t *testing.T) {
	mockClient := &client.MockClient{
		RedoLogWaitsMetricsList: []models.RedoLogWaitsMetric{
			{TotalWaits: sql.NullInt64{Valid: false}, InstID: "1", Event: "log file parallel write"},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		RedoLogWaitsMetricsList: []models.RedoLogWaitsMetric{},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{
		RedoLogWaitsMetricsList: []models.RedoLogWaitsMetric{
			{TotalWaits: sql.NullInt64{Int64: 100, Valid: true}, InstID: "1", Event: "log file parallel write"},
			{TotalWaits: sql.NullInt64{Int64: 80, Valid: true}, InstID: "2", Event: "log file parallel write"},
			{TotalWaits: sql.NullInt64{Int64: 50, Valid: true}, InstID: 3, Event: "log file switch completion"},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeSysstatMetrics_UnknownMetric(t *testing.T) {
	mockClient := &client.MockClient{
		SysstatMetricsList: []models.SysstatMetric{
			{InstID: "1", Name: "unknown metric name", Value: sql.NullInt64{Int64: 100, Valid: true}},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeSysstatMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		SysstatMetricsList: []models.SysstatMetric{},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeSysstatMetrics_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{
		SysstatMetricsList: []models.SysstatMetric{
			{InstID: "1", Name: "redo buffer allocation retries", Value: sql.NullInt64{Int64: 100, Valid: true}},
			{InstID: "2", Name: "redo buffer allocation retries", Value: sql.NullInt64{Int64: 80, Valid: true}},
			{InstID: 3, Name: "redo entries", Value: sql.NullInt64{Int64: 5000, Valid: true}},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeSysstatMetrics_ZeroValues(t *testing.T) {
	mockClient := &client.MockClient{
		SysstatMetricsList: []models.SysstatMetric{
			{InstID: "1", Name: "sorts (memory)", Value: sql.NullInt64{Int64: 0, Valid: true}},
			{InstID: "1", Name: "sorts (disk)", Value: sql.NullInt64{Int64: 0, Valid: true}},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSysstatMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_NullValues(t *testing.T) {
	mockClient := &client.MockClient{
		RollbackSegmentsMetricsList: []models.RollbackSegmentsMetric{
			{
				Gets:   sql.NullInt64{Valid: false},
				Waits:  sql.NullInt64{Valid: false},
				Ratio:  sql.NullFloat64{Valid: false},
				InstID: "1",
			},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		RollbackSegmentsMetricsList: []models.RollbackSegmentsMetric{},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{
		RollbackSegmentsMetricsList: []models.RollbackSegmentsMetric{
			{
				Gets:   sql.NullInt64{Int64: 1000, Valid: true},
				Waits:  sql.NullInt64{Int64: 10, Valid: true},
				Ratio:  sql.NullFloat64{Float64: 0.01, Valid: true},
				InstID: "1",
			},
			{
				Gets:   sql.NullInt64{Int64: 800, Valid: true},
				Waits:  sql.NullInt64{Int64: 8, Valid: true},
				Ratio:  sql.NullFloat64{Float64: 0.01, Valid: true},
				InstID: "2",
			},
			{
				Gets:   sql.NullInt64{Int64: 1200, Valid: true},
				Waits:  sql.NullInt64{Int64: 12, Valid: true},
				Ratio:  sql.NullFloat64{Float64: 0.01, Valid: true},
				InstID: 3,
			},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, testTS())
	require.Empty(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_ZeroRatio(t *testing.T) {
	mockClient := &client.MockClient{
		RollbackSegmentsMetricsList: []models.RollbackSegmentsMetric{
			{
				Gets:   sql.NullInt64{Int64: 1000, Valid: true},
				Waits:  sql.NullInt64{Int64: 0, Valid: true},
				Ratio:  sql.NullFloat64{Float64: 0.0, Valid: true},
				InstID: "1",
			},
		},
	}

	scraper := &CoreScraper{
		client:       mockClient,
		logger:       zap.NewNop(),
		mb:           metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, testTS())
	require.Empty(t, errs)
}