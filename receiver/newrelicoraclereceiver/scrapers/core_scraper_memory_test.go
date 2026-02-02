// Copyright New Relic, Inc. All rights reserved.
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

func testTimestamp() pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(time.Now())
}

func TestScrapePGAMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		PGAMetricsList: []models.PGAMetric{
			{InstID: "1", Name: "total PGA inuse", Value: 1024000},
			{InstID: "1", Name: "total PGA allocated", Value: 2048000},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapePGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapePGAMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("query failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapePGAMetrics(ctx, testTimestamp())
	require.Len(t, errs, 1)
}

func TestScrapeSGAMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SGAMetricsList: []models.SGAMetric{
			{InstID: "1", Name: "Fixed Size", Value: sql.NullInt64{Int64: 8388608, Valid: true}},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
		config: metadata.DefaultMetricsBuilderConfig(),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}
func TestScrapeSGAMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("query failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAMetrics(ctx, testTimestamp())
	require.Len(t, errs, 1)
}

func TestScrapeSGAMetrics_AllMetricTypes(t *testing.T) {
	mockClient := &client.MockClient{
		SGAMetricsList: []models.SGAMetric{
			{InstID: "1", Name: "Fixed Size", Value: sql.NullInt64{Int64: 8388608, Valid: true}},
			{InstID: "1", Name: "Redo Buffers", Value: sql.NullInt64{Int64: 2097152, Valid: true}},
			{InstID: "1", Name: "Unknown Metric", Value: sql.NullInt64{Int64: 1024, Valid: true}},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGAMetrics_NullValue(t *testing.T) {
	mockClient := &client.MockClient{
		SGAMetricsList: []models.SGAMetric{
			{InstID: "1", Name: "Fixed Size", Value: sql.NullInt64{Valid: false}},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGAMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		SGAMetricsList: []models.SGAMetric{},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapePGAMetrics_AllMetricTypes(t *testing.T) {
	mockClient := &client.MockClient{
		PGAMetricsList: []models.PGAMetric{
			{InstID: "1", Name: "total PGA inuse", Value: 1024000},
			{InstID: "1", Name: "total PGA allocated", Value: 2048000},
			{InstID: "1", Name: "total freeable PGA memory", Value: 512000},
			{InstID: "1", Name: "global memory bound", Value: 4096000},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapePGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapePGAMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		PGAMetricsList: []models.PGAMetric{},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapePGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapePGAMetrics_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{
		PGAMetricsList: []models.PGAMetric{
			{InstID: "1", Name: "total PGA inuse", Value: 1024000},
			{InstID: "1", Name: "total PGA allocated", Value: 2048000},
			{InstID: 2, Name: "total PGA inuse", Value: 512000},
			{InstID: 2, Name: "total PGA allocated", Value: 1024000},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapePGAMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

// Test scrapeSGAUGATotalMemoryMetrics
func TestScrapeSGAUGATotalMemoryMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SGAUGATotalMemoryList: []models.SGAUGATotalMemoryMetric{
			{InstID: "1", Sum: 1073741824},
			{InstID: "2", Sum: 536870912},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGAUGATotalMemoryMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("database connection failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, testTimestamp())
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), "database connection failed")
}

func TestScrapeSGAUGATotalMemoryMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		SGAUGATotalMemoryList: []models.SGAUGATotalMemoryMetric{},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGAUGATotalMemoryMetrics_ZeroValue(t *testing.T) {
	mockClient := &client.MockClient{
		SGAUGATotalMemoryList: []models.SGAUGATotalMemoryMetric{
			{InstID: "1", Sum: 0},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGAUGATotalMemoryMetrics_NilInstanceID(t *testing.T) {
	mockClient := &client.MockClient{
		SGAUGATotalMemoryList: []models.SGAUGATotalMemoryMetric{
			{InstID: nil, Sum: 1073741824},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

// Test scrapeSGASharedPoolLibraryCacheMetrics
func TestScrapeSGASharedPoolLibraryCacheMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SGASharedPoolLibraryCacheList: []models.SGASharedPoolLibraryCacheMetric{
			{InstID: "1", Sum: 268435456},
			{InstID: "2", Sum: 134217728},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("query execution failed")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheMetrics(ctx, testTimestamp())
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), "query execution failed")
}

func TestScrapeSGASharedPoolLibraryCacheMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		SGASharedPoolLibraryCacheList: []models.SGASharedPoolLibraryCacheMetric{},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheMetrics_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{
		SGASharedPoolLibraryCacheList: []models.SGASharedPoolLibraryCacheMetric{
			{InstID: "1", Sum: 268435456},
			{InstID: "2", Sum: 134217728},
			{InstID: "3", Sum: 402653184},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

// Test scrapeSGASharedPoolLibraryCacheUserMetrics
func TestScrapeSGASharedPoolLibraryCacheUserMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		SGASharedPoolLibraryCacheUserList: []models.SGASharedPoolLibraryCacheUserMetric{
			{InstID: "1", Sum: 33554432},
			{InstID: "2", Sum: 16777216},
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheUserMetrics_QueryError(t *testing.T) {
	expectedErr := errors.New("connection timeout")
	mockClient := &client.MockClient{QueryErr: expectedErr}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, testTimestamp())
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), "connection timeout")
}

func TestScrapeSGASharedPoolLibraryCacheUserMetrics_EmptyResult(t *testing.T) {
	mockClient := &client.MockClient{
		SGASharedPoolLibraryCacheUserList: []models.SGASharedPoolLibraryCacheUserMetric{},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheUserMetrics_LargeValue(t *testing.T) {
	mockClient := &client.MockClient{
		SGASharedPoolLibraryCacheUserList: []models.SGASharedPoolLibraryCacheUserMetric{
			{InstID: "1", Sum: 9223372036854775807}, // max int64
		},
	}

	scraper := &CoreScraper{
		client: mockClient,
		logger: zap.NewNop(),
		mb:     metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type)),
	}

	ctx := context.Background()
	errs := scraper.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, testTimestamp())
	require.Empty(t, errs)
}
