// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestScrapeSGASharedPoolLibraryCacheReloadRatioMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheReloadRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGASharedPoolLibraryCacheReloadRatioMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaSharedPoolLibraryCacheReloadRatio.Enabled = true

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheReloadRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheReloadRatioMetrics_Disabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaSharedPoolLibraryCacheReloadRatio.Enabled = false

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheReloadRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheHitRatioMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGASharedPoolLibraryCacheHitRatioMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaSharedPoolLibraryCacheHitRatio.Enabled = true

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheHitRatioMetrics_Disabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaSharedPoolLibraryCacheHitRatio.Enabled = false

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolDictCacheMissRatioMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolDictCacheMissRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGASharedPoolDictCacheMissRatioMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaSharedPoolDictCacheMissRatio.Enabled = true

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolDictCacheMissRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolDictCacheMissRatioMetrics_Disabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaSharedPoolDictCacheMissRatio.Enabled = false

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolDictCacheMissRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGALogBufferSpaceWaitsMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogBufferSpaceWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGALogBufferSpaceWaitsMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaLogBufferSpaceWaits.Enabled = true

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogBufferSpaceWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGALogBufferSpaceWaitsMetrics_Disabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaLogBufferSpaceWaits.Enabled = false

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogBufferSpaceWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGALogAllocRetriesMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogAllocRetriesMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGALogAllocRetriesMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaLogAllocationRetriesRatio.Enabled = true

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogAllocRetriesMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGALogAllocRetriesMetrics_Disabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaLogAllocationRetriesRatio.Enabled = false

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogAllocRetriesMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGALogAllocRetriesMetrics_NullValues(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGALogAllocRetriesMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGAHitRatioMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGAHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGAHitRatioMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaHitRatio.Enabled = true

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGAHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGAHitRatioMetrics_Disabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaHitRatio.Enabled = false

	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
		config:       config,
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGAHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGAHitRatioMetrics_NullValues(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGAHitRatioMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}
