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

func TestScrapePGAMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapePGAMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapePGAMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbMemoryPgaAllocatedBytes.Enabled = true
	config.Metrics.NewrelicoracledbMemoryPgaInUseBytes.Enabled = true
	config.Metrics.NewrelicoracledbMemoryPgaFreeableBytes.Enabled = true
	config.Metrics.NewrelicoracledbMemoryPgaMaxSizeBytes.Enabled = true

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

	errs := scraper.scrapePGAMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapePGAMetrics_AllMetricsDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbMemoryPgaAllocatedBytes.Enabled = false
	config.Metrics.NewrelicoracledbMemoryPgaInUseBytes.Enabled = false
	config.Metrics.NewrelicoracledbMemoryPgaFreeableBytes.Enabled = false
	config.Metrics.NewrelicoracledbMemoryPgaMaxSizeBytes.Enabled = false

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

	errs := scraper.scrapePGAMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGAUGATotalMemoryMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGAUGATotalMemoryMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbMemorySgaUgaTotalBytes.Enabled = true

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

	errs := scraper.scrapeSGAUGATotalMemoryMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGASharedPoolLibraryCacheMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbMemorySgaSharedPoolLibraryCacheSharableBytes.Enabled = true

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

	errs := scraper.scrapeSGASharedPoolLibraryCacheMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGASharedPoolLibraryCacheUserMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGASharedPoolLibraryCacheUserMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbMemorySgaSharedPoolLibraryCacheUserBytes.Enabled = true

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

	errs := scraper.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGAMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSGAMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSGAMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaFixedSizeBytes.Enabled = true
	config.Metrics.NewrelicoracledbSgaRedoBuffersBytes.Enabled = true

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

	errs := scraper.scrapeSGAMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSGAMetrics_AllMetricsDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaFixedSizeBytes.Enabled = false
	config.Metrics.NewrelicoracledbSgaRedoBuffersBytes.Enabled = false

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

	errs := scraper.scrapeSGAMetrics(ctx, now)

	assert.NotNil(t, errs)
}
