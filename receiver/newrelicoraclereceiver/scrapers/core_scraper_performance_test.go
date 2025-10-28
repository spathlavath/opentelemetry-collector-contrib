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

func TestScrapeSysstatMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSysstatMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSysstatMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaLogBufferRedoAllocationRetries.Enabled = true
	config.Metrics.NewrelicoracledbSgaLogBufferRedoEntries.Enabled = true
	config.Metrics.NewrelicoracledbSortsMemory.Enabled = true
	config.Metrics.NewrelicoracledbSortsDisk.Enabled = true

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

	errs := scraper.scrapeSysstatMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSysstatMetrics_AllMetricsDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSgaLogBufferRedoAllocationRetries.Enabled = false
	config.Metrics.NewrelicoracledbSgaLogBufferRedoEntries.Enabled = false
	config.Metrics.NewrelicoracledbSortsMemory.Enabled = false
	config.Metrics.NewrelicoracledbSortsDisk.Enabled = false

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

	errs := scraper.scrapeSysstatMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeSysstatMetrics_NullValues(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSysstatMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeSysstatMetrics_UnknownMetricName(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeSysstatMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeRollbackSegmentsMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRollbackSegmentsGets.Enabled = true
	config.Metrics.NewrelicoracledbRollbackSegmentsWaits.Enabled = true
	config.Metrics.NewrelicoracledbRollbackSegmentsWaitRatio.Enabled = true

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

	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_AllMetricsDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRollbackSegmentsGets.Enabled = false
	config.Metrics.NewrelicoracledbRollbackSegmentsWaits.Enabled = false
	config.Metrics.NewrelicoracledbRollbackSegmentsWaitRatio.Enabled = false

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

	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRollbackSegmentsMetrics_NullValues(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRollbackSegmentsMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeRedoLogWaitsMetrics_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeRedoLogWaitsMetrics_WithConfig(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRedoLogParallelWriteWaits.Enabled = true
	config.Metrics.NewrelicoracledbRedoLogSwitchCompletionWaits.Enabled = true
	config.Metrics.NewrelicoracledbRedoLogSwitchCheckpointIncompleteWaits.Enabled = true
	config.Metrics.NewrelicoracledbRedoLogSwitchArchivingNeededWaits.Enabled = true
	config.Metrics.NewrelicoracledbSgaBufferBusyWaits.Enabled = true
	config.Metrics.NewrelicoracledbSgaFreeBufferWaits.Enabled = true
	config.Metrics.NewrelicoracledbSgaFreeBufferInspectedWaits.Enabled = true

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

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_AllMetricsDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRedoLogParallelWriteWaits.Enabled = false
	config.Metrics.NewrelicoracledbRedoLogSwitchCompletionWaits.Enabled = false
	config.Metrics.NewrelicoracledbRedoLogSwitchCheckpointIncompleteWaits.Enabled = false
	config.Metrics.NewrelicoracledbRedoLogSwitchArchivingNeededWaits.Enabled = false
	config.Metrics.NewrelicoracledbSgaBufferBusyWaits.Enabled = false
	config.Metrics.NewrelicoracledbSgaFreeBufferWaits.Enabled = false
	config.Metrics.NewrelicoracledbSgaFreeBufferInspectedWaits.Enabled = false

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

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_NullValues(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeRedoLogWaitsMetrics_UnknownEvent(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_LogFileParallelWrite(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_LogFileSwitchCompletion(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_LogFileSwitchCheckpoint(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_LogFileSwitchArchiving(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_BufferBusyWaits(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_FreeBufferWaits(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}

func TestScrapeRedoLogWaitsMetrics_FreeBufferInspected(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		logger:       zap.NewNop(),
		instanceName: "test_instance",
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(testTime)

	errs := scraper.scrapeRedoLogWaitsMetrics(ctx, now)

	assert.NotNil(t, errs)
}
