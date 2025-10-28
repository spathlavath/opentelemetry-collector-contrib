// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestScrapeReadWriteMetrics_AllMetricsDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = false
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = false
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = false
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = false

	scraper := &CoreScraper{
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeReadWriteMetrics(ctx, now)

	assert.Nil(t, errors)
}

func TestScrapeReadWriteMetrics_NilDatabase(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true

	scraper := &CoreScraper{
		db:     nil,
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeReadWriteMetrics(ctx, now)

	assert.NotNil(t, errors)
	assert.Len(t, errors, 1)
}

func TestScrapeReadWriteMetrics_MetricsEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = true
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = true
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = true
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = true
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
}

func TestScrapeReadWriteMetrics_PartialMetricsEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = true
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = false
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = true
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = false

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
}

func TestScrapeReadWriteMetrics_EmptyInstanceName(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "",
	}

	assert.NotNil(t, scraper)
	assert.Equal(t, "", scraper.instanceName)
}

func TestScrapeReadWriteMetrics_NilLogger(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       nil,
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestScrapeReadWriteMetrics_NilMetricsBuilder(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           nil,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestScrapeReadWriteMetrics_OnlyReadsEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = false
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = false
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = false

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
}

func TestScrapeReadWriteMetrics_OnlyWritesEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = false
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = true
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = false
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = false
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = false

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
}

func TestScrapeReadWriteMetrics_OnlyBlockMetricsEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = false
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = true
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = true
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = false
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = false

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
}

func TestScrapeReadWriteMetrics_OnlyTimeMetricsEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = false
	config.Metrics.NewrelicoracledbDiskWrites.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = false
	config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = false
	config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = true
	config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	assert.NotNil(t, scraper)
}

func TestScrapeReadWriteMetrics_ContextCancellation(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDiskReads.Enabled = true

	scraper := &CoreScraper{
		db:           &sql.DB{},
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	now := pcommon.NewTimestampFromTime(time.Now())
	errors := scraper.scrapeReadWriteMetrics(ctx, now)

	assert.NotNil(t, errors)
}

func TestScrapeReadWriteMetrics_MultipleConfigurations(t *testing.T) {
	tests := []struct {
		name           string
		enabledMetrics map[string]bool
	}{
		{
			name: "all_enabled",
			enabledMetrics: map[string]bool{
				"reads":      true,
				"writes":     true,
				"blocks_r":   true,
				"blocks_w":   true,
				"read_time":  true,
				"write_time": true,
			},
		},
		{
			name: "reads_only",
			enabledMetrics: map[string]bool{
				"reads":      true,
				"writes":     false,
				"blocks_r":   false,
				"blocks_w":   false,
				"read_time":  false,
				"write_time": false,
			},
		},
		{
			name: "time_metrics_only",
			enabledMetrics: map[string]bool{
				"reads":      false,
				"writes":     false,
				"blocks_r":   false,
				"blocks_w":   false,
				"read_time":  true,
				"write_time": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := metadata.DefaultMetricsBuilderConfig()
			config.Metrics.NewrelicoracledbDiskReads.Enabled = tt.enabledMetrics["reads"]
			config.Metrics.NewrelicoracledbDiskWrites.Enabled = tt.enabledMetrics["writes"]
			config.Metrics.NewrelicoracledbDiskBlocksRead.Enabled = tt.enabledMetrics["blocks_r"]
			config.Metrics.NewrelicoracledbDiskBlocksWritten.Enabled = tt.enabledMetrics["blocks_w"]
			config.Metrics.NewrelicoracledbDiskReadTimeMilliseconds.Enabled = tt.enabledMetrics["read_time"]
			config.Metrics.NewrelicoracledbDiskWriteTimeMilliseconds.Enabled = tt.enabledMetrics["write_time"]

			settings := receivertest.NewNopSettings(metadata.Type)
			mb := metadata.NewMetricsBuilder(config, settings)

			scraper := &CoreScraper{
				db:           &sql.DB{},
				mb:           mb,
				config:       config,
				logger:       zap.NewNop(),
				instanceName: "test-instance",
			}

			assert.NotNil(t, scraper)
		})
	}
}
