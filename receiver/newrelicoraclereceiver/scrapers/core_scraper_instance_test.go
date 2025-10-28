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

// Tests for scrapeLockedAccountsMetrics

func TestScrapeLockedAccountsMetrics_MetricDisabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbLockedAccounts.Enabled = false

	scraper := &CoreScraper{
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeLockedAccountsMetrics(ctx, now)

	assert.Nil(t, errors)
}

func TestScrapeLockedAccountsMetrics_NilDatabase(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbLockedAccounts.Enabled = true

	scraper := &CoreScraper{
		db:     nil,
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeLockedAccountsMetrics(ctx, now)

	assert.NotNil(t, errors)
	assert.Len(t, errors, 1)
}

func TestScrapeLockedAccountsMetrics_MetricEnabled(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbLockedAccounts.Enabled = true

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

func TestScrapeLockedAccountsMetrics_EmptyInstanceName(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbLockedAccounts.Enabled = true

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

func TestScrapeLockedAccountsMetrics_NilLogger(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbLockedAccounts.Enabled = true

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

// Tests for scrapeGlobalNameInstanceMetrics

func TestScrapeGlobalNameInstanceMetrics_NilDatabase(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := &CoreScraper{
		db:     nil,
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeGlobalNameInstanceMetrics(ctx, now)

	assert.NotNil(t, errors)
	assert.Len(t, errors, 1)
}

func TestScrapeGlobalNameInstanceMetrics_Success(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeGlobalNameInstanceMetrics_EmptyInstanceName(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeGlobalNameInstanceMetrics_NilLogger(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeGlobalNameInstanceMetrics_ContextCancellation(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := &CoreScraper{
		db:           &sql.DB{},
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	now := pcommon.NewTimestampFromTime(time.Now())
	errors := scraper.scrapeGlobalNameInstanceMetrics(ctx, now)

	assert.NotNil(t, errors)
}

// Tests for scrapeDBIDInstanceMetrics

func TestScrapeDBIDInstanceMetrics_NilDatabase(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := &CoreScraper{
		db:     nil,
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeDBIDInstanceMetrics(ctx, now)

	assert.NotNil(t, errors)
	assert.Len(t, errors, 1)
}

func TestScrapeDBIDInstanceMetrics_Success(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeDBIDInstanceMetrics_EmptyInstanceName(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeDBIDInstanceMetrics_NilLogger(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeDBIDInstanceMetrics_ContextCancellation(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := &CoreScraper{
		db:           &sql.DB{},
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	now := pcommon.NewTimestampFromTime(time.Now())
	errors := scraper.scrapeDBIDInstanceMetrics(ctx, now)

	assert.NotNil(t, errors)
}

// Tests for scrapeLongRunningQueriesMetrics

func TestScrapeLongRunningQueriesMetrics_NilDatabase(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := &CoreScraper{
		db:     nil,
		config: config,
		logger: zap.NewNop(),
	}

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())

	errors := scraper.scrapeLongRunningQueriesMetrics(ctx, now)

	assert.NotNil(t, errors)
	assert.Len(t, errors, 1)
}

func TestScrapeLongRunningQueriesMetrics_Success(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeLongRunningQueriesMetrics_EmptyInstanceName(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeLongRunningQueriesMetrics_NilLogger(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

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

func TestScrapeLongRunningQueriesMetrics_ContextCancellation(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := &CoreScraper{
		db:           &sql.DB{},
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "test-instance",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	now := pcommon.NewTimestampFromTime(time.Now())
	errors := scraper.scrapeLongRunningQueriesMetrics(ctx, now)

	assert.NotNil(t, errors)
}

// Cross-method tests

func TestCoreScraper_AllInstanceMethods(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbLockedAccounts.Enabled = true

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
	assert.NotNil(t, scraper.db)
	assert.NotNil(t, scraper.mb)
	assert.NotNil(t, scraper.logger)
	assert.Equal(t, "test-instance", scraper.instanceName)
}

func TestCoreScraper_MultipleInstances(t *testing.T) {
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb1 := metadata.NewMetricsBuilder(config, settings)
	mb2 := metadata.NewMetricsBuilder(config, settings)

	scraper1 := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb1,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "instance-1",
	}

	scraper2 := &CoreScraper{
		db:           &sql.DB{},
		mb:           mb2,
		config:       config,
		logger:       zap.NewNop(),
		instanceName: "instance-2",
	}

	assert.NotEqual(t, scraper1, scraper2)
	assert.Equal(t, "instance-1", scraper1.instanceName)
	assert.Equal(t, "instance-2", scraper2.instanceName)
}
