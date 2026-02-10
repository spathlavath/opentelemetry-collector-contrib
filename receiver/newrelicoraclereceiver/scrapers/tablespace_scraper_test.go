// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewTablespaceScraper(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.config)
}

func TestTablespaceScraper_NilDatabase(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(nil, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.client)
}

func TestTablespaceScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, nil, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestTablespaceScraper_NilLogger(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper := NewTablespaceScraper(mockClient, mb, nil, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestTablespaceScraper_EmptyInstanceName(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.NotNil(t, scraper)
}

func TestTablespaceScraper_Config(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	assert.NotNil(t, scraper)
	assert.Equal(t, config, scraper.config)
}

func TestTablespaceScraper_MultipleInstances(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper1 := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)
	scraper2 := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	// Verify that two separate instances are created (different memory addresses)
	assert.NotSame(t, scraper1, scraper2)
}

func TestTablespaceScraper_IsCDBSupported_NotChecked(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.False(t, scraper.isCDBSupported())
}

func TestTablespaceScraper_IsCDBSupported_Checked(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	cdbCapable := true
	scraper.isCDBCapable = &cdbCapable

	assert.True(t, scraper.isCDBSupported())
}

func TestTablespaceScraper_IsPDBSupported_NotChecked(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.False(t, scraper.isPDBSupported())
}

func TestTablespaceScraper_IsPDBSupported_Checked(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	pdbCapable := true
	scraper.isPDBCapable = &pdbCapable

	assert.True(t, scraper.isPDBSupported())
}

func TestTablespaceScraper_IsConnectedToCDBRoot_Empty(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.False(t, scraper.isConnectedToCDBRoot())
}

func TestTablespaceScraper_IsConnectedToCDBRoot_CDBRoot(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)
	scraper.currentContainer = "CDB$ROOT"

	assert.True(t, scraper.isConnectedToCDBRoot())
}

func TestTablespaceScraper_IsConnectedToPDB_Empty(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)

	assert.False(t, scraper.isConnectedToPDB())
}

func TestTablespaceScraper_IsConnectedToPDB_PDB(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)
	scraper.currentContainer = "FREEPDB1"

	assert.True(t, scraper.isConnectedToPDB())
}

func TestTablespaceScraper_IsConnectedToPDB_CDBRoot(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig(), nil, nil)
	scraper.currentContainer = "CDB$ROOT"

	assert.False(t, scraper.isConnectedToPDB())
}

func TestTablespaceScraper_IsAnyTablespaceMetricEnabled_AllDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceReservedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceUsedPercentage.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceIsOffline.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	assert.False(t, scraper.isAnyTablespaceMetricEnabled())
}

func TestTablespaceScraper_IsAnyTablespaceMetricEnabled_OneEnabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled = true
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	assert.True(t, scraper.isAnyTablespaceMetricEnabled())
}

func TestCheckEnvironmentCapability_NonCDB(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCDBFeatureResult: 0,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkEnvironmentCapability(ctx)

	assert.NoError(t, err)
	assert.False(t, scraper.isCDBSupported())
	assert.False(t, scraper.isPDBSupported())
	assert.True(t, scraper.environmentChecked)
}

func TestCheckEnvironmentCapability_CDBWithPDB(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCDBFeatureResult:    1,
		CheckPDBCapabilityResult: 2,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkEnvironmentCapability(ctx)

	assert.NoError(t, err)
	assert.True(t, scraper.isCDBSupported())
	assert.True(t, scraper.isPDBSupported())
	assert.True(t, scraper.environmentChecked)
}

func TestCheckEnvironmentCapability_CDBWithoutPDB(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCDBFeatureResult:    1,
		CheckPDBCapabilityResult: 0,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkEnvironmentCapability(ctx)

	assert.NoError(t, err)
	assert.True(t, scraper.isCDBSupported())
	assert.False(t, scraper.isPDBSupported())
}

func TestCheckEnvironmentCapability_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("database error"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkEnvironmentCapability(ctx)

	assert.Error(t, err)
}

func TestCheckEnvironmentCapability_CachedResult(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCDBFeatureResult: 1,
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()

	// First call
	err := scraper.checkEnvironmentCapability(ctx)
	assert.NoError(t, err)
	assert.True(t, scraper.environmentChecked)

	// Modify mock to return error, but should use cached result
	mockClient.QueryErr = errors.New("should not be called")

	// Second call - should use cached result
	err = scraper.checkEnvironmentCapability(ctx)
	assert.NoError(t, err)
}

func TestCheckCurrentContext_CDBRoot(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCurrentContainerResult: models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkCurrentContext(ctx)

	assert.NoError(t, err)
	assert.Equal(t, "CDB$ROOT", scraper.currentContainer)
	assert.Equal(t, "1", scraper.currentContainerID)
	assert.True(t, scraper.isConnectedToCDBRoot())
	assert.False(t, scraper.isConnectedToPDB())
	assert.True(t, scraper.contextChecked)
}

func TestCheckCurrentContext_PDB(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCurrentContainerResult: models.ContainerContext{
			ContainerName: sql.NullString{String: "PDB1", Valid: true},
			ContainerID:   sql.NullString{String: "3", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkCurrentContext(ctx)

	assert.NoError(t, err)
	assert.Equal(t, "PDB1", scraper.currentContainer)
	assert.Equal(t, "3", scraper.currentContainerID)
	assert.False(t, scraper.isConnectedToCDBRoot())
	assert.True(t, scraper.isConnectedToPDB())
}

func TestCheckCurrentContext_InvalidContainer(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCurrentContainerResult: models.ContainerContext{
			ContainerName: sql.NullString{Valid: false},
			ContainerID:   sql.NullString{Valid: false},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkCurrentContext(ctx)

	assert.NoError(t, err)
	assert.Empty(t, scraper.currentContainer)
	assert.Empty(t, scraper.currentContainerID)
	assert.False(t, scraper.isConnectedToCDBRoot())
	assert.False(t, scraper.isConnectedToPDB())
}

func TestCheckCurrentContext_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	err := scraper.checkCurrentContext(ctx)

	assert.Error(t, err)
}

func TestCheckCurrentContext_CachedResult(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCurrentContainerResult: models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()

	// First call
	err := scraper.checkCurrentContext(ctx)
	assert.NoError(t, err)
	assert.True(t, scraper.contextChecked)

	// Modify mock to return error, but should use cached result
	mockClient.QueryErr = errors.New("should not be called")

	// Second call - should use cached result
	err = scraper.checkCurrentContext(ctx)
	assert.NoError(t, err)
}

func TestProcessTablespaceUsage_AllMetricsEnabled(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := &TablespaceScraper{
		mb:     mb,
		logger: logger,
		config: config,
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	tablespaces := []models.TablespaceUsage{
		{TablespaceName: "USERS", Used: 1024, Size: 2048, UsedPercent: 50, Offline: 0},
		{TablespaceName: "SYSTEM", Used: 512, Size: 1024, UsedPercent: 50, Offline: 1},
	}

	errs := scraper.processTablespaceUsage(tablespaces, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 2, metricCount)
}

func TestProcessTablespaceUsage_SelectiveMetrics(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled = true
	config.Metrics.NewrelicoracledbTablespaceSpaceReservedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceUsedPercentage.Enabled = true
	config.Metrics.NewrelicoracledbTablespaceIsOffline.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := &TablespaceScraper{
		mb:     mb,
		logger: logger,
		config: config,
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	tablespaces := []models.TablespaceUsage{
		{TablespaceName: "USERS", Used: 1024, Size: 2048, UsedPercent: 50, Offline: 0},
	}

	errs := scraper.processTablespaceUsage(tablespaces, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 1, metricCount)
}

func TestProcessTablespaceUsage_EmptyList(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := &TablespaceScraper{
		mb:     mb,
		logger: logger,
		config: config,
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	tablespaces := []models.TablespaceUsage{}

	errs := scraper.processTablespaceUsage(tablespaces, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapeTablespaceUsageMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		TablespaceUsageList: []models.TablespaceUsage{
			{TablespaceName: "USERS", Used: 1024, Size: 2048, UsedPercent: 50, Offline: 0},
			{TablespaceName: "SYSTEM", Used: 512, Size: 1024, UsedPercent: 50, Offline: 1},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, []string{"USERS"}, []string{"TEMP"})

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeTablespaceUsageMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 2, metricCount)
}

func TestScrapeTablespaceUsageMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeTablespaceUsageMetrics(ctx, now, &metricCount)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "error executing tablespace metrics query")
	assert.Equal(t, 0, metricCount)
}

func TestScrapeTablespaceUsageMetrics_EmptyResults(t *testing.T) {
	mockClient := &client.MockClient{
		TablespaceUsageList: []models.TablespaceUsage{},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeTablespaceUsageMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapeTablespaceUsageMetrics_MetricsDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceReservedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceUsedPercentage.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceIsOffline.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeTablespaceUsageMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapeGlobalNameTablespaceMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		TablespaceGlobalNameList: []models.TablespaceGlobalName{
			{TablespaceName: "USERS", GlobalName: "ORCL.WORLD"},
			{TablespaceName: "SYSTEM", GlobalName: "ORCL.WORLD"},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeGlobalNameTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 2, metricCount)
}

func TestScrapeGlobalNameTablespaceMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeGlobalNameTablespaceMetrics(ctx, now, &metricCount)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "error executing global name tablespace query")
	assert.Equal(t, 0, metricCount)
}

func TestScrapeGlobalNameTablespaceMetrics_Disabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceGlobalName.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeGlobalNameTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapeDBIDTablespaceMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		TablespaceDBIDList: []models.TablespaceDBID{
			{TablespaceName: "USERS", DBID: 123456},
			{TablespaceName: "SYSTEM", DBID: 789012},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeDBIDTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 2, metricCount)
}

func TestScrapeDBIDTablespaceMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeDBIDTablespaceMetrics(ctx, now, &metricCount)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "error executing DB ID tablespace query")
	assert.Equal(t, 0, metricCount)
}

func TestScrapeDBIDTablespaceMetrics_Disabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceDbID.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeDBIDTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapeCDBDatafilesOfflineTablespaceMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		TablespaceCDBDatafilesOfflineList: []models.TablespaceCDBDatafilesOffline{
			{TablespaceName: "USERS", OfflineCount: 2},
			{TablespaceName: "SYSTEM", OfflineCount: 0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeCDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 2, metricCount)
}

func TestScrapeCDBDatafilesOfflineTablespaceMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeCDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "error executing CDB datafiles offline tablespace query")
	assert.Equal(t, 0, metricCount)
}

func TestScrapeCDBDatafilesOfflineTablespaceMetrics_Disabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceOfflineCdbDatafiles.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapeCDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapePDBDatafilesOfflineTablespaceMetrics_CDBRoot(t *testing.T) {
	mockClient := &client.MockClient{
		TablespacePDBDatafilesOfflineList: []models.TablespacePDBDatafilesOffline{
			{TablespaceName: "USERS", OfflineCount: 1},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "CDB$ROOT"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 1, metricCount)
}

func TestScrapePDBDatafilesOfflineTablespaceMetrics_PDB(t *testing.T) {
	mockClient := &client.MockClient{
		TablespacePDBDatafilesOfflineCurrentContainerList: []models.TablespacePDBDatafilesOffline{
			{TablespaceName: "USERS", OfflineCount: 0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "PDB1"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 1, metricCount)
}

func TestScrapePDBDatafilesOfflineTablespaceMetrics_NotInCDB(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = ""
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapePDBDatafilesOfflineTablespaceMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "CDB$ROOT"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "error executing PDB datafiles offline query")
	assert.Equal(t, 0, metricCount)
}

func TestScrapePDBDatafilesOfflineTablespaceMetrics_Disabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceOfflinePdbDatafiles.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "CDB$ROOT"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBDatafilesOfflineTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapePDBNonWriteTablespaceMetrics_CDBRoot(t *testing.T) {
	mockClient := &client.MockClient{
		TablespacePDBNonWriteList: []models.TablespacePDBNonWrite{
			{TablespaceName: "USERS", NonWriteCount: 1},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "CDB$ROOT"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBNonWriteTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 1, metricCount)
}

func TestScrapePDBNonWriteTablespaceMetrics_PDB(t *testing.T) {
	mockClient := &client.MockClient{
		TablespacePDBNonWriteCurrentContainerList: []models.TablespacePDBNonWrite{
			{TablespaceName: "USERS", NonWriteCount: 0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "PDB1"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBNonWriteTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 1, metricCount)
}

func TestScrapePDBNonWriteTablespaceMetrics_NotInCDB(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = ""
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBNonWriteTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapePDBNonWriteTablespaceMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "CDB$ROOT"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBNonWriteTablespaceMetrics(ctx, now, &metricCount)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "error executing PDB non-write mode query")
	assert.Equal(t, 0, metricCount)
}

func TestScrapePDBNonWriteTablespaceMetrics_Disabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespacePdbNonWriteMode.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)
	scraper.currentContainer = "CDB$ROOT"
	scraper.contextChecked = true

	ctx := t.Context()
	now := pcommon.NewTimestampFromTime(time.Now())
	metricCount := 0

	errs := scraper.scrapePDBNonWriteTablespaceMetrics(ctx, now, &metricCount)

	assert.Empty(t, errs)
	assert.Equal(t, 0, metricCount)
}

func TestScrapeTablespaceMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCDBFeatureResult: 0,
		CheckCurrentContainerResult: models.ContainerContext{
			ContainerName: sql.NullString{String: "ORCL", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		},
		TablespaceUsageList: []models.TablespaceUsage{
			{TablespaceName: "USERS", Used: 1024, Size: 2048, UsedPercent: 50, Offline: 0},
		},
		TablespaceGlobalNameList: []models.TablespaceGlobalName{
			{TablespaceName: "USERS", GlobalName: "ORCL.WORLD"},
		},
		TablespaceDBIDList: []models.TablespaceDBID{
			{TablespaceName: "USERS", DBID: 123456},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	errs := scraper.ScrapeTablespaceMetrics(ctx)

	assert.Empty(t, errs)
}

func TestScrapeTablespaceMetrics_WithCDBAndPDB(t *testing.T) {
	mockClient := &client.MockClient{
		CheckCDBFeatureResult:    1,
		CheckPDBCapabilityResult: 2,
		CheckCurrentContainerResult: models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		},
		TablespaceUsageList: []models.TablespaceUsage{
			{TablespaceName: "SYSTEM", Used: 2048, Size: 4096, UsedPercent: 50, Offline: 0},
		},
		TablespaceGlobalNameList: []models.TablespaceGlobalName{
			{TablespaceName: "SYSTEM", GlobalName: "CDB.WORLD"},
		},
		TablespaceDBIDList: []models.TablespaceDBID{
			{TablespaceName: "SYSTEM", DBID: 789012},
		},
		TablespaceCDBDatafilesOfflineList: []models.TablespaceCDBDatafilesOffline{
			{TablespaceName: "SYSTEM", OfflineCount: 0},
		},
		TablespacePDBDatafilesOfflineList: []models.TablespacePDBDatafilesOffline{
			{TablespaceName: "USERS", OfflineCount: 1},
		},
		TablespacePDBNonWriteList: []models.TablespacePDBNonWrite{
			{TablespaceName: "USERS", NonWriteCount: 0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	errs := scraper.ScrapeTablespaceMetrics(ctx)

	assert.Empty(t, errs)
}

func TestScrapeTablespaceMetrics_EnvironmentCheckError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(mockClient, mb, logger, config, nil, nil)

	ctx := t.Context()
	errs := scraper.ScrapeTablespaceMetrics(ctx)

	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "cdb_capability_check")
}
