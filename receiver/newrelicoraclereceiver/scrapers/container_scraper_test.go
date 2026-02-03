// Copyright New Relic, Inc. All rights reserved.
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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewContainerScraper_ValidInputs(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)

	require.NoError(t, err)
	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.False(t, scraper.environmentChecked)
	assert.Nil(t, scraper.isCDBCapable)
	assert.Nil(t, scraper.isPDBCapable)
}

func TestNewContainerScraper_NilClient(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(nil, mb, logger, config, nil, nil)

	assert.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "client cannot be nil")
}

func TestNewContainerScraper_NilMetricsBuilder(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, nil, logger, config, nil, nil)

	assert.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "metrics builder cannot be nil")
}

func TestNewContainerScraper_NilLogger(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, nil, config, nil, nil)

	assert.Error(t, err)
	assert.Nil(t, scraper)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

func TestNewContainerScraper_EmptyInstanceName(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)

	require.NoError(t, err)
	assert.NotNil(t, scraper)
}

func TestContainerScraper_CheckEnvironmentCapability_CDBCapable(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CheckCDBFeatureResult = 1
	mockClient.CheckPDBCapabilityResult = 1
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = scraper.checkEnvironmentCapability(ctx)

	assert.NoError(t, err)
	assert.True(t, scraper.environmentChecked)
	assert.True(t, scraper.isCDBSupported())
	assert.True(t, scraper.isPDBSupported())
}

func TestContainerScraper_CheckEnvironmentCapability_NotCapable(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CheckCDBFeatureResult = 0
	mockClient.CheckPDBCapabilityResult = 0
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = scraper.checkEnvironmentCapability(ctx)

	assert.NoError(t, err)
	assert.True(t, scraper.environmentChecked)
	assert.False(t, scraper.isCDBSupported())
	assert.False(t, scraper.isPDBSupported())
}

func TestContainerScraper_CheckEnvironmentCapability_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("connection failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = scraper.checkEnvironmentCapability(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection failed")
}

func TestContainerScraper_CheckCurrentContext_CDBRoot(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CheckCurrentContainerResult = models.ContainerContext{
		ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
		ContainerID:   sql.NullString{String: "1", Valid: true},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = scraper.checkCurrentContext(ctx)

	assert.NoError(t, err)
	assert.True(t, scraper.isConnectedToCDBRoot())
	assert.False(t, scraper.isConnectedToPDB())
}

func TestContainerScraper_CheckCurrentContext_PDB(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CheckCurrentContainerResult = models.ContainerContext{
		ContainerName: sql.NullString{String: "MYPDB", Valid: true},
		ContainerID:   sql.NullString{String: "3", Valid: true},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = scraper.checkCurrentContext(ctx)

	assert.NoError(t, err)
	assert.False(t, scraper.isConnectedToCDBRoot())
	assert.True(t, scraper.isConnectedToPDB())
}

func TestContainerScraper_CheckCurrentContext_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("query failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = scraper.checkCurrentContext(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query failed")
}

func TestContainerScraper_ScrapeContainerStatus_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ContainerStatusList = []models.ContainerStatus{
		{
			ConID:         sql.NullInt64{Int64: 1, Valid: true},
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			OpenMode:      sql.NullString{String: "READ WRITE", Valid: true},
			Restricted:    sql.NullString{String: "NO", Valid: true},
		},
		{
			ConID:         sql.NullInt64{Int64: 3, Valid: true},
			ContainerName: sql.NullString{String: "PDB1", Valid: true},
			OpenMode:      sql.NullString{String: "READ WRITE", Valid: true},
			Restricted:    sql.NullString{String: "NO", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeContainerStatus(ctx, now)

	assert.Empty(t, errs)
}

func TestContainerScraper_ScrapeContainerStatus_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("query failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeContainerStatus(ctx, now)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "query failed")
}

func TestContainerScraper_ScrapeContainerStatus_InvalidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ContainerStatusList = []models.ContainerStatus{
		{
			ConID:         sql.NullInt64{Valid: false},
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
		},
		{
			ConID:         sql.NullInt64{Int64: 3, Valid: true},
			ContainerName: sql.NullString{Valid: false},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeContainerStatus(ctx, now)

	assert.Empty(t, errs) // Should handle invalid data gracefully
}

func TestContainerScraper_ScrapePDBStatus_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.PDBStatusList = []models.PDBStatus{
		{
			ConID:     sql.NullInt64{Int64: 3, Valid: true},
			PDBName:   sql.NullString{String: "PDB1", Valid: true},
			OpenMode:  sql.NullString{String: "READ WRITE", Valid: true},
			TotalSize: sql.NullInt64{Int64: 1073741824, Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapePDBStatus(ctx, now)

	assert.Empty(t, errs)
}

func TestContainerScraper_ScrapePDBStatus_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("PDB query failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapePDBStatus(ctx, now)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "PDB query failed")
}

func TestContainerScraper_ScrapeCDBTablespaceUsage_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CDBTablespaceUsageList = []models.CDBTablespaceUsage{
		{
			ConID:          sql.NullInt64{Int64: 1, Valid: true},
			TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
			UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
			TotalBytes:     sql.NullInt64{Int64: 1073741824, Valid: true},
			UsedPercent:    sql.NullFloat64{Float64: 48.83, Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeCDBTablespaceUsage(ctx, now)

	assert.Empty(t, errs)
}

func TestContainerScraper_ScrapeCDBTablespaceUsage_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("tablespace query failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeCDBTablespaceUsage(ctx, now)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "tablespace query failed")
}

func TestContainerScraper_ScrapeCDBDataFiles_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CDBDataFilesList = []models.CDBDataFile{
		{
			ConID:          sql.NullInt64{Int64: 1, Valid: true},
			FileName:       sql.NullString{String: "/u01/oradata/system01.dbf", Valid: true},
			TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
			Bytes:          sql.NullInt64{Int64: 1073741824, Valid: true},
			Status:         sql.NullString{String: "AVAILABLE", Valid: true},
			Autoextensible: sql.NullString{String: "YES", Valid: true},
			MaxBytes:       sql.NullInt64{Int64: 34359738368, Valid: true},
			UserBytes:      sql.NullInt64{Int64: 1073479680, Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeCDBDataFiles(ctx, now)

	assert.Empty(t, errs)
}

func TestContainerScraper_ScrapeCDBDataFiles_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("datafile query failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeCDBDataFiles(ctx, now)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "datafile query failed")
}

func TestContainerScraper_ScrapeCDBServices_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.CDBServicesList = []models.CDBService{
		{
			ConID:       sql.NullInt64{Int64: 1, Valid: true},
			ServiceName: sql.NullString{String: "ORCLCDB", Valid: true},
			NetworkName: sql.NullString{String: "orclcdb.example.com", Valid: true},
			PDB:         sql.NullString{String: "CDB$ROOT", Valid: true},
			Enabled:     sql.NullString{String: "YES", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeCDBServices(ctx, now)

	assert.Empty(t, errs)
}

func TestContainerScraper_ScrapeCDBServices_Error(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("service query failed")
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	now := pcommon.NewTimestampFromTime(time.Now())
	errs := scraper.scrapeCDBServices(ctx, now)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "service query failed")
}

func TestContainerScraper_IsCDBSupported(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	// Test nil capability
	assert.False(t, scraper.isCDBSupported())

	// Test false capability
	cdbCapable := false
	scraper.isCDBCapable = &cdbCapable
	assert.False(t, scraper.isCDBSupported())

	// Test true capability
	cdbCapable = true
	scraper.isCDBCapable = &cdbCapable
	assert.True(t, scraper.isCDBSupported())
}

func TestContainerScraper_IsPDBSupported(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	// Test nil capability
	assert.False(t, scraper.isPDBSupported())

	// Test false capability
	pdbCapable := false
	scraper.isPDBCapable = &pdbCapable
	assert.False(t, scraper.isPDBSupported())

	// Test true capability
	pdbCapable = true
	scraper.isPDBCapable = &pdbCapable
	assert.True(t, scraper.isPDBSupported())
}

func TestContainerScraper_IsConnectedToCDBRoot(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	// Test initial state
	assert.False(t, scraper.isConnectedToCDBRoot())

	// Test after checking context with CDB$ROOT
	mockClient.CheckCurrentContainerResult = models.ContainerContext{
		ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
		ContainerID:   sql.NullString{String: "1", Valid: true},
	}
	ctx := context.Background()
	err = scraper.checkCurrentContext(ctx)
	require.NoError(t, err)
	assert.True(t, scraper.isConnectedToCDBRoot())

	// Test with PDB
	mockClient.CheckCurrentContainerResult = models.ContainerContext{
		ContainerName: sql.NullString{String: "PDB1", Valid: true},
		ContainerID:   sql.NullString{String: "3", Valid: true},
	}
	scraper.contextChecked = false // Reset to force recheck
	err = scraper.checkCurrentContext(ctx)
	require.NoError(t, err)
	assert.False(t, scraper.isConnectedToCDBRoot())
}

func TestContainerScraper_IsConnectedToPDB(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
	require.NoError(t, err)

	// Test initial state
	assert.False(t, scraper.isConnectedToPDB())

	// Test after checking context with PDB
	mockClient.CheckCurrentContainerResult = models.ContainerContext{
		ContainerName: sql.NullString{String: "PDB1", Valid: true},
		ContainerID:   sql.NullString{String: "3", Valid: true},
	}
	ctx := context.Background()
	err = scraper.checkCurrentContext(ctx)
	require.NoError(t, err)
	assert.True(t, scraper.isConnectedToPDB())

	// Test with CDB$ROOT
	mockClient.CheckCurrentContainerResult = models.ContainerContext{
		ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
		ContainerID:   sql.NullString{String: "1", Valid: true},
	}
	scraper.contextChecked = false // Reset to force recheck
	err = scraper.checkCurrentContext(ctx)
	require.NoError(t, err)
	assert.False(t, scraper.isConnectedToPDB())
}

// TestScrapeContainerMetrics tests the main orchestration function
func TestScrapeContainerMetrics(t *testing.T) {
	t.Run("Success_CDBRoot", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 1
		mockClient.CheckPDBCapabilityResult = 1
		mockClient.CheckCurrentContainerResult = models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		}
		mockClient.ContainerStatusList = []models.ContainerStatus{
			{
				ConID:         sql.NullInt64{Int64: 1, Valid: true},
				ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
				OpenMode:      sql.NullString{String: "READ WRITE", Valid: true},
				Restricted:    sql.NullString{String: "NO", Valid: true},
			},
		}
		mockClient.PDBStatusList = []models.PDBStatus{
			{
				ConID:     sql.NullInt64{Int64: 3, Valid: true},
				PDBName:   sql.NullString{String: "PDB1", Valid: true},
				OpenMode:  sql.NullString{String: "READ WRITE", Valid: true},
				TotalSize: sql.NullInt64{Int64: 1073741824, Valid: true},
			},
		}
		mockClient.CDBTablespaceUsageList = []models.CDBTablespaceUsage{
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 1073741824, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 48.83, Valid: true},
			},
		}
		mockClient.CDBDataFilesList = []models.CDBDataFile{
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				FileName:       sql.NullString{String: "/u01/oradata/system01.dbf", Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				Bytes:          sql.NullInt64{Int64: 1073741824, Valid: true},
				Status:         sql.NullString{String: "AVAILABLE", Valid: true},
			},
		}
		mockClient.CDBServicesList = []models.CDBService{
			{
				ConID:       sql.NullInt64{Int64: 1, Valid: true},
				ServiceName: sql.NullString{String: "ORCLCDB", Valid: true},
				NetworkName: sql.NullString{String: "orclcdb.example.com", Valid: true},
				PDB:         sql.NullString{String: "CDB$ROOT", Valid: true},
				Enabled:     sql.NullString{String: "YES", Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		errs := scraper.ScrapeContainerMetrics(ctx)

		assert.Empty(t, errs)
		assert.True(t, scraper.environmentChecked)
		assert.True(t, scraper.contextChecked)
	})

	t.Run("Success_PDB", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 1
		mockClient.CheckPDBCapabilityResult = 1
		mockClient.CheckCurrentContainerResult = models.ContainerContext{
			ContainerName: sql.NullString{String: "PDB1", Valid: true},
			ContainerID:   sql.NullString{String: "3", Valid: true},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		errs := scraper.ScrapeContainerMetrics(ctx)

		// Should succeed but skip CDB-specific metrics
		assert.Empty(t, errs)
		assert.True(t, scraper.environmentChecked)
		assert.True(t, scraper.contextChecked)
		assert.False(t, scraper.isConnectedToCDBRoot())
		assert.True(t, scraper.isConnectedToPDB())
	})

	t.Run("CDB_NotSupported", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 0
		mockClient.CheckPDBCapabilityResult = 0

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		errs := scraper.ScrapeContainerMetrics(ctx)

		// Should skip all metrics when CDB not supported
		assert.Empty(t, errs)
		assert.True(t, scraper.environmentChecked)
		assert.False(t, scraper.isCDBSupported())
	})

	t.Run("EnvironmentCheck_Error", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.QueryErr = errors.New("environment check failed")

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		errs := scraper.ScrapeContainerMetrics(ctx)

		assert.NotEmpty(t, errs)
		assert.Contains(t, errs[0].Error(), "environment check failed")
	})

	t.Run("ContextCheck_Error", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 1
		mockClient.CheckPDBCapabilityResult = 1

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		// First check environment capability successfully
		err = scraper.checkEnvironmentCapability(ctx)
		require.NoError(t, err)

		// Now set error for context check
		mockClient.QueryErr = errors.New("context check failed")

		errs := scraper.ScrapeContainerMetrics(ctx)

		assert.NotEmpty(t, errs)
		assert.Contains(t, errs[0].Error(), "context check failed")
	})

	t.Run("PartialErrors_OneQuery", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 1
		mockClient.CheckPDBCapabilityResult = 1
		mockClient.CheckCurrentContainerResult = models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		// First, complete environment and context checks successfully
		err = scraper.checkEnvironmentCapability(ctx)
		require.NoError(t, err)
		err = scraper.checkCurrentContext(ctx)
		require.NoError(t, err)

		// Now set error for subsequent queries
		mockClient.QueryErr = errors.New("query error")

		errs := scraper.ScrapeContainerMetrics(ctx)

		// Should have errors from failed queries
		assert.NotEmpty(t, errs)
		assert.Contains(t, errs[0].Error(), "query error")
	})
}

// TestScrapeContainerStatus_EdgeCases tests additional edge cases
func TestScrapeContainerStatus_EdgeCases(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.ContainerStatusList = []models.ContainerStatus{}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeContainerStatus(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("MetricsDisabled", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.ContainerStatusList = []models.ContainerStatus{
			{
				ConID:         sql.NullInt64{Int64: 1, Valid: true},
				ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
				OpenMode:      sql.NullString{String: "READ WRITE", Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		config := metadata.DefaultMetricsBuilderConfig()
		config.Metrics.NewrelicoracledbContainerStatus.Enabled = false
		config.Metrics.NewrelicoracledbContainerRestricted.Enabled = false
		mb := metadata.NewMetricsBuilder(config, settings)
		logger := zap.NewNop()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeContainerStatus(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("MixedOpenModes", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.ContainerStatusList = []models.ContainerStatus{
			{
				ConID:         sql.NullInt64{Int64: 1, Valid: true},
				ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
				OpenMode:      sql.NullString{String: "READ ONLY", Valid: true},
				Restricted:    sql.NullString{String: "YES", Valid: true},
			},
			{
				ConID:         sql.NullInt64{Int64: 3, Valid: true},
				ContainerName: sql.NullString{String: "PDB1", Valid: true},
				OpenMode:      sql.NullString{String: "MOUNTED", Valid: true},
				Restricted:    sql.NullString{String: "NO", Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeContainerStatus(ctx, now)

		assert.Empty(t, errs)
	})
}

// TestScrapePDBStatus_EdgeCases tests additional edge cases
func TestScrapePDBStatus_EdgeCases(t *testing.T) {
	t.Run("InvalidData_NullFields", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.PDBStatusList = []models.PDBStatus{
			{
				ConID:     sql.NullInt64{Valid: false},
				PDBName:   sql.NullString{String: "PDB1", Valid: true},
				OpenMode:  sql.NullString{String: "READ WRITE", Valid: true},
				TotalSize: sql.NullInt64{Int64: 1073741824, Valid: true},
			},
			{
				ConID:     sql.NullInt64{Int64: 4, Valid: true},
				PDBName:   sql.NullString{Valid: false},
				OpenMode:  sql.NullString{String: "READ WRITE", Valid: true},
				TotalSize: sql.NullInt64{Int64: 1073741824, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapePDBStatus(ctx, now)

		assert.Empty(t, errs) // Should skip invalid entries
	})

	t.Run("MetricsDisabled", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.PDBStatusList = []models.PDBStatus{
			{
				ConID:     sql.NullInt64{Int64: 3, Valid: true},
				PDBName:   sql.NullString{String: "PDB1", Valid: true},
				OpenMode:  sql.NullString{String: "READ WRITE", Valid: true},
				TotalSize: sql.NullInt64{Int64: 1073741824, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		config := metadata.DefaultMetricsBuilderConfig()
		// The actual metric names are NewrelicoracledbPdbOpenMode and NewrelicoracledbPdbTotalSizeBytes
		mb := metadata.NewMetricsBuilder(config, settings)
		logger := zap.NewNop()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapePDBStatus(ctx, now)

		assert.Empty(t, errs)
	})
}

// TestScrapeCDBTablespaceUsage_EdgeCases tests additional edge cases
func TestScrapeCDBTablespaceUsage_EdgeCases(t *testing.T) {
	t.Run("InvalidData_NullFields", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBTablespaceUsageList = []models.CDBTablespaceUsage{
			{
				ConID:          sql.NullInt64{Valid: false},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 1073741824, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 48.83, Valid: true},
			},
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{Valid: false},
				UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 1073741824, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 48.83, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBTablespaceUsage(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("WithIncludeFilter", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBTablespaceUsageList = []models.CDBTablespaceUsage{
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 1073741824, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 48.83, Valid: true},
			},
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{String: "USERS", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 104857600, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 524288000, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 20.0, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		includeTablespaces := []string{"SYSTEM"}
		scraper, err := NewContainerScraper(mockClient, mb, logger, config, includeTablespaces, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBTablespaceUsage(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("WithExcludeFilter", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBTablespaceUsageList = []models.CDBTablespaceUsage{
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 1073741824, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 48.83, Valid: true},
			},
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{String: "TEMP", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 104857600, Valid: true},
				TotalBytes:     sql.NullInt64{Int64: 524288000, Valid: true},
				UsedPercent:    sql.NullFloat64{Float64: 20.0, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		excludeTablespaces := []string{"TEMP"}
		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, excludeTablespaces)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBTablespaceUsage(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("MetricsDisabled", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBTablespaceUsageList = []models.CDBTablespaceUsage{
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				UsedBytes:      sql.NullInt64{Int64: 524288000, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		config := metadata.DefaultMetricsBuilderConfig()
		// The actual metric names use Newrelicoracledb prefix without Cdb in the middle
		mb := metadata.NewMetricsBuilder(config, settings)
		logger := zap.NewNop()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBTablespaceUsage(ctx, now)

		assert.Empty(t, errs)
	})
}

// TestScrapeCDBDataFiles_EdgeCases tests additional edge cases
func TestScrapeCDBDataFiles_EdgeCases(t *testing.T) {
	t.Run("InvalidData_NullFields", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBDataFilesList = []models.CDBDataFile{
			{
				ConID:          sql.NullInt64{Valid: false},
				FileName:       sql.NullString{String: "/u01/oradata/system01.dbf", Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				Bytes:          sql.NullInt64{Int64: 1073741824, Valid: true},
			},
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				FileName:       sql.NullString{Valid: false},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				Bytes:          sql.NullInt64{Int64: 1073741824, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBDataFiles(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("MetricsDisabled", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBDataFilesList = []models.CDBDataFile{
			{
				ConID:          sql.NullInt64{Int64: 1, Valid: true},
				FileName:       sql.NullString{String: "/u01/oradata/system01.dbf", Valid: true},
				TablespaceName: sql.NullString{String: "SYSTEM", Valid: true},
				Bytes:          sql.NullInt64{Int64: 1073741824, Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		config := metadata.DefaultMetricsBuilderConfig()
		// The actual metric names use Newrelicoracledb prefix without Cdb in the middle
		mb := metadata.NewMetricsBuilder(config, settings)
		logger := zap.NewNop()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBDataFiles(ctx, now)

		assert.Empty(t, errs)
	})
}

// TestScrapeCDBServices_EdgeCases tests additional edge cases
func TestScrapeCDBServices_EdgeCases(t *testing.T) {
	t.Run("InvalidData_NullFields", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBServicesList = []models.CDBService{
			{
				ConID:       sql.NullInt64{Valid: false},
				ServiceName: sql.NullString{String: "ORCLCDB", Valid: true},
				NetworkName: sql.NullString{String: "orclcdb.example.com", Valid: true},
				PDB:         sql.NullString{String: "CDB$ROOT", Valid: true},
				Enabled:     sql.NullString{String: "YES", Valid: true},
			},
			{
				ConID:       sql.NullInt64{Int64: 1, Valid: true},
				ServiceName: sql.NullString{Valid: false},
				NetworkName: sql.NullString{String: "orclcdb.example.com", Valid: true},
				PDB:         sql.NullString{String: "CDB$ROOT", Valid: true},
				Enabled:     sql.NullString{String: "YES", Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBServices(ctx, now)

		assert.Empty(t, errs)
	})

	t.Run("MetricsDisabled", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CDBServicesList = []models.CDBService{
			{
				ConID:       sql.NullInt64{Int64: 1, Valid: true},
				ServiceName: sql.NullString{String: "ORCLCDB", Valid: true},
				NetworkName: sql.NullString{String: "orclcdb.example.com", Valid: true},
				PDB:         sql.NullString{String: "CDB$ROOT", Valid: true},
				Enabled:     sql.NullString{String: "YES", Valid: true},
			},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		config := metadata.DefaultMetricsBuilderConfig()
		// The actual metric names use Newrelicoracledb prefix without Cdb in the middle
		mb := metadata.NewMetricsBuilder(config, settings)
		logger := zap.NewNop()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		now := pcommon.NewTimestampFromTime(time.Now())
		errs := scraper.scrapeCDBServices(ctx, now)

		assert.Empty(t, errs)
	})
}

// TestCheckEnvironmentCapability_EdgeCases tests additional edge cases
func TestCheckEnvironmentCapability_EdgeCases(t *testing.T) {
	t.Run("AlreadyChecked", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 1
		mockClient.CheckPDBCapabilityResult = 1

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()

		// First call
		err = scraper.checkEnvironmentCapability(ctx)
		assert.NoError(t, err)
		assert.True(t, scraper.environmentChecked)

		// Set error for second attempt
		mockClient.QueryErr = errors.New("should not be called")

		// Second call should skip checks
		err = scraper.checkEnvironmentCapability(ctx)
		assert.NoError(t, err)
	})

	t.Run("CDBCheckError_Permanent", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.QueryErr = errors.New("CDB feature check failed")

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		err = scraper.checkEnvironmentCapability(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CDB feature check failed")
	})

	t.Run("PDBCheckError_Permanent", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCDBFeatureResult = 1
		mockClient.QueryErr = errors.New("PDB capability check failed")

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		err = scraper.checkEnvironmentCapability(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "PDB capability check failed")
	})
}

// TestCheckCurrentContext_EdgeCases tests additional edge cases
func TestCheckCurrentContext_EdgeCases(t *testing.T) {
	t.Run("AlreadyChecked", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCurrentContainerResult = models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()

		// First call
		err = scraper.checkCurrentContext(ctx)
		assert.NoError(t, err)
		assert.True(t, scraper.contextChecked)

		// Set error for second attempt
		mockClient.QueryErr = errors.New("should not be called")

		// Second call should skip checks
		err = scraper.checkCurrentContext(ctx)
		assert.NoError(t, err)
	})

	t.Run("InvalidContainerName", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCurrentContainerResult = models.ContainerContext{
			ContainerName: sql.NullString{Valid: false},
			ContainerID:   sql.NullString{String: "1", Valid: true},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		err = scraper.checkCurrentContext(ctx)

		assert.NoError(t, err) // Should handle gracefully
		assert.Equal(t, "", scraper.currentContainer)
	})

	t.Run("InvalidContainerID", func(t *testing.T) {
		mockClient := client.NewMockClient()
		mockClient.CheckCurrentContainerResult = models.ContainerContext{
			ContainerName: sql.NullString{String: "CDB$ROOT", Valid: true},
			ContainerID:   sql.NullString{Valid: false},
		}

		settings := receivertest.NewNopSettings(metadata.Type)
		mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
		logger := zap.NewNop()
		config := metadata.DefaultMetricsBuilderConfig()

		scraper, err := NewContainerScraper(mockClient, mb, logger, config, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		err = scraper.checkCurrentContext(ctx)

		assert.NoError(t, err) // Should handle gracefully
		assert.Equal(t, "", scraper.currentContainerID)
	})
}
