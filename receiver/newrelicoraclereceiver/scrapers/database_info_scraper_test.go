// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewDatabaseInfoScraper(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, 1*time.Hour, scraper.cacheDuration)
}

func TestExtractCloudProviderForOTEL(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		expected string
	}{
		{"AWS", "aws", "aws"},
		{"Azure", "azure", "azure"},
		{"GCP", "gcp", "gcp"},
		{"OCI", "oci", "oci"},
		{"Kubernetes", "kubernetes", ""},
		{"Container", "container", ""},
		{"On-premises", "on-premises", ""},
		{"Empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractCloudProviderForOTEL(tt.provider)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractVersionFromFull(t *testing.T) {
	tests := []struct {
		name        string
		versionFull string
		expected    string
	}{
		{"Oracle 23", "23.0.0.0.0", "23.0"},
		{"Oracle 19", "19.3.0.0.0", "19.3"},
		{"Oracle 21", "21.0.0.0.0", "21.0"},
		{"Oracle 11", "11.2.0.4.0", "11.2"},
		{"Single part", "23", "23"},
		{"Empty", "", "unknown"},
		{"Two parts", "19.3", "19.3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractVersionFromFull(tt.versionFull)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectEditionFromVersion(t *testing.T) {
	tests := []struct {
		name        string
		versionFull string
		expected    string
	}{
		{"Oracle 23 (Free)", "23.0.0.0.0", "free"},
		{"Oracle 19 (Standard)", "19.3.0.0.0", "standard"},
		{"Oracle 21 (Standard)", "21.0.0.0.0", "standard"},
		{"Oracle 11 (Standard)", "11.2.0.4.0", "standard"},
		{"Empty", "", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectEditionFromVersion(tt.versionFull)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScrapeDatabaseInfo_MetricDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseInfo.Enabled = false
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseInfo(ctx)
	assert.Empty(t, errs)
}

func TestScrapeHostingInfo_MetricDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbHostingInfo.Enabled = false
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeHostingInfo(ctx)
	assert.Empty(t, errs)
}

func TestEnsureCacheValid_UsesCacheWhenValid(t *testing.T) {
	mockClient := &client.MockClient{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)

	scraper.cachedInfo = &DatabaseInfo{
		Version:     "19.3",
		VersionFull: "19.3.0.0.0",
		Edition:     "standard",
	}
	scraper.cacheValidUntil = time.Now().Add(30 * time.Minute)

	ctx := context.Background()
	err := scraper.ensureCacheValid(ctx)

	assert.NoError(t, err)
	assert.Equal(t, "19.3", scraper.cachedInfo.Version)
}

func TestScrapeDatabaseInfo_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "19.3.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseInfo(ctx)
	assert.Empty(t, errs)
	assert.NotNil(t, scraper.cachedInfo)
	assert.Equal(t, "19.3", scraper.cachedInfo.Version)
	assert.Equal(t, "standard", scraper.cachedInfo.Edition)
}

func TestScrapeDatabaseInfo_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = assert.AnError

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseInfo(ctx)
	assert.NotEmpty(t, errs)
}

func TestScrapeDatabaseInfo_EmptyMetrics(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseInfo(ctx)
	assert.Empty(t, errs)
}

func TestScrapeDatabaseInfo_UsesCachedData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "19.3.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	// First call populates cache
	errs := scraper.ScrapeDatabaseInfo(ctx)
	assert.Empty(t, errs)

	// Verify cache was populated
	assert.NotNil(t, scraper.cachedInfo)
	cachedVersion := scraper.cachedInfo.Version

	// Change mock data
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "23.0.0.0.0", Valid: true}},
	}

	// Second call should use cached data
	errs = scraper.ScrapeDatabaseInfo(ctx)
	assert.Empty(t, errs)
	assert.Equal(t, cachedVersion, scraper.cachedInfo.Version)
}

func TestScrapeHostingInfo_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "19.3.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbHostingInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeHostingInfo(ctx)
	assert.Empty(t, errs)
	assert.NotNil(t, scraper.cachedInfo)
}

func TestScrapeHostingInfo_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = assert.AnError

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbHostingInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeHostingInfo(ctx)
	assert.NotEmpty(t, errs)
}

func TestScrapeHostingInfo_UsesCachedData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "19.3.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbHostingInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	// First call populates cache
	errs := scraper.ScrapeHostingInfo(ctx)
	assert.Empty(t, errs)
	assert.NotNil(t, scraper.cachedInfo)

	// Second call should use cached data
	errs = scraper.ScrapeHostingInfo(ctx)
	assert.Empty(t, errs)
}

func TestScrapeDatabaseRole_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseRole = &models.DatabaseRole{
		DatabaseRole:    sql.NullString{String: "PRIMARY", Valid: true},
		OpenMode:        sql.NullString{String: "READ WRITE", Valid: true},
		ProtectionMode:  sql.NullString{String: "MAXIMUM PERFORMANCE", Valid: true},
		ProtectionLevel: sql.NullString{String: "UNPROTECTED", Valid: true},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseRole.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseRole(ctx)
	assert.Empty(t, errs)
}

func TestScrapeDatabaseRole_MetricDisabled(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseRole.Enabled = false
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseRole(ctx)
	assert.Empty(t, errs)
}

func TestScrapeDatabaseRole_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = assert.AnError

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseRole.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseRole(ctx)
	assert.NotEmpty(t, errs)
}

func TestScrapeDatabaseRole_NullValues(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseRole = &models.DatabaseRole{
		DatabaseRole:    sql.NullString{String: "", Valid: true},
		OpenMode:        sql.NullString{String: "", Valid: true},
		ProtectionMode:  sql.NullString{String: "", Valid: true},
		ProtectionLevel: sql.NullString{String: "", Valid: true},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseRole.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseRole(ctx)
	assert.Empty(t, errs)
}

func TestScrapeDatabaseRole_StandbyRole(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseRole = &models.DatabaseRole{
		DatabaseRole:    sql.NullString{String: "PHYSICAL STANDBY", Valid: true},
		OpenMode:        sql.NullString{String: "READ ONLY", Valid: true},
		ProtectionMode:  sql.NullString{String: "MAXIMUM AVAILABILITY", Valid: true},
		ProtectionLevel: sql.NullString{String: "RESYNCHRONIZATION", Valid: true},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbDatabaseRole.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeDatabaseRole(ctx)
	assert.Empty(t, errs)
}

func TestEnsureCacheValid_RefreshesExpiredCache(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "19.3.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)

	// Set expired cache
	scraper.cachedInfo = &DatabaseInfo{
		Version:     "11.2",
		VersionFull: "11.2.0.4.0",
		Edition:     "standard",
	}
	scraper.cacheValidUntil = time.Now().Add(-1 * time.Minute)

	ctx := context.Background()
	err := scraper.ensureCacheValid(ctx)

	assert.NoError(t, err)
	assert.Equal(t, "19.3", scraper.cachedInfo.Version)
}

func TestProcessDatabaseInfoMetrics_Oracle23(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)

	metrics := []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "23.0.0.0.0", Valid: true}},
	}

	err := scraper.processDatabaseInfoMetrics(metrics)
	assert.NoError(t, err)
	assert.Equal(t, "23.0", scraper.cachedInfo.Version)
	assert.Equal(t, "free", scraper.cachedInfo.Edition)
}

func TestProcessDatabaseInfoMetrics_EmptyList(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)

	metrics := []models.DatabaseInfoMetric{}

	err := scraper.processDatabaseInfoMetrics(metrics)
	assert.NoError(t, err)
	assert.Nil(t, scraper.cachedInfo)
}

func TestExtractVersionFromFull_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		versionFull string
		expected    string
	}{
		{"With spaces", "  19.3.0.0.0  ", "19.3"},
		{"Many parts", "19.3.0.0.0.1.2.3", "19.3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractVersionFromFull(tt.versionFull)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnsureCacheValid_ConcurrentAccess(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "19.3.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	// Simulate concurrent access
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func() {
			err := scraper.ensureCacheValid(ctx)
			assert.NoError(t, err)
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		<-done
	}

	assert.NotNil(t, scraper.cachedInfo)
}

func TestScrapeHostingInfo_EmptyCache(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbHostingInfo.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeHostingInfo(ctx)
	assert.Empty(t, errs)
}

func TestEnsureCacheValid_DoubleCheckLocking(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.DatabaseInfoList = []models.DatabaseInfoMetric{
		{VersionFull: sql.NullString{String: "21.0.0.0.0", Valid: true}},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewDatabaseInfoScraper(mockClient, mb, logger, config)

	// Set expired cache
	scraper.cachedInfo = &DatabaseInfo{
		Version: "19.3",
	}
	scraper.cacheValidUntil = time.Now().Add(-1 * time.Minute)

	ctx := context.Background()

	// Multiple goroutines trying to refresh at once
	done := make(chan bool)
	for i := 0; i < 3; i++ {
		go func() {
			err := scraper.ensureCacheValid(ctx)
			assert.NoError(t, err)
			done <- true
		}()
	}

	for i := 0; i < 3; i++ {
		<-done
	}

	// Should be refreshed to 21.0
	assert.Equal(t, "21.0", scraper.cachedInfo.Version)
}

func TestExtractVersionFromFull_FallbackToFull(t *testing.T) {
	// When split returns empty, should return full version
	result := extractVersionFromFull("invalid")
	assert.Equal(t, "invalid", result)
}
