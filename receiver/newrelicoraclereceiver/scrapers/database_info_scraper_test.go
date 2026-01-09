// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
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

