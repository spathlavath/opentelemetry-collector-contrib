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
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/client"
	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/models"
)

func TestNewRacScraper(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewRacScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.Nil(t, scraper.isRacMode)
}

func TestNullStringToString_Valid(t *testing.T) {
	ns := sql.NullString{String: "test", Valid: true}
	result := nullStringToString(ns)
	assert.Equal(t, "test", result)
}

func TestNullStringToString_Invalid(t *testing.T) {
	ns := sql.NullString{String: "", Valid: false}
	result := nullStringToString(ns)
	assert.Empty(t, result)
}

func TestStringStatusToBinary_Match(t *testing.T) {
	result := stringStatusToBinary("OPEN", "OPEN")
	assert.Equal(t, int64(1), result)
}

func TestStringStatusToBinary_MatchCaseInsensitive(t *testing.T) {
	result := stringStatusToBinary("open", "OPEN")
	assert.Equal(t, int64(1), result)
}

func TestStringStatusToBinary_NoMatch(t *testing.T) {
	result := stringStatusToBinary("CLOSED", "OPEN")
	assert.Equal(t, int64(0), result)
}

func TestStringStatusToBinary_EmptyString(t *testing.T) {
	result := stringStatusToBinary("", "OPEN")
	assert.Equal(t, int64(0), result)
}

func TestRacScraper_CachedRacMode(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	t.Run("uninitialized", func(t *testing.T) {
		scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
		assert.Nil(t, scraper.isRacMode)
	})

	t.Run("cached_enabled", func(t *testing.T) {
		scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
		racEnabled := true
		scraper.isRacMode = &racEnabled
		assert.NotNil(t, scraper.isRacMode)
		assert.True(t, *scraper.isRacMode)
	})

	t.Run("cached_disabled", func(t *testing.T) {
		scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
		racDisabled := false
		scraper.isRacMode = &racDisabled
		assert.NotNil(t, scraper.isRacMode)
		assert.False(t, *scraper.isRacMode)
	})
}

func TestRacScraper_MetricConfiguration(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	t.Run("default_config", func(t *testing.T) {
		scraper := NewRacScraper(mockClient, mb, logger, config)
		assert.NotNil(t, scraper.metricsBuilderConfig)
	})

	t.Run("custom_config", func(t *testing.T) {
		customConfig := metadata.DefaultMetricsBuilderConfig()
		customConfig.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled = false
		scraper := NewRacScraper(mockClient, mb, logger, customConfig)
		assert.False(t, scraper.metricsBuilderConfig.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled)
	})
}

func TestRacScraper_ClientAssignment(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewRacScraper(mockClient, mb, logger, config)
	assert.NotNil(t, scraper.client)
	assert.Equal(t, mockClient, scraper.client)
}

func TestRacScraper_ConcurrentAccess(_ *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	done := make(chan bool)
	racEnabled := true

	go func() {
		scraper.racModeMutex.Lock()
		scraper.isRacMode = &racEnabled
		scraper.racModeMutex.Unlock()
		done <- true
	}()

	go func() {
		scraper.racModeMutex.RLock()
		_ = scraper.isRacMode
		scraper.racModeMutex.RUnlock()
		done <- true
	}()

	<-done
	<-done
}

func TestRacScraper_EmptyInstanceName(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewRacScraper(mockClient, mb, logger, config)
	assert.NotNil(t, scraper)
}

func TestRacScraper_LoggerConfiguration(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewRacScraper(mockClient, mb, logger, config)
	assert.NotNil(t, scraper.logger)
	assert.Equal(t, logger, scraper.logger)
}

// Tests for isRacEnabled

func TestIsRacEnabled_FirstCall_Enabled(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "TRUE", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	enabled, err := scraper.isRacEnabled(ctx)

	assert.NoError(t, err)
	assert.True(t, enabled)
	assert.NotNil(t, scraper.isRacMode)
	assert.True(t, *scraper.isRacMode)
}

func TestIsRacEnabled_FirstCall_Disabled(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "FALSE", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	enabled, err := scraper.isRacEnabled(ctx)

	assert.NoError(t, err)
	assert.False(t, enabled)
	assert.NotNil(t, scraper.isRacMode)
	assert.False(t, *scraper.isRacMode)
}

func TestIsRacEnabled_InvalidClusterDB(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "", Valid: false},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	enabled, err := scraper.isRacEnabled(ctx)

	assert.NoError(t, err)
	assert.False(t, enabled)
}

func TestIsRacEnabled_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("database connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	enabled, err := scraper.isRacEnabled(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection failed")
	assert.False(t, enabled)
}

func TestIsRacEnabled_CachedValue(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "TRUE", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()

	// First call
	enabled1, err1 := scraper.isRacEnabled(ctx)
	assert.NoError(t, err1)
	assert.True(t, enabled1)

	// Set error to verify cache is used
	mockClient.QueryErr = errors.New("should not be called")

	// Second call should use cached value
	enabled2, err2 := scraper.isRacEnabled(ctx)
	assert.NoError(t, err2)
	assert.True(t, enabled2)
}

func TestIsRacEnabled_CaseInsensitive(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "true", Valid: true},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	enabled, err := scraper.isRacEnabled(ctx)

	assert.NoError(t, err)
	assert.True(t, enabled)
}

// Tests for isASMAvailable

func TestIsASMAvailable_Available(t *testing.T) {
	mockClient := &client.MockClient{
		ASMDetection: &models.ASMDetection{ASMCount: 2},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	available, err := scraper.isASMAvailable(ctx)

	assert.NoError(t, err)
	assert.True(t, available)
}

func TestIsASMAvailable_NotAvailable(t *testing.T) {
	mockClient := &client.MockClient{
		ASMDetection: &models.ASMDetection{ASMCount: 0},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	available, err := scraper.isASMAvailable(ctx)

	assert.NoError(t, err)
	assert.False(t, available)
}

func TestIsASMAvailable_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("ASM query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	available, err := scraper.isASMAvailable(ctx)

	assert.NoError(t, err)
	assert.False(t, available)
}

// Tests for ScrapeRacMetrics

func TestScrapeRacMetrics_RACAndASMDisabled(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "FALSE", Valid: true},
		},
		ASMDetection: &models.ASMDetection{ASMCount: 0},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeRacMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeRacMetrics_RACEnabledOnly(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "TRUE", Valid: true},
		},
		ASMDetection:          &models.ASMDetection{ASMCount: 0},
		ClusterWaitEventsList: []models.ClusterWaitEvent{},
		RACInstanceStatusList: []models.RACInstanceStatus{},
		RACActiveServicesList: []models.RACActiveService{},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeRacMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeRacMetrics_ASMEnabledOnly(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "FALSE", Valid: true},
		},
		ASMDetection:      &models.ASMDetection{ASMCount: 1},
		ASMDiskGroupsList: []models.ASMDiskGroup{},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeRacMetrics(ctx)

	assert.Nil(t, errs)
}

func TestScrapeRacMetrics_RACDetectionError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("RAC detection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.ScrapeRacMetrics(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "RAC detection failed")
}

func TestScrapeRacMetrics_ContextCanceled(t *testing.T) {
	mockClient := &client.MockClient{
		RACDetection: &models.RACDetection{
			ClusterDB: sql.NullString{String: "TRUE", Valid: true},
		},
		ASMDetection: &models.ASMDetection{ASMCount: 0},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	errs := scraper.ScrapeRacMetrics(ctx)

	assert.NotNil(t, errs)
	assert.Contains(t, errs[0].Error(), "context canceled")
}

// Tests for scrapeASMDiskGroups

func TestScrapeASMDiskGroups_Success(t *testing.T) {
	mockClient := &client.MockClient{
		ASMDiskGroupsList: []models.ASMDiskGroup{
			{
				Name:         sql.NullString{String: "DATA", Valid: true},
				TotalMB:      sql.NullFloat64{Float64: 10240.0, Valid: true},
				FreeMB:       sql.NullFloat64{Float64: 5120.0, Valid: true},
				OfflineDisks: sql.NullFloat64{Float64: 0, Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeASMDiskGroups(ctx)

	assert.Nil(t, errs)
}

func TestScrapeASMDiskGroups_MetricsDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbAsmDiskgroupTotalMb.Enabled = false
	config.Metrics.NewrelicoracledbAsmDiskgroupFreeMb.Enabled = false
	config.Metrics.NewrelicoracledbAsmDiskgroupOfflineDisks.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, config)

	ctx := t.Context()
	errs := scraper.scrapeASMDiskGroups(ctx)

	assert.Nil(t, errs)
}

func TestScrapeASMDiskGroups_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("disk group query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeASMDiskGroups(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeASMDiskGroups_InvalidName(t *testing.T) {
	mockClient := &client.MockClient{
		ASMDiskGroupsList: []models.ASMDiskGroup{
			{
				Name:         sql.NullString{Valid: false},
				TotalMB:      sql.NullFloat64{Float64: 10240.0, Valid: true},
				FreeMB:       sql.NullFloat64{Float64: 5120.0, Valid: true},
				OfflineDisks: sql.NullFloat64{Float64: 0, Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeASMDiskGroups(ctx)

	assert.Nil(t, errs)
}

// Tests for scrapeClusterWaitEvents

func TestScrapeClusterWaitEvents_Success(t *testing.T) {
	mockClient := &client.MockClient{
		ClusterWaitEventsList: []models.ClusterWaitEvent{
			{
				InstID:          sql.NullString{String: "1", Valid: true},
				Event:           sql.NullString{String: "gc buffer busy", Valid: true},
				TotalWaits:      sql.NullFloat64{Float64: 100.0, Valid: true},
				TimeWaitedMicro: sql.NullFloat64{Float64: 50000.0, Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeClusterWaitEvents(ctx)

	assert.Nil(t, errs)
}

func TestScrapeClusterWaitEvents_MetricsDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRacWaitTime.Enabled = false
	config.Metrics.NewrelicoracledbRacTotalWaits.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, config)

	ctx := t.Context()
	errs := scraper.scrapeClusterWaitEvents(ctx)

	assert.Nil(t, errs)
}

func TestScrapeClusterWaitEvents_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("wait events query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeClusterWaitEvents(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeClusterWaitEvents_InvalidInstID(t *testing.T) {
	mockClient := &client.MockClient{
		ClusterWaitEventsList: []models.ClusterWaitEvent{
			{
				InstID:          sql.NullString{Valid: false},
				Event:           sql.NullString{String: "gc buffer busy", Valid: true},
				TotalWaits:      sql.NullFloat64{Float64: 100.0, Valid: true},
				TimeWaitedMicro: sql.NullFloat64{Float64: 50000.0, Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeClusterWaitEvents(ctx)

	assert.Nil(t, errs)
}

// Tests for scrapeInstanceStatus

func TestScrapeInstanceStatus_Success(t *testing.T) {
	now := time.Now().Add(-1 * time.Hour)
	mockClient := &client.MockClient{
		RACInstanceStatusList: []models.RACInstanceStatus{
			{
				InstID:         sql.NullString{String: "1", Valid: true},
				InstanceName:   sql.NullString{String: "ORCL1", Valid: true},
				HostName:       sql.NullString{String: "host1", Valid: true},
				Status:         sql.NullString{String: "OPEN", Valid: true},
				StartupTime:    sql.NullTime{Time: now, Valid: true},
				DatabaseStatus: sql.NullString{String: "ACTIVE", Valid: true},
				ActiveState:    sql.NullString{String: "NORMAL", Valid: true},
				Logins:         sql.NullString{String: "ALLOWED", Valid: true},
				Archiver:       sql.NullString{String: "STARTED", Valid: true},
				Version:        sql.NullString{String: "19.0.0.0.0", Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeInstanceStatus(ctx)

	assert.Nil(t, errs)
}

func TestScrapeInstanceStatus_MetricsDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRacInstanceStatus.Enabled = false
	config.Metrics.NewrelicoracledbRacInstanceUptimeSeconds.Enabled = false
	config.Metrics.NewrelicoracledbRacInstanceDatabaseStatus.Enabled = false
	config.Metrics.NewrelicoracledbRacInstanceActiveState.Enabled = false
	config.Metrics.NewrelicoracledbRacInstanceLoginsAllowed.Enabled = false
	config.Metrics.NewrelicoracledbRacInstanceArchiverStarted.Enabled = false
	config.Metrics.NewrelicoracledbRacInstanceVersionInfo.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, config)

	ctx := t.Context()
	errs := scraper.scrapeInstanceStatus(ctx)

	assert.Nil(t, errs)
}

func TestScrapeInstanceStatus_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("instance status query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeInstanceStatus(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeInstanceStatus_InvalidStatus(t *testing.T) {
	mockClient := &client.MockClient{
		RACInstanceStatusList: []models.RACInstanceStatus{
			{
				InstID: sql.NullString{String: "1", Valid: true},
				Status: sql.NullString{Valid: false},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeInstanceStatus(ctx)

	assert.Nil(t, errs)
}

// Tests for scrapeActiveServices

func TestScrapeActiveServices_Success(t *testing.T) {
	mockClient := &client.MockClient{
		RACActiveServicesList: []models.RACActiveService{
			{
				InstID:                  sql.NullString{String: "1", Valid: true},
				ServiceName:             sql.NullString{String: "SALES", Valid: true},
				NetworkName:             sql.NullString{String: "SALES_NET", Valid: true},
				Goal:                    sql.NullString{String: "SERVICE_TIME", Valid: true},
				ClbGoal:                 sql.NullString{String: "SHORT", Valid: true},
				Blocked:                 sql.NullString{String: "NO", Valid: true},
				AqHaNotification:        sql.NullString{String: "YES", Valid: true},
				CommitOutcome:           sql.NullString{String: "TRUE", Valid: true},
				DrainTimeout:            sql.NullFloat64{Float64: 60.0, Valid: true},
				ReplayInitiationTimeout: sql.NullFloat64{Float64: 300.0, Valid: true},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeActiveServices(ctx)

	assert.Nil(t, errs)
}

func TestScrapeActiveServices_MetricsDisabled(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbRacServiceInstanceID.Enabled = false
	config.Metrics.NewrelicoracledbRacServiceNetworkConfig.Enabled = false
	config.Metrics.NewrelicoracledbRacServiceClbConfig.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, config)

	ctx := t.Context()
	errs := scraper.scrapeActiveServices(ctx)

	assert.Nil(t, errs)
}

func TestScrapeActiveServices_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("services query failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeActiveServices(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
}

func TestScrapeActiveServices_InvalidServiceName(t *testing.T) {
	mockClient := &client.MockClient{
		RACActiveServicesList: []models.RACActiveService{
			{
				InstID:      sql.NullString{String: "1", Valid: true},
				ServiceName: sql.NullString{Valid: false},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeActiveServices(ctx)

	assert.Nil(t, errs)
}

func TestScrapeActiveServices_InvalidTimeout(t *testing.T) {
	mockClient := &client.MockClient{
		RACActiveServicesList: []models.RACActiveService{
			{
				InstID:                  sql.NullString{String: "1", Valid: true},
				ServiceName:             sql.NullString{String: "SALES", Valid: true},
				DrainTimeout:            sql.NullFloat64{Valid: false},
				ReplayInitiationTimeout: sql.NullFloat64{Valid: false},
			},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewRacScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := t.Context()
	errs := scraper.scrapeActiveServices(ctx)

	assert.Nil(t, errs)
}
