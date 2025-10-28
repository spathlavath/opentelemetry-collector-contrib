// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestNewTablespaceScraper(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	instanceName := "test-instance"
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(db, mb, logger, instanceName, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, db, scraper.db)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, config, scraper.config)
}

func TestTablespaceScraper_NilDatabase(t *testing.T) {
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(nil, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.db)
}

func TestTablespaceScraper_NilMetricsBuilder(t *testing.T) {
	db := &sql.DB{}
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, nil, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.mb)
}

func TestTablespaceScraper_NilLogger(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	scraper := NewTablespaceScraper(db, mb, nil, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Nil(t, scraper.logger)
}

func TestTablespaceScraper_EmptyInstanceName(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "", metadata.DefaultMetricsBuilderConfig())

	assert.NotNil(t, scraper)
	assert.Equal(t, "", scraper.instanceName)
}

func TestTablespaceScraper_Config(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", config)

	assert.NotNil(t, scraper)
	assert.Equal(t, config, scraper.config)
}

func TestTablespaceScraper_MultipleInstances(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper1 := NewTablespaceScraper(db, mb, logger, "instance-1", metadata.DefaultMetricsBuilderConfig())
	scraper2 := NewTablespaceScraper(db, mb, logger, "instance-2", metadata.DefaultMetricsBuilderConfig())

	assert.NotEqual(t, scraper1, scraper2)
	assert.Equal(t, "instance-1", scraper1.instanceName)
	assert.Equal(t, "instance-2", scraper2.instanceName)
}

func TestTablespaceScraper_IsCDBSupported_NotChecked(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.False(t, scraper.isCDBSupported())
}

func TestTablespaceScraper_IsCDBSupported_Checked(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	cdbCapable := true
	scraper.isCDBCapable = &cdbCapable

	assert.True(t, scraper.isCDBSupported())
}

func TestTablespaceScraper_IsPDBSupported_NotChecked(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.False(t, scraper.isPDBSupported())
}

func TestTablespaceScraper_IsPDBSupported_Checked(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	pdbCapable := true
	scraper.isPDBCapable = &pdbCapable

	assert.True(t, scraper.isPDBSupported())
}

func TestTablespaceScraper_IsConnectedToCDBRoot_Empty(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.False(t, scraper.isConnectedToCDBRoot())
}

func TestTablespaceScraper_IsConnectedToCDBRoot_CDBRoot(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())
	scraper.currentContainer = "CDB$ROOT"

	assert.True(t, scraper.isConnectedToCDBRoot())
}

func TestTablespaceScraper_IsConnectedToPDB_Empty(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())

	assert.False(t, scraper.isConnectedToPDB())
}

func TestTablespaceScraper_IsConnectedToPDB_PDB(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())
	scraper.currentContainer = "FREEPDB1"

	assert.True(t, scraper.isConnectedToPDB())
}

func TestTablespaceScraper_IsConnectedToPDB_CDBRoot(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", metadata.DefaultMetricsBuilderConfig())
	scraper.currentContainer = "CDB$ROOT"

	assert.False(t, scraper.isConnectedToPDB())
}

func TestTablespaceScraper_IsAnyTablespaceMetricEnabled_AllDisabled(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceReservedBytes.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceSpaceUsedPercentage.Enabled = false
	config.Metrics.NewrelicoracledbTablespaceIsOffline.Enabled = false
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", config)

	assert.False(t, scraper.isAnyTablespaceMetricEnabled())
}

func TestTablespaceScraper_IsAnyTablespaceMetricEnabled_OneEnabled(t *testing.T) {
	db := &sql.DB{}
	settings := receivertest.NewNopSettings(metadata.Type)
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbTablespaceSpaceConsumedBytes.Enabled = true
	mb := metadata.NewMetricsBuilder(config, settings)
	logger := zap.NewNop()

	scraper := NewTablespaceScraper(db, mb, logger, "test-instance", config)

	assert.True(t, scraper.isAnyTablespaceMetricEnabled())
}
