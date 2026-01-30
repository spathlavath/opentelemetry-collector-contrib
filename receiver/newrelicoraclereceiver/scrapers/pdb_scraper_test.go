// Copyright 2025 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewPdbScraper(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewPdbScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.config)
	assert.Nil(t, scraper.isCDBCapable)
	assert.False(t, scraper.environmentChecked)
}

func TestIsCDBSupported_True(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	cdbCapable := true
	scraper.isCDBCapable = &cdbCapable

	assert.True(t, scraper.isCDBSupported())
}

func TestIsCDBSupported_False(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	cdbCapable := false
	scraper.isCDBCapable = &cdbCapable

	assert.False(t, scraper.isCDBSupported())
}

func TestIsCDBSupported_Nil(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	assert.False(t, scraper.isCDBSupported())
}

func TestContainsORACode_ORA00942(t *testing.T) {
	err := errors.New("ORA-00942: table or view does not exist")
	assert.True(t, containsORACode(err, "ORA-00942"))
}

func TestContainsORACode_ORA01722(t *testing.T) {
	err := errors.New("ORA-01722: invalid number")
	assert.True(t, containsORACode(err, "ORA-01722"))
}

func TestContainsORACode_MultipleCodes(t *testing.T) {
	err := errors.New("ORA-01722: invalid number")
	assert.True(t, containsORACode(err, "ORA-00942", "ORA-01722"))
}

func TestContainsORACode_NoMatch(t *testing.T) {
	err := errors.New("network timeout")
	assert.False(t, containsORACode(err, "ORA-00942"))
}

func TestContainsORACode_NilError(t *testing.T) {
	assert.False(t, containsORACode(nil, "ORA-00942"))
}

func TestRecordMetric_SessionMetrics(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	testCases := []string{
		"Active Parallel Sessions",
		"Active Serial Sessions",
		"Average Active Sessions",
		"Session Count",
	}

	for _, metricName := range testCases {
		scraper.recordMetric(0, metricName, 100.0, "1", "TEST_PDB")
	}
}

func TestRecordMetric_CPUMetrics(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	testCases := []string{
		"Background CPU Usage Per Sec",
		"CPU Usage Per Sec",
		"CPU Usage Per Txn",
		"Database CPU Time Ratio",
	}

	for _, metricName := range testCases {
		scraper.recordMetric(0, metricName, 50.5, "1", "TEST_PDB")
	}
}

func TestRecordMetric_IOMetrics(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	testCases := []string{
		"Physical Read Total Bytes Per Sec",
		"Physical Reads Per Sec",
		"Physical Write Total Bytes Per Sec",
		"Physical Writes Per Sec",
	}

	for _, metricName := range testCases {
		scraper.recordMetric(0, metricName, 1024.0, "1", "TEST_PDB")
	}
}

func TestRecordMetric_ParseMetrics(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	testCases := []string{
		"Hard Parse Count Per Sec",
		"Soft Parse Ratio",
		"Total Parse Count Per Sec",
	}

	for _, metricName := range testCases {
		scraper.recordMetric(0, metricName, 75.0, "1", "TEST_PDB")
	}
}

func TestRecordMetric_TransactionMetrics(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	testCases := []string{
		"User Transaction Per Sec",
		"User Commits Per Sec",
		"User Rollbacks Per Sec",
	}

	for _, metricName := range testCases {
		scraper.recordMetric(0, metricName, 10.0, "1", "TEST_PDB")
	}
}

func TestRecordMetric_UnknownMetric(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	scraper.recordMetric(0, "Unknown Metric Name", 100.0, "1", "TEST_PDB")
}

func TestPdbScraper_CDBCapableStates(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()

	t.Run("uninitialized", func(t *testing.T) {
		scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
		assert.False(t, scraper.isCDBSupported())
		assert.Nil(t, scraper.isCDBCapable)
	})

	t.Run("capable_true", func(t *testing.T) {
		scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
		capable := true
		scraper.isCDBCapable = &capable
		assert.True(t, scraper.isCDBSupported())
	})

	t.Run("capable_false", func(t *testing.T) {
		scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())
		capable := false
		scraper.isCDBCapable = &capable
		assert.False(t, scraper.isCDBSupported())
	})
}

// Tests for ScrapePdbMetrics

func TestScrapePdbMetrics_CDBNotSupported(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 0},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapePdbMetrics(ctx)

	assert.Nil(t, errs)
	assert.False(t, scraper.isCDBSupported())
}

func TestScrapePdbMetrics_CDBSupported_Success(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 1},
		PDBSysMetricsList: []models.PDBSysMetric{
			{InstID: 1, PDBName: "PDB1", MetricName: "Active Parallel Sessions", Value: 10.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "CPU Usage Per Sec", Value: 45.5},
			{InstID: 2, PDBName: "PDB2", MetricName: "Session Count", Value: 25.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapePdbMetrics(ctx)

	assert.Nil(t, errs)
	assert.True(t, scraper.isCDBSupported())
}

func TestScrapePdbMetrics_CDBCapabilityCheckError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("database connection failed"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapePdbMetrics(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection failed")
}

func TestScrapePdbMetrics_QueryMetricsError(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 1},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	// First call succeeds for CDB capability check
	ctx := context.Background()
	_ = scraper.ScrapePdbMetrics(ctx)

	// Set error for metrics query
	mockClient.QueryErr = errors.New("query execution failed")

	// Second call should fail on metrics query
	errs := scraper.ScrapePdbMetrics(ctx)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "query execution failed")
}

func TestScrapePdbMetrics_MultipleMetrics(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 1},
		PDBSysMetricsList: []models.PDBSysMetric{
			{InstID: 1, PDBName: "PDB1", MetricName: "Active Parallel Sessions", Value: 5.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "Active Serial Sessions", Value: 15.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "Average Active Sessions", Value: 20.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "CPU Usage Per Sec", Value: 75.5},
			{InstID: 1, PDBName: "PDB1", MetricName: "Database CPU Time Ratio", Value: 85.2},
			{InstID: 2, PDBName: "PDB2", MetricName: "Session Count", Value: 30.0},
			{InstID: 2, PDBName: "PDB2", MetricName: "User Transaction Per Sec", Value: 12.5},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapePdbMetrics(ctx)

	assert.Nil(t, errs)
	assert.True(t, scraper.isCDBSupported())
}

func TestScrapePdbMetrics_EmptyMetricsList(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability:     &models.CDBCapability{IsCDB: 1},
		PDBSysMetricsList: []models.PDBSysMetric{},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	errs := scraper.ScrapePdbMetrics(ctx)

	assert.Nil(t, errs)
	assert.True(t, scraper.isCDBSupported())
}

// Tests for scrapePDBSysMetrics

func TestScrapePDBSysMetrics_Success(t *testing.T) {
	mockClient := &client.MockClient{
		PDBSysMetricsList: []models.PDBSysMetric{
			{InstID: 1, PDBName: "PDB1", MetricName: "Session Count", Value: 50.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "CPU Usage Per Sec", Value: 60.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	now := pcommon.Timestamp(0)
	errs := scraper.scrapePDBSysMetrics(ctx, now)

	assert.Nil(t, errs)
}

func TestScrapePDBSysMetrics_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("network timeout"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	now := pcommon.Timestamp(0)
	errs := scraper.scrapePDBSysMetrics(ctx, now)

	assert.NotNil(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "network timeout")
}

func TestScrapePDBSysMetrics_UnknownMetrics(t *testing.T) {
	mockClient := &client.MockClient{
		PDBSysMetricsList: []models.PDBSysMetric{
			{InstID: 1, PDBName: "PDB1", MetricName: "Unknown Metric 1", Value: 10.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "Session Count", Value: 20.0},
			{InstID: 1, PDBName: "PDB1", MetricName: "Unknown Metric 2", Value: 30.0},
		},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	now := pcommon.Timestamp(0)
	errs := scraper.scrapePDBSysMetrics(ctx, now)

	assert.Nil(t, errs)
}

// Tests for recordMetric

func TestRecordMetric_KnownMetric(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	scraper.recordMetric(0, "Session Count", 100.0, "1", "TEST_PDB")
}

func TestRecordMetric_UnknownMetricLogged(t *testing.T) {
	mockClient := &client.MockClient{}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	scraper.recordMetric(0, "NonExistent Metric Name", 999.0, "1", "TEST_PDB")
}

// Tests for checkCDBCapability

func TestCheckCDBCapability_FirstCall_Success(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 1},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	err := scraper.checkCDBCapability(ctx)

	assert.Nil(t, err)
	assert.True(t, scraper.environmentChecked)
	assert.NotNil(t, scraper.isCDBCapable)
	assert.True(t, *scraper.isCDBCapable)
}

func TestCheckCDBCapability_FirstCall_NotCDB(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 0},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	err := scraper.checkCDBCapability(ctx)

	assert.Nil(t, err)
	assert.True(t, scraper.environmentChecked)
	assert.NotNil(t, scraper.isCDBCapable)
	assert.False(t, *scraper.isCDBCapable)
}

func TestCheckCDBCapability_QueryError(t *testing.T) {
	mockClient := &client.MockClient{
		QueryErr: errors.New("connection refused"),
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()
	err := scraper.checkCDBCapability(ctx)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.False(t, scraper.environmentChecked)
	assert.Nil(t, scraper.isCDBCapable)
}

func TestCheckCDBCapability_AlreadyChecked(t *testing.T) {
	mockClient := &client.MockClient{
		CDBCapability: &models.CDBCapability{IsCDB: 1},
	}
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	scraper := NewPdbScraper(mockClient, mb, logger, metadata.DefaultMetricsBuilderConfig())

	ctx := context.Background()

	// First check
	err1 := scraper.checkCDBCapability(ctx)
	assert.Nil(t, err1)
	assert.True(t, scraper.environmentChecked)

	// Set error to verify it's not called again
	mockClient.QueryErr = errors.New("should not be called")

	// Second check should skip query
	err2 := scraper.checkCDBCapability(ctx)
	assert.Nil(t, err2)
	assert.True(t, scraper.environmentChecked)
}

// Tests for containsORACode

func TestContainsORACode_SingleCode_Found(t *testing.T) {
	err := errors.New("ORA-00942: table or view does not exist")
	assert.True(t, containsORACode(err, "ORA-00942"))
}

func TestContainsORACode_SingleCode_NotFound(t *testing.T) {
	err := errors.New("ORA-00942: table or view does not exist")
	assert.False(t, containsORACode(err, "ORA-01722"))
}

func TestContainsORACode_MultipleCodes_FirstMatch(t *testing.T) {
	err := errors.New("ORA-00942: table or view does not exist")
	assert.True(t, containsORACode(err, "ORA-00942", "ORA-01722", "ORA-00904"))
}

func TestContainsORACode_MultipleCodes_SecondMatch(t *testing.T) {
	err := errors.New("ORA-01722: invalid number")
	assert.True(t, containsORACode(err, "ORA-00942", "ORA-01722", "ORA-00904"))
}

func TestContainsORACode_MultipleCodes_NoMatch(t *testing.T) {
	err := errors.New("network connection lost")
	assert.False(t, containsORACode(err, "ORA-00942", "ORA-01722", "ORA-00904"))
}

func TestContainsORACode_EmptyErrorMessage(t *testing.T) {
	err := errors.New("")
	assert.False(t, containsORACode(err, "ORA-00942"))
}

func TestContainsORACode_PartialMatch(t *testing.T) {
	err := errors.New("Error: ORA-009 something")
	assert.False(t, containsORACode(err, "ORA-00942"))
	assert.True(t, containsORACode(err, "ORA-009"))
}
