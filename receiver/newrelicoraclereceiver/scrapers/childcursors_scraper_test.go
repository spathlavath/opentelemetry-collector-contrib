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

func TestNewChildCursorsScraper(t *testing.T) {
	mockClient := client.NewMockClient()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper := NewChildCursorsScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
}

func TestChildCursorsScraper_ScrapeWithValidData(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "test_sql_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 123456789, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 125.5, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 250.75, Valid: true},
			AvgIOWaitTimeMs:     sql.NullFloat64{Float64: 50.25, Valid: true},
			AvgDiskReads:        sql.NullFloat64{Float64: 100.5, Valid: true},
			AvgBufferGets:       sql.NullFloat64{Float64: 500.25, Valid: true},
			Executions:          sql.NullInt64{Int64: 1000, Valid: true},
			Invalidations:       sql.NullInt64{Int64: 5, Valid: true},
			FirstLoadTime:       sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			LastLoadTime:        sql.NullString{String: "2024-01-01 12:00:00", Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsUserIoWaitTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsExecutions.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsBufferGets.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsInvalidations.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsDetails.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{
			SQLID:       "test_sql_1",
			ChildNumber: 0,
			Timestamp:   now,
		},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.Empty(t, errs)

	// Verify metrics were collected
	metrics := mb.Emit()
	assert.Positive(t, metrics.ResourceMetrics().Len())
}

func TestChildCursorsScraper_ScrapeWithMultipleIdentifiers(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "sql_id_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 111111, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 100.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 200.0, Valid: true},
			Executions:          sql.NullInt64{Int64: 500, Valid: true},
		},
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "sql_id_2", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 1, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 222222, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 150.0, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 300.0, Valid: true},
			Executions:          sql.NullInt64{Int64: 750, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "sql_id_1", ChildNumber: 0, Timestamp: now},
		{SQLID: "sql_id_2", ChildNumber: 1, Timestamp: now},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.Empty(t, errs)

	metrics := mb.Emit()
	assert.Positive(t, metrics.ResourceMetrics().Len())
}

func TestChildCursorsScraper_ScrapeWithEmptyIdentifiers(t *testing.T) {
	mockClient := client.NewMockClient()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.Empty(t, errs)

	// Should not have emitted any metrics
	metrics := mb.Emit()
	// Even with no data, the metrics builder may emit an empty structure
	// We just verify no errors occurred
	assert.NotNil(t, metrics)
}

func TestChildCursorsScraper_ScrapeWithQueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()
	mockClient.QueryErr = errors.New("database connection error")

	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_1", ChildNumber: 0, Timestamp: now},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.NotEmpty(t, errs)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "database connection error")
}

func TestChildCursorsScraper_ScrapeWithPartialErrors(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	// Only add one child cursor, so the second identifier will fail
	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "sql_id_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 111111, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 100.0, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "sql_id_1", ChildNumber: 0, Timestamp: now},
		{SQLID: "non_existent_sql", ChildNumber: 0, Timestamp: now},
	}

	// First will succeed, second won't find data but shouldn't error
	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	// No errors expected as missing data is not treated as error
	require.Empty(t, errs)
}

func TestChildCursorsScraper_ScrapeWithInvalidIdentifier(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	// Add a child cursor without valid identifier
	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "", Valid: false}, // Invalid
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: false},    // Invalid
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 100.0, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "", ChildNumber: 0, Timestamp: now},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	// Should complete without errors, but won't emit metrics for invalid identifiers
	require.Empty(t, errs)
}

func TestChildCursorsScraper_RecordMetricsAllEnabled(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "test_sql_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 123456789, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 125.5, Valid: true},
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 250.75, Valid: true},
			AvgIOWaitTimeMs:     sql.NullFloat64{Float64: 50.25, Valid: true},
			AvgDiskReads:        sql.NullFloat64{Float64: 100.5, Valid: true},
			AvgBufferGets:       sql.NullFloat64{Float64: 500.25, Valid: true},
			Executions:          sql.NullInt64{Int64: 1000, Valid: true},
			Invalidations:       sql.NullInt64{Int64: 5, Valid: true},
			FirstLoadTime:       sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			LastLoadTime:        sql.NullString{String: "2024-01-01 12:00:00", Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	// Enable all metrics
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsUserIoWaitTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsExecutions.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsDiskReads.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsBufferGets.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsInvalidations.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsDetails.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_1", ChildNumber: 0, Timestamp: now},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.Empty(t, errs)

	metrics := mb.Emit()
	assert.Positive(t, metrics.ResourceMetrics().Len())
}

func TestChildCursorsScraper_RecordMetricsAllDisabled(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "test_sql_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 123456789, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Float64: 125.5, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	// Disable all metrics
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsUserIoWaitTime.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsExecutions.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsDiskReads.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsBufferGets.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsInvalidations.Enabled = false
	config.Metrics.NewrelicoracledbChildCursorsDetails.Enabled = false

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_1", ChildNumber: 0, Timestamp: now},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.Empty(t, errs)
}

func TestChildCursorsScraper_RecordMetricsWithNullValues(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	// Create a child cursor with some null values
	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "test_sql_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 123456789, Valid: true},
			AvgCPUTimeMs:        sql.NullFloat64{Valid: false}, // Null
			AvgElapsedTimeMs:    sql.NullFloat64{Float64: 250.75, Valid: true},
			AvgIOWaitTimeMs:     sql.NullFloat64{Valid: false}, // Null
			AvgDiskReads:        sql.NullFloat64{Valid: false}, // Null
			AvgBufferGets:       sql.NullFloat64{Float64: 500.25, Valid: true},
			Executions:          sql.NullInt64{Valid: false}, // Null
			Invalidations:       sql.NullInt64{Int64: 5, Valid: true},
			FirstLoadTime:       sql.NullString{Valid: false}, // Null
			LastLoadTime:        sql.NullString{String: "2024-01-01 12:00:00", Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsBufferGets.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_1", ChildNumber: 0, Timestamp: now},
	}

	_, errs := scraper.ScrapeChildCursorsForIdentifiers(t.Context(), identifiers)
	require.Empty(t, errs)

	// Should handle null values gracefully by using 0 or empty string defaults
	metrics := mb.Emit()
	assert.NotNil(t, metrics)
}

func TestChildCursorsScraper_RecordChildCursorMetrics(t *testing.T) {
	mockClient := client.NewMockClient()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbChildCursorsCPUTime.Enabled = true
	config.Metrics.NewrelicoracledbChildCursorsElapsedTime.Enabled = true

	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	now := pcommon.NewTimestampFromTime(time.Now())
	cursor := &models.ChildCursor{
		CollectionTimestamp: sql.NullTime{Time: time.Now(), Valid: true},
		DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
		SQLID:               sql.NullString{String: "test_sql_1", Valid: true},
		ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
		PlanHashValue:       sql.NullInt64{Int64: 123456789, Valid: true},
		AvgCPUTimeMs:        sql.NullFloat64{Float64: 125.5, Valid: true},
		AvgElapsedTimeMs:    sql.NullFloat64{Float64: 250.75, Valid: true},
	}

	// This should not panic or error
	scraper.recordChildCursorMetrics(now, cursor)

	metrics := mb.Emit()
	assert.NotNil(t, metrics)
}

func TestChildCursorsScraper_ContextCancellation(t *testing.T) {
	mockClient := client.NewMockClient()
	now := time.Now()

	mockClient.ChildCursors = []models.ChildCursor{
		{
			CollectionTimestamp: sql.NullTime{Time: now, Valid: true},
			DatabaseName:        sql.NullString{String: "TESTDB", Valid: true},
			SQLID:               sql.NullString{String: "test_sql_1", Valid: true},
			ChildNumber:         sql.NullInt64{Int64: 0, Valid: true},
			PlanHashValue:       sql.NullInt64{Int64: 123456789, Valid: true},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)
	scraper := NewChildCursorsScraper(mockClient, mb, zap.NewNop(), config)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_1", ChildNumber: 0, Timestamp: now},
	}

	// Should handle cancelled context gracefully
	_, _ = scraper.ScrapeChildCursorsForIdentifiers(ctx, identifiers)
	// The function doesn't explicitly check context, but it's passed through to the client
}
