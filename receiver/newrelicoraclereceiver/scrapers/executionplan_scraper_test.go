// Copyright The OpenTelemetry Authors
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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewExecutionPlanScraper(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
}

func TestScrapeExecutionPlans_EmptyIdentifiers(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	errs := scraper.ScrapeExecutionPlans(ctx, []models.SQLIdentifier{})

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_Success(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlanRows = []models.ExecutionPlanRow{
		{
			SQLID:         sql.NullString{String: "test_sql_id", Valid: true},
			PlanHashValue: sql.NullInt64{Int64: 12345, Valid: true},
			ChildNumber:   sql.NullInt64{Int64: 0, Valid: true},
			ID:            sql.NullInt64{Int64: 0, Valid: true},
			ParentID:      sql.NullInt64{Int64: -1, Valid: true},
			Depth:         sql.NullInt64{Int64: 0, Valid: true},
			Operation:     sql.NullString{String: "SELECT STATEMENT", Valid: true},
			Options:       sql.NullString{String: "", Valid: true},
			ObjectOwner:   sql.NullString{String: "SYS", Valid: true},
			ObjectName:    sql.NullString{String: "DUAL", Valid: true},
			Position:      sql.NullInt64{Int64: 1, Valid: true},
			Cost:          sql.NullString{String: "2", Valid: true},
			Cardinality:   sql.NullString{String: "1", Valid: true},
			Bytes:         sql.NullString{String: "100", Valid: true},
			CPUCost:       sql.NullString{String: "100", Valid: true},
			IOCost:        sql.NullString{String: "2", Valid: true},
			Timestamp:     sql.NullString{String: "2024-01-01 12:00:00", Valid: true},
		},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{
			SQLID:       "test_sql_id",
			ChildNumber: 0,
			Timestamp:   time.Now(),
		},
	}

	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("database error")

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{
			SQLID:       "test_sql_id",
			ChildNumber: 0,
			Timestamp:   time.Now(),
		},
	}

	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "failed to query execution plan")
}

func TestScrapeExecutionPlans_ContextTimeout(t *testing.T) {
	mockClient := client.NewMockClient()

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	// Create already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	identifiers := []models.SQLIdentifier{
		{
			SQLID:       "test_sql_id",
			ChildNumber: 0,
			Timestamp:   time.Now(),
		},
	}

	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "context cancelled")
}

func TestScrapeExecutionPlans_MultipleIdentifiers(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlanRows = []models.ExecutionPlanRow{
		{
			SQLID:       sql.NullString{String: "sql_1", Valid: true},
			ChildNumber: sql.NullInt64{Int64: 0, Valid: true},
			Operation:   sql.NullString{String: "SELECT STATEMENT", Valid: true},
		},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{SQLID: "sql_1", ChildNumber: 0, Timestamp: time.Now()},
		{SQLID: "sql_2", ChildNumber: 1, Timestamp: time.Now()},
		{SQLID: "sql_3", ChildNumber: 0, Timestamp: time.Now()},
	}

	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_InvalidSQLID(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlanRows = []models.ExecutionPlanRow{
		{
			SQLID:       sql.NullString{String: "", Valid: false},
			ChildNumber: sql.NullInt64{Int64: 0, Valid: true},
		},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_id", ChildNumber: 0, Timestamp: time.Now()},
	}

	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)

	assert.Empty(t, errs)
}

func TestBuildExecutionPlanMetrics_EventDisabled(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = false
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	row := &models.ExecutionPlanRow{
		SQLID: sql.NullString{String: "test_sql_id", Valid: true},
	}

	err := scraper.buildExecutionPlanMetrics(row, time.Now())

	assert.NoError(t, err)
}

func TestBuildExecutionPlanMetrics_AllFields(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	row := &models.ExecutionPlanRow{
		SQLID:            sql.NullString{String: "test_sql_id", Valid: true},
		PlanHashValue:    sql.NullInt64{Int64: 12345, Valid: true},
		ChildNumber:      sql.NullInt64{Int64: 0, Valid: true},
		ID:               sql.NullInt64{Int64: 1, Valid: true},
		ParentID:         sql.NullInt64{Int64: 0, Valid: true},
		Depth:            sql.NullInt64{Int64: 1, Valid: true},
		Operation:        sql.NullString{String: "TABLE ACCESS", Valid: true},
		Options:          sql.NullString{String: "FULL", Valid: true},
		ObjectOwner:      sql.NullString{String: "SYS", Valid: true},
		ObjectName:       sql.NullString{String: "DUAL", Valid: true},
		Position:         sql.NullInt64{Int64: 1, Valid: true},
		Cost:             sql.NullString{String: "100", Valid: true},
		Cardinality:      sql.NullString{String: "1000", Valid: true},
		Bytes:            sql.NullString{String: "2000", Valid: true},
		CPUCost:          sql.NullString{String: "500", Valid: true},
		IOCost:           sql.NullString{String: "50", Valid: true},
		Timestamp:        sql.NullString{String: "2024-01-01 12:00:00", Valid: true},
		TempSpace:        sql.NullString{String: "1024", Valid: true},
		AccessPredicates: sql.NullString{String: "ID=123", Valid: true},
		Projection:       sql.NullString{String: "COL1, COL2", Valid: true},
		Time:             sql.NullString{String: "1000", Valid: true},
		FilterPredicates: sql.NullString{String: "STATUS='ACTIVE'", Valid: true},
	}

	err := scraper.buildExecutionPlanMetrics(row, time.Now())

	assert.NoError(t, err)
}

func TestBuildExecutionPlanMetrics_NullFields(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	row := &models.ExecutionPlanRow{
		SQLID:         sql.NullString{String: "test_sql_id", Valid: true},
		PlanHashValue: sql.NullInt64{Valid: false},
		ChildNumber:   sql.NullInt64{Valid: false},
		ID:            sql.NullInt64{Valid: false},
		ParentID:      sql.NullInt64{Valid: false},
		Depth:         sql.NullInt64{Valid: false},
		Operation:     sql.NullString{Valid: false},
		Options:       sql.NullString{Valid: false},
		ObjectOwner:   sql.NullString{Valid: false},
		ObjectName:    sql.NullString{Valid: false},
		Position:      sql.NullInt64{Valid: false},
		Cost:          sql.NullString{Valid: false},
		Cardinality:   sql.NullString{Valid: false},
		Bytes:         sql.NullString{Valid: false},
		CPUCost:       sql.NullString{Valid: false},
		IOCost:        sql.NullString{Valid: false},
		Timestamp:     sql.NullString{Valid: false},
		TempSpace:     sql.NullString{Valid: false},
	}

	err := scraper.buildExecutionPlanMetrics(row, time.Now())

	assert.NoError(t, err)
}

func TestBuildExecutionPlanMetrics_EmptyStrings(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	row := &models.ExecutionPlanRow{
		SQLID:       sql.NullString{String: "test_sql_id", Valid: true},
		Cost:        sql.NullString{String: "", Valid: true},
		Cardinality: sql.NullString{String: "", Valid: true},
		Bytes:       sql.NullString{String: "", Valid: true},
		CPUCost:     sql.NullString{String: "", Valid: true},
		IOCost:      sql.NullString{String: "", Valid: true},
		TempSpace:   sql.NullString{String: "", Valid: true},
		Time:        sql.NullString{String: "", Valid: true},
	}

	err := scraper.buildExecutionPlanMetrics(row, time.Now())

	assert.NoError(t, err)
}

func TestParseIntSafe_ValidNumbers(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"positive number", "12345", 12345},
		{"zero", "0", 0},
		{"negative number", "-100", -100},
		{"large number", "9223372036854775807", 9223372036854775807},
		{"empty string", "", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scraper.parseIntSafe(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseIntSafe_InvalidNumbers(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	tests := []struct {
		name  string
		input string
	}{
		{"invalid format", "abc123"},
		{"too large", "18446744073709551615"},
		{"decimal", "123.45"},
		{"special chars", "12#45"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scraper.parseIntSafe(tt.input)
			// Should return -1 for unparseable values
			assert.Equal(t, int64(-1), result)
		})
	}
}

func TestScrapeExecutionPlans_PartialFailure(t *testing.T) {
	mockClient := client.NewMockClient()

	// Set up mock to return one row for the first query
	mockClient.ExecutionPlanRows = []models.ExecutionPlanRow{
		{
			SQLID:       sql.NullString{String: "sql_1", Valid: true},
			ChildNumber: sql.NullInt64{Int64: 0, Valid: true},
			Operation:   sql.NullString{String: "SELECT STATEMENT", Valid: true},
		},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{SQLID: "sql_1", ChildNumber: 0, Timestamp: time.Now()},
		{SQLID: "sql_2", ChildNumber: 1, Timestamp: time.Now()},
	}

	// First call to query will succeed, but only return data for sql_1
	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)

	// Should have no errors since query succeeded for both
	assert.Empty(t, errs)
}

func TestBuildExecutionPlanMetrics_WithPredicates(t *testing.T) {
	mockClient := client.NewMockClient()
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)

	row := &models.ExecutionPlanRow{
		SQLID:            sql.NullString{String: "test_sql_id", Valid: true},
		AccessPredicates: sql.NullString{String: "USER_ID = 12345 AND STATUS = 'ACTIVE'", Valid: true},
		FilterPredicates: sql.NullString{String: "CREATED_DATE > TO_DATE('2024-01-01')", Valid: true},
	}

	err := scraper.buildExecutionPlanMetrics(row, time.Now())

	assert.NoError(t, err)
}

func TestScrapeExecutionPlans_CachedPlans(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlanRows = []models.ExecutionPlanRow{
		{
			SQLID:       sql.NullString{String: "test_sql_id", Valid: true},
			ChildNumber: sql.NullInt64{Int64: 0, Valid: true},
			Operation:   sql.NullString{String: "SELECT STATEMENT", Valid: true},
		},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_id", ChildNumber: 0, PlanHash: "12345", Timestamp: time.Now()},
	}

	// First scrape - should add to cache
	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)
	assert.Empty(t, errs)

	// Second scrape - should use cache and skip
	errs = scraper.ScrapeExecutionPlans(ctx, identifiers)
	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_BuildMetricsError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlanRows = []models.ExecutionPlanRow{
		{
			SQLID:       sql.NullString{String: "test_sql_id", Valid: true},
			ChildNumber: sql.NullInt64{Int64: 0, Valid: true},
		},
	}

	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()
	// Disable the metric to cause buildExecutionPlanMetrics to not record
	config.Metrics.NewrelicoracledbExecutionPlan.Enabled = false
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, config)
	ctx := context.Background()

	identifiers := []models.SQLIdentifier{
		{SQLID: "test_sql_id", ChildNumber: 0, PlanHash: "12345", Timestamp: time.Now()},
	}

	errs := scraper.ScrapeExecutionPlans(ctx, identifiers)
	assert.Empty(t, errs) // Should succeed but not record metrics
}
