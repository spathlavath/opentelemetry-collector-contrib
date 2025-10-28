// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewExecutionPlanScraper(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	logger := zap.NewNop()
	instanceName := "test-instance"
	metricsConfig := metadata.DefaultMetricsBuilderConfig()

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, instanceName, metricsConfig)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, metricsConfig, scraper.metricsBuilderConfig)
}

func TestScrapeExecutionPlans_EmptySQLIDs(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{})

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_WithValidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlans["test_sql_id"] = []models.ExecutionPlan{
		{
			DatabaseName:  sql.NullString{String: "TESTDB", Valid: true},
			QueryID:       sql.NullString{String: "test_sql_id", Valid: true},
			PlanHashValue: sql.NullInt64{Int64: 12345, Valid: true},
			ExecutionPlanText: sql.NullString{
				String: "Plan hash value: 12345\nSELECT * FROM table",
				Valid:  true,
			},
		},
	}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{"test_sql_id"})

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("database connection failed")

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{"test_sql_id"})

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "database connection failed")
}

func TestScrapeExecutionPlans_ContextCancelled(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sqlIDs := []string{"sql_id_1"}

	errs := scraper.ScrapeExecutionPlans(ctx, sqlIDs)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "cancelled")
}

func TestBuildExecutionPlanMetrics_Enabled(t *testing.T) {
	mockClient := client.NewMockClient()

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	plan := &models.ExecutionPlan{
		DatabaseName:      sql.NullString{String: "TESTDB", Valid: true},
		QueryID:           sql.NullString{String: "test_query_id", Valid: true},
		PlanHashValue:     sql.NullInt64{Int64: 123456, Valid: true},
		ExecutionPlanText: sql.NullString{String: "Plan hash value: 123456\n| Id | Operation |", Valid: true},
	}

	scraper.buildExecutionPlanMetrics(plan)

	metrics := mb.Emit()
	assert.Greater(t, metrics.ResourceMetrics().Len(), 0)
}

func TestBuildExecutionPlanMetrics_Disabled(t *testing.T) {
	mockClient := client.NewMockClient()

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = false
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	plan := &models.ExecutionPlan{
		DatabaseName:      sql.NullString{String: "TESTDB", Valid: true},
		QueryID:           sql.NullString{String: "test_query_id", Valid: true},
		PlanHashValue:     sql.NullInt64{Int64: 123456, Valid: true},
		ExecutionPlanText: sql.NullString{String: "Plan hash value: 123456", Valid: true},
	}

	scraper.buildExecutionPlanMetrics(plan)

	metrics := mb.Emit()
	assert.Equal(t, 0, metrics.ResourceMetrics().Len())
}

func TestTrimExecutionPlanText_WithPlanHashValue(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test_sql_id_123
Child Number: 0
Plan hash value: 123456
----------------------------
| Id | Operation | Name |
|  0 | SELECT    | TEST |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "Plan hash value: 123456")
	assert.NotContains(t, trimmed, "SQL_ID test_sql_id_123")
	assert.Contains(t, trimmed, "| Id | Operation | Name |")
}

func TestTrimExecutionPlanText_WithoutPlanHashValue(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test_sql_id_123
Child Number: 0
| Id | Operation | Name |
|  0 | SELECT    | TEST |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_WithMultiplePlans(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `Plan hash value: 123456
----------------------------
| Id | Operation | Name |
|  0 | SELECT    | TEST |

SQL_ID another_sql_id
Plan hash value: 789012
| Id | Operation | Name |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "Plan hash value: 123456")
	assert.NotContains(t, trimmed, "SQL_ID another_sql_id")
	assert.NotContains(t, trimmed, "Plan hash value: 789012")
}

func TestTrimExecutionPlanText_EmptyString(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := ""

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, "", trimmed)
}

func TestTrimExecutionPlanText_OnlyPlanHashValue(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := "Plan hash value: 123456"

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, "Plan hash value: 123456", trimmed)
}

func TestTrimExecutionPlanText_WithWhitespace(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `   
	SQL_ID test_sql_id
	Plan hash value: 123456   
	| Id | Operation |   
	`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "Plan hash value: 123456")
	assert.True(t, len(trimmed) < len(planText))
}

func TestTrimExecutionPlanText_AtStartOfString(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `Plan hash value: 123456
----------------------------
| Id | Operation | Name |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_CaseInsensitivityCheck(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test
PLAN HASH VALUE: 123
| Operation |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_PartialMarker(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test
Plan hash: 123456
| Operation |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_MarkerInMiddle(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test_sql_id
Child Number: 0
Plan hash value: 123456
----------------------------
| Id | Operation |
|  0 | SELECT    |
More data here`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "Plan hash value: 123456")
	assert.NotContains(t, trimmed, "SQL_ID test_sql_id")
	assert.NotContains(t, trimmed, "Child Number: 0")
	assert.Contains(t, trimmed, "| Id | Operation |")
}

func TestTrimExecutionPlanText_SQLIDMarkerInPlan(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `Plan hash value: 123456
| Id | Operation |
|  0 | SELECT    |

SQL_ID second_query`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "Plan hash value: 123456")
	assert.Contains(t, trimmed, "| Id | Operation |")
	assert.NotContains(t, trimmed, "SQL_ID second_query")
}

func TestTrimExecutionPlanText_NewlinesPreserved(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test
Plan hash value: 123456

| Id | Operation |

|  0 | SELECT    |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "\n\n")
	assert.Contains(t, trimmed, "Plan hash value: 123456")
}

func TestBuildExecutionPlanMetrics_WithInvalidPlan(t *testing.T) {
	mockClient := client.NewMockClient()

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	plan := &models.ExecutionPlan{
		DatabaseName:      sql.NullString{String: "TESTDB", Valid: true},
		QueryID:           sql.NullString{Valid: false},
		PlanHashValue:     sql.NullInt64{Valid: false},
		ExecutionPlanText: sql.NullString{Valid: false},
	}

	initialMetrics := mb.Emit()
	initialCount := initialMetrics.ResourceMetrics().Len()

	scraper.buildExecutionPlanMetrics(plan)

	metrics := mb.Emit()
	assert.Equal(t, initialCount, metrics.ResourceMetrics().Len())
}

func TestBuildExecutionPlanMetrics_WithEmptyPlanText(t *testing.T) {
	mockClient := client.NewMockClient()

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	plan := &models.ExecutionPlan{
		DatabaseName:      sql.NullString{String: "TESTDB", Valid: true},
		QueryID:           sql.NullString{String: "test_query", Valid: true},
		PlanHashValue:     sql.NullInt64{Int64: 123456, Valid: true},
		ExecutionPlanText: sql.NullString{String: "", Valid: true},
	}

	initialMetrics := mb.Emit()
	initialCount := initialMetrics.ResourceMetrics().Len()

	scraper.buildExecutionPlanMetrics(plan)

	metrics := mb.Emit()
	assert.Equal(t, initialCount, metrics.ResourceMetrics().Len())
}

func TestTrimExecutionPlanText_LongPlan(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID long_sql_id
Plan hash value: 999999
----------------------------
| Id | Operation        | Name  |
|  0 | SELECT STATEMENT |       |
|  1 |  NESTED LOOPS    |       |
|  2 |   TABLE ACCESS   | TAB1  |
|  3 |   INDEX RANGE    | IDX1  |
...many more lines...`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "Plan hash value: 999999")
	assert.NotContains(t, trimmed, "SQL_ID long_sql_id")
	assert.Contains(t, trimmed, "| Id | Operation")
	assert.Contains(t, trimmed, "...many more lines...")
}

// TestTrimExecutionPlanText_TableDriven uses table-driven approach for comprehensive coverage
func TestTrimExecutionPlanText_TableDriven(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	tests := []struct {
		name                string
		input               string
		expectedContains    []string
		expectedNotContains []string
		expectedLength      int  // -1 means don't check
		shouldBeTrimmed     bool // Should output be shorter than input
	}{
		{
			name:                "standard_plan_with_marker",
			input:               "SQL_ID test\nPlan hash value: 123\n| Operation |",
			expectedContains:    []string{"Plan hash value: 123", "| Operation |"},
			expectedNotContains: []string{"SQL_ID test"},
			expectedLength:      -1,
			shouldBeTrimmed:     true,
		},
		{
			name:                "no_marker_returns_original",
			input:               "SQL_ID test\n| Operation |\nNo plan here",
			expectedContains:    []string{"SQL_ID test", "| Operation |", "No plan here"},
			expectedNotContains: []string{},
			expectedLength:      -1,
			shouldBeTrimmed:     false,
		},
		{
			name:                "empty_string",
			input:               "",
			expectedContains:    []string{},
			expectedNotContains: []string{},
			expectedLength:      0,
			shouldBeTrimmed:     false,
		},
		{
			name:                "marker_at_beginning",
			input:               "Plan hash value: 456\n| Id | Operation |",
			expectedContains:    []string{"Plan hash value: 456", "| Id | Operation |"},
			expectedNotContains: []string{},
			expectedLength:      -1,
			shouldBeTrimmed:     false, // Same length since marker is at start
		},
		{
			name: "multiple_plans_stops_at_next_sql_id",
			input: strings.Join([]string{
				"Plan hash value: 111",
				"| Operation |",
				"",
				"SQL_ID next_query",
				"Plan hash value: 222",
			}, "\n"),
			expectedContains:    []string{"Plan hash value: 111", "| Operation |"},
			expectedNotContains: []string{"SQL_ID next_query", "Plan hash value: 222"},
			expectedLength:      -1,
			shouldBeTrimmed:     true,
		},
		{
			name:                "whitespace_handling",
			input:               "   \n  SQL_ID test\n  Plan hash value: 789   \n  | Op |  \n  ",
			expectedContains:    []string{"Plan hash value: 789", "| Op |"},
			expectedNotContains: []string{"SQL_ID test"},
			expectedLength:      -1,
			shouldBeTrimmed:     true,
		},
		{
			name:                "plan_with_special_characters",
			input:               "SQL_ID test@123\nPlan hash value: 999\n|*| Special |*|",
			expectedContains:    []string{"Plan hash value: 999", "|*| Special |*|"},
			expectedNotContains: []string{"SQL_ID test@123"},
			expectedLength:      -1,
			shouldBeTrimmed:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scraper.trimExecutionPlanText(tt.input)

			// Check expected contains
			for _, expected := range tt.expectedContains {
				assert.Contains(t, result, expected, "Result should contain: %s", expected)
			}

			// Check expected not contains
			for _, notExpected := range tt.expectedNotContains {
				assert.NotContains(t, result, notExpected, "Result should not contain: %s", notExpected)
			}

			// Check length if specified
			if tt.expectedLength >= 0 {
				assert.Equal(t, tt.expectedLength, len(result), "Result length mismatch")
			}

			// Check if trimmed
			if tt.shouldBeTrimmed {
				assert.Less(t, len(result), len(tt.input), "Result should be shorter than input")
			}

			// Ensure result is always trimmed of whitespace
			assert.Equal(t, strings.TrimSpace(result), result, "Result should not have leading/trailing whitespace")
		})
	}
}

// TestBuildExecutionPlanMetrics_TableDriven tests metric building with various plan states
func TestBuildExecutionPlanMetrics_TableDriven(t *testing.T) {
	tests := []struct {
		name           string
		metricsEnabled bool
		plan           *models.ExecutionPlan
		expectMetrics  bool
		description    string
	}{
		{
			name:           "valid_plan_metrics_enabled",
			metricsEnabled: true,
			plan: &models.ExecutionPlan{
				DatabaseName:      sql.NullString{String: "PROD_DB", Valid: true},
				QueryID:           sql.NullString{String: "query_123", Valid: true},
				PlanHashValue:     sql.NullInt64{Int64: 987654, Valid: true},
				ExecutionPlanText: sql.NullString{String: "Plan hash value: 987654\n| Full plan |", Valid: true},
			},
			expectMetrics: true,
			description:   "Should emit metrics for valid plan when enabled",
		},
		{
			name:           "valid_plan_metrics_disabled",
			metricsEnabled: false,
			plan: &models.ExecutionPlan{
				DatabaseName:      sql.NullString{String: "PROD_DB", Valid: true},
				QueryID:           sql.NullString{String: "query_123", Valid: true},
				PlanHashValue:     sql.NullInt64{Int64: 987654, Valid: true},
				ExecutionPlanText: sql.NullString{String: "Plan hash value: 987654", Valid: true},
			},
			expectMetrics: false,
			description:   "Should not emit metrics when disabled",
		},
		{
			name:           "invalid_plan_missing_query_id",
			metricsEnabled: true,
			plan: &models.ExecutionPlan{
				DatabaseName:      sql.NullString{String: "PROD_DB", Valid: true},
				QueryID:           sql.NullString{Valid: false},
				PlanHashValue:     sql.NullInt64{Int64: 123, Valid: true},
				ExecutionPlanText: sql.NullString{String: "Plan text", Valid: true},
			},
			expectMetrics: false,
			description:   "Should not emit metrics for invalid plan (no query ID)",
		},
		{
			name:           "invalid_plan_missing_hash_value",
			metricsEnabled: true,
			plan: &models.ExecutionPlan{
				DatabaseName:      sql.NullString{String: "PROD_DB", Valid: true},
				QueryID:           sql.NullString{String: "query_123", Valid: true},
				PlanHashValue:     sql.NullInt64{Valid: false},
				ExecutionPlanText: sql.NullString{String: "Plan text", Valid: true},
			},
			expectMetrics: false,
			description:   "Should not emit metrics for invalid plan (no hash value)",
		},
		{
			name:           "invalid_plan_empty_text",
			metricsEnabled: true,
			plan: &models.ExecutionPlan{
				DatabaseName:      sql.NullString{String: "PROD_DB", Valid: true},
				QueryID:           sql.NullString{String: "query_123", Valid: true},
				PlanHashValue:     sql.NullInt64{Int64: 123, Valid: true},
				ExecutionPlanText: sql.NullString{String: "", Valid: true},
			},
			expectMetrics: false,
			description:   "Should not emit metrics for plan with empty text",
		},
		{
			name:           "plan_with_null_database_name",
			metricsEnabled: true,
			plan: &models.ExecutionPlan{
				DatabaseName:      sql.NullString{Valid: false},
				QueryID:           sql.NullString{String: "query_123", Valid: true},
				PlanHashValue:     sql.NullInt64{Int64: 123, Valid: true},
				ExecutionPlanText: sql.NullString{String: "Plan text", Valid: true},
			},
			expectMetrics: true,
			description:   "Should emit metrics even with null database name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := client.NewMockClient()
			config := metadata.DefaultMetricsBuilderConfig()
			config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = tt.metricsEnabled
			mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
			scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

			// Get initial metric count
			initialMetrics := mb.Emit()
			initialCount := initialMetrics.ResourceMetrics().Len()

			// Build metrics
			scraper.buildExecutionPlanMetrics(tt.plan)

			// Get final metric count
			finalMetrics := mb.Emit()
			finalCount := finalMetrics.ResourceMetrics().Len()

			if tt.expectMetrics && tt.plan.IsValidForMetrics() {
				assert.Greater(t, finalCount, initialCount, tt.description)
			} else {
				assert.Equal(t, initialCount, finalCount, tt.description)
			}
		})
	}
}

func TestScrapeExecutionPlans_NilClient(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(nil, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	require.Nil(t, scraper.client)

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{})
	assert.Empty(t, errs)
}

func TestNewExecutionPlanScraper_AllFieldsSet(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	logger := zap.NewNop()
	instanceName := "oracle-prod-01"
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = false

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, instanceName, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockClient, scraper.client)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, "oracle-prod-01", scraper.instanceName)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.False(t, scraper.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled)
}

// TestScrapeExecutionPlans_TimeoutHandling tests timeout behavior
func TestScrapeExecutionPlans_TimeoutHandling(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	// Context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond) // Ensure timeout occurs

	sqlIDs := []string{"sql_id_1", "sql_id_2"}
	errs := scraper.ScrapeExecutionPlans(ctx, sqlIDs)

	// Should get cancellation error
	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "cancelled")
}

// TestScrapeExecutionPlans_LoggingBehavior tests logging output
func TestScrapeExecutionPlans_LoggingBehavior(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))

	// Create observable logger to capture logs
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, "test", metadata.DefaultMetricsBuilderConfig())

	// Test with empty SQL IDs
	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{})
	assert.Empty(t, errs)

	// No logs should be generated for empty input
	assert.Equal(t, 0, logs.Len())
}

// TestScrapeExecutionPlans_MultipleFailures tests accumulation of errors
func TestScrapeExecutionPlans_MultipleFailures(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))

	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	scraper := NewExecutionPlanScraper(mockClient, mb, logger, "test", metadata.DefaultMetricsBuilderConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Multiple SQL IDs should each fail
	sqlIDs := []string{"id1", "id2", "id3"}
	errs := scraper.ScrapeExecutionPlans(ctx, sqlIDs)

	// Should have captured warnings for failures
	assert.NotEmpty(t, errs)
	assert.Greater(t, logs.Len(), 0, "Should have logged warnings for failures")
}

// TestProcessSingleSQLID_ContextCancellation tests context handling
func TestProcessSingleSQLID_ContextCancellation(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before processing

	err := scraper.processSingleSQLID(ctx, "test_sql_id", 1)

	// Should fail due to cancelled context (when trying to query)
	assert.Error(t, err)
}
