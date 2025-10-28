// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewExecutionPlanScraper(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	logger := zap.NewNop()
	instanceName := "test-instance"
	metricsConfig := metadata.DefaultMetricsBuilderConfig()

	scraper := NewExecutionPlanScraper(db, mb, logger, instanceName, metricsConfig)

	assert.NotNil(t, scraper)
	assert.Equal(t, db, scraper.db)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, instanceName, scraper.instanceName)
	assert.Equal(t, metricsConfig, scraper.metricsBuilderConfig)
}

func TestScrapeExecutionPlans_EmptySQLIDs(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{})

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlans_ContextCancelled(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sqlIDs := []string{"sql_id_1"}

	errs := scraper.ScrapeExecutionPlans(ctx, sqlIDs)

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "cancelled")
}

func TestBuildExecutionPlanMetrics_Enabled(t *testing.T) {
	db := &sql.DB{}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", config)

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
	db := &sql.DB{}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = false
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", config)

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test_sql_id_123
Child Number: 0
| Id | Operation | Name |
|  0 | SELECT    | TEST |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_WithMultiplePlans(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := ""

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, "", trimmed)
}

func TestTrimExecutionPlanText_OnlyPlanHashValue(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := "Plan hash value: 123456"

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, "Plan hash value: 123456", trimmed)
}

func TestTrimExecutionPlanText_WithWhitespace(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `Plan hash value: 123456
----------------------------
| Id | Operation | Name |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_CaseInsensitivityCheck(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test
PLAN HASH VALUE: 123
| Operation |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_PartialMarker(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test
Plan hash: 123456
| Operation |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Equal(t, planText, trimmed)
}

func TestTrimExecutionPlanText_MarkerInMiddle(t *testing.T) {
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	planText := `SQL_ID test
Plan hash value: 123456

| Id | Operation |

|  0 | SELECT    |`

	trimmed := scraper.trimExecutionPlanText(planText)

	assert.Contains(t, trimmed, "\n\n")
	assert.Contains(t, trimmed, "Plan hash value: 123456")
}

func TestBuildExecutionPlanMetrics_WithInvalidPlan(t *testing.T) {
	db := &sql.DB{}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", config)

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
	db := &sql.DB{}

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", config)

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
	db := &sql.DB{}

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(db, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

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

func TestScrapeExecutionPlans_NilDB(t *testing.T) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(nil, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	require.Nil(t, scraper.db)

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{})
	assert.Empty(t, errs)
}

func TestNewExecutionPlanScraper_AllFieldsSet(t *testing.T) {
	db := &sql.DB{}
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	logger := zap.NewNop()
	instanceName := "oracle-prod-01"
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = false

	scraper := NewExecutionPlanScraper(db, mb, logger, instanceName, config)

	assert.NotNil(t, scraper)
	assert.Equal(t, db, scraper.db)
	assert.Equal(t, mb, scraper.mb)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, "oracle-prod-01", scraper.instanceName)
	assert.Equal(t, config, scraper.metricsBuilderConfig)
	assert.False(t, scraper.metricsBuilderConfig.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled)
}
