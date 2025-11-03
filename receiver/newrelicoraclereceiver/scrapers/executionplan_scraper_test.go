// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
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

func TestNewExecutionPlanScraperV2(t *testing.T) {
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

func TestScrapeExecutionPlansV2_EmptySQLIDs(t *testing.T) {
	mockClient := client.NewMockClient()
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{})

	assert.Empty(t, errs)
}

func TestScrapeExecutionPlansV2_WithValidData(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.ExecutionPlans["'test_sql_id'"] = []models.ExecutionPlan{
		{
			SQLID:         "test_sql_id",
			ChildNumber:   0,
			PlanHashValue: 12345,
			PlanTree: &models.PlanNode{
				ID:            0,
				Operation:     "SELECT STATEMENT",
				PlanHashValue: 12345,
				Children:      []*models.PlanNode{},
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

func TestScrapeExecutionPlansV2_QueryError(t *testing.T) {
	mockClient := client.NewMockClient()
	mockClient.QueryErr = errors.New("database connection failed")

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	errs := scraper.ScrapeExecutionPlans(context.Background(), []string{"test_sql_id"})

	assert.NotEmpty(t, errs)
	assert.Contains(t, errs[0].Error(), "database connection failed")
}

func TestScrapeExecutionPlansV2_ContextCancelled(t *testing.T) {
	mockClient := client.NewMockClient()

	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", metadata.DefaultMetricsBuilderConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	sqlIDs := []string{"sql_id_1"}

	errs := scraper.ScrapeExecutionPlans(ctx, sqlIDs)

	assert.NotEmpty(t, errs)
}

func TestBuildExecutionPlanMetricsV2_Enabled(t *testing.T) {
	mockClient := client.NewMockClient()

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = true
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	plan := &models.ExecutionPlan{
		SQLID:         "test_query_id",
		ChildNumber:   0,
		PlanHashValue: 123456,
		PlanTree: &models.PlanNode{
			ID:            0,
			Operation:     "SELECT STATEMENT",
			PlanHashValue: 123456,
			Children:      []*models.PlanNode{},
		},
	}

	err := scraper.buildExecutionPlanMetrics(plan)
	assert.NoError(t, err)

	metrics := mb.Emit()
	assert.Greater(t, metrics.ResourceMetrics().Len(), 0)
}

func TestBuildExecutionPlanMetricsV2_Disabled(t *testing.T) {
	mockClient := client.NewMockClient()

	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbExecutionPlanInfo.Enabled = false
	mb := metadata.NewMetricsBuilder(config, receivertest.NewNopSettings(metadata.Type))
	scraper := NewExecutionPlanScraper(mockClient, mb, zap.NewNop(), "test", config)

	plan := &models.ExecutionPlan{
		SQLID:         "test_query_id",
		ChildNumber:   0,
		PlanHashValue: 123456,
		PlanTree: &models.PlanNode{
			ID:            0,
			Operation:     "SELECT STATEMENT",
			PlanHashValue: 123456,
			Children:      []*models.PlanNode{},
		},
	}

	err := scraper.buildExecutionPlanMetrics(plan)
	assert.NoError(t, err)

	metrics := mb.Emit()
	assert.Equal(t, 0, metrics.ResourceMetrics().Len())
}
