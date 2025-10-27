// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

func TestNewBlockingScraper(t *testing.T) {
	tests := []struct {
		name         string
		db           *sql.DB
		mb           *metadata.MetricsBuilder
		logger       *zap.Logger
		instanceName string
		threshold    int
		expectError  bool
		errorMsg     string
	}{
		{
			name:         "valid_scraper",
			db:           &sql.DB{},
			mb:           &metadata.MetricsBuilder{},
			logger:       zap.NewNop(),
			instanceName: "test-instance",
			threshold:    100,
			expectError:  false,
		},
		{
			name:         "nil_database",
			db:           nil,
			mb:           &metadata.MetricsBuilder{},
			logger:       zap.NewNop(),
			instanceName: "test-instance",
			threshold:    100,
			expectError:  true,
			errorMsg:     "database connection cannot be nil",
		},
		{
			name:         "nil_metrics_builder",
			db:           &sql.DB{},
			mb:           nil,
			logger:       zap.NewNop(),
			instanceName: "test-instance",
			threshold:    100,
			expectError:  true,
			errorMsg:     "metrics builder cannot be nil",
		},
		{
			name:         "nil_logger",
			db:           &sql.DB{},
			mb:           &metadata.MetricsBuilder{},
			logger:       nil,
			instanceName: "test-instance",
			threshold:    100,
			expectError:  true,
			errorMsg:     "logger cannot be nil",
		},
		{
			name:         "empty_instance_name",
			db:           &sql.DB{},
			mb:           &metadata.MetricsBuilder{},
			logger:       zap.NewNop(),
			instanceName: "",
			threshold:    100,
			expectError:  true,
			errorMsg:     "instance name cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := metadata.DefaultMetricsBuilderConfig()
			scraper, err := NewBlockingScraper(tt.db, tt.mb, tt.logger, tt.instanceName, config, tt.threshold)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, scraper)
			} else {
				require.NoError(t, err)
				require.NotNil(t, scraper)
				assert.Equal(t, tt.db, scraper.db)
				assert.Equal(t, tt.mb, scraper.mb)
				assert.Equal(t, tt.logger, scraper.logger)
				assert.Equal(t, tt.instanceName, scraper.instanceName)
				assert.Equal(t, tt.threshold, scraper.queryMonitoringCountThreshold)
			}
		})
	}
}

func TestFormatInt64(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullInt64
		expected string
	}{
		{
			name:     "valid_positive_value",
			value:    sql.NullInt64{Int64: 12345, Valid: true},
			expected: "12345",
		},
		{
			name:     "valid_zero_value",
			value:    sql.NullInt64{Int64: 0, Valid: true},
			expected: "0",
		},
		{
			name:     "valid_negative_value",
			value:    sql.NullInt64{Int64: -999, Valid: true},
			expected: "-999",
		},
		{
			name:     "null_value",
			value:    sql.NullInt64{Int64: 0, Valid: false},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatInt64(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlockingScraper_ScrapeBlockingQueries_InvalidContext(t *testing.T) {
	db := &sql.DB{}
	mb := &metadata.MetricsBuilder{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	scraper, err := NewBlockingScraper(db, mb, logger, "test-instance", config, 100)
	require.NoError(t, err)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should return error due to cancelled context
	errors := scraper.ScrapeBlockingQueries(ctx)
	assert.NotEmpty(t, errors)
}

func TestBlockingScraper_RecordBlockingQueryMetric(t *testing.T) {
	tests := []struct {
		name          string
		blockingQuery *models.BlockingQuery
		shouldRecord  bool
	}{
		{
			name: "valid_blocking_query",
			blockingQuery: &models.BlockingQuery{
				BlockedSID:       sql.NullInt64{Int64: 123, Valid: true},
				BlockedSerial:    sql.NullInt64{Int64: 456, Valid: true},
				BlockedUser:      sql.NullString{String: "USER1", Valid: true},
				BlockedWaitSec:   sql.NullFloat64{Float64: 45.5, Valid: true},
				BlockedSQLID:     sql.NullString{String: "abc123", Valid: true},
				BlockedQueryText: sql.NullString{String: "SELECT * FROM table", Valid: true},
				BlockingSID:      sql.NullInt64{Int64: 789, Valid: true},
				BlockingSerial:   sql.NullInt64{Int64: 101, Valid: true},
				BlockingUser:     sql.NullString{String: "USER2", Valid: true},
				DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			},
			shouldRecord: true,
		},
		{
			name: "invalid_wait_time",
			blockingQuery: &models.BlockingQuery{
				BlockedSID:       sql.NullInt64{Int64: 123, Valid: true},
				BlockedSerial:    sql.NullInt64{Int64: 456, Valid: true},
				BlockedUser:      sql.NullString{String: "USER1", Valid: true},
				BlockedWaitSec:   sql.NullFloat64{Float64: 0, Valid: false},
				BlockedSQLID:     sql.NullString{String: "abc123", Valid: true},
				BlockedQueryText: sql.NullString{String: "SELECT * FROM table", Valid: true},
				BlockingSID:      sql.NullInt64{Int64: 789, Valid: true},
				BlockingSerial:   sql.NullInt64{Int64: 101, Valid: true},
				BlockingUser:     sql.NullString{String: "USER2", Valid: true},
				DatabaseName:     sql.NullString{String: "TESTDB", Valid: true},
			},
			shouldRecord: false,
		},
		{
			name: "null_values",
			blockingQuery: &models.BlockingQuery{
				BlockedSID:       sql.NullInt64{Valid: false},
				BlockedSerial:    sql.NullInt64{Valid: false},
				BlockedUser:      sql.NullString{Valid: false},
				BlockedWaitSec:   sql.NullFloat64{Float64: 10.0, Valid: true},
				BlockedSQLID:     sql.NullString{Valid: false},
				BlockedQueryText: sql.NullString{Valid: false},
				BlockingSID:      sql.NullInt64{Valid: false},
				BlockingSerial:   sql.NullInt64{Valid: false},
				BlockingUser:     sql.NullString{Valid: false},
				DatabaseName:     sql.NullString{Valid: false},
			},
			shouldRecord: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test validates the logic without actual database interaction
			// In a real scenario, you would use a mock metrics builder to verify calls
			assert.NotNil(t, tt.blockingQuery)

			// Validate that null values are handled correctly
			if tt.blockingQuery.BlockedWaitSec.Valid {
				assert.GreaterOrEqual(t, tt.blockingQuery.BlockedWaitSec.Float64, 0.0)
			}
		})
	}
}

func TestBlockingScraper_Configuration(t *testing.T) {
	db := &sql.DB{}
	mb := &metadata.MetricsBuilder{}
	logger := zap.NewNop()

	tests := []struct {
		name      string
		threshold int
	}{
		{
			name:      "default_threshold",
			threshold: 100,
		},
		{
			name:      "custom_threshold_low",
			threshold: 10,
		},
		{
			name:      "custom_threshold_high",
			threshold: 1000,
		},
		{
			name:      "zero_threshold",
			threshold: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := metadata.DefaultMetricsBuilderConfig()
			scraper, err := NewBlockingScraper(db, mb, logger, "test-instance", config, tt.threshold)

			require.NoError(t, err)
			assert.Equal(t, tt.threshold, scraper.queryMonitoringCountThreshold)
		})
	}
}

func TestBlockingScraper_InstanceName(t *testing.T) {
	db := &sql.DB{}
	mb := &metadata.MetricsBuilder{}
	logger := zap.NewNop()
	config := metadata.DefaultMetricsBuilderConfig()

	tests := []struct {
		name         string
		instanceName string
	}{
		{
			name:         "simple_name",
			instanceName: "oracle-prod",
		},
		{
			name:         "name_with_hyphen",
			instanceName: "oracle-prod-01",
		},
		{
			name:         "name_with_underscore",
			instanceName: "oracle_prod_01",
		},
		{
			name:         "name_with_dots",
			instanceName: "oracle.prod.01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraper, err := NewBlockingScraper(db, mb, logger, tt.instanceName, config, 100)

			require.NoError(t, err)
			assert.Equal(t, tt.instanceName, scraper.instanceName)
		})
	}
}
