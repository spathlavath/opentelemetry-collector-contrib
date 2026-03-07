// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package models

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlowQuery_GetCollectionTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullString
		expected string
	}{
		{
			name:     "valid_timestamp",
			value:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			expected: "2024-01-01 10:00:00",
		},
		{
			name:     "null_timestamp",
			value:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty_string",
			value:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				CollectionTimestamp: tt.value,
			}
			result := sq.GetCollectionTimestamp()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_GetDatabaseName(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullString
		expected string
	}{
		{
			name:     "valid_database_name",
			value:    sql.NullString{String: "testdb", Valid: true},
			expected: "testdb",
		},
		{
			name:     "null_database_name",
			value:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty_string",
			value:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				DatabaseName: tt.value,
			}
			result := sq.GetDatabaseName()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_GetQueryID(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullString
		expected string
	}{
		{
			name:     "valid_query_id",
			value:    sql.NullString{String: "abc123def456", Valid: true},
			expected: "abc123def456",
		},
		{
			name:     "null_query_id",
			value:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty_string",
			value:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				QueryID: tt.value,
			}
			result := sq.GetQueryID()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_GetQueryText(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullString
		expected string
	}{
		{
			name:     "valid_query_text",
			value:    sql.NullString{String: "SELECT * FROM users WHERE id = ?", Valid: true},
			expected: "SELECT * FROM users WHERE id = ?",
		},
		{
			name:     "null_query_text",
			value:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty_string",
			value:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
		{
			name:     "long_query_text",
			value:    sql.NullString{String: "SELECT * FROM very_long_table_name WHERE column1 = ? AND column2 = ? AND column3 = ?", Valid: true},
			expected: "SELECT * FROM very_long_table_name WHERE column1 = ? AND column2 = ? AND column3 = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				QueryText: tt.value,
			}
			result := sq.GetQueryText()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_GetLastExecutionTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullString
		expected string
	}{
		{
			name:     "valid_timestamp",
			value:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
			expected: "2024-01-01 10:00:00",
		},
		{
			name:     "null_timestamp",
			value:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty_string",
			value:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				LastExecutionTimestamp: tt.value,
			}
			result := sq.GetLastExecutionTimestamp()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_GetFirstSeen(t *testing.T) {
	tests := []struct {
		name     string
		value    sql.NullString
		expected string
	}{
		{
			name:     "valid_timestamp",
			value:    sql.NullString{String: "2024-01-01 09:00:00", Valid: true},
			expected: "2024-01-01 09:00:00",
		},
		{
			name:     "null_timestamp",
			value:    sql.NullString{Valid: false},
			expected: "",
		},
		{
			name:     "empty_string",
			value:    sql.NullString{String: "", Valid: true},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				FirstSeen: tt.value,
			}
			result := sq.GetFirstSeen()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_HasValidQueryID(t *testing.T) {
	tests := []struct {
		name     string
		queryID  sql.NullString
		expected bool
	}{
		{
			name:     "valid_query_id",
			queryID:  sql.NullString{String: "abc123", Valid: true},
			expected: true,
		},
		{
			name:     "null_query_id",
			queryID:  sql.NullString{Valid: false},
			expected: false,
		},
		{
			name:     "empty_string",
			queryID:  sql.NullString{String: "", Valid: true},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				QueryID: tt.queryID,
			}
			result := sq.HasValidQueryID()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_HasValidElapsedTime(t *testing.T) {
	tests := []struct {
		name           string
		avgElapsedTime sql.NullFloat64
		expected       bool
	}{
		{
			name:           "valid_elapsed_time",
			avgElapsedTime: sql.NullFloat64{Float64: 1500.0, Valid: true},
			expected:       true,
		},
		{
			name:           "null_elapsed_time",
			avgElapsedTime: sql.NullFloat64{Valid: false},
			expected:       false,
		},
		{
			name:           "zero_elapsed_time",
			avgElapsedTime: sql.NullFloat64{Float64: 0.0, Valid: true},
			expected:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				AvgElapsedTimeMS: tt.avgElapsedTime,
			}
			result := sq.HasValidElapsedTime()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_IsValidForMetrics(t *testing.T) {
	tests := []struct {
		name           string
		queryID        sql.NullString
		avgElapsedTime sql.NullFloat64
		expected       bool
	}{
		{
			name:           "valid_query",
			queryID:        sql.NullString{String: "abc123", Valid: true},
			avgElapsedTime: sql.NullFloat64{Float64: 1500.0, Valid: true},
			expected:       true,
		},
		{
			name:           "missing_query_id",
			queryID:        sql.NullString{Valid: false},
			avgElapsedTime: sql.NullFloat64{Float64: 1500.0, Valid: true},
			expected:       false,
		},
		{
			name:           "missing_elapsed_time",
			queryID:        sql.NullString{String: "abc123", Valid: true},
			avgElapsedTime: sql.NullFloat64{Valid: false},
			expected:       false,
		},
		{
			name:           "missing_both",
			queryID:        sql.NullString{Valid: false},
			avgElapsedTime: sql.NullFloat64{Valid: false},
			expected:       false,
		},
		{
			name:           "empty_query_id",
			queryID:        sql.NullString{String: "", Valid: true},
			avgElapsedTime: sql.NullFloat64{Float64: 1500.0, Valid: true},
			expected:       false,
		},
		{
			name:           "zero_elapsed_time",
			queryID:        sql.NullString{String: "abc123", Valid: true},
			avgElapsedTime: sql.NullFloat64{Float64: 0.0, Valid: true},
			expected:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sq := &SlowQuery{
				QueryID:          tt.queryID,
				AvgElapsedTimeMS: tt.avgElapsedTime,
			}
			result := sq.IsValidForMetrics()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSlowQuery_CompleteQuery(t *testing.T) {
	sq := &SlowQuery{
		CollectionTimestamp:    sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
		QueryID:                sql.NullString{String: "abc123def456", Valid: true},
		QueryText:              sql.NullString{String: "SELECT * FROM users WHERE status = ?", Valid: true},
		DatabaseName:           sql.NullString{String: "testdb", Valid: true},
		ExecutionCount:         sql.NullInt64{Int64: 150, Valid: true},
		TotalElapsedTimeMS:     sql.NullFloat64{Float64: 225000.0, Valid: true},
		AvgElapsedTimeMS:       sql.NullFloat64{Float64: 1500.0, Valid: true},
		AvgCPUTimeMS:           sql.NullFloat64{Float64: 125.5, Valid: true},
		AvgLockTimeMS:          sql.NullFloat64{Float64: 10.2, Valid: true},
		AvgRowsExamined:        sql.NullFloat64{Float64: 5000.0, Valid: true},
		AvgRowsSent:            sql.NullFloat64{Float64: 100.0, Valid: true},
		TotalErrors:            sql.NullInt64{Int64: 0, Valid: true},
		FirstSeen:              sql.NullString{String: "2024-01-01 09:00:00", Valid: true},
		LastExecutionTimestamp: sql.NullString{String: "2024-01-01 10:00:00", Valid: true},
	}

	// Test all getter methods
	assert.Equal(t, "2024-01-01 10:00:00", sq.GetCollectionTimestamp())
	assert.Equal(t, "abc123def456", sq.GetQueryID())
	assert.Equal(t, "SELECT * FROM users WHERE status = ?", sq.GetQueryText())
	assert.Equal(t, "testdb", sq.GetDatabaseName())
	assert.Equal(t, "2024-01-01 09:00:00", sq.GetFirstSeen())
	assert.Equal(t, "2024-01-01 10:00:00", sq.GetLastExecutionTimestamp())

	// Test validation methods
	assert.True(t, sq.HasValidQueryID())
	assert.True(t, sq.HasValidElapsedTime())
	assert.True(t, sq.IsValidForMetrics())
}

func TestSlowQuery_IntervalMetrics(t *testing.T) {
	intervalAvg := 1800.0
	intervalCount := int64(50)

	sq := &SlowQuery{
		QueryID:                  sql.NullString{String: "test_query", Valid: true},
		AvgElapsedTimeMS:         sql.NullFloat64{Float64: 1500.0, Valid: true},
		IntervalAvgElapsedTimeMS: &intervalAvg,
		IntervalExecutionCount:   &intervalCount,
	}

	assert.True(t, sq.IsValidForMetrics())
	assert.NotNil(t, sq.IntervalAvgElapsedTimeMS)
	assert.NotNil(t, sq.IntervalExecutionCount)
	assert.Equal(t, 1800.0, *sq.IntervalAvgElapsedTimeMS)
	assert.Equal(t, int64(50), *sq.IntervalExecutionCount)
}

func TestSlowQuery_NilIntervalMetrics(t *testing.T) {
	sq := &SlowQuery{
		QueryID:                  sql.NullString{String: "test_query", Valid: true},
		AvgElapsedTimeMS:         sql.NullFloat64{Float64: 1500.0, Valid: true},
		IntervalAvgElapsedTimeMS: nil,
		IntervalExecutionCount:   nil,
	}

	assert.True(t, sq.IsValidForMetrics())
	assert.Nil(t, sq.IntervalAvgElapsedTimeMS)
	assert.Nil(t, sq.IntervalExecutionCount)
}

func TestSlowQuery_AllFieldsNull(t *testing.T) {
	sq := &SlowQuery{}

	assert.Equal(t, "", sq.GetCollectionTimestamp())
	assert.Equal(t, "", sq.GetQueryID())
	assert.Equal(t, "", sq.GetQueryText())
	assert.Equal(t, "", sq.GetDatabaseName())
	assert.Equal(t, "", sq.GetFirstSeen())
	assert.Equal(t, "", sq.GetLastExecutionTimestamp())
	assert.False(t, sq.HasValidQueryID())
	assert.False(t, sq.HasValidElapsedTime())
	assert.False(t, sq.IsValidForMetrics())
}

func TestSlowQuery_PartialData(t *testing.T) {
	sq := &SlowQuery{
		QueryID:          sql.NullString{String: "partial_query", Valid: true},
		DatabaseName:     sql.NullString{String: "testdb", Valid: true},
		AvgElapsedTimeMS: sql.NullFloat64{Float64: 1000.0, Valid: true},
		// Other fields are null
	}

	assert.Equal(t, "partial_query", sq.GetQueryID())
	assert.Equal(t, "testdb", sq.GetDatabaseName())
	assert.Equal(t, "", sq.GetQueryText())
	assert.Equal(t, "", sq.GetCollectionTimestamp())
	assert.Equal(t, "", sq.GetFirstSeen())
	assert.Equal(t, "", sq.GetLastExecutionTimestamp())
	assert.True(t, sq.IsValidForMetrics())
}
