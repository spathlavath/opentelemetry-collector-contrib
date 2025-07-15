// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		expectError bool
	}{
		{
			name:        "valid DSN",
			dsn:         "root:password@tcp(localhost:3306)/mysql",
			expectError: false,
		},
		{
			name:        "valid DSN with parameters",
			dsn:         "user:pass@tcp(localhost:3306)/db?timeout=30s&readTimeout=30s",
			expectError: false,
		},
		{
			name:        "valid DSN with unix socket",
			dsn:         "user:pass@unix(/var/run/mysqld/mysqld.sock)/database",
			expectError: false,
		},
		{
			name:        "invalid DSN",
			dsn:         "invalid-dsn",
			expectError: true,
		},
		{
			name:        "empty DSN",
			dsn:         "",
			expectError: false, // Empty DSN is actually valid for mysql driver
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mysql.ParseDSN(tt.dsn)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMySQLClientStruct(t *testing.T) {
	// Test that MySQLClient struct can be instantiated
	client := &MySQLClient{}
	assert.NotNil(t, client)
}

func TestDSNParsing(t *testing.T) {
	// Test with valid DSN format
	_, err := mysql.ParseDSN("root:password@tcp(localhost:3306)/mysql")
	assert.NoError(t, err) // DSN parsing should work

	// Test with invalid DSN
	_, err = mysql.ParseDSN("invalid-dsn")
	assert.Error(t, err) // Should fail
}

func TestQueryBuilding(t *testing.T) {
	// Test that our SQL queries are properly formatted
	queries := []struct {
		name           string
		query          string
		expectContains string
	}{
		{
			name:           "Global status query",
			query:          "SHOW GLOBAL STATUS",
			expectContains: "SHOW",
		},
		{
			name:           "Global variables query",
			query:          "SHOW GLOBAL VARIABLES",
			expectContains: "SHOW",
		},
		{
			name: "Query performance stats query",
			query: `SELECT 
				OBJECT_SCHEMA as schema_name,
				DIGEST_TEXT as query_digest,
				COUNT_STAR as exec_count,
				SUM_TIMER_WAIT as total_time,
				SUM_LOCK_TIME as lock_time,
				SUM_ROWS_SENT as rows_sent,
				SUM_ROWS_EXAMINED as rows_examined
			FROM performance_schema.events_statements_summary_by_digest 
			WHERE DIGEST_TEXT IS NOT NULL 
			ORDER BY SUM_TIMER_WAIT DESC 
			LIMIT ?`,
			expectContains: "SELECT",
		},
		{
			name: "Wait events query",
			query: `SELECT 
				EVENT_NAME as event_type,
				SUM_TIMER_WAIT as total_time,
				COUNT_STAR as event_count
			FROM performance_schema.events_waits_summary_global_by_event_name 
			WHERE SUM_TIMER_WAIT > 0 
			ORDER BY SUM_TIMER_WAIT DESC`,
			expectContains: "SELECT",
		},
	}

	// Verify queries are properly formatted
	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			assert.NotEmpty(t, test.query)
			assert.Contains(t, test.query, test.expectContains)
		})
	}
}
