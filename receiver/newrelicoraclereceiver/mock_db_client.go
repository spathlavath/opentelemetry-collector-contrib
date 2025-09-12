// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver

import (
	"database/sql"
	"fmt"
)

// mockDBClient implements DBClient interface for testing
type mockDBClient struct {
	queryResponses map[string]*mockRows
	queryError     error
}

// mockRows implements sql.Rows interface for testing
type mockRows struct {
	columns []string
	rows    [][]interface{}
	current int
	closed  bool
}

// newMockDBClient creates a new mock database client
func newMockDBClient() *mockDBClient {
	return &mockDBClient{
		queryResponses: make(map[string]*mockRows),
	}
}

// SetQueryResponse sets a mock response for a given query
func (m *mockDBClient) SetQueryResponse(query string, columns []string, rows [][]interface{}) {
	m.queryResponses[query] = &mockRows{
		columns: columns,
		rows:    rows,
		current: -1,
	}
}

// SetQueryError sets an error to be returned for all queries
func (m *mockDBClient) SetQueryError(err error) {
	m.queryError = err
}

// Query implements DBClient interface
func (m *mockDBClient) Query(query string) (*sql.Rows, error) {
	if m.queryError != nil {
		return nil, m.queryError
	}

	_, exists := m.queryResponses[query]
	if !exists {
		return nil, fmt.Errorf("no mock response for query: %s", query)
	}

	// Return nil for testing - this is a simplified mock
	return nil, nil
}

// QueryRow implements DBClient interface
func (m *mockDBClient) QueryRow(query string) *sql.Row {
	// Return nil for testing - this is a simplified mock
	return nil
}

// Close implements DBClient interface
func (m *mockDBClient) Close() error {
	return nil
}

// mockRows methods
func (m *mockRows) Next() bool {
	if m.closed {
		return false
	}
	m.current++
	return m.current < len(m.rows)
}

func (m *mockRows) Scan(dest ...interface{}) error {
	if m.closed || m.current < 0 || m.current >= len(m.rows) {
		return fmt.Errorf("no current row")
	}

	row := m.rows[m.current]
	if len(dest) != len(row) {
		return fmt.Errorf("expected %d values, got %d", len(row), len(dest))
	}

	for i, val := range row {
		switch v := dest[i].(type) {
		case *int:
			*v = val.(int)
		case *int64:
			*v = val.(int64)
		case *float64:
			*v = val.(float64)
		case *string:
			*v = val.(string)
		default:
			return fmt.Errorf("unsupported type for destination %d", i)
		}
	}

	return nil
}

func (m *mockRows) Close() error {
	m.closed = true
	return nil
}

func (m *mockRows) Columns() ([]string, error) {
	return m.columns, nil
}

func (m *mockRows) Err() error {
	return nil
}
