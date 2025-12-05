// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"
)

// MockSQLServerClient is a mock implementation of SQLServerClient for testing
type MockSQLServerClient struct {
	CloseFunc    func() error
	PingFunc     func(ctx context.Context) error
	QueryFunc    func(ctx context.Context, dest interface{}, query string) error
	QueryRowFunc func(ctx context.Context, query string) *sql.Row
	StatsFunc    func() sql.DBStats
}

// Close mocks the Close method
func (m *MockSQLServerClient) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// Ping mocks the Ping method
func (m *MockSQLServerClient) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

// Query mocks the Query method
func (m *MockSQLServerClient) Query(ctx context.Context, dest interface{}, query string) error {
	if m.QueryFunc != nil {
		return m.QueryFunc(ctx, dest, query)
	}
	return nil
}

// QueryRow mocks the QueryRow method
func (m *MockSQLServerClient) QueryRow(ctx context.Context, query string) *sql.Row {
	if m.QueryRowFunc != nil {
		return m.QueryRowFunc(ctx, query)
	}
	return nil
}

// Stats mocks the Stats method
func (m *MockSQLServerClient) Stats() sql.DBStats {
	if m.StatsFunc != nil {
		return m.StatsFunc()
	}
	return sql.DBStats{}
}
