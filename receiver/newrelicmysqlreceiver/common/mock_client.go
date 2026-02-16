// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"context"
	"database/sql"
)

// MockClient is a shared mock implementation of common.Client for testing.
// It can be configured to return different data or errors for different test scenarios.
type MockClient struct {
	// Global stats
	GlobalStats    map[string]string
	GlobalStatsErr error

	// Global variables
	GlobalVariables    map[string]string
	GlobalVariablesErr error

	// Replication status
	ReplicationStatus    map[string]string
	ReplicationStatusErr error

	// Master status
	MasterStatus    map[string]string
	MasterStatusErr error

	// Group replication stats
	GroupReplicationStats    map[string]string
	GroupReplicationStatsErr error

	// Version
	Version    string
	VersionErr error

	// Optional: custom functions to support complex scenarios
	GetGlobalStatsFunc     func() (map[string]string, error)
	GetGlobalVariablesFunc func() (map[string]string, error)
	GetVersionFunc         func() (string, error)
}

// NewMockClient creates a new MockClient with default values.
func NewMockClient() *MockClient {
	return &MockClient{
		GlobalStats:           make(map[string]string),
		GlobalVariables:       make(map[string]string),
		ReplicationStatus:     make(map[string]string),
		MasterStatus:          make(map[string]string),
		GroupReplicationStats: make(map[string]string),
		Version:               "8.0.0",
	}
}

var _ Client = (*MockClient)(nil)

func (m *MockClient) Connect() error {
	return nil
}

func (m *MockClient) GetGlobalStats() (map[string]string, error) {
	if m.GetGlobalStatsFunc != nil {
		return m.GetGlobalStatsFunc()
	}
	return m.GlobalStats, m.GlobalStatsErr
}

func (m *MockClient) GetGlobalVariables() (map[string]string, error) {
	if m.GetGlobalVariablesFunc != nil {
		return m.GetGlobalVariablesFunc()
	}
	return m.GlobalVariables, m.GlobalVariablesErr
}

func (m *MockClient) GetReplicationStatus() (map[string]string, error) {
	return m.ReplicationStatus, m.ReplicationStatusErr
}

func (m *MockClient) GetMasterStatus() (map[string]string, error) {
	return m.MasterStatus, m.MasterStatusErr
}

func (m *MockClient) GetGroupReplicationStats() (map[string]string, error) {
	return m.GroupReplicationStats, m.GroupReplicationStatsErr
}

func (m *MockClient) GetVersion() (string, error) {
	if m.GetVersionFunc != nil {
		return m.GetVersionFunc()
	}
	return m.Version, m.VersionErr
}

func (m *MockClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}

func (m *MockClient) Close() error {
	return nil
}
