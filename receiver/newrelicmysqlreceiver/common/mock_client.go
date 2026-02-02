// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package common

// MockClient is a mock implementation of Client for testing.
// It supports both direct field assignment and function callbacks for flexibility.
type MockClient struct {
	// Direct value fields
	GlobalStats       map[string]string
	GlobalVariables   map[string]string
	ReplicationStatus map[string]string
	Version           string

	// Error fields
	ConnectErr        error
	GetGlobalStatsErr error
	GetGlobalVarsErr  error
	GetReplicationErr error
	GetVersionErr     error
	CloseErr          error

	// Function callback fields (optional, take precedence over direct fields)
	ConnectFunc              func() error
	GetGlobalStatsFunc       func() (map[string]string, error)
	GetGlobalVariablesFunc   func() (map[string]string, error)
	GetReplicationStatusFunc func() (map[string]string, error)
	GetVersionFunc           func() (string, error)
	CloseFunc                func() error
}

// NewMockClient creates a new mock client for testing.
func NewMockClient() *MockClient {
	return &MockClient{
		GlobalStats:       make(map[string]string),
		GlobalVariables:   make(map[string]string),
		ReplicationStatus: make(map[string]string),
		Version:           "",
	}
}

func (m *MockClient) Connect() error {
	if m.ConnectFunc != nil {
		return m.ConnectFunc()
	}
	return m.ConnectErr
}

func (m *MockClient) GetGlobalStats() (map[string]string, error) {
	if m.GetGlobalStatsFunc != nil {
		return m.GetGlobalStatsFunc()
	}
	if m.GetGlobalStatsErr != nil {
		return nil, m.GetGlobalStatsErr
	}
	return m.GlobalStats, nil
}

func (m *MockClient) GetGlobalVariables() (map[string]string, error) {
	if m.GetGlobalVariablesFunc != nil {
		return m.GetGlobalVariablesFunc()
	}
	if m.GetGlobalVarsErr != nil {
		return nil, m.GetGlobalVarsErr
	}
	return m.GlobalVariables, nil
}

func (m *MockClient) GetReplicationStatus() (map[string]string, error) {
	if m.GetReplicationStatusFunc != nil {
		return m.GetReplicationStatusFunc()
	}
	if m.GetReplicationErr != nil {
		return nil, m.GetReplicationErr
	}
	return m.ReplicationStatus, nil
}

func (m *MockClient) GetVersion() (string, error) {
	if m.GetVersionFunc != nil {
		return m.GetVersionFunc()
	}
	if m.GetVersionErr != nil {
		return "", m.GetVersionErr
	}
	return m.Version, nil
}

func (m *MockClient) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return m.CloseErr
}
