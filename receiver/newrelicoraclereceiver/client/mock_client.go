// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// MockClient is a mock implementation of OracleClient for testing.
type MockClient struct {
	ExecutionPlans  map[string][]models.ExecutionPlan
	SlowQueries     []models.SlowQuery
	BlockingQueries []models.BlockingQuery
	WaitEvents      []models.WaitEvent

	ConnectErr error
	CloseErr   error
	PingErr    error
	QueryErr   error
}

// NewMockClient creates a new mock client for testing.
func NewMockClient() *MockClient {
	return &MockClient{
		ExecutionPlans:  make(map[string][]models.ExecutionPlan),
		SlowQueries:     []models.SlowQuery{},
		BlockingQueries: []models.BlockingQuery{},
		WaitEvents:      []models.WaitEvent{},
	}
}

func (m *MockClient) Connect() error {
	return m.ConnectErr
}

func (m *MockClient) Close() error {
	return m.CloseErr
}

func (m *MockClient) Ping(ctx context.Context) error {
	return m.PingErr
}

func (m *MockClient) QueryExecutionPlans(ctx context.Context, sqlID string) ([]models.ExecutionPlan, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}

	if plans, ok := m.ExecutionPlans[sqlID]; ok {
		return plans, nil
	}

	return []models.ExecutionPlan{}, nil
}

func (m *MockClient) QuerySlowQueries(ctx context.Context, responseTimeThreshold, countThreshold int) ([]models.SlowQuery, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SlowQueries, nil
}

func (m *MockClient) QueryBlockingQueries(ctx context.Context, countThreshold int) ([]models.BlockingQuery, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.BlockingQueries, nil
}

func (m *MockClient) QueryWaitEvents(ctx context.Context, countThreshold int) ([]models.WaitEvent, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.WaitEvents, nil
}
