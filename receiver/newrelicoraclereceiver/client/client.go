// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// OracleClient defines the interface for Oracle database operations.
// This abstraction allows for easy testing by injecting mock implementations.
type OracleClient interface {
	// Connection management
	Connect() error
	Close() error
	Ping(ctx context.Context) error

	// Execution plan queries
	QueryExecutionPlans(ctx context.Context, sqlID string) ([]models.ExecutionPlan, error)

	// Slow queries
	QuerySlowQueries(ctx context.Context, responseTimeThreshold, countThreshold int) ([]models.SlowQuery, error)

	// Blocking queries
	QueryBlockingQueries(ctx context.Context, countThreshold int) ([]models.BlockingQuery, error)

	// Wait events
	QueryWaitEvents(ctx context.Context, countThreshold int) ([]models.WaitEvent, error)
}
