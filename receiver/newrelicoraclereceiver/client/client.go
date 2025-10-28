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

	// Connection metrics - simple counts
	QueryTotalSessions(ctx context.Context) (int64, error)
	QueryActiveSessions(ctx context.Context) (int64, error)
	QueryInactiveSessions(ctx context.Context) (int64, error)

	// Connection metrics - breakdowns
	QuerySessionStatus(ctx context.Context) ([]models.SessionStatus, error)
	QuerySessionTypes(ctx context.Context) ([]models.SessionType, error)
	QueryLogonStats(ctx context.Context) ([]models.LogonStat, error)

	// Connection metrics - resource consumption
	QuerySessionResources(ctx context.Context) ([]models.SessionResource, error)
	QueryCurrentWaitEvents(ctx context.Context) ([]models.CurrentWaitEvent, error)
	QueryBlockingSessions(ctx context.Context) ([]models.BlockingSession, error)
	QueryWaitEventSummary(ctx context.Context) ([]models.WaitEventSummary, error)

	// Connection metrics - pool and limits
	QueryConnectionPoolMetrics(ctx context.Context) ([]models.ConnectionPoolMetric, error)
	QuerySessionLimits(ctx context.Context) ([]models.SessionLimit, error)
	QueryConnectionQuality(ctx context.Context) ([]models.ConnectionQualityMetric, error)
}
