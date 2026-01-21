// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver/models"
)

// PostgreSQLClient defines the interface for PostgreSQL database operations.
// This abstraction allows for easy testing by injecting mock implementations.
type PostgreSQLClient interface {
	// Connection management
	Close() error
	Ping(ctx context.Context) error

	// Database metrics from pg_stat_database
	QueryDatabaseMetrics(ctx context.Context) ([]models.PgStatDatabaseMetric, error)
}
