// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
)

// PostgreSQLClient defines the interface for interacting with PostgreSQL
type PostgreSQLClient interface {
	// Ping verifies the database connection is alive
	Ping(ctx context.Context) error

	// GetVersion returns the PostgreSQL server version as an integer
	// Format: Major * 10000 + Minor * 100 + Patch
	// Example: PostgreSQL 15.2 returns 150002
	GetVersion(ctx context.Context) (int, error)

	// Close closes the database connection
	Close() error

	// QueryReplicationMetrics retrieves replication statistics from pg_stat_replication
	// Uses version-specific queries:
	// - PostgreSQL 9.6: Uses pg_xlog_location_diff, returns NULL for lag times
	// - PostgreSQL 10+: Uses pg_wal_lsn_diff, includes write_lag/flush_lag/replay_lag
	// Returns empty slice if no replication is configured or if server is a standby
	// Available in PostgreSQL 9.6+
	QueryReplicationMetrics(ctx context.Context, version int) ([]models.PgStatReplicationMetric, error)
}
