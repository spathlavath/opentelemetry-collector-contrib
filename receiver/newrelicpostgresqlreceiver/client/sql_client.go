// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/queries"
)

// SQLClient is the PostgreSQL client implementation using database/sql
type SQLClient struct {
	db *sql.DB
}

// NewSQLClient creates a new PostgreSQL SQL client
func NewSQLClient(connStr string) (*SQLClient, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	return &SQLClient{db: db}, nil
}

// Ping verifies the database connection is alive
func (c *SQLClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// GetVersion returns the PostgreSQL server version as an integer
func (c *SQLClient) GetVersion(ctx context.Context) (int, error) {
	var version int
	err := c.db.QueryRowContext(ctx, queries.VersionQuery).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to query PostgreSQL version: %w", err)
	}
	return version, nil
}

// Close closes the database connection
func (c *SQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// QueryReplicationMetrics retrieves replication statistics from pg_stat_replication
// Uses version-specific queries based on PostgreSQL version
func (c *SQLClient) QueryReplicationMetrics(ctx context.Context, version int) ([]models.PgStatReplicationMetric, error) {
	// Select appropriate query based on PostgreSQL version
	var query string
	if version >= 100000 { // PostgreSQL 10.0+
		query = queries.PgStatReplicationMetricsPG10SQL
	} else { // PostgreSQL 9.6
		query = queries.PgStatReplicationMetricsPG96SQL
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_replication: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatReplicationMetric

	for rows.Next() {
		var metric models.PgStatReplicationMetric

		err = rows.Scan(
			&metric.ApplicationName,
			&metric.State,
			&metric.SyncState,
			&metric.ClientAddr,
			&metric.BackendXminAge,
			&metric.SentLsnDelay,
			&metric.WriteLsnDelay,
			&metric.FlushLsnDelay,
			&metric.ReplayLsnDelay,
			&metric.WriteLag,
			&metric.FlushLag,
			&metric.ReplayLag,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_replication row: %w", err)
		}

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_replication rows: %w", err)
	}

	return metrics, nil
}

// QueryRow executes a query that returns a single row
func (c *SQLClient) QueryRow(ctx context.Context, query string, args interface{}, dest interface{}) error {
	var row *sql.Row
	if args != nil {
		row = c.db.QueryRowContext(ctx, query, args)
	} else {
		row = c.db.QueryRowContext(ctx, query)
	}
	return row.Scan(dest)
}
