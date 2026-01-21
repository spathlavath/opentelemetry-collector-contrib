// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/queries"
)

// SQLClient implements the PostgreSQLClient interface using database/sql
type SQLClient struct {
	db *sql.DB
}

// NewSQLClient creates a new SQLClient instance
func NewSQLClient(db *sql.DB) *SQLClient {
	return &SQLClient{db: db}
}

// Close closes the database connection
func (c *SQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping verifies the database connection is alive
func (c *SQLClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// QueryDatabaseMetrics retrieves database statistics from pg_stat_database
func (c *SQLClient) QueryDatabaseMetrics(ctx context.Context) ([]models.PgStatDatabaseMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatDatabaseMetricsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseMetric

	for rows.Next() {
		var metric models.PgStatDatabaseMetric
		err := rows.Scan(
			&metric.DatName,
			&metric.NumBackends,
			&metric.XactCommit,
			&metric.XactRollback,
			&metric.BlksRead,
			&metric.BlksHit,
			&metric.TupReturned,
			&metric.TupFetched,
			&metric.TupInserted,
			&metric.TupUpdated,
			&metric.TupDeleted,
			&metric.Deadlocks,
			&metric.TempFiles,
			&metric.TempBytes,
			&metric.BlkReadTime,
			&metric.BlkWriteTime,
			&metric.BeforeXIDWraparound,
			&metric.DatabaseSize,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_database row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_database rows: %w", err)
	}

	return metrics, nil
}
