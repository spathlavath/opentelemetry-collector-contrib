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
// For PostgreSQL 12+, includes checksum metrics when supportsPG12 is true
func (c *SQLClient) QueryDatabaseMetrics(ctx context.Context, supportsPG12 bool) ([]models.PgStatDatabaseMetric, error) {
	// Select query based on PostgreSQL version
	query := queries.PgStatDatabaseMetricsSQL
	if supportsPG12 {
		query = queries.PgStatDatabaseMetricsWithChecksumsSQL
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseMetric

	for rows.Next() {
		var metric models.PgStatDatabaseMetric

		if supportsPG12 {
			// Scan with checksum fields (PostgreSQL 12+)
			err = rows.Scan(
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
				&metric.Conflicts,
				&metric.TempFiles,
				&metric.TempBytes,
				&metric.Deadlocks,
				&metric.BlkReadTime,
				&metric.BlkWriteTime,
				&metric.BeforeXIDWraparound,
				&metric.DatabaseSize,
				&metric.ChecksumFailures,
				&metric.ChecksumLastFailure,
				&metric.ChecksumsEnabled,
			)
		} else {
			// Scan without checksum fields (PostgreSQL < 12)
			err = rows.Scan(
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
				&metric.Conflicts,
				&metric.TempFiles,
				&metric.TempBytes,
				&metric.Deadlocks,
				&metric.BlkReadTime,
				&metric.BlkWriteTime,
				&metric.BeforeXIDWraparound,
				&metric.DatabaseSize,
			)
		}

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

// GetVersion retrieves the PostgreSQL server version number
func (c *SQLClient) GetVersion(ctx context.Context) (int, error) {
	var version int
	err := c.db.QueryRowContext(ctx, queries.VersionQuery).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get PostgreSQL version: %w", err)
	}
	return version, nil
}

// QuerySessionMetrics retrieves session statistics from pg_stat_database (PostgreSQL 14+)
func (c *SQLClient) QuerySessionMetrics(ctx context.Context) ([]models.PgStatDatabaseSessionMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatDatabaseSessionMetricsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database sessions: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseSessionMetric

	for rows.Next() {
		var metric models.PgStatDatabaseSessionMetric
		err := rows.Scan(
			&metric.DatName,
			&metric.SessionTime,
			&metric.ActiveTime,
			&metric.IdleInTransactionTime,
			&metric.SessionCount,
			&metric.SessionsAbandoned,
			&metric.SessionsFatal,
			&metric.SessionsKilled,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_database session row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_database session rows: %w", err)
	}

	return metrics, nil
}

// QueryConflictMetrics retrieves conflict statistics from pg_stat_database_conflicts (PostgreSQL 9.6+)
func (c *SQLClient) QueryConflictMetrics(ctx context.Context) ([]models.PgStatDatabaseConflictsMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PgStatDatabaseConflictsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_stat_database_conflicts: %w", err)
	}
	defer rows.Close()

	var metrics []models.PgStatDatabaseConflictsMetric

	for rows.Next() {
		var metric models.PgStatDatabaseConflictsMetric
		err := rows.Scan(
			&metric.DatName,
			&metric.ConflTablespace,
			&metric.ConflLock,
			&metric.ConflSnapshot,
			&metric.ConflBufferpin,
			&metric.ConflDeadlock,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pg_stat_database_conflicts row: %w", err)
		}
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pg_stat_database_conflicts rows: %w", err)
	}

	return metrics, nil
}
