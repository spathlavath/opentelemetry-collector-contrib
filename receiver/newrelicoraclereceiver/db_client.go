// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

import (
	"database/sql"
	"time"

	_ "github.com/sijms/go-ora/v2" // Oracle database driver
)

// DBClient is an interface for database operations
type DBClient interface {
	Query(query string) (*sql.Rows, error)
	QueryRow(query string) *sql.Row
	Close() error
}

// oracleDBClient implements DBClient interface
type oracleDBClient struct {
	db *sql.DB
}

// newOracleDBClient creates a new Oracle database client
func newOracleDBClient(cfg *Config) (DBClient, error) {
	connectionString := cfg.GetConnectionString()

	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return &oracleDBClient{db: db}, nil
}

// Query executes a query that returns rows
func (c *oracleDBClient) Query(query string) (*sql.Rows, error) {
	return c.db.Query(query)
}

// QueryRow executes a query that is expected to return at most one row
func (c *oracleDBClient) QueryRow(query string) *sql.Row {
	return c.db.QueryRow(query)
}

// Close closes the database connection
func (c *oracleDBClient) Close() error {
	return c.db.Close()
}
