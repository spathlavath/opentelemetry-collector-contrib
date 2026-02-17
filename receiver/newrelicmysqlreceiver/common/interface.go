// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"context"
	"database/sql"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
)

// Client defines the interface for MySQL database operations.
type Client interface {
	// Connect establishes the database connection
	Connect() error
	// GetGlobalStats retrieves global status metrics from MySQL
	GetGlobalStats() (map[string]string, error)
	// GetGlobalVariables retrieves global configuration variables
	GetGlobalVariables() (map[string]string, error)
	// GetReplicationStatus retrieves slave replication status information
	GetReplicationStatus() (map[string]string, error)
	// GetMasterStatus retrieves master/source status information
	GetMasterStatus() (map[string]string, error)
	// GetGroupReplicationStats retrieves group replication performance schema stats
	GetGroupReplicationStats() (map[string]string, error)
	// GetVersion returns the MySQL server version
	GetVersion() (string, error)
	// QueryContext executes a query and returns sql.Rows for custom processing
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	// GetSlowQueries retrieves slow queries from performance_schema
	// intervalSeconds: Time window to fetch queries from the last N seconds
	GetSlowQueries(ctx context.Context, intervalSeconds int) ([]models.SlowQuery, error)
	// Close closes the database connection
	Close() error
}
