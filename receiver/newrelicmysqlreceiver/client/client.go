// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

// Client defines the interface for MySQL database operations.
// This interface allows for easy mocking and testing of scrapers.
type Client interface {
	// Connect establishes a connection to the MySQL database.
	Connect() error

	// GetGlobalStats retrieves MySQL global status variables as numeric values.
	GetGlobalStats() (map[string]int64, error)

	// GetGlobalVariables retrieves MySQL global variables as numeric values.
	GetGlobalVariables() (map[string]int64, error)

	// GetReplicationStatus retrieves replication status from SHOW SLAVE/REPLICA STATUS.
	GetReplicationStatus() (map[string]string, error)

	// GetVersion retrieves the MySQL server version string.
	GetVersion() (string, error)

	// Close closes the database connection.
	Close() error
}
