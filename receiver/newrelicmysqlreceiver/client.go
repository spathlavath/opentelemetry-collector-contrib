// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
)

// mySQLClient is the concrete implementation of Client interface.
type mySQLClient struct {
	connStr string
	db      *sql.DB
	logger  *zap.Logger
}

// NewMySQLClient creates a new MySQL client with the given configuration.
func NewMySQLClient(cfg *Config) (common.Client, error) {
	driverConf := mysql.NewConfig()
	driverConf.User = cfg.Username
	driverConf.Passwd = string(cfg.Password)
	driverConf.Net = string(cfg.Transport)
	driverConf.Addr = cfg.Endpoint
	driverConf.DBName = cfg.Database
	driverConf.AllowNativePasswords = cfg.AllowNativePasswords

	if !cfg.TLS.Insecure {
		tlsConfig, err := cfg.TLS.LoadTLSConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
		if tlsConfig != nil {
			if err := mysql.RegisterTLSConfig("custom", tlsConfig); err != nil {
				return nil, fmt.Errorf("failed to register TLS config: %w", err)
			}
			driverConf.TLSConfig = "custom"
		}
	}

	connStr := driverConf.FormatDSN()

	return &mySQLClient{
		connStr: connStr,
		logger:  zap.NewNop(),
	}, nil
}

// Connect establishes a connection to the MySQL database.
func (c *mySQLClient) Connect() error {
	clientDB, err := sql.Open("mysql", c.connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := clientDB.Ping(); err != nil {
		clientDB.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	c.db = clientDB
	return nil
}

// queryKeyValues executes a query and returns key-value pairs as strings.
func (c *mySQLClient) queryKeyValues(query string) (map[string]string, error) {
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		result[name] = value
	}

	return result, rows.Err()
}

// GetGlobalStats retrieves MySQL global status variables.
func (c *mySQLClient) GetGlobalStats() (map[string]string, error) {
	return c.queryKeyValues("SHOW GLOBAL STATUS")
}

// GetGlobalVariables retrieves MySQL global variables.
func (c *mySQLClient) GetGlobalVariables() (map[string]string, error) {
	return c.queryKeyValues("SHOW GLOBAL VARIABLES")
}

// GetReplicationStatus retrieves replication status from SHOW SLAVE/REPLICA STATUS.
func (c *mySQLClient) GetReplicationStatus() (map[string]string, error) {
	// Try MySQL 8.0+ command first (SHOW REPLICA STATUS)
	query := "SHOW REPLICA STATUS"
	rows, err := c.db.Query(query)
	if err != nil {
		// Fall back to MySQL 5.7 command (SHOW SLAVE STATUS)
		query = "SHOW SLAVE STATUS"
		rows, err = c.db.Query(query)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Check if there are any rows (only applies to replicas)
	if !rows.Next() {
		// Not a replica, return empty map
		return make(map[string]string), nil
	}

	// Create a slice of any to hold the values
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Scan the row
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Convert to map
	result := make(map[string]string)
	for i, col := range columns {
		var v string
		val := values[i]
		if val != nil {
			switch t := val.(type) {
			case []byte:
				v = string(t)
			case string:
				v = t
			default:
				v = fmt.Sprintf("%v", val)
			}
		}
		result[col] = v
	}

	return result, rows.Err()
}

// GetVersion retrieves the MySQL server version string.
func (c *mySQLClient) GetVersion() (string, error) {
	var version string
	err := c.db.QueryRow("SELECT VERSION()").Scan(&version)
	return version, err
}

// QueryContext executes a query and returns sql.Rows for custom processing.
func (c *mySQLClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	return c.db.QueryContext(ctx, query, args...)
}

// Close closes the database connection.
func (c *mySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
