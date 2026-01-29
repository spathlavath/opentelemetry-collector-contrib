// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
)

// mySQLClient is the concrete implementation of Client interface.
type mySQLClient struct {
	connStr string
	db      *sql.DB
}

// Ensure mySQLClient implements Client interface.
var _ Client = (*mySQLClient)(nil)

// Config represents the configuration needed to create a MySQL client.
type Config struct {
	Username             string
	Password             string
	Endpoint             string
	Database             string
	Transport            string
	AllowNativePasswords bool
	TLSConfig            TLSConfig
}

// TLSConfig represents TLS configuration.
type TLSConfig struct {
	Insecure bool
	LoadFunc func(context.Context) (interface{}, error)
}

// NewMySQLClient creates a new MySQL client with the given configuration.
func NewMySQLClient(cfg Config) (Client, error) {
	driverConf := mysql.NewConfig()
	driverConf.User = cfg.Username
	driverConf.Passwd = cfg.Password
	driverConf.Net = cfg.Transport
	driverConf.Addr = cfg.Endpoint
	driverConf.DBName = cfg.Database
	driverConf.AllowNativePasswords = cfg.AllowNativePasswords

	if !cfg.TLSConfig.Insecure && cfg.TLSConfig.LoadFunc != nil {
		tlsConfig, err := cfg.TLSConfig.LoadFunc(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
		driverConf.TLSConfig = "custom"
		if tlsConfigTyped, ok := tlsConfig.(*tls.Config); ok {
			if err := mysql.RegisterTLSConfig("custom", tlsConfigTyped); err != nil {
				return nil, fmt.Errorf("failed to register TLS config: %w", err)
			}
		}
	}

	connStr := driverConf.FormatDSN()

	return &mySQLClient{
		connStr: connStr,
	}, nil
}

// Connect establishes a connection to the MySQL database.
func (c *mySQLClient) Connect() error {
	clientDB, err := sql.Open("mysql", c.connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	c.db = clientDB
	return c.db.Ping()
}

// queryNumericKeyValues executes a query and returns numeric key-value pairs.
// Non-numeric values are skipped.
func (c *mySQLClient) queryNumericKeyValues(query string) (map[string]int64, error) {
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		// Convert string value to int64
		intValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			// Skip non-numeric values
			continue
		}
		result[name] = intValue
	}

	return result, rows.Err()
}

// GetGlobalStats retrieves MySQL global status variables as numeric values.
func (c *mySQLClient) GetGlobalStats() (map[string]int64, error) {
	return c.queryNumericKeyValues("SHOW GLOBAL STATUS")
}

// GetGlobalVariables retrieves MySQL global variables as numeric values.
func (c *mySQLClient) GetGlobalVariables() (map[string]int64, error) {
	return c.queryNumericKeyValues("SHOW GLOBAL VARIABLES")
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

// Close closes the database connection.
func (c *mySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
