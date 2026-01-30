// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	// "github.com/docker/docker/client"
	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
)

// mySQLClient implements the Client interface using the standard database/sql package.
type mySQLClient struct {
	db     *sql.DB
	config *Config
	logger *zap.Logger
}

// NewMySQLClient creates a new MySQL client with the provided configuration.
func NewMySQLClient(cfg *Config) (common.Client, error) {
	return &mySQLClient{
		config: cfg,
		logger: zap.NewNop(), // Using a no-op logger by default
	}, nil
}

// Connect establishes a connection to the MySQL database.
func (c *mySQLClient) Connect() error {
	// Build DSN with TLS configuration
	dsn, err := c.buildDSN()
	if err != nil {
		return fmt.Errorf("failed to build DSN: %w", err)
	}

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	c.db = db
	return nil
}

// buildDSN constructs the Data Source Name for MySQL connection.
func (c *mySQLClient) buildDSN() (string, error) {
	if c.config.Endpoint == "" {
		return "", fmt.Errorf("endpoint cannot be empty")
	}

	// Build DSN: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	var dsnBuilder strings.Builder

	// Add credentials
	if c.config.Username != "" {
		dsnBuilder.WriteString(c.config.Username)
		if c.config.Password != "" {
			dsnBuilder.WriteString(":")
			dsnBuilder.WriteString(string(c.config.Password))
		}
		dsnBuilder.WriteString("@")
	}

	// Add transport and address
	transport := c.config.Transport
	if transport == "" {
		transport = "tcp" // Default to TCP
	}
	dsnBuilder.WriteString(string(transport))
	dsnBuilder.WriteString("(")
	dsnBuilder.WriteString(c.config.Endpoint)
	dsnBuilder.WriteString(")")

	// Add database name
	dsnBuilder.WriteString("/")
	if c.config.Database != "" {
		dsnBuilder.WriteString(c.config.Database)
	}

	// Build query parameters
	params := url.Values{}

	// Add allow native passwords
	if c.config.AllowNativePasswords {
		params.Add("allowNativePasswords", "true")
	}

	// Handle TLS configuration
	if !c.config.TLS.Insecure {
		// Load TLS config from configtls.ClientConfig
		tlsConfig, err := c.config.TLS.LoadTLSConfig(context.Background())
		if err != nil {
			return "", fmt.Errorf("failed to load TLS config: %w", err)
		}

		if tlsConfig != nil {
			// Register custom TLS config with MySQL driver
			tlsConfigName := "custom"
			if err := mysql.RegisterTLSConfig(tlsConfigName, tlsConfig); err != nil {
				return "", fmt.Errorf("failed to register TLS config: %w", err)
			}
			params.Add("tls", tlsConfigName)
		} else {
			// Use default TLS
			params.Add("tls", "true")
		}
	}

	// Append query parameters if any
	if len(params) > 0 {
		dsnBuilder.WriteString("?")
		dsnBuilder.WriteString(params.Encode())
	}

	return dsnBuilder.String(), nil
}

// GetGlobalStats retrieves global status variables from MySQL.
func (c *mySQLClient) GetGlobalStats() (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	rows, err := c.db.Query("SHOW GLOBAL STATUS")
	if err != nil {
		return nil, fmt.Errorf("failed to query global status: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		stats[name] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return stats, nil
}

// GetGlobalVariables retrieves global configuration variables from MySQL.
func (c *mySQLClient) GetGlobalVariables() (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	rows, err := c.db.Query("SHOW GLOBAL VARIABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to query global variables: %w", err)
	}
	defer rows.Close()

	variables := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		variables[name] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return variables, nil
}

// GetReplicationStatus retrieves slave replication status from MySQL.
// Tries SHOW REPLICA STATUS first (MySQL 8.0.22+), falls back to SHOW SLAVE STATUS for older versions.
func (c *mySQLClient) GetReplicationStatus() (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Try new syntax first (MySQL 8.0.22+)
	rows, err := c.db.Query("SHOW REPLICA STATUS")
	if err != nil {
		// Fall back to old syntax for older MySQL versions
		rows, err = c.db.Query("SHOW SLAVE STATUS")
		if err != nil {
			return nil, fmt.Errorf("failed to query replication status: %w", err)
		}
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Create a slice of interface{} to hold each column's value
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	status := make(map[string]string)

	// Fetch the first (and should be only) row
	if rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to strings and populate the map
		for i, col := range columns {
			val := values[i]
			if val == nil {
				status[col] = ""
			} else {
				switch v := val.(type) {
				case []byte:
					status[col] = string(v)
				case string:
					status[col] = v
				default:
					status[col] = fmt.Sprintf("%v", v)
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return status, nil
}

// GetVersion retrieves the MySQL server version.
func (c *mySQLClient) GetVersion() (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("database connection not established")
	}

	var version string
	err := c.db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to query version: %w", err)
	}

	return version, nil
}

// Close closes the database connection.
func (c *mySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
