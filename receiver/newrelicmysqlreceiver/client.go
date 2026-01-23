// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
)

type client interface {
	Connect() error
	getGlobalStats() (map[string]int64, error)
	getGlobalVariables() (map[string]int64, error)
	getReplicationStatus() (map[string]string, error)
	getVersion() (string, error)
	Close() error
}

type mySQLClient struct {
	connStr string
	db      *sql.DB
}

var _ client = (*mySQLClient)(nil)

func newMySQLClient(cfg *Config) (client, error) {
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
		driverConf.TLSConfig = "custom"
		if err := mysql.RegisterTLSConfig("custom", tlsConfig); err != nil {
			return nil, fmt.Errorf("failed to register TLS config: %w", err)
		}
	}

	connStr := driverConf.FormatDSN()

	return &mySQLClient{
		connStr: connStr,
	}, nil
}

func (c *mySQLClient) Connect() error {
	clientDB, err := sql.Open("mysql", c.connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	c.db = clientDB
	return c.db.Ping()
}

func (c *mySQLClient) getGlobalStats() (map[string]int64, error) {
	query := "SHOW GLOBAL STATUS"
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make(map[string]int64)
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
		stats[name] = intValue
	}

	return stats, rows.Err()
}

func (c *mySQLClient) getGlobalVariables() (map[string]int64, error) {
	query := "SHOW GLOBAL VARIABLES"
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	vars := make(map[string]int64)
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
		vars[name] = intValue
	}

	return vars, rows.Err()
}

func (c *mySQLClient) getReplicationStatus() (map[string]string, error) {
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

func (c *mySQLClient) getVersion() (string, error) {
	var version string
	err := c.db.QueryRow("SELECT VERSION()").Scan(&version)
	return version, err
}

func (c *mySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
