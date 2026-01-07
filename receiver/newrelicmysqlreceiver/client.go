// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

type client interface {
	Connect() error
	getGlobalStats() (map[string]string, error)
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

func (c *mySQLClient) getGlobalStats() (map[string]string, error) {
	query := "SHOW GLOBAL STATUS"
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		stats[name] = value
	}

	return stats, rows.Err()
}

func (c *mySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
