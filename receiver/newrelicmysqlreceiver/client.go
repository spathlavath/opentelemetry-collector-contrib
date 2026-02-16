// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

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

// GetMasterStatus retrieves master/source status information.
func (c *mySQLClient) GetMasterStatus() (map[string]string, error) {
	result := make(map[string]string)

	count, err := c.queryReplicaCount()
	if err != nil {
		c.logger.Debug("Could not determine replica count", zap.Error(err))
		return result, nil
	}

	if count > 0 {
		countStr := strconv.Itoa(count)
		result["Slaves_Connected"] = countStr
		result["Replicas_Connected"] = countStr
		c.logger.Info("Master replication status detected", zap.Int("replicas_connected", count))
	} else {
		c.logger.Debug("No replicas connected to this master server")
	}

	return result, nil
}

// queryReplicaCount attempts multiple methods to determine the number of connected replicas.
func (c *mySQLClient) queryReplicaCount() (int, error) {
	var count int

	// Method 1: performance_schema.threads (MySQL 5.7+)
	err := c.db.QueryRow(`SELECT COUNT(*)
		FROM performance_schema.threads
		WHERE PROCESSLIST_COMMAND LIKE 'Binlog Dump%'`).Scan(&count)
	if err == nil {
		return count, nil
	}
	c.logger.Debug("Method 1 failed", zap.Error(err))

	// Method 2: INFORMATION_SCHEMA.PROCESSLIST
	err = c.db.QueryRow(`SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.PROCESSLIST
		WHERE COMMAND LIKE 'Binlog Dump%'`).Scan(&count)
	if err == nil {
		return count, nil
	}
	c.logger.Debug("Method 2 failed", zap.Error(err))

	// Method 3: global_status (restricted privileges)
	var countStr string
	err = c.db.QueryRow(`SELECT VARIABLE_VALUE
		FROM performance_schema.global_status
		WHERE VARIABLE_NAME IN ('Slaves_connected', 'Replicas_connected')
		LIMIT 1`).Scan(&countStr)
	if err != nil {
		c.logger.Debug("Method 3 failed", zap.Error(err))
		return 0, fmt.Errorf("all methods failed: %w", err)
	}

	return strconv.Atoi(countStr)
}

// GetGroupReplicationStats retrieves group replication statistics from performance schema.
// Queries per-server metrics filtered by current server UUID.
func (c *mySQLClient) GetGroupReplicationStats() (map[string]string, error) {
	result := make(map[string]string)

	if !c.isGroupReplicationActive() {
		return result, nil
	}

	c.logger.Debug("Group Replication plugin is active, querying metrics")

	stats, err := c.queryGroupReplicationModern()
	if err == nil {
		return stats, nil
	}

	c.logger.Debug("Modern query failed, trying legacy", zap.Error(err))
	return c.queryGroupReplicationLegacy()
}

// isGroupReplicationActive checks if the Group Replication plugin is active.
func (c *mySQLClient) isGroupReplicationActive() bool {
	var pluginStatus string
	err := c.db.QueryRow(`SELECT plugin_status
		FROM information_schema.plugins
		WHERE plugin_name='group_replication'`).Scan(&pluginStatus)

	active := err == nil && pluginStatus == "ACTIVE"
	if !active {
		c.logger.Debug("Group Replication plugin not active or not installed")
	}
	return active
}

// queryGroupReplicationModern queries Group Replication stats for MySQL >= 8.0.2.
// Returns all 8 available metrics.
func (c *mySQLClient) queryGroupReplicationModern() (map[string]string, error) {
	result := make(map[string]string)

	query := `SELECT
		COUNT_TRANSACTIONS_IN_QUEUE,
		COUNT_TRANSACTIONS_CHECKED,
		COUNT_CONFLICTS_DETECTED,
		COUNT_TRANSACTIONS_ROWS_VALIDATING,
		COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE,
		COUNT_TRANSACTIONS_REMOTE_APPLIED,
		COUNT_TRANSACTIONS_LOCAL_PROPOSED,
		COUNT_TRANSACTIONS_LOCAL_ROLLBACK
	FROM performance_schema.replication_group_member_stats
	WHERE member_id = @@server_uuid
		AND channel_name = 'group_replication_applier'`

	var (
		inQueue, checked, conflicts, validating          sql.NullInt64
		remoteInQueue, remoteApplied, proposed, rollback sql.NullInt64
	)

	err := c.db.QueryRow(query).Scan(&inQueue, &checked, &conflicts, &validating,
		&remoteInQueue, &remoteApplied, &proposed, &rollback)
	if err != nil {
		return nil, fmt.Errorf("modern query failed: %w", err)
	}

	c.logger.Debug("Using MySQL >= 8.0.2 query, collected 8 Group Replication metrics")

	// Set all 8 metrics
	setIfValid(result, "group_replication_transactions", inQueue)
	setIfValid(result, "group_replication_transactions_check", checked)
	setIfValid(result, "group_replication_conflicts_detected", conflicts)
	setIfValid(result, "group_replication_transactions_validating", validating)
	setIfValid(result, "group_replication_transactions_in_applier_queue", remoteInQueue)
	setIfValid(result, "group_replication_transactions_applied", remoteApplied)
	setIfValid(result, "group_replication_transactions_proposed", proposed)
	setIfValid(result, "group_replication_transactions_rollback", rollback)

	c.logger.Info("Group Replication metrics collected (MySQL >= 8.0.2)", zap.Int("metric_count", len(result)))

	return result, nil
}

// queryGroupReplicationLegacy queries Group Replication stats for MySQL < 8.0.2.
// Returns only 4 available metrics.
func (c *mySQLClient) queryGroupReplicationLegacy() (map[string]string, error) {
	result := make(map[string]string)

	query := `SELECT
		COUNT_TRANSACTIONS_IN_QUEUE,
		COUNT_TRANSACTIONS_CHECKED,
		COUNT_CONFLICTS_DETECTED,
		COUNT_TRANSACTIONS_ROWS_VALIDATING
	FROM performance_schema.replication_group_member_stats
	WHERE member_id = @@server_uuid
		AND channel_name = 'group_replication_applier'`

	var inQueue, checked, conflicts, validating sql.NullInt64

	err := c.db.QueryRow(query).Scan(&inQueue, &checked, &conflicts, &validating)
	if err != nil {
		// Table might not exist or no matching rows
		c.logger.Debug("Group Replication stats table not found or no data for this server")
		return result, nil
	}

	c.logger.Debug("Using MySQL < 8.0.2 query, collected 4 Group Replication metrics")

	// Set only 4 metrics available in legacy version
	setIfValid(result, "group_replication_transactions", inQueue)
	setIfValid(result, "group_replication_transactions_check", checked)
	setIfValid(result, "group_replication_conflicts_detected", conflicts)
	setIfValid(result, "group_replication_transactions_validating", validating)

	return result, nil
}

// setIfValid is a helper to reduce boilerplate when setting map values from sql.NullInt64.
func setIfValid(result map[string]string, key string, value sql.NullInt64) {
	if value.Valid {
		result[key] = strconv.FormatInt(value.Int64, 10)
	}
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
