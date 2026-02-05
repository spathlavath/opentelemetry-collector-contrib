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

	// Method 1: Try performance_schema.threads (most reliable, MySQL 5.7+)
	var count int
	err := c.db.QueryRow(`SELECT COUNT(*) 
		FROM performance_schema.threads 
		WHERE PROCESSLIST_COMMAND LIKE 'Binlog Dump%'`).Scan(&count)

	if err != nil {
		// Method 2: Fallback to INFORMATION_SCHEMA.PROCESSLIST
		err = c.db.QueryRow(`SELECT COUNT(*) 
			FROM INFORMATION_SCHEMA.PROCESSLIST 
			WHERE COMMAND LIKE 'Binlog Dump%'`).Scan(&count)

		if err != nil {
			// Method 3: Try performance_schema.global_status (for restricted privilege users)
			var countStr string
			err = c.db.QueryRow(`SELECT VARIABLE_VALUE 
				FROM performance_schema.global_status 
				WHERE VARIABLE_NAME IN ('Slaves_connected', 'Replicas_connected') 
				LIMIT 1`).Scan(&countStr)
			if err != nil {
				// Not a master, performance_schema disabled, or binary logging not enabled
				// Return empty result without error
				return result, nil
			}
			// Parse the string value to int
			if parsedCount, parseErr := strconv.Atoi(countStr); parseErr == nil {
				count = parsedCount
			} else {
				return result, nil
			}
		}
	}

	// Set the result if we have connected replicas
	if count > 0 {
		countStr := strconv.Itoa(count)
		result["Slaves_Connected"] = countStr
		result["Replicas_Connected"] = countStr
	}

	return result, nil
}

// GetGroupReplicationStats retrieves group replication statistics from performance schema.
// Queries per-server metrics filtered by current server UUID.
func (c *mySQLClient) GetGroupReplicationStats() (map[string]string, error) {
	result := make(map[string]string)

	// Check if group replication plugin is active
	var pluginStatus string
	err := c.db.QueryRow(`SELECT plugin_status FROM information_schema.plugins WHERE plugin_name='group_replication'`).Scan(&pluginStatus)
	if err != nil || pluginStatus != "ACTIVE" {
		// Plugin not installed or not active
		return result, nil
	}

	// Try MySQL >= 8.0.2 query first (all 8 columns available)
	// Filters by current server UUID and applier channel
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

	row := c.db.QueryRow(query)
	err = row.Scan(&inQueue, &checked, &conflicts, &validating, &remoteInQueue, &remoteApplied, &proposed, &rollback)

	// If MySQL >= 8.0.2 query fails, try MySQL < 8.0.2 fallback (only 4 columns)
	if err != nil {
		// MySQL < 8.0.2 query (only 4 columns available)
		queryLegacy := `SELECT 
                        COUNT_TRANSACTIONS_IN_QUEUE,
                        COUNT_TRANSACTIONS_CHECKED,
                        COUNT_CONFLICTS_DETECTED,
                        COUNT_TRANSACTIONS_ROWS_VALIDATING
                      FROM performance_schema.replication_group_member_stats
                      WHERE member_id = @@server_uuid
                        AND channel_name = 'group_replication_applier'`

		row = c.db.QueryRow(queryLegacy)
		err = row.Scan(&inQueue, &checked, &conflicts, &validating)
		if err != nil {
			// Table might not exist or no matching rows
			return result, nil
		}

		// MySQL < 8.0.2: Only 4 metrics available
		if inQueue.Valid {
			result["group_replication_transactions"] = strconv.FormatInt(inQueue.Int64, 10)
		}
		if checked.Valid {
			result["group_replication_transactions_check"] = strconv.FormatInt(checked.Int64, 10)
		}
		if conflicts.Valid {
			result["group_replication_conflicts_detected"] = strconv.FormatInt(conflicts.Int64, 10)
		}
		if validating.Valid {
			result["group_replication_transactions_validating"] = strconv.FormatInt(validating.Int64, 10)
		}

		return result, nil
	}

	// MySQL >= 8.0.2: All 8 metrics available
	if inQueue.Valid {
		result["group_replication_transactions"] = strconv.FormatInt(inQueue.Int64, 10)
	}
	if checked.Valid {
		result["group_replication_transactions_check"] = strconv.FormatInt(checked.Int64, 10)
	}
	if conflicts.Valid {
		result["group_replication_conflicts_detected"] = strconv.FormatInt(conflicts.Int64, 10)
	}
	if validating.Valid {
		result["group_replication_transactions_validating"] = strconv.FormatInt(validating.Int64, 10)
	}
	if remoteInQueue.Valid {
		result["group_replication_transactions_in_applier_queue"] = strconv.FormatInt(remoteInQueue.Int64, 10)
	}
	if remoteApplied.Valid {
		result["group_replication_transactions_applied"] = strconv.FormatInt(remoteApplied.Int64, 10)
	}
	if proposed.Valid {
		result["group_replication_transactions_proposed"] = strconv.FormatInt(proposed.Int64, 10)
	}
	if rollback.Valid {
		result["group_replication_transactions_rollback"] = strconv.FormatInt(rollback.Int64, 10)
	}

	return result, nil
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
