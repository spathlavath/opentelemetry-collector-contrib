// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/queries"
)

type client interface {
	Connect() error
	GetGlobalStats() (map[string]string, error)
	GetSlowQueries(ctx context.Context, intervalSeconds int) ([]models.SlowQuery, error)
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

func (c *mySQLClient) GetGlobalStats() (map[string]string, error) {
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

// GetSlowQueries fetches slow query data from performance_schema.events_statements_summary_by_digest
// This method queries MySQL's performance schema to get aggregated query statistics
//
// Parameters:
//   - ctx: Context for query cancellation/timeout
//   - intervalSeconds: Time window in seconds (e.g., 60 = last 60 seconds)
//                      Filters queries by LAST_SEEN >= NOW() - INTERVAL X SECOND
//
// Returns:
//   - []models.SlowQuery: Array of slow query records with cumulative metrics
//   - error: Any error encountered during query execution or scanning
//
// How it works:
// 1. Execute SQL query with time window parameter
// 2. Scan each row into SlowQuery struct
// 3. Return array for delta calculation by scraper
//
// Performance:
// - Typical execution time: < 10ms for 1000s of query patterns
// - Result set limited by time window filter
// - No TOP N filtering here (done in Go after delta calculation)
func (c *mySQLClient) GetSlowQueries(ctx context.Context, intervalSeconds int) ([]models.SlowQuery, error) {
	// Get the SQL query string
	query := queries.GetSlowQueriesSQL

	// Execute query with parameterized time window
	// The ? placeholder is replaced with intervalSeconds
	rows, err := c.db.QueryContext(ctx, query, intervalSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to query slow queries from performance_schema: %w", err)
	}
	defer rows.Close()

	// Pre-allocate slice with reasonable capacity to avoid reallocations
	// Most systems have < 1000 unique query patterns in a 60 second window
	slowQueries := make([]models.SlowQuery, 0, 100)

	// Scan each row into SlowQuery struct
	for rows.Next() {
		var sq models.SlowQuery

		// Scan all fields in the same order as SELECT statement
		// This is critical: field order must match exactly!
		err := rows.Scan(
			// Metadata
			&sq.CollectionTimestamp, // NOW()
			&sq.QueryID,              // DIGEST
			&sq.QueryText,            // DIGEST_TEXT
			&sq.DatabaseName,         // SCHEMA_NAME

			// Execution metrics (cumulative - for delta calculation)
			&sq.ExecutionCount,      // COUNT_STAR
			&sq.TotalElapsedTimeMS,  // SUM_TIMER_WAIT / 1e9
			&sq.TotalLockTimeMS,     // SUM_LOCK_TIME / 1e9

			// Historical averages (from database)
			&sq.AvgElapsedTimeMs,    // AVG_TIMER_WAIT / 1e9
			&sq.AvgLockTimeMs,       // AVG_LOCK_TIME / 1e9
			&sq.TotalCPUTimeMs,      // SUM_CPU_TIME / 1e9 (MySQL 8.0.28+)
			&sq.AvgCPUTimeMs,        // AVG_CPU_TIME / 1e9 (MySQL 8.0.28+)

			// Row metrics
			&sq.TotalRowsExamined,   // SUM_ROWS_EXAMINED
			&sq.AvgRowsExamined,     // AVG_ROWS_EXAMINED
			&sq.TotalRowsSent,       // SUM_ROWS_SENT
			&sq.AvgRowsSent,         // AVG_ROWS_SENT
			&sq.TotalRowsAffected,   // SUM_ROWS_AFFECTED
			&sq.AvgRowsAffected,     // AVG_ROWS_AFFECTED

			// Disk I/O metrics
			&sq.TotalSelectScan,     // SUM_SELECT_SCAN
			&sq.TotalSelectFullJoin, // SUM_SELECT_FULL_JOIN
			&sq.TotalNoIndexUsed,    // SUM_NO_INDEX_USED
			&sq.TotalNoGoodIndexUsed, // SUM_NO_GOOD_INDEX_USED
			&sq.TotalSortRows,       // SUM_SORT_ROWS
			&sq.TotalSortMergePasses, // SUM_SORT_MERGE_PASSES

			// Temporary table metrics
			&sq.TotalTmpTables,       // SUM_CREATED_TMP_TABLES
			&sq.TotalTmpDiskTables,   // SUM_CREATED_TMP_DISK_TABLES

			// Error and warning metrics
			&sq.TotalErrors,          // SUM_ERRORS
			&sq.TotalWarnings,        // SUM_WARNINGS

			// Timestamps
			&sq.FirstSeen,                // FIRST_SEEN
			&sq.LastExecutionTimestamp,   // LAST_SEEN

			// Performance variance (RCA)
			&sq.MinElapsedTimeMs,    // MIN_TIMER_WAIT / 1e9
			&sq.MaxElapsedTimeMs,    // MAX_TIMER_WAIT / 1e9
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan slow query row: %w", err)
		}

		// Add to result set
		slowQueries = append(slowQueries, sq)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating slow query rows: %w", err)
	}

	return slowQueries, nil
}

func (c *mySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
