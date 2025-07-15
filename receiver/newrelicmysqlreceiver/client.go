// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

// MySQLClient represents a MySQL database client with New Relic monitoring capabilities
type MySQLClient struct {
	db     *sql.DB
	config *Config
	logger *zap.Logger
}

// NewMySQLClient creates a new MySQL client instance
func NewMySQLClient(config *Config, logger *zap.Logger) (*MySQLClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	return &MySQLClient{
		config: config,
		logger: logger,
	}, nil
}

// Connect establishes a connection to the MySQL database
func (c *MySQLClient) Connect(ctx context.Context) error {
	dsn := c.config.GetDSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(ctx, c.config.ConnectTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	c.db = db
	c.logger.Info("Successfully connected to MySQL database")
	return nil
}

// Close closes the database connection
func (c *MySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// GetVersion retrieves MySQL server version
func (c *MySQLClient) GetVersion(ctx context.Context) (*MySQLVersion, error) {
	query := "SELECT VERSION(), @@version_comment"

	row := c.db.QueryRowContext(ctx, query)

	var version MySQLVersion
	err := row.Scan(&version.Version, &version.Comment)
	if err != nil {
		return nil, fmt.Errorf("failed to get MySQL version: %w", err)
	}

	return &version, nil
}

// GetGlobalStatus retrieves MySQL global status variables
func (c *MySQLClient) GetGlobalStatus(ctx context.Context) (GlobalStatus, error) {
	query := "SHOW GLOBAL STATUS"

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query global status: %w", err)
	}
	defer rows.Close()

	status := make(GlobalStatus)

	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			c.logger.Warn("Failed to scan status row", zap.Error(err))
			continue
		}
		status[name] = value
	}

	return status, rows.Err()
}

// Data type definitions for New Relic query patterns

// MySQLVersion represents MySQL version information
type MySQLVersion struct {
	Version string
	Comment string
}

// GlobalStatus represents MySQL global status variables
type GlobalStatus map[string]string

// SlowQueryData represents slow query metrics following New Relic patterns
type SlowQueryData struct {
	// Original fields expected by scraper (maintain compatibility)
	DigestText      string `json:"digest_text"`
	CountStar       int64  `json:"count_star"`
	Digest          string `json:"digest"`
	Schema          string `json:"schema"`
	SumLockTime     int64  `json:"sum_lock_time"`
	SumRowsSent     int64  `json:"sum_rows_sent"`
	SumRowsExamined int64  `json:"sum_rows_examined"`

	// New Relic fields matching nri-mysql models.go exactly
	QueryID                string  `json:"query_id"`
	QueryText              string  `json:"query_text"`
	DatabaseName           string  `json:"database_name"`
	SchemaName             string  `json:"schema_name"`
	ExecutionCount         int64   `json:"execution_count"`
	AvgCPUTimeMs           float64 `json:"avg_cpu_time_ms"`
	AvgElapsedTimeMs       float64 `json:"avg_elapsed_time_ms"`
	AvgDiskReads           float64 `json:"avg_disk_reads"`
	AvgDiskWrites          float64 `json:"avg_disk_writes"`
	HasFullTableScan       string  `json:"has_full_table_scan"`
	StatementType          string  `json:"statement_type"`
	LastExecutionTimestamp string  `json:"last_execution_timestamp"`
	CollectionTimestamp    string  `json:"collection_timestamp"`
}

// CurrentRunningQueryData represents currently executing queries
type CurrentRunningQueryData struct {
	QueryID         string  `json:"query_id"`
	QueryText       string  `json:"query_text"`
	QuerySampleText string  `json:"query_sample_text"`
	EventID         int64   `json:"event_id"`
	ThreadID        int64   `json:"thread_id"`
	ExecutionTimeMs float64 `json:"execution_time_ms"`
	RowsSent        int64   `json:"rows_sent"`
	RowsExamined    int64   `json:"rows_examined"`
	DatabaseName    string  `json:"database_name"`
}

// WaitEventData represents wait event metrics with New Relic categorization
type WaitEventData struct {
	// Original fields expected by scraper (maintain compatibility)
	EventName    string `json:"event_name"`
	SumTimerWait int64  `json:"sum_timer_wait"`
	CountStar    int64  `json:"count_star"`

	// New Relic fields matching nri-mysql patterns
	QueryID             string  `json:"query_id"`
	DatabaseName        string  `json:"database_name"`
	WaitEventName       string  `json:"wait_event_name"`
	WaitCategory        string  `json:"wait_category"`
	TotalWaitTimeMs     float64 `json:"total_wait_time_ms"`
	WaitEventCount      int64   `json:"wait_event_count"`
	AvgWaitTimeMs       float64 `json:"avg_wait_time_ms"`
	QueryText           string  `json:"query_text"`
	CollectionTimestamp string  `json:"collection_timestamp"`
}

// BlockingSessionData represents detailed blocking session information
type BlockingSessionData struct {
	// Original fields expected by scraper (maintain compatibility)
	BlockingSessionID int64 `json:"blocking_session_id"`
	BlockedSessionID  int64 `json:"blocked_session_id"`
	WaitTime          int64 `json:"wait_time"`

	// New Relic fields matching nri-mysql patterns
	BlockedTxnID         string  `json:"blocked_txn_id"`
	BlockedThreadID      int64   `json:"blocked_thread_id"`
	BlockedPID           int64   `json:"blocked_pid"`
	BlockedHost          string  `json:"blocked_host"`
	DatabaseName         string  `json:"database_name"`
	BlockedStatus        string  `json:"blocked_status"`
	BlockingTxnID        string  `json:"blocking_txn_id"`
	BlockingThreadID     int64   `json:"blocking_thread_id"`
	BlockingPID          int64   `json:"blocking_pid"`
	BlockingHost         string  `json:"blocking_host"`
	BlockedQuery         string  `json:"blocked_query"`
	BlockingQuery        string  `json:"blocking_query"`
	BlockedQueryID       string  `json:"blocked_query_id"`
	BlockingQueryID      string  `json:"blocking_query_id"`
	BlockingStatus       string  `json:"blocking_status"`
	BlockedQueryTimeMs   float64 `json:"blocked_query_time_ms"`
	BlockingQueryTimeMs  float64 `json:"blocking_query_time_ms"`
	BlockedTxnStartTime  string  `json:"blocked_txn_start_time"`
	BlockingTxnStartTime string  `json:"blocking_txn_start_time"`
	CollectionTimestamp  string  `json:"collection_timestamp"`
}

// New Relic Query Methods with Threshold Validation

// GetSlowQueries retrieves slow queries based on New Relic patterns with threshold validation
func (c *MySQLClient) GetSlowQueries(ctx context.Context, intervalSeconds int, excludedDatabases []string, limit int) ([]SlowQueryData, error) {
	// Apply threshold validation from New Relic patterns
	validIntervalSeconds := c.validateSlowQueryInterval(intervalSeconds)
	validLimit := c.validateQueryCountThreshold(limit)
	validExcludedDatabases := c.validateExcludedDatabases(excludedDatabases)
	excludedDBList := "'" + strings.Join(validExcludedDatabases, "','") + "'"
	query := `
        SELECT
			DIGEST AS query_id,
			CASE
				WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
				ELSE DIGEST_TEXT
			END AS query_text,
			SCHEMA_NAME AS database_name,
			'N/A' AS schema_name,
			COUNT_STAR AS execution_count,
			ROUND((COALESCE(SUM_CPU_TIME, 0) / COUNT_STAR) / 1000000000, 3) AS avg_cpu_time_ms,
			ROUND((SUM_TIMER_WAIT / COUNT_STAR) / 1000000000, 3) AS avg_elapsed_time_ms,
			SUM_ROWS_EXAMINED / COUNT_STAR AS avg_disk_reads,
			SUM_ROWS_AFFECTED / COUNT_STAR AS avg_disk_writes,
			CASE
				WHEN SUM_NO_INDEX_USED > 0 THEN 'Yes'
				ELSE 'No'
			END AS has_full_table_scan,
			CASE
				WHEN DIGEST_TEXT LIKE 'SELECT%' THEN 'SELECT'
				WHEN DIGEST_TEXT LIKE 'INSERT%' THEN 'INSERT'
				WHEN DIGEST_TEXT LIKE 'UPDATE%' THEN 'UPDATE'
				WHEN DIGEST_TEXT LIKE 'DELETE%' THEN 'DELETE'
				ELSE 'OTHER'
			END AS statement_type,
			DATE_FORMAT(LAST_SEEN, '%Y-%m-%dT%H:%i:%sZ') AS last_execution_timestamp,
			DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp,
			-- Additional fields for scraper compatibility
			DIGEST_TEXT,
			DIGEST,
			SCHEMA_NAME,
			COUNT_STAR,
			COALESCE(SUM_LOCK_TIME, 0) AS sum_lock_time,
			SUM_ROWS_SENT,
			SUM_ROWS_EXAMINED
		FROM performance_schema.events_statements_summary_by_digest
		WHERE LAST_SEEN >= UTC_TIMESTAMP() - INTERVAL ? SECOND
			AND SCHEMA_NAME IS NOT NULL
			AND SCHEMA_NAME NOT IN (` + excludedDBList + `)
		ORDER BY avg_elapsed_time_ms DESC
		LIMIT ?`

	rows, err := c.db.QueryContext(ctx, query, validIntervalSeconds, validLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query slow queries: %w", err)
	}
	defer rows.Close()

	var results []SlowQueryData
	for rows.Next() {
		var data SlowQueryData
		// Scan all fields including the additional ones for scraper compatibility
		err := rows.Scan(
			&data.QueryID, &data.QueryText, &data.DatabaseName, &data.SchemaName,
			&data.ExecutionCount, &data.AvgCPUTimeMs, &data.AvgElapsedTimeMs,
			&data.AvgDiskReads, &data.AvgDiskWrites, &data.HasFullTableScan,
			&data.StatementType, &data.LastExecutionTimestamp, &data.CollectionTimestamp,
			// Additional fields for scraper compatibility
			&data.DigestText, &data.Digest, &data.Schema, &data.CountStar,
			&data.SumLockTime, &data.SumRowsSent, &data.SumRowsExamined,
		)
		if err != nil {
			c.logger.Warn("Failed to scan slow query row", zap.Error(err))
			continue
		}

		// Apply post-query filtering based on configuration thresholds
		if c.meetsSlowQueryCriteria(data) {
			results = append(results, data)
		}
	}

	return results, rows.Err()
}

// GetCurrentRunningQueries retrieves currently running queries matching a digest with threshold validation
func (c *MySQLClient) GetCurrentRunningQueries(ctx context.Context, digest string, minExecutionTimeSeconds int, limit int) ([]CurrentRunningQueryData, error) {
	// Apply threshold validation
	validMinExecutionTime := c.validateQueryResponseTimeThreshold(minExecutionTimeSeconds)
	validLimit := c.validateQueryCountThreshold(limit)

	query := `
		SELECT
			DIGEST AS query_id,
			CASE
				WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
				ELSE DIGEST_TEXT
			END AS query_text,
			SQL_TEXT AS query_sample_text,
			EVENT_ID AS event_id,
			THREAD_ID AS thread_id,
			ROUND(TIMER_WAIT / 1000000000, 3) AS execution_time_ms,
			ROWS_SENT AS rows_sent,
			ROWS_EXAMINED AS rows_examined,
			CURRENT_SCHEMA AS database_name
		FROM performance_schema.events_statements_current
		WHERE DIGEST = ?
			AND TIMER_WAIT / 1000000000 > ?
			AND CURRENT_SCHEMA IS NOT NULL
		ORDER BY TIMER_WAIT DESC
		LIMIT ?`

	rows, err := c.db.QueryContext(ctx, query, digest, validMinExecutionTime, validLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query current running queries: %w", err)
	}
	defer rows.Close()

	var results []CurrentRunningQueryData
	for rows.Next() {
		var data CurrentRunningQueryData
		err := rows.Scan(
			&data.QueryID, &data.QueryText, &data.QuerySampleText,
			&data.EventID, &data.ThreadID, &data.ExecutionTimeMs,
			&data.RowsSent, &data.RowsExamined, &data.DatabaseName,
		)
		if err != nil {
			c.logger.Warn("Failed to scan current running query row", zap.Error(err))
			continue
		}
		results = append(results, data)
	}

	return results, rows.Err()
}

// GetRecentQueries retrieves recent queries from history matching a digest
func (c *MySQLClient) GetRecentQueries(ctx context.Context, digest string, minExecutionTimeSeconds int, limit int) ([]CurrentRunningQueryData, error) {
	// Apply threshold validation
	validMinExecutionTime := c.validateQueryResponseTimeThreshold(minExecutionTimeSeconds)
	validLimit := c.validateQueryCountThreshold(limit)

	query := `
		SELECT
			DIGEST AS query_id,
			CASE
				WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
				ELSE DIGEST_TEXT
			END AS query_text,
			SQL_TEXT AS query_sample_text,
			EVENT_ID AS event_id,
			THREAD_ID AS thread_id,
			ROUND(TIMER_WAIT / 1000000000, 3) AS execution_time_ms,
			ROWS_SENT AS rows_sent,
			ROWS_EXAMINED AS rows_examined,
			CURRENT_SCHEMA AS database_name
		FROM performance_schema.events_statements_history
		WHERE DIGEST = ?
			AND TIMER_WAIT / 1000000000 > ?
			AND CURRENT_SCHEMA IS NOT NULL
		ORDER BY TIMER_WAIT DESC
		LIMIT ?`

	rows, err := c.db.QueryContext(ctx, query, digest, validMinExecutionTime, validLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent queries: %w", err)
	}
	defer rows.Close()

	var results []CurrentRunningQueryData
	for rows.Next() {
		var data CurrentRunningQueryData
		err := rows.Scan(
			&data.QueryID, &data.QueryText, &data.QuerySampleText,
			&data.EventID, &data.ThreadID, &data.ExecutionTimeMs,
			&data.RowsSent, &data.RowsExamined, &data.DatabaseName,
		)
		if err != nil {
			c.logger.Warn("Failed to scan recent query row", zap.Error(err))
			continue
		}
		results = append(results, data)
	}

	return results, rows.Err()
}

// GetPastQueries retrieves past long-running queries from history_long
func (c *MySQLClient) GetPastQueries(ctx context.Context, digest string, minExecutionTimeSeconds int, limit int) ([]CurrentRunningQueryData, error) {
	// Apply threshold validation
	validMinExecutionTime := c.validateQueryResponseTimeThreshold(minExecutionTimeSeconds)
	validLimit := c.validateQueryCountThreshold(limit)

	query := `
		SELECT
			DIGEST AS query_id,
			CASE
				WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
				ELSE DIGEST_TEXT
			END AS query_text,
			SQL_TEXT AS query_sample_text,
			EVENT_ID AS event_id,
			THREAD_ID AS thread_id,
			ROUND(TIMER_WAIT / 1000000000, 3) AS execution_time_ms,
			ROWS_SENT AS rows_sent,
			ROWS_EXAMINED AS rows_examined,
			CURRENT_SCHEMA AS database_name
		FROM performance_schema.events_statements_history_long
		WHERE DIGEST = ?
			AND TIMER_WAIT / 1000000000 > ?
			AND CURRENT_SCHEMA IS NOT NULL
		ORDER BY TIMER_WAIT DESC
		LIMIT ?`

	rows, err := c.db.QueryContext(ctx, query, digest, validMinExecutionTime, validLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query past queries: %w", err)
	}
	defer rows.Close()

	var results []CurrentRunningQueryData
	for rows.Next() {
		var data CurrentRunningQueryData
		err := rows.Scan(
			&data.QueryID, &data.QueryText, &data.QuerySampleText,
			&data.EventID, &data.ThreadID, &data.ExecutionTimeMs,
			&data.RowsSent, &data.RowsExamined, &data.DatabaseName,
		)
		if err != nil {
			c.logger.Warn("Failed to scan past query row", zap.Error(err))
			continue
		}
		results = append(results, data)
	}

	return results, rows.Err()
}

// GetWaitEvents retrieves wait events with categorization and threshold validation
func (c *MySQLClient) GetWaitEvents(ctx context.Context, excludedDatabases []string, limit int) ([]WaitEventData, error) {
	// Apply threshold validation
	validLimit := c.validateQueryCountThreshold(limit)
	validExcludedDatabases := c.validateExcludedDatabases(excludedDatabases)
	excludedDBList := "'" + strings.Join(validExcludedDatabases, "','") + "'"

	query := `
		WITH wait_data_aggregated AS (
			SELECT
				w.THREAD_ID,
				w.EVENT_NAME AS wait_event_name,
				SUM(w.TIMER_WAIT) AS total_wait_time,
				COUNT(*) AS wait_event_count
			FROM performance_schema.events_waits_current w
			WHERE w.TIMER_WAIT > 1000000 -- Filter out events less than 1ms
			GROUP BY w.THREAD_ID, w.EVENT_NAME
			UNION ALL
			SELECT
				w.THREAD_ID,
				w.EVENT_NAME AS wait_event_name,
				SUM(w.TIMER_WAIT) AS total_wait_time,
				COUNT(*) AS wait_event_count
			FROM performance_schema.events_waits_history w
			WHERE w.TIMER_WAIT > 1000000 -- Filter out events less than 1ms
			GROUP BY w.THREAD_ID, w.EVENT_NAME
		),
		schema_data AS (
			SELECT
				s.THREAD_ID,
				s.DIGEST AS query_id,
				s.CURRENT_SCHEMA AS database_name,
				s.DIGEST_TEXT AS query_text
			FROM performance_schema.events_statements_current s
			WHERE s.CURRENT_SCHEMA NOT IN (` + excludedDBList + `)
			UNION ALL
			SELECT
				s.THREAD_ID,
				s.DIGEST AS query_id,
				s.CURRENT_SCHEMA AS database_name,
				s.DIGEST_TEXT AS query_text
			FROM performance_schema.events_statements_history s
			WHERE s.CURRENT_SCHEMA NOT IN (` + excludedDBList + `)
		)
		SELECT
			sd.query_id,
			sd.database_name,
			wd.wait_event_name,
			CASE
				WHEN wd.wait_event_name LIKE 'wait/io/file/innodb/%' THEN 'InnoDB File IO'
				WHEN wd.wait_event_name LIKE 'wait/io/file/sql/%' THEN 'SQL File IO'
				WHEN wd.wait_event_name LIKE 'wait/io/socket/%' THEN 'Network IO'
				WHEN wd.wait_event_name LIKE 'wait/synch/cond/%' THEN 'Condition Wait'
				WHEN wd.wait_event_name LIKE 'wait/synch/mutex/%' THEN 'Mutex'
				WHEN wd.wait_event_name LIKE 'wait/lock/table/%' THEN 'Table Lock'
				WHEN wd.wait_event_name LIKE 'wait/lock/metadata/%' THEN 'Metadata Lock'
				WHEN wd.wait_event_name LIKE 'wait/lock/transaction/%' THEN 'Transaction Lock'
				ELSE 'Other'
			END AS wait_category,
			ROUND(SUM(wd.total_wait_time) / 1000000000, 3) AS total_wait_time_ms,
			SUM(wd.wait_event_count) AS wait_event_count,
			ROUND(SUM(wd.total_wait_time) / 1000000000 / SUM(wd.wait_event_count), 3) AS avg_wait_time_ms,
			CASE
				WHEN CHAR_LENGTH(sd.query_text) > 4000 THEN CONCAT(LEFT(sd.query_text, 3997), '...')
				ELSE sd.query_text
			END AS query_text,
			DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
		FROM wait_data_aggregated wd
		JOIN schema_data sd ON wd.THREAD_ID = sd.THREAD_ID
		WHERE sd.query_id IS NOT NULL
		GROUP BY sd.query_id, sd.database_name, wd.wait_event_name, wait_category, sd.query_text
		HAVING total_wait_time_ms >= 1.0 -- Only include events with at least 1ms total wait time
		ORDER BY total_wait_time_ms DESC
		LIMIT ?`

	rows, err := c.db.QueryContext(ctx, query, validLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query wait events: %w", err)
	}
	defer rows.Close()

	var results []WaitEventData
	for rows.Next() {
		var data WaitEventData
		err := rows.Scan(
			&data.QueryID, &data.DatabaseName, &data.WaitEventName,
			&data.WaitCategory, &data.TotalWaitTimeMs, &data.WaitEventCount,
			&data.AvgWaitTimeMs, &data.QueryText, &data.CollectionTimestamp,
		)
		if err != nil {
			c.logger.Warn("Failed to scan wait event row", zap.Error(err))
			continue
		}

		// Populate compatibility fields expected by scraper
		data.EventName = data.WaitEventName
		data.SumTimerWait = int64(data.TotalWaitTimeMs * 1000000) // Convert to nanoseconds
		data.CountStar = data.WaitEventCount

		// Apply post-query filtering based on configuration thresholds
		if c.meetsWaitEventCriteria(data.WaitEventName, data.TotalWaitTimeMs) {
			results = append(results, data)
		}
	}

	return results, rows.Err()
}

// GetBlockingSessions retrieves detailed blocking session information with threshold validation
func (c *MySQLClient) GetBlockingSessions(ctx context.Context, excludedDatabases []string, limit int) ([]BlockingSessionData, error) {
	// Apply threshold validation
	validLimit := c.validateQueryCountThreshold(limit)
	validExcludedDatabases := c.validateExcludedDatabases(excludedDatabases)
	excludedDBList := "'" + strings.Join(validExcludedDatabases, "','") + "'"

	// Get minimum wait time threshold from configuration (in seconds)
	minWaitTimeSeconds := c.config.BlockingSessionsConfig.MinWaitTime.Seconds()

	query := `
		SELECT 
			r.trx_id AS blocked_txn_id,
			r.trx_mysql_thread_id AS blocked_thread_id,
			wt.PROCESSLIST_ID AS blocked_pid,
			wt.PROCESSLIST_HOST AS blocked_host,
			wt.PROCESSLIST_DB AS database_name,
			wt.PROCESSLIST_STATE AS blocked_status,
			b.trx_id AS blocking_txn_id,
			b.trx_mysql_thread_id AS blocking_thread_id,
			bt.PROCESSLIST_ID AS blocking_pid,
			bt.PROCESSLIST_HOST AS blocking_host,
			es_waiting.DIGEST_TEXT AS blocked_query,
			es_blocking.DIGEST_TEXT AS blocking_query,
			es_waiting.DIGEST AS blocked_query_id,
			es_blocking.DIGEST AS blocking_query_id,
			bt.PROCESSLIST_STATE AS blocking_status,
			ROUND(esc_waiting.TIMER_WAIT / 1000000000, 3) AS blocked_query_time_ms,
			ROUND(esc_blocking.TIMER_WAIT / 1000000000, 3) AS blocking_query_time_ms,
			DATE_FORMAT(CONVERT_TZ(r.trx_started, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%sZ') AS blocked_txn_start_time,
			DATE_FORMAT(CONVERT_TZ(b.trx_started, @@session.time_zone, '+00:00'), '%Y-%m-%dT%H:%i:%sZ') AS blocking_txn_start_time,
			DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%dT%H:%i:%sZ') AS collection_timestamp
		FROM performance_schema.data_lock_waits w
		JOIN performance_schema.threads wt ON wt.THREAD_ID = w.REQUESTING_THREAD_ID
		JOIN information_schema.innodb_trx r ON r.trx_mysql_thread_id = wt.PROCESSLIST_ID
		JOIN performance_schema.threads bt ON bt.THREAD_ID = w.BLOCKING_THREAD_ID
		JOIN information_schema.innodb_trx b ON b.trx_mysql_thread_id = bt.PROCESSLIST_ID
		JOIN performance_schema.events_statements_current esc_waiting ON esc_waiting.THREAD_ID = wt.THREAD_ID
		JOIN performance_schema.events_statements_summary_by_digest es_waiting ON esc_waiting.DIGEST = es_waiting.DIGEST
		JOIN performance_schema.events_statements_current esc_blocking ON esc_blocking.THREAD_ID = bt.THREAD_ID
		JOIN performance_schema.events_statements_summary_by_digest es_blocking ON esc_blocking.DIGEST = es_blocking.DIGEST
		WHERE wt.PROCESSLIST_DB IS NOT NULL
			AND wt.PROCESSLIST_DB NOT IN (` + excludedDBList + `)
			AND TIMESTAMPDIFF(SECOND, r.trx_started, NOW()) >= ?
		ORDER BY blocked_txn_start_time ASC
		LIMIT ?`

	rows, err := c.db.QueryContext(ctx, query, minWaitTimeSeconds, validLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to query blocking sessions: %w", err)
	}
	defer rows.Close()

	var results []BlockingSessionData
	for rows.Next() {
		var data BlockingSessionData
		err := rows.Scan(
			&data.BlockedTxnID, &data.BlockedThreadID, &data.BlockedPID,
			&data.BlockedHost, &data.DatabaseName, &data.BlockedStatus,
			&data.BlockingTxnID, &data.BlockingThreadID, &data.BlockingPID,
			&data.BlockingHost, &data.BlockedQuery, &data.BlockingQuery,
			&data.BlockedQueryID, &data.BlockingQueryID, &data.BlockingStatus,
			&data.BlockedQueryTimeMs, &data.BlockingQueryTimeMs,
			&data.BlockedTxnStartTime, &data.BlockingTxnStartTime, &data.CollectionTimestamp,
		)
		if err != nil {
			c.logger.Warn("Failed to scan blocking session row", zap.Error(err))
			continue
		}

		// Populate compatibility fields expected by scraper
		data.BlockingSessionID = data.BlockingThreadID
		data.BlockedSessionID = data.BlockedThreadID
		data.WaitTime = int64(data.BlockedQueryTimeMs * 1000) // Convert to milliseconds

		// Apply post-query filtering based on configuration thresholds
		if c.meetsBlockingSessionCriteria(data.BlockedQueryTimeMs) {
			results = append(results, data)
		}
	}

	return results, rows.Err()
}

// Backward compatibility methods for scraper

// GetQueryPerformanceData is a compatibility method that delegates to GetSlowQueries
func (c *MySQLClient) GetQueryPerformanceData(ctx context.Context) ([]SlowQueryData, error) {
	// Use default values for backward compatibility
	return c.GetSlowQueries(ctx, 30, []string{}, c.config.QueryPerformanceConfig.MaxDigests)
}

// GetWaitEventData is a compatibility method that delegates to GetWaitEvents
func (c *MySQLClient) GetWaitEventData(ctx context.Context) ([]WaitEventData, error) {
	// Use default values for backward compatibility
	return c.GetWaitEvents(ctx, []string{}, c.config.WaitEventsConfig.MaxEvents)
}

// GetAdvancedWaitEvents is an alias for GetWaitEvents
func (c *MySQLClient) GetAdvancedWaitEvents(ctx context.Context, excludedDatabases []string, limit int) ([]WaitEventData, error) {
	return c.GetWaitEvents(ctx, excludedDatabases, limit)
}

// GetBlockingSessionData is a compatibility method that delegates to GetBlockingSessions
func (c *MySQLClient) GetBlockingSessionData(ctx context.Context) ([]BlockingSessionData, error) {
	// Use default values for backward compatibility
	return c.GetBlockingSessions(ctx, []string{}, c.config.BlockingSessionsConfig.MaxSessions)
}

// GetAdvancedBlockingSessions is an alias for GetBlockingSessions
func (c *MySQLClient) GetAdvancedBlockingSessions(ctx context.Context, excludedDatabases []string, limit int) ([]BlockingSessionData, error) {
	return c.GetBlockingSessions(ctx, excludedDatabases, limit)
}

// GetSlaveStatus retrieves MySQL slave status - minimal implementation for compatibility
func (c *MySQLClient) GetSlaveStatus(ctx context.Context) (*SlaveStatus, error) {
	query := "SHOW SLAVE STATUS"

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query slave status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil // No slave status (not a slave)
	}

	// For now, return minimal status structure
	// This would need to be enhanced for production use
	return &SlaveStatus{}, nil
}

// SlaveStatus represents MySQL slave status for compatibility
type SlaveStatus struct {
	SlaveIORunning      string
	SlaveSQLRunning     string
	SecondsBehindMaster sql.NullInt64
	MasterHost          string
	MasterPort          int
	ReplicateDoDB       string
	ReplicateIgnoreDB   string
	LastIOError         string
	LastSQLError        string
	LastIOErrorNo       int
	LastSQLErrorNo      int
}

// Helper methods for scraper compatibility

// GetStatusInt64 retrieves an integer status value
func (c *MySQLClient) GetStatusInt64(status GlobalStatus, key string) int64 {
	if val, ok := status[key]; ok {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			return intVal
		}
	}
	return 0
}

// GetStatusFloat64 retrieves a float status value
func (c *MySQLClient) GetStatusFloat64(status GlobalStatus, key string) float64 {
	if val, ok := status[key]; ok {
		if floatVal, err := strconv.ParseFloat(val, 64); err == nil {
			return floatVal
		}
	}
	return 0.0
}

// GetStatusString retrieves a string status value
func (c *MySQLClient) GetStatusString(status GlobalStatus, key string) string {
	if val, ok := status[key]; ok {
		return val
	}
	return ""
}

// HasPerformanceSchema checks if performance_schema is enabled
func (c *MySQLClient) HasPerformanceSchema(ctx context.Context) (bool, error) {
	query := "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'performance_schema'"

	var count int
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check performance_schema: %w", err)
	}

	return count > 0, nil
}

// HasSysSchema checks if sys schema is enabled
func (c *MySQLClient) HasSysSchema(ctx context.Context) (bool, error) {
	query := "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'sys'"

	var count int
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check sys schema: %w", err)
	}

	return count > 0, nil
}

// Threshold validation helper methods following New Relic patterns

// validateSlowQueryInterval validates and returns the appropriate slow query fetch interval
func (c *MySQLClient) validateSlowQueryInterval(threshold int) int {
	const (
		DefaultSlowQueryFetchInterval = 30
		MaxSlowQueryFetchInterval     = 300
	)

	if threshold < 0 {
		c.logger.Warn("Slow query fetch interval threshold is negative, setting to default value",
			zap.Int("provided", threshold), zap.Int("default", DefaultSlowQueryFetchInterval))
		return DefaultSlowQueryFetchInterval
	}
	if threshold > MaxSlowQueryFetchInterval {
		c.logger.Warn("Slow query fetch interval threshold exceeds maximum, setting to maximum value",
			zap.Int("provided", threshold), zap.Int("max", MaxSlowQueryFetchInterval))
		return MaxSlowQueryFetchInterval
	}
	return threshold
}

// validateQueryCountThreshold validates and returns the appropriate query count threshold
func (c *MySQLClient) validateQueryCountThreshold(threshold int) int {
	const (
		DefaultQueryCountThreshold = 20
		MaxQueryCountThreshold     = 100
	)

	if threshold < 0 {
		c.logger.Warn("Query count threshold is negative, setting to default value",
			zap.Int("provided", threshold), zap.Int("default", DefaultQueryCountThreshold))
		return DefaultQueryCountThreshold
	}
	if threshold > MaxQueryCountThreshold {
		c.logger.Warn("Query count threshold exceeds maximum, setting to maximum value",
			zap.Int("provided", threshold), zap.Int("max", MaxQueryCountThreshold))
		return MaxQueryCountThreshold
	}
	return threshold
}

// validateQueryResponseTimeThreshold validates response time threshold
func (c *MySQLClient) validateQueryResponseTimeThreshold(threshold int) int {
	const DefaultQueryResponseTimeThreshold = 500 // milliseconds

	if threshold < 0 {
		c.logger.Warn("Query response time threshold is negative, setting to default value",
			zap.Int("provided", threshold), zap.Int("default", DefaultQueryResponseTimeThreshold))
		return DefaultQueryResponseTimeThreshold
	}
	return threshold
}

// meetsSlowQueryCriteria checks if a query meets the slow query criteria based on configuration thresholds
func (c *MySQLClient) meetsSlowQueryCriteria(data SlowQueryData) bool {
	minQueryTimeMs := float64(c.config.QueryPerformanceConfig.MinQueryTime.Milliseconds())

	// Check if query execution time meets minimum threshold
	if data.AvgElapsedTimeMs < minQueryTimeMs {
		return false
	}

	// Apply additional filtering based on configuration
	if c.config.QueryPerformanceConfig.ExcludeSystemQueries {
		// Exclude common system queries
		systemPatterns := []string{
			"SELECT @@",
			"SHOW ",
			"DESCRIBE ",
			"EXPLAIN ",
			"SET ",
		}

		for _, pattern := range systemPatterns {
			if strings.HasPrefix(strings.ToUpper(data.QueryText), pattern) {
				return false
			}
		}
	}

	return true
}

// meetsWaitEventCriteria checks if a wait event meets collection criteria
func (c *MySQLClient) meetsWaitEventCriteria(eventName string, waitTimeMs float64) bool {
	// Filter by event types if configured
	if len(c.config.WaitEventsConfig.EventTypes) > 0 {
		eventTypeMatched := false
		for _, eventType := range c.config.WaitEventsConfig.EventTypes {
			if strings.Contains(strings.ToLower(eventName), strings.ToLower(eventType)) {
				eventTypeMatched = true
				break
			}
		}
		if !eventTypeMatched {
			return false
		}
	}

	// Apply minimum wait time threshold (default 1ms)
	const minWaitTimeMs = 1.0
	return waitTimeMs >= minWaitTimeMs
}

// meetsBlockingSessionCriteria checks if a blocking session meets collection criteria
func (c *MySQLClient) meetsBlockingSessionCriteria(waitTimeMs float64) bool {
	minWaitTimeMs := float64(c.config.BlockingSessionsConfig.MinWaitTime.Milliseconds())
	return waitTimeMs >= minWaitTimeMs
}

// validateExcludedDatabases validates and filters excluded databases list
func (c *MySQLClient) validateExcludedDatabases(excludedDatabases []string) []string {
	// Default excluded databases following New Relic patterns
	defaultExcluded := []string{
		"information_schema",
		"performance_schema",
		"mysql",
		"sys",
		"test",
	}

	// Merge with provided exclusions
	excludedMap := make(map[string]bool)
	for _, db := range defaultExcluded {
		excludedMap[db] = true
	}
	for _, db := range excludedDatabases {
		if db != "" {
			excludedMap[db] = true
		}
	}

	var result []string
	for db := range excludedMap {
		result = append(result, db)
	}

	return result
}

// ============================================================================
// CORE MYSQL METRICS METHODS
// Core MySQL functionality for OpenTelemetry compatibility
// ============================================================================

// GetGlobalStats queries MySQL for global status metrics
func (c *MySQLClient) GetGlobalStats(ctx context.Context) (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection is not established")
	}

	query := "SHOW GLOBAL STATUS"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute global stats query: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			c.logger.Warn("Failed to scan global stats row", zap.Error(err))
			continue
		}
		stats[name] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading global stats rows: %w", err)
	}

	c.logger.Debug("Retrieved global stats", zap.Int("count", len(stats)))
	return stats, nil
}

// GetInnoDBStats queries MySQL for InnoDB-specific metrics
func (c *MySQLClient) GetInnoDBStats(ctx context.Context) (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection is not established")
	}

	query := "SELECT name, count FROM information_schema.innodb_metrics WHERE name LIKE '%buffer_pool_size%'"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		// InnoDB metrics may not be available on all MySQL versions
		c.logger.Debug("Failed to execute InnoDB stats query (may not be supported)", zap.Error(err))
		return make(map[string]string), nil
	}
	defer rows.Close()

	stats := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			c.logger.Warn("Failed to scan InnoDB stats row", zap.Error(err))
			continue
		}
		stats[name] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading InnoDB stats rows: %w", err)
	}

	c.logger.Debug("Retrieved InnoDB stats", zap.Int("count", len(stats)))
	return stats, nil
}

// CoreMetricsData represents aggregated core MySQL metrics
type CoreMetricsData struct {
	// Connection metrics
	Connections         int64
	MaxUsedConnections  int64
	ConnectionsReceived int64
	ConnectionsSent     int64

	// Buffer pool metrics
	BufferPoolSize       int64
	BufferPoolPagesData  int64
	BufferPoolPagesFree  int64
	BufferPoolPagesDirty int64
	BufferPoolReads      int64
	BufferPoolWrites     int64

	// InnoDB metrics
	InnoDBDataReads    int64
	InnoDBDataWrites   int64
	InnoDBDataRead     int64
	InnoDBDataWritten  int64
	InnoDBRowsRead     int64
	InnoDBRowsInserted int64
	InnoDBRowsUpdated  int64
	InnoDBRowsDeleted  int64

	// Query metrics
	Queries        int64
	SlowQueries    int64
	QuestionsTotal int64

	// Handler operations
	HandlerRead   int64
	HandlerWrite  int64
	HandlerUpdate int64
	HandlerDelete int64

	// Thread metrics
	ThreadsConnected int64
	ThreadsRunning   int64
	ThreadsCached    int64
	ThreadsCreated   int64

	// Uptime
	Uptime int64
}

// GetCoreMetrics retrieves and aggregates core MySQL metrics in a single call
func (c *MySQLClient) GetCoreMetrics(ctx context.Context) (*CoreMetricsData, error) {
	globalStats, err := c.GetGlobalStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get global stats: %w", err)
	}

	data := &CoreMetricsData{}

	// Helper function to safely parse int64
	parseInt64 := func(value string) int64 {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
		return 0
	}

	// Parse connection metrics
	data.Connections = parseInt64(globalStats["Connections"])
	data.MaxUsedConnections = parseInt64(globalStats["Max_used_connections"])
	data.ConnectionsReceived = parseInt64(globalStats["Bytes_received"])
	data.ConnectionsSent = parseInt64(globalStats["Bytes_sent"])

	// Parse buffer pool metrics
	data.BufferPoolSize = parseInt64(globalStats["Innodb_buffer_pool_size"])
	data.BufferPoolPagesData = parseInt64(globalStats["Innodb_buffer_pool_pages_data"])
	data.BufferPoolPagesFree = parseInt64(globalStats["Innodb_buffer_pool_pages_free"])
	data.BufferPoolPagesDirty = parseInt64(globalStats["Innodb_buffer_pool_pages_dirty"])
	data.BufferPoolReads = parseInt64(globalStats["Innodb_buffer_pool_reads"])
	data.BufferPoolWrites = parseInt64(globalStats["Innodb_buffer_pool_write_requests"])

	// Parse InnoDB metrics
	data.InnoDBDataReads = parseInt64(globalStats["Innodb_data_reads"])
	data.InnoDBDataWrites = parseInt64(globalStats["Innodb_data_writes"])
	data.InnoDBDataRead = parseInt64(globalStats["Innodb_data_read"])
	data.InnoDBDataWritten = parseInt64(globalStats["Innodb_data_written"])
	data.InnoDBRowsRead = parseInt64(globalStats["Innodb_rows_read"])
	data.InnoDBRowsInserted = parseInt64(globalStats["Innodb_rows_inserted"])
	data.InnoDBRowsUpdated = parseInt64(globalStats["Innodb_rows_updated"])
	data.InnoDBRowsDeleted = parseInt64(globalStats["Innodb_rows_deleted"])

	// Parse query metrics
	data.Queries = parseInt64(globalStats["Queries"])
	data.SlowQueries = parseInt64(globalStats["Slow_queries"])
	data.QuestionsTotal = parseInt64(globalStats["Questions"])

	// Parse handler operations
	data.HandlerRead = parseInt64(globalStats["Handler_read_first"]) +
		parseInt64(globalStats["Handler_read_key"]) +
		parseInt64(globalStats["Handler_read_next"]) +
		parseInt64(globalStats["Handler_read_prev"]) +
		parseInt64(globalStats["Handler_read_rnd"]) +
		parseInt64(globalStats["Handler_read_rnd_next"])
	data.HandlerWrite = parseInt64(globalStats["Handler_write"])
	data.HandlerUpdate = parseInt64(globalStats["Handler_update"])
	data.HandlerDelete = parseInt64(globalStats["Handler_delete"])

	// Parse thread metrics
	data.ThreadsConnected = parseInt64(globalStats["Threads_connected"])
	data.ThreadsRunning = parseInt64(globalStats["Threads_running"])
	data.ThreadsCached = parseInt64(globalStats["Threads_cached"])
	data.ThreadsCreated = parseInt64(globalStats["Threads_created"])

	// Parse uptime
	data.Uptime = parseInt64(globalStats["Uptime"])

	c.logger.Debug("Retrieved core metrics",
		zap.Int64("connections", data.Connections),
		zap.Int64("queries", data.Queries),
		zap.Int64("slow_queries", data.SlowQueries),
		zap.Int64("uptime", data.Uptime))

	return data, nil
}

// NetworkIOData represents network I/O statistics
type NetworkIOData struct {
	BytesReceived int64
	BytesSent     int64
}

// GetNetworkIOStats retrieves network I/O statistics
func (c *MySQLClient) GetNetworkIOStats(ctx context.Context) (*NetworkIOData, error) {
	globalStats, err := c.GetGlobalStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get global stats for network I/O: %w", err)
	}

	parseInt64 := func(value string) int64 {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
		return 0
	}

	data := &NetworkIOData{
		BytesReceived: parseInt64(globalStats["Bytes_received"]),
		BytesSent:     parseInt64(globalStats["Bytes_sent"]),
	}

	c.logger.Debug("Retrieved network I/O stats",
		zap.Int64("bytes_received", data.BytesReceived),
		zap.Int64("bytes_sent", data.BytesSent))

	return data, nil
}

// CommandStatsData represents command execution statistics
type CommandStatsData struct {
	Select   int64
	Insert   int64
	Update   int64
	Delete   int64
	Replace  int64
	Begin    int64
	Commit   int64
	Rollback int64
}

// GetCommandStats retrieves command execution statistics
func (c *MySQLClient) GetCommandStats(ctx context.Context) (*CommandStatsData, error) {
	globalStats, err := c.GetGlobalStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get global stats for commands: %w", err)
	}

	parseInt64 := func(value string) int64 {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
		return 0
	}

	data := &CommandStatsData{
		Select:   parseInt64(globalStats["Com_select"]),
		Insert:   parseInt64(globalStats["Com_insert"]),
		Update:   parseInt64(globalStats["Com_update"]),
		Delete:   parseInt64(globalStats["Com_delete"]),
		Replace:  parseInt64(globalStats["Com_replace"]),
		Begin:    parseInt64(globalStats["Com_begin"]),
		Commit:   parseInt64(globalStats["Com_commit"]),
		Rollback: parseInt64(globalStats["Com_rollback"]),
	}

	c.logger.Debug("Retrieved command stats",
		zap.Int64("select", data.Select),
		zap.Int64("insert", data.Insert),
		zap.Int64("update", data.Update),
		zap.Int64("delete", data.Delete))

	return data, nil
}

// TableStatsData represents table-level statistics
type TableStatsData struct {
	Schema           string
	TableName        string
	Rows             int64
	AverageRowLength int64
	DataLength       int64
	IndexLength      int64
}

// GetTableStats retrieves table-level statistics
func (c *MySQLClient) GetTableStats(ctx context.Context) ([]TableStatsData, error) {
	if c.db == nil {
		return nil, fmt.Errorf("database connection is not established")
	}

	query := `SELECT TABLE_SCHEMA, TABLE_NAME, 
		COALESCE(TABLE_ROWS, 0) as TABLE_ROWS,
		COALESCE(AVG_ROW_LENGTH, 0) as AVG_ROW_LENGTH,
		COALESCE(DATA_LENGTH, 0) as DATA_LENGTH,
		COALESCE(INDEX_LENGTH, 0) as INDEX_LENGTH
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute table stats query: %w", err)
	}
	defer rows.Close()

	var stats []TableStatsData
	for rows.Next() {
		var stat TableStatsData
		if err := rows.Scan(&stat.Schema, &stat.TableName, &stat.Rows,
			&stat.AverageRowLength, &stat.DataLength, &stat.IndexLength); err != nil {
			c.logger.Warn("Failed to scan table stats row", zap.Error(err))
			continue
		}
		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading table stats rows: %w", err)
	}

	c.logger.Debug("Retrieved table stats", zap.Int("table_count", len(stats)))
	return stats, nil
}

// GetMySQLVersion retrieves the MySQL server version
func (c *MySQLClient) GetMySQLVersion(ctx context.Context) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("database connection is not established")
	}

	var version string
	query := "SELECT VERSION()"
	if err := c.db.QueryRowContext(ctx, query).Scan(&version); err != nil {
		return "", fmt.Errorf("failed to get MySQL version: %w", err)
	}

	c.logger.Debug("Retrieved MySQL version", zap.String("version", version))
	return version, nil
}
