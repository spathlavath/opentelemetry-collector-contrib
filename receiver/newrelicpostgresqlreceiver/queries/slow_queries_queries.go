// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

import "fmt"

// PostgreSQL slow queries monitoring from pg_stat_statements

// GetSlowQueriesSQL returns the appropriate SQL query based on PostgreSQL version
// Parameters:
// - version: PostgreSQL version number (e.g., 130000 for 13.0.0)
// - sqlRowLimit: Maximum rows to fetch (SQL pre-filter optimization)
//
// This function selects between pre-PG13 and PG13+ SQL variants.
// The sqlRowLimit parameter pre-filters by historical average to reduce memory usage,
// since PostgreSQL < 17 doesn't have a stats_since field for time-based filtering.
func GetSlowQueriesSQL(version, sqlRowLimit int) string {
	if version >= 130000 {
		return GetSlowQueriesPG13SQL(sqlRowLimit)
	}
	return GetSlowQueriesPrePG13SQL(sqlRowLimit)
}

// GetSlowQueriesPrePG13SQL returns slow query statistics from pg_stat_statements (PostgreSQL 9.6-12)
// This query retrieves slowest queries by average execution time
// Does NOT include mean_plan_time (not available in PostgreSQL < 13)
// Requires pg_stat_statements extension to be installed and enabled
// Available in PostgreSQL 9.6+
// Parameters:
// - sqlRowLimit: Maximum rows to fetch (SQL pre-filter by historical average)
func GetSlowQueriesPrePG13SQL(sqlRowLimit int) string {
	return fmt.Sprintf(`
		SELECT
			-- Collection metadata
			CURRENT_TIMESTAMP AS collection_timestamp,

			-- Query identification (STABLE across parameterized queries)
			s.queryid::TEXT AS query_id,

			-- Database and user context
			d.datname AS database_name,
			u.usename AS user_name,

			-- Execution count (for delta calculation)
			s.calls AS execution_count,

			-- Query text (normalized/parameterized by pg_stat_statements)
			s.query AS query_text,

			-- ========================================================================
			-- EXECUTION TIME METRICS
			-- ========================================================================

			-- Execution time metrics (milliseconds)
			(s.mean_exec_time)::NUMERIC(20,3) AS avg_elapsed_time_ms,
			(s.min_exec_time)::NUMERIC(20,3) AS min_elapsed_time_ms,
			(s.max_exec_time)::NUMERIC(20,3) AS max_elapsed_time_ms,
			(s.stddev_exec_time)::NUMERIC(20,3) AS stddev_elapsed_time_ms,

			-- *** CRITICAL FOR DELTA CALCULATION ***
			-- Total elapsed time (NOT average) - used for precise interval-based delta calculation
			(s.total_exec_time)::NUMERIC(20,3) AS total_elapsed_time_ms,

			-- Planning time metrics (NOT AVAILABLE in PostgreSQL < 13)
			0::NUMERIC(20,3) AS avg_plan_time_ms,

			-- CPU time approximation (execution only, no planning time)
			(s.mean_exec_time)::NUMERIC(20,3) AS avg_cpu_time_ms,

			-- ========================================================================
			-- I/O METRICS
			-- ========================================================================

			-- Disk reads (cache misses - actual I/O)
			CASE WHEN s.calls > 0 THEN (s.shared_blks_read::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_disk_reads,
			s.shared_blks_read AS total_disk_reads,

			-- Buffer cache hits (no disk I/O)
			CASE WHEN s.calls > 0 THEN (s.shared_blks_hit::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_buffer_hits,
			s.shared_blks_hit AS total_buffer_hits,

			-- Disk writes (dirty pages flushed to disk)
			CASE WHEN s.calls > 0 THEN (s.shared_blks_written::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_disk_writes,
			s.shared_blks_written AS total_disk_writes,

			-- ========================================================================
			-- ROW STATISTICS
			-- ========================================================================

			-- Rows metrics
			CASE WHEN s.calls > 0 THEN (s.rows::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_rows_returned,
			s.rows AS total_rows

			-- ========================================================================
			-- TIME TRACKING (PostgreSQL 17+ only - REMOVED for compatibility)
			-- ========================================================================
			-- NOTE: stats_since field removed for backward compatibility
			-- Time tracking for query execution
			-- PostgreSQL 17+ has: stats_since (when stats collection started)
			-- PostgreSQL < 17: Field NOT available
			--
			-- ⚠️ RECOMMENDATION: Use delta calculation in Go layer instead
			--    deltaCalls = currentCalls - previousCalls
			--    if deltaCalls > 0 { /* query executed in this interval */ }
			-- ========================================================================

		FROM
			pg_stat_statements s
		INNER JOIN
			pg_database d ON s.dbid = d.oid
		INNER JOIN
			pg_user u ON s.userid = u.usesysid

		WHERE
			-- Basic filters
			s.calls > 0
			AND s.queryid IS NOT NULL

			-- ========================================================================
			-- TIME INTERVAL FILTER - REMOVED FOR COMPATIBILITY
			-- ========================================================================
			-- PostgreSQL 17+: WHERE s.stats_since >= NOW() - INTERVAL '3600 seconds'
			-- PostgreSQL < 17: ❌ NOT AVAILABLE (stats_since field doesn't exist)
			--
			-- ✅ SOLUTION: Implement delta calculation in Go layer
			--    - This works on ALL PostgreSQL versions (13+)
			--    - More accurate for "recently executed" detection
			--    - See "DELTA CALCULATION" section below
			-- ========================================================================

			-- ========================================================================
			-- EXCLUSION FILTERS
			-- ========================================================================

			-- Exclude system databases (optional - uncomment if needed)
			-- AND d.datname NOT IN ('postgres', 'template0', 'template1')

			-- Exclude system users (optional - uncomment if needed)
			-- AND u.usename NOT IN ('postgres', 'rdsadmin', 'replication', 'monitoring')

			-- Exclude monitoring queries (pattern matching)
			AND s.query NOT LIKE '%%pg_stat_statements%%'
			AND s.query NOT LIKE '%%pg_stat_activity%%'
			AND s.query NOT LIKE '%%pg_locks%%'
			AND s.query NOT LIKE '%%pg_database%%'
			AND s.query NOT LIKE '%%EXPLAIN (FORMAT JSON)%%'

		ORDER BY
			-- Order by average elapsed time
			s.mean_exec_time DESC
		LIMIT %d`, sqlRowLimit)
}

// GetSlowQueriesPG13SQL returns slow query statistics from pg_stat_statements (PostgreSQL 13+)
// This query retrieves slowest queries by average execution time with cross-database compatibility
// Includes mean_plan_time (available in PostgreSQL 13+)
// 
// Requires pg_stat_statements extension to be installed and enabled
// Available in PostgreSQL 13+
// Parameters:
// - sqlRowLimit: Maximum rows to fetch (SQL pre-filter by historical average)
func GetSlowQueriesPG13SQL(sqlRowLimit int) string {
	return fmt.Sprintf(`
		SELECT
			-- Collection metadata
			CURRENT_TIMESTAMP AS collection_timestamp,

			-- Query identification (STABLE across parameterized queries)
			s.queryid::TEXT AS query_id,

			-- Database and user context
			d.datname AS database_name,
			u.usename AS user_name,

			-- Execution count (for delta calculation)
			s.calls AS execution_count,

			-- Query text (normalized/parameterized by pg_stat_statements)
			s.query AS query_text,

			-- ========================================================================
			-- EXECUTION TIME METRICS
			-- ========================================================================

			-- Execution time metrics (milliseconds)
			(s.mean_exec_time)::NUMERIC(20,3) AS avg_elapsed_time_ms,
			(s.min_exec_time)::NUMERIC(20,3) AS min_elapsed_time_ms,
			(s.max_exec_time)::NUMERIC(20,3) AS max_elapsed_time_ms,
			(s.stddev_exec_time)::NUMERIC(20,3) AS stddev_elapsed_time_ms,

			-- *** CRITICAL FOR DELTA CALCULATION ***
			-- Total elapsed time (NOT average) - used for precise interval-based delta calculation
			(s.total_exec_time)::NUMERIC(20,3) AS total_elapsed_time_ms,

			-- Planning time metrics (PostgreSQL 13+)
			(s.mean_plan_time)::NUMERIC(20,3) AS avg_plan_time_ms,

			-- CPU time approximation (planning + execution)
			((s.mean_plan_time + s.mean_exec_time))::NUMERIC(20,3) AS avg_cpu_time_ms,

			-- ========================================================================
			-- I/O METRICS
			-- ========================================================================

			-- Disk reads (cache misses - actual I/O)
			CASE WHEN s.calls > 0 THEN (s.shared_blks_read::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_disk_reads,
			s.shared_blks_read AS total_disk_reads,

			-- Buffer cache hits (no disk I/O)
			CASE WHEN s.calls > 0 THEN (s.shared_blks_hit::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_buffer_hits,
			s.shared_blks_hit AS total_buffer_hits,

			-- Disk writes (dirty pages flushed to disk)
			CASE WHEN s.calls > 0 THEN (s.shared_blks_written::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_disk_writes,
			s.shared_blks_written AS total_disk_writes,

			-- ========================================================================
			-- ROW STATISTICS
			-- ========================================================================

			-- Rows metrics
			CASE WHEN s.calls > 0 THEN (s.rows::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_rows_returned,
			s.rows AS total_rows

			-- ========================================================================
			-- TIME TRACKING (PostgreSQL 17+ only - REMOVED for compatibility)
			-- ========================================================================
			-- NOTE: stats_since field removed for backward compatibility
			-- Time tracking for query execution
			-- PostgreSQL 17+ has: stats_since (when stats collection started)
			-- PostgreSQL < 17: Field NOT available
			--
			-- ⚠️ RECOMMENDATION: Use delta calculation in Go layer instead
			--    deltaCalls = currentCalls - previousCalls
			--    if deltaCalls > 0 { /* query executed in this interval */ }
			-- ========================================================================

		FROM
			pg_stat_statements s
		INNER JOIN
			pg_database d ON s.dbid = d.oid
		INNER JOIN
			pg_user u ON s.userid = u.usesysid

		WHERE
			-- Basic filters
			s.calls > 0
			AND s.queryid IS NOT NULL

			-- ========================================================================
			-- TIME INTERVAL FILTER - REMOVED FOR COMPATIBILITY
			-- ========================================================================
			-- PostgreSQL 17+: WHERE s.stats_since >= NOW() - INTERVAL '3600 seconds'
			-- PostgreSQL < 17: ❌ NOT AVAILABLE (stats_since field doesn't exist)
			--
			-- ✅ SOLUTION: Implement delta calculation in Go layer
			--    - This works on ALL PostgreSQL versions (13+)
			--    - More accurate for "recently executed" detection
			--    - See "DELTA CALCULATION" section below
			-- ========================================================================

			-- ========================================================================
			-- EXCLUSION FILTERS
			-- ========================================================================

			-- Exclude system databases (optional - uncomment if needed)
			-- AND d.datname NOT IN ('postgres', 'template0', 'template1')

			-- Exclude system users (optional - uncomment if needed)
			-- AND u.usename NOT IN ('postgres', 'rdsadmin', 'replication', 'monitoring')

			-- Exclude monitoring queries (pattern matching)
			AND s.query NOT LIKE '%%pg_stat_statements%%'
			AND s.query NOT LIKE '%%pg_stat_activity%%'
			AND s.query NOT LIKE '%%pg_locks%%'
			AND s.query NOT LIKE '%%pg_database%%'
			AND s.query NOT LIKE '%%EXPLAIN (FORMAT JSON)%%'

		ORDER BY
			-- Order by average elapsed time
			s.mean_exec_time DESC
		LIMIT %d`, sqlRowLimit)
}
