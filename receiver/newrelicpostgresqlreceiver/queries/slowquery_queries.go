// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// GetSlowQueriesSQL returns SQL for slow queries using pg_stat_statements
// Returns total_elapsed_time_ms for delta calculation in addition to avg_elapsed_time_ms
//
// IMPORTANT: TOP N filtering and threshold filtering are applied in Go code AFTER delta calculation
// This ensures we get enough candidates for interval-based (delta) averaging
//
// Parameters:
// - intervalSeconds: Time window to fetch queries (e.g., 60 = last 60 seconds)
//   NOTE: This parameter is included for API compatibility with Oracle/MSSQL implementations,
//   but is NOT used in WHERE clause for PostgreSQL < 17 (stats_since field unavailable)
//   Delta calculation is done in Go layer instead.
//
// Requirements:
// - pg_stat_statements extension must be installed
// - PostgreSQL 13+ recommended (for planning time metrics)
// - Works on PostgreSQL 9.6+ (without planning metrics)
//
// Note: This function signature accepts intervalSeconds parameter for consistency
// with Oracle/MSSQL, but the time window filtering is done via delta calculation in Go
// rather than SQL WHERE clause (for compatibility with PostgreSQL < 17)
func GetSlowQueriesSQL(intervalSeconds int) string {
	// Note: intervalSeconds is intentionally not used in the WHERE clause
	// Delta calculation handles "recently executed" detection in Go layer
	_ = intervalSeconds // Suppress unused parameter warning

	return `
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
    -- CROSS-DATABASE COMPATIBLE METRICS (Matches Oracle V$SQLAREA)
    -- ========================================================================

    -- Execution time metrics (milliseconds)
    -- Oracle: sa.elapsed_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000
    (s.mean_exec_time)::NUMERIC(20,3) AS avg_elapsed_time_ms,
    (s.min_exec_time)::NUMERIC(20,3) AS min_elapsed_time_ms,
    (s.max_exec_time)::NUMERIC(20,3) AS max_elapsed_time_ms,
    (s.stddev_exec_time)::NUMERIC(20,3) AS stddev_elapsed_time_ms,

    -- *** CRITICAL FOR DELTA CALCULATION ***
    -- Total elapsed time (NOT average) - used for precise interval-based delta calculation
    -- Oracle: sa.elapsed_time / 1000
    (s.total_exec_time)::NUMERIC(20,3) AS total_elapsed_time_ms,

    -- Planning time metrics (PostgreSQL 13+)
    (s.mean_plan_time)::NUMERIC(20,3) AS avg_plan_time_ms,

    -- CPU time approximation (planning + execution)
    -- Oracle: sa.cpu_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000
    ((s.mean_plan_time + s.mean_exec_time))::NUMERIC(20,3) AS avg_cpu_time_ms,

    -- ========================================================================
    -- I/O METRICS (Cross-database compatible)
    -- ========================================================================

    -- Disk reads (cache misses - actual I/O)
    -- Oracle: sa.disk_reads / DECODE(sa.executions, 0, 1, sa.executions)
    CASE WHEN s.calls > 0 THEN (s.shared_blks_read::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_disk_reads,
    s.shared_blks_read AS total_disk_reads,

    -- Buffer cache hits (no disk I/O)
    -- Oracle: sa.buffer_gets / DECODE(sa.executions, 0, 1, sa.executions)
    CASE WHEN s.calls > 0 THEN (s.shared_blks_hit::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_buffer_hits,
    s.shared_blks_hit AS total_buffer_hits,

    -- Disk writes (dirty pages flushed to disk)
    -- Oracle: sa.direct_writes / DECODE(sa.executions, 0, 1, sa.executions)
    CASE WHEN s.calls > 0 THEN (s.shared_blks_written::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_disk_writes,
    s.shared_blks_written AS total_disk_writes,

    -- ========================================================================
    -- ROW STATISTICS (Cross-database compatible)
    -- ========================================================================

    -- Rows metrics
    CASE WHEN s.calls > 0 THEN (s.rows::NUMERIC / s.calls)::NUMERIC(20,3) ELSE 0 END AS avg_rows_returned,
    s.rows AS total_rows

    -- ========================================================================
    -- TIME TRACKING (PostgreSQL 17+ only - REMOVED for compatibility)
    -- ========================================================================
    -- NOTE: stats_since field removed for backward compatibility
    -- Oracle has: sa.last_active_time (when query was last executed)
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
    -- Oracle: WHERE sa.last_active_time >= SYSDATE - INTERVAL '%d' SECOND
    -- PostgreSQL 17+: WHERE s.stats_since >= NOW() - INTERVAL '3600 seconds'
    -- PostgreSQL < 17: ❌ NOT AVAILABLE (stats_since field doesn't exist)
    --
    -- ✅ SOLUTION: Implement delta calculation in Go layer
    --    - This works on ALL PostgreSQL versions (13+)
    --    - More accurate for "recently executed" detection
    --    - See SlowQueriesScraper for implementation
    -- ========================================================================

    -- ========================================================================
    -- EXCLUSION FILTERS (Match Oracle/MSSQL pattern)
    -- ========================================================================

    -- Exclude system databases (optional - uncomment if needed)
    -- Oracle: Implicit (v$sqlarea doesn't show system schemas by default)
    -- AND d.datname NOT IN ('postgres', 'template0', 'template1')

    -- Exclude system users (optional - uncomment if needed)
    -- Oracle: AND au.username NOT IN ('SYS', 'SYSTEM', 'DBSNMP', ...)
    -- AND u.usename NOT IN ('postgres', 'rdsadmin', 'replication', 'monitoring')

    -- Exclude monitoring queries (pattern matching)
    -- Oracle: AND sa.sql_text NOT LIKE '%%V$SQLAREA%%'
    AND s.query NOT LIKE '%pg_stat_statements%'
    AND s.query NOT LIKE '%pg_stat_activity%'
    AND s.query NOT LIKE '%pg_locks%'
    AND s.query NOT LIKE '%pg_database%'
    AND s.query NOT LIKE '%EXPLAIN (FORMAT JSON)%'

ORDER BY
    -- Order by average elapsed time (matches Oracle pattern)
    -- Oracle: ORDER BY sa.elapsed_time / DECODE(sa.executions, 0, 1, sa.executions) DESC
    s.mean_exec_time DESC`
}
