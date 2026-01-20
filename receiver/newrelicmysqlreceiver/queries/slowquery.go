// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// ===========================================
// MYSQL SLOW QUERY COLLECTION
// ===========================================
// Data Source: performance_schema.events_statements_summary_by_digest
// This table aggregates query execution statistics by normalized query pattern (DIGEST)
//
// Key Features:
// 1. Automatic query normalization: "SELECT * FROM users WHERE id = 1" and "id = 2" become one pattern
// 2. Real-time aggregation: No need to parse slow query log files
// 3. Rich metrics: Execution counts, timing, I/O, locks, temporary tables, etc.
// 4. Cumulative counters: Perfect for delta calculation between scrapes
//
// Requirements:
// - MySQL 5.6+ (5.6.24+, 5.7.6+, 8.0+ recommended)
// - performance_schema enabled (default in MySQL 8.0+)
//
// Performance Impact: Low (< 1% overhead in most cases)

// GetSlowQueriesSQL returns SQL for MySQL slow queries with configurable time window
// Returns cumulative counters (total_elapsed_time_ms, execution_count) for delta calculation
//
// IMPORTANT: TOP N filtering and threshold filtering are applied in Go code AFTER delta calculation
// This ensures we get enough candidates for accurate interval-based (delta) averaging
//
// WHY NOT filter in SQL?
// - Filtering by historical avg (AVG_TIMER_WAIT) would miss queries that JUST became slow
// - We need to fetch candidates and compute interval averages first
// - Then sort by interval average (not historical average) and take Top N
//
// Time Window Filtering:
// - Parameter: intervalSeconds (e.g., 60 = queries executed in last 60 seconds)
// - Uses LAST_SEEN timestamp to filter recent queries
// - This reduces result set while ensuring we capture active slow queries
//
// Example: If collection_interval = 60s and intervalSeconds = 60:
//   - First scrape (10:00): Fetches queries with LAST_SEEN >= 9:59
//   - Second scrape (10:01): Fetches queries with LAST_SEEN >= 10:00
//   - Delta calculation: Compares cumulative counters between scrapes
//
// Query Structure:
// 1. SELECT: All relevant metrics from performance_schema
// 2. WHERE: Time window + basic filters (exclude system schemas, NULL digests)
// 3. ORDER BY: Pre-sort by total time (optimization, not strict requirement)
// 4. NO LIMIT: Top N applied in Go after delta calculation
const GetSlowQueriesSQL = `
SELECT
    -- ===========================================
    -- METADATA
    -- ===========================================
    NOW() AS collection_timestamp,

    -- Query identification
    COALESCE(esd.DIGEST, '') AS query_id,
    COALESCE(esd.DIGEST_TEXT, '') AS query_text,
    COALESCE(esd.SCHEMA_NAME, '') AS database_name,

    -- ===========================================
    -- EXECUTION METRICS (CUMULATIVE)
    -- ===========================================
    -- These are cumulative counters since server start (or since performance_schema reset)
    -- Used for delta calculation: interval_avg = (current_total - previous_total) / (current_count - previous_count)

    esd.COUNT_STAR AS execution_count,

    -- Convert picoseconds to milliseconds: divide by 1,000,000,000 (1e9)
    -- MySQL stores timer values in picoseconds for precision
    esd.SUM_TIMER_WAIT / 1000000000 AS total_elapsed_time_ms,

    -- Total lock time (cumulative) - helps identify lock contention issues
    esd.SUM_LOCK_TIME / 1000000000 AS total_lock_time_ms,

    -- ===========================================
    -- HISTORICAL AVERAGES (FROM DATABASE)
    -- ===========================================
    -- These are all-time averages calculated by MySQL
    -- NOT used for Top N selection (we compute interval averages instead)

    esd.AVG_TIMER_WAIT / 1000000000 AS avg_elapsed_time_ms,

    -- Lock time average - calculated from SUM_LOCK_TIME / COUNT_STAR
    -- MySQL doesn't provide AVG_LOCK_TIME directly
    CASE
        WHEN esd.COUNT_STAR > 0 THEN (esd.SUM_LOCK_TIME / 1000000000) / esd.COUNT_STAR
        ELSE 0
    END AS avg_lock_time_ms,

    -- CPU metrics (MySQL 8.0.28+, NULL on older versions)
    -- Note: SUM_CPU_TIME added in MySQL 8.0.28, may not be available on older versions
    COALESCE(esd.SUM_CPU_TIME / 1000000000, 0) AS total_cpu_time_ms,

    -- CPU time average - calculated from SUM_CPU_TIME / COUNT_STAR
    CASE
        WHEN esd.COUNT_STAR > 0 AND esd.SUM_CPU_TIME IS NOT NULL THEN (esd.SUM_CPU_TIME / 1000000000) / esd.COUNT_STAR
        ELSE 0
    END AS avg_cpu_time_ms,

    -- ===========================================
    -- ROW METRICS
    -- ===========================================
    -- Help identify inefficient queries (full table scans, excessive row processing)
    -- Note: MySQL doesn't provide AVG_ROWS_* fields, we calculate them manually

    esd.SUM_ROWS_EXAMINED AS total_rows_examined,
    CASE
        WHEN esd.COUNT_STAR > 0 THEN esd.SUM_ROWS_EXAMINED / esd.COUNT_STAR
        ELSE 0
    END AS avg_rows_examined,
    esd.SUM_ROWS_SENT AS total_rows_sent,
    CASE
        WHEN esd.COUNT_STAR > 0 THEN esd.SUM_ROWS_SENT / esd.COUNT_STAR
        ELSE 0
    END AS avg_rows_sent,
    esd.SUM_ROWS_AFFECTED AS total_rows_affected,
    CASE
        WHEN esd.COUNT_STAR > 0 THEN esd.SUM_ROWS_AFFECTED / esd.COUNT_STAR
        ELSE 0
    END AS avg_rows_affected,

    -- ===========================================
    -- DISK I/O METRICS (PERFORMANCE INDICATORS)
    -- ===========================================
    -- These indicate potential optimization opportunities

    -- Full table scans: High value = missing indexes
    -- Example: 1500 means this query pattern did 1,500 full table scans total
    esd.SUM_SELECT_SCAN AS total_select_scan,

    -- Joins without indexes: High value = missing join indexes
    -- Example: 200 means this query pattern did 200 full joins (no index used)
    esd.SUM_SELECT_FULL_JOIN AS total_select_full_join,

    -- Index usage metrics (optimization indicators)
    -- No index used at all (RED FLAG)
    esd.SUM_NO_INDEX_USED AS total_no_index_used,

    -- Suboptimal index used (YELLOW FLAG - index exists but MySQL chose not to use it)
    esd.SUM_NO_GOOD_INDEX_USED AS total_no_good_index_used,

    -- Sort operations
    esd.SUM_SORT_ROWS AS total_sort_rows,
    esd.SUM_SORT_MERGE_PASSES AS total_sort_merge_passes,  -- Non-zero = sort didn't fit in memory

    -- ===========================================
    -- TEMPORARY TABLE METRICS (MEMORY PRESSURE)
    -- ===========================================
    -- Indicate memory pressure and potential query optimization opportunities

    esd.SUM_CREATED_TMP_TABLES AS total_tmp_tables,

    -- RED FLAG: Temporary tables created on disk (very expensive)
    -- Non-zero value indicates memory pressure and expensive disk I/O
    esd.SUM_CREATED_TMP_DISK_TABLES AS total_tmp_disk_tables,

    -- ===========================================
    -- ERROR AND WARNING METRICS (RELIABILITY)
    -- ===========================================
    -- These help identify problematic queries that fail or generate warnings

    -- Total execution errors (queries that failed)
    -- Non-zero value indicates queries with syntax errors, constraint violations, etc.
    esd.SUM_ERRORS AS total_errors,

    -- Total warnings generated
    -- Non-zero value indicates potential data issues, type mismatches, etc.
    esd.SUM_WARNINGS AS total_warnings,

    -- ===========================================
    -- TIMESTAMP FIELDS
    -- ===========================================

    esd.FIRST_SEEN AS first_seen,
    esd.LAST_SEEN AS last_execution_timestamp,

    -- ===========================================
    -- PERFORMANCE VARIANCE (ROOT CAUSE ANALYSIS)
    -- ===========================================
    -- Large gap between min/max indicates inconsistent performance
    -- Possible causes: caching, lock contention, resource competition

    esd.MIN_TIMER_WAIT / 1000000000 AS min_elapsed_time_ms,
    esd.MAX_TIMER_WAIT / 1000000000 AS max_elapsed_time_ms

FROM
    performance_schema.events_statements_summary_by_digest esd

WHERE
    -- ===========================================
    -- FILTERING CRITERIA
    -- ===========================================

    -- Time window filter: Only queries executed in the last N seconds
    -- Parameter: ? = intervalSeconds (e.g., 60 for last 60 seconds)
    -- This reduces result set while capturing recent activity
    esd.LAST_SEEN >= DATE_SUB(NOW(), INTERVAL ? SECOND)

    -- Basic validation filters
    AND esd.COUNT_STAR > 0              -- Must have at least one execution
    AND esd.DIGEST IS NOT NULL          -- Must have a valid query digest (hash)
    AND esd.SCHEMA_NAME IS NOT NULL     -- Must have a database/schema name

    -- Exclude system schemas
    -- These are internal MySQL tables that we don't want to monitor
    AND esd.SCHEMA_NAME NOT IN (
        'performance_schema',   -- Performance schema tables
        'information_schema',   -- Metadata tables
        'mysql',                -- MySQL system database
        'sys'                   -- MySQL sys schema (views on performance_schema)
    )

ORDER BY
    -- Pre-sort by total elapsed time (descending)
    -- This is an optimization: database can efficiently return candidates
    -- Note: Final Top N selection is done in Go after delta calculation
    esd.SUM_TIMER_WAIT DESC;

-- ===========================================
-- IMPORTANT NOTES
-- ===========================================
-- 1. NO LIMIT clause: Top N selection happens in Go after delta calculation
-- 2. NO threshold filter (e.g., AVG_TIMER_WAIT > ?): Applied in Go after delta
-- 3. Time window (LAST_SEEN >= ...) is essential for reducing result set
-- 4. ORDER BY is an optimization, not a requirement (final sorting in Go)
`

// ===========================================
// EXAMPLE USAGE SCENARIOS
// ===========================================

// Example 1: Default configuration (60 second interval)
// Query: GetSlowQueriesSQL with intervalSeconds = 60
// Result: Fetches all queries executed in the last 60 seconds
// Go processing: Computes delta, filters by threshold (e.g., 100ms), sorts, takes Top 10

// Example 2: High-frequency monitoring (15 second interval)
// Query: GetSlowQueriesSQL with intervalSeconds = 15
// Result: Fetches queries executed in the last 15 seconds
// Go processing: More frequent scrapes = faster detection of sudden slowdowns

// Example 3: First scrape after server start
// Query: Returns all queries since server started
// Go processing: No previous baseline → uses historical averages for interval metrics

// ===========================================
// MYSQL VERSION COMPATIBILITY
// ===========================================

// MySQL 5.6.24+ / 5.7.6+:
// - All fields available except SUM_CPU_TIME, AVG_CPU_TIME
// - Use COALESCE(..., 0) for CPU fields (returns 0 if NULL)

// MySQL 8.0+:
// - All fields available including CPU metrics
// - Performance schema enabled by default
// - Best performance and most complete metrics

// MySQL 5.6.0 - 5.6.23:
// - Some fields may be missing (check MySQL documentation)
// - Consider upgrading to 5.6.24+ for full feature support

// ===========================================
// PERFORMANCE CONSIDERATIONS
// ===========================================

// Table Size:
// - performance_schema.events_statements_summary_by_digest is limited by:
//   performance_schema_digests_size (default: 10000 in MySQL 8.0+)
// - When limit is reached, oldest entries are evicted

// Query Performance:
// - Time window filter (LAST_SEEN >= ...) uses index
// - Typical query execution time: < 10ms for 1000s of unique query patterns
// - ORDER BY may cause filesort if result set is large (but limited by time window)

// Overhead:
// - Performance schema overhead: < 1% in most production workloads
// - Negligible impact on application performance
// - Can be disabled if needed: SET GLOBAL performance_schema = OFF (requires restart)
