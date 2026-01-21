// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// GetSlowQueriesSQL fetches query statistics from MySQL performance_schema.
// Data source: performance_schema.events_statements_summary_by_digest

// Note: Top N filtering and threshold filtering are applied in Go code after delta calculation
const GetSlowQueriesSQL = `
SELECT
    NOW() AS collection_timestamp,
    COALESCE(esd.DIGEST, '') AS query_id,
    COALESCE(esd.DIGEST_TEXT, '') AS query_text,
    COALESCE(esd.SCHEMA_NAME, '') AS database_name,

    -- Execution metrics (cumulative counters for delta calculation)
    esd.COUNT_STAR AS execution_count,
    esd.SUM_TIMER_WAIT / 1000000000 AS total_elapsed_time_ms,
    esd.SUM_LOCK_TIME / 1000000000 AS total_lock_time_ms,

    -- Historical averages
    esd.AVG_TIMER_WAIT / 1000000000 AS avg_elapsed_time_ms,
    CASE
        WHEN esd.COUNT_STAR > 0 THEN (esd.SUM_LOCK_TIME / 1000000000) / esd.COUNT_STAR
        ELSE 0
    END AS avg_lock_time_ms,
    COALESCE(esd.SUM_CPU_TIME / 1000000000, 0) AS total_cpu_time_ms,
    CASE
        WHEN esd.COUNT_STAR > 0 AND esd.SUM_CPU_TIME IS NOT NULL THEN (esd.SUM_CPU_TIME / 1000000000) / esd.COUNT_STAR
        ELSE 0
    END AS avg_cpu_time_ms,

    -- Row metrics
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

    -- Disk I/O metrics
    esd.SUM_SELECT_SCAN AS total_select_scan,
    esd.SUM_SELECT_FULL_JOIN AS total_select_full_join,
    esd.SUM_NO_INDEX_USED AS total_no_index_used,
    esd.SUM_NO_GOOD_INDEX_USED AS total_no_good_index_used,
    esd.SUM_SORT_ROWS AS total_sort_rows,
    esd.SUM_SORT_MERGE_PASSES AS total_sort_merge_passes,

    -- Temporary table metrics
    esd.SUM_CREATED_TMP_TABLES AS total_tmp_tables,
    esd.SUM_CREATED_TMP_DISK_TABLES AS total_tmp_disk_tables,

    -- Error and warning metrics
    esd.SUM_ERRORS AS total_errors,
    esd.SUM_WARNINGS AS total_warnings,

    -- Timestamps
    esd.FIRST_SEEN AS first_seen,
    esd.LAST_SEEN AS last_execution_timestamp,

    -- Performance variance
    esd.MIN_TIMER_WAIT / 1000000000 AS min_elapsed_time_ms,
    esd.MAX_TIMER_WAIT / 1000000000 AS max_elapsed_time_ms

FROM
    performance_schema.events_statements_summary_by_digest esd

WHERE
    esd.LAST_SEEN >= DATE_SUB(NOW(), INTERVAL ? SECOND)
    AND esd.COUNT_STAR > 0
    AND esd.DIGEST IS NOT NULL
    AND esd.SCHEMA_NAME IS NOT NULL
    AND esd.SCHEMA_NAME NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')

ORDER BY
    esd.SUM_TIMER_WAIT DESC;
`
