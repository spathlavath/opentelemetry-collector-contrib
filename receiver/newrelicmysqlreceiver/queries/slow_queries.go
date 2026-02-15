// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

import "fmt"

// GetSlowQueriesSQL returns SQL for slow queries from performance_schema
// This query fetches cumulative statistics from events_statements_summary_by_digest
// and is designed to work with the interval calculator for delta-based metrics.
//
// Key Design Decisions:
// - NO top N filtering in SQL - done in Go AFTER delta calculation for accuracy
// - NO elapsed time threshold in SQL - applied in Go on interval averages
// - Time window filter ensures we get recent activity for delta calculation
//
// Parameters:
// - intervalSeconds: Time window to fetch queries (e.g., 60 = last 60 seconds)
//
// Returns both historical (cumulative) stats AND total_elapsed_time_ms for delta calculation
func GetSlowQueriesSQL(intervalSeconds int) string {
	return fmt.Sprintf(`
SELECT
    CONVERT_TZ(NOW(), @@session.time_zone, '+00:00') AS collection_timestamp,
    COALESCE(esd.DIGEST, '') AS query_id,
    COALESCE(esd.DIGEST_TEXT, '') AS query_text,
    COALESCE(esd.SCHEMA_NAME, '') AS database_name,

    -- Core: Execution count (cumulative)
    esd.COUNT_STAR AS execution_count,

    -- Core: Elapsed time metrics
    esd.SUM_TIMER_WAIT / 1000000000.0 AS total_elapsed_time_ms,
    esd.AVG_TIMER_WAIT / 1000000000.0 AS avg_elapsed_time_ms,

    -- Core: CPU time (cumulative average)
    CASE
        WHEN esd.COUNT_STAR > 0 AND esd.SUM_CPU_TIME IS NOT NULL
        THEN (esd.SUM_CPU_TIME / 1000000000.0) / esd.COUNT_STAR
        ELSE 0
    END AS avg_cpu_time_ms,

    -- Core: Lock time (cumulative average)
    CASE
        WHEN esd.COUNT_STAR > 0
        THEN (esd.SUM_LOCK_TIME / 1000000000.0) / esd.COUNT_STAR
        ELSE 0
    END AS avg_lock_time_ms,

    -- Core: Row metrics (cumulative averages)
    CASE
        WHEN esd.COUNT_STAR > 0 THEN esd.SUM_ROWS_EXAMINED / esd.COUNT_STAR
        ELSE 0
    END AS avg_rows_examined,
    CASE
        WHEN esd.COUNT_STAR > 0 THEN esd.SUM_ROWS_SENT / esd.COUNT_STAR
        ELSE 0
    END AS avg_rows_sent,

    -- Errors (query reliability indicator)
    esd.SUM_ERRORS AS total_errors,

    -- Timestamps (UTC format for consistency with other receivers)
    CONVERT_TZ(esd.FIRST_SEEN, @@session.time_zone, '+00:00') AS first_seen,
    CONVERT_TZ(esd.LAST_SEEN, @@session.time_zone, '+00:00') AS last_execution_timestamp

FROM
    performance_schema.events_statements_summary_by_digest esd
WHERE
    -- Time window filter: Only fetch queries active in the last N seconds
    -- This is CRITICAL for delta calculation to work correctly
    esd.LAST_SEEN >= DATE_SUB(NOW(), INTERVAL %d SECOND)
    AND esd.COUNT_STAR > 0
    AND esd.DIGEST IS NOT NULL
    AND esd.SCHEMA_NAME IS NOT NULL
    -- Exclude system schemas
    AND esd.SCHEMA_NAME NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')
ORDER BY
    -- Sort by total elapsed time (highest impact queries first)
    -- This is preliminary sorting; final top-N selection happens in Go after delta calculation
    esd.SUM_TIMER_WAIT DESC`, intervalSeconds)
}
