package queries

const (
	QueryWaitMetricsQuery = `
SELECT
    d.name AS "database_name",
    ash.sql_id AS "query_id",
    MAX(s.sql_text) AS "query_text", -- Use MAX() and remove from GROUP BY
    ash.wait_class AS "wait_category",
    ash.event AS "wait_event_name",
    SYSTIMESTAMP AS "collection_timestamp",
    COUNT(DISTINCT ash.session_id || ',' || ash.session_serial#) AS "waiting_tasks_count",
    ROUND(
        (SUM(ash.time_waited) / 1000) +
        (SUM(CASE WHEN ash.time_waited = 0 THEN 1 ELSE 0 END) * 1000)
    ) AS "total_wait_time_ms"
FROM
    v$active_session_history ash
JOIN
    v$sql s ON ash.sql_id = s.sql_id
CROSS JOIN
    v$database d
WHERE
    ash.sql_id IS NOT NULL
    AND ash.wait_class <> 'Idle'
    AND ash.sample_time >= SYSDATE - INTERVAL '5' MINUTE
GROUP BY
    d.name,
    ash.sql_id,
    -- s.sql_text is removed from here
    ash.wait_class,
    ash.event
ORDER BY
    "total_wait_time_ms" DESC
FETCH FIRST 10 ROWS ONLY
	`

	// Simple test query to check ASH data availability
	TestASHDataQuery = `
SELECT 
    COUNT(*) AS total_ash_rows,
    COUNT(CASE WHEN sample_time >= SYSDATE - INTERVAL '10' MINUTE THEN 1 END) AS recent_rows,
    COUNT(CASE WHEN wait_class <> 'Idle' THEN 1 END) AS non_idle_rows,
    COUNT(CASE WHEN sql_id IS NOT NULL THEN 1 END) AS rows_with_sql
FROM v$active_session_history
WHERE ROWNUM <= 1000
	`
)
