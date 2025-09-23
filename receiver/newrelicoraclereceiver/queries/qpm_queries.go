package queries

const (
	WaitQuery = `SELECT
    d.name AS "database_name",
    ash.sql_id AS "query_id",
    MAX(s.sql_text) AS "query_text", -- Use MAX() and remove from GROUP BY
    ash.wait_class AS "wait_category",
    ash.event AS "wait_event_name",
    SYSTIMESTAMP AS "collection_timestamp",
    COUNT(DISTINCT ash.session_id || ',' || ash.session_serial#) AS "waiting_tasks_count",
    ROUND(
        (SUM(ash.time_waited) / 1000) +
        (SUM(CASE WHEN ash.time_waited = 0 THEN 1000 ELSE 0 END))
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
FETCH FIRST 10 ROWS ONLY;
	`

	// Query for collecting query wait performance metrics in New Relic format
	QueryWaitMetricsQuery = `SELECT
    EXTRACT(EPOCH FROM SYSTIMESTAMP) * 1000 AS "timestamp",
    MAX(s.sql_text) AS "query_text",
    ash.sql_id AS "query_id", 
    d.name AS "database",
    ash.event AS "wait_event_name",
    ash.wait_class AS "wait_category",
    ROUND(SUM(ash.time_waited) / 1000) AS "total_wait_time_ms"
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
    ash.wait_class,
    ash.event
ORDER BY
    "total_wait_time_ms" DESC
FETCH FIRST 100 ROWS ONLY;
	`
)
