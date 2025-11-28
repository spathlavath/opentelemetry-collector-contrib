// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

import "fmt"

// GetSlowQueriesSQL returns SQL for slow queries with configurable time window
// Returns total_elapsed_time_ms for delta calculation in addition to avg_elapsed_time_ms
//
// IMPORTANT: TOP N filtering and threshold filtering are applied in Go code AFTER delta calculation
// This ensures we get enough candidates for interval-based (delta) averaging
//
// Parameters:
// - intervalSeconds: Time window to fetch queries (e.g., 60 = last 60 seconds)
//
// Note: This function signature previously accepted responseTimeThreshold and rowLimit parameters,
// but those are no longer used in the SQL query. Filtering and TOP N selection are now done in Go
// after delta calculation for accurate results.
func GetSlowQueriesSQL(intervalSeconds int) string {
	return fmt.Sprintf(`
		SELECT
			d.name AS database_name,
			sa.sql_id AS query_id,
			sa.parsing_schema_name AS schema_name,
			au.username AS user_name,
			sa.executions AS execution_count,
			sa.sql_text AS query_text,
			sa.cpu_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000 AS avg_cpu_time_ms,
			sa.disk_reads / DECODE(sa.executions, 0, 1, sa.executions) AS avg_disk_reads,
			sa.direct_writes / DECODE(sa.executions, 0, 1, sa.executions) AS avg_disk_writes,
			sa.elapsed_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000 AS avg_elapsed_time_ms,
			sa.buffer_gets / DECODE(sa.executions, 0, 1, sa.executions) AS rows_examined,
			sa.concurrency_wait_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000 AS avg_lock_time_ms,
			sa.last_active_time AS last_active_time_ms,
			sa.elapsed_time / 1000 AS total_elapsed_time_ms
		FROM
			v$sqlarea sa
		INNER JOIN
			ALL_USERS au ON sa.parsing_user_id = au.user_id
		CROSS JOIN
			v$database d
		WHERE
			sa.executions > 0
			AND sa.sql_text NOT LIKE '%%ALL_USERS%%'
			AND sa.sql_text NOT LIKE '%%V$SQL_PLAN%%'
			AND sa.sql_text NOT LIKE '%%V$SQLAREA%%'
			AND sa.sql_text NOT LIKE '%%V$SESSION%%'
			AND sa.sql_text NOT LIKE '%%V$ACTIVE_SESSION_HISTORY%%'
			AND au.username NOT IN ('SYS', 'SYSTEM', 'DBSNMP', 'SYSMAN', 'OUTLN', 'MDSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'APPQOSSYS', 'APEX_030200', 'OWBSYS', 'GSMADMIN_INTERNAL', 'OLAPSYS', 'XDB', 'ANONYMOUS', 'CTXSYS', 'SI_INFORMTN_SCHEMA', 'ORDDATA', 'DVSYS', 'LBACSYS', 'OJVMSYS','C##JS_USER')
			-- KEY FILTER: Only fetch queries that ran in the last N seconds (interval window)
			-- This is critical for delta calculation to work correctly
			AND sa.last_active_time >= SYSDATE - INTERVAL '%d' SECOND
		ORDER BY
			sa.elapsed_time / DECODE(sa.executions, 0, 1, sa.executions) DESC`, intervalSeconds)
}

// GetBlockingQueriesSQL returns SQL for blocking queries with configurable row limit
func GetBlockingQueriesSQL(rowLimit int) string {
	return fmt.Sprintf(`
		SELECT
			SYSTIMESTAMP AS COLLECTION_TIMESTAMP,			
			blocked.sid AS session_id,
			blocked.serial# AS blocked_serial,
			blocked.username AS blocked_user,
			blocked.seconds_in_wait AS blocked_wait_sec,
			blocked.sql_id AS query_id,
			blocked.sql_exec_id AS sql_exec_id,
			blocking_sql.sql_text AS blocking_query_text,
			blocking.sid AS blocking_sid,
			blocking.serial# AS blocking_serial,
			blocking.username AS blocking_user,
			blocking.sql_id AS blocking_query_id,
			d.name AS database_name
		FROM
			v$session blocked
		JOIN
			v$session blocking ON blocked.blocking_session = blocking.sid
		LEFT JOIN
			v$sql blocking_sql ON blocking.sql_id = blocking_sql.sql_id
		CROSS JOIN
			v$database d
		WHERE
			blocked.blocking_session IS NOT NULL
			AND blocked.seconds_in_wait > 0
		ORDER BY
			blocked.seconds_in_wait DESC
		FETCH FIRST %d ROWS ONLY`, rowLimit)
}

// GetWaitEventQueriesSQL returns SQL for wait event queries with configurable row limit
func GetWaitEventQueriesSQL(rowLimit int) string {
	return fmt.Sprintf(`
		SELECT
			SYSTIMESTAMP AS COLLECTION_TIMESTAMP,
			s.username,
			s.sid,
			s.serial#,
			s.status,
			s.sql_id,
			s.SQL_CHILD_NUMBER,
			s.wait_class,
			s.event,
			s.SECONDS_IN_WAIT,
			s.TIME_REMAINING_MICRO / 1000000.0 AS time_remaining_seconds,
			s.SQL_EXEC_START,
			s.SQL_EXEC_ID,
			s.PROGRAM,
			s.MACHINE,
			s.ROW_WAIT_OBJ#,
			o.OWNER,
			o.OBJECT_NAME,
			o.OBJECT_TYPE,
			s.ROW_WAIT_FILE#,
			s.ROW_WAIT_BLOCK#,
			s.p1text,
			s.p1,
			s.p2text,
			s.p2,
			s.p3text,
			s.p3
		FROM
			v$session s
		LEFT JOIN
			DBA_OBJECTS o ON s.ROW_WAIT_OBJ# = o.OBJECT_ID
		WHERE
			s.status = 'ACTIVE'
			AND s.wait_class <> 'Idle'
			AND s.SECONDS_IN_WAIT > 0
		ORDER BY
			s.SECONDS_IN_WAIT DESC
		FETCH FIRST %d ROWS ONLY`, rowLimit)
}

// GetChildCursorsQuery returns SQL to get top N child cursors for a given SQL_ID from V$SQL
// Returns average metrics per execution to normalize performance data
// Ordered by most recent load time to get the latest child cursor versions
func GetChildCursorsQuery(sqlID string, childLimit int) string {
	return fmt.Sprintf(`
		SELECT
			d.name AS database_name,
			s.sql_id,
			s.child_number,
			CASE WHEN s.executions > 0 THEN s.cpu_time / s.executions ELSE 0 END AS avg_cpu_time_us,
			CASE WHEN s.executions > 0 THEN s.elapsed_time / s.executions ELSE 0 END AS avg_elapsed_time_us,
			CASE WHEN s.executions > 0 THEN s.user_io_wait_time / s.executions ELSE 0 END AS avg_io_wait_time_us,
			CASE WHEN s.executions > 0 THEN s.disk_reads / s.executions ELSE 0 END AS avg_disk_reads,
			CASE WHEN s.executions > 0 THEN s.buffer_gets / s.executions ELSE 0 END AS avg_buffer_gets,
			s.executions,
			s.invalidations,
			s.first_load_time,
			s.last_load_time
		FROM
			v$sql s
		CROSS JOIN
			v$database d
		WHERE
			s.sql_id = '%s'
		ORDER BY
			s.last_load_time DESC
		FETCH FIRST %d ROWS ONLY`, sqlID, childLimit)
}

// GetExecutionPlanForChildQuery returns SQL to get execution plan from V$SQL_PLAN for specific child number
func GetExecutionPlanForChildQuery(sqlID string, childNumber int64) string {
	return fmt.Sprintf(`
		SELECT
			SQL_ID,
			TIMESTAMP,
			TEMP_SPACE,
			ACCESS_PREDICATES,
			PROJECTION,
			TIME,
			FILTER_PREDICATES,
			CHILD_NUMBER,
			ID,
			PARENT_ID,
			DEPTH,
			OPERATION,
			OPTIONS,
			OBJECT_OWNER,
			OBJECT_NAME,
			POSITION,
			PLAN_HASH_VALUE,
			COST,
			CARDINALITY,
			BYTES,
			CPU_COST,
			IO_COST
	FROM
		V$SQL_PLAN
	WHERE
		SQL_ID = '%s'
		AND CHILD_NUMBER = %d`, sqlID, childNumber)
}
