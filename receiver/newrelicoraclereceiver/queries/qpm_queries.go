// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

import "fmt"

// GetSlowQueriesSQL returns SQL for slow queries with configurable response time threshold and row limit
func GetSlowQueriesSQL(responseTimeThreshold, rowLimit int) string {
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
			sa.last_active_time AS last_active_time_ms
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
			AND sa.last_active_time >= TRUNC(SYSDATE)
			AND sa.elapsed_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000 >= %d
		ORDER BY
			avg_elapsed_time_ms DESC
		FETCH FIRST %d ROWS ONLY`, responseTimeThreshold, rowLimit)
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
			s2.blocking_session IS NOT NULL
			AND s2.seconds_in_wait > 0
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
			s.status,
			s.sql_id,
			s.wait_class,
			s.event,
			s.SECONDS_IN_WAIT,
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

// GetExecutionPlanQuery returns SQL to get execution plan from V$SQL_PLAN
func GetExecutionPlanQuery(sqlIDs string) string {
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
		SQL_ID = '%s'`, sqlIDs)
}

// GetActiveSessionQueriesSQL returns SQL to fetch active session queries by SQL IDs
func GetActiveSessionQueriesSQL(sqlIDs string) string {
	return fmt.Sprintf(`
		SELECT
			SYSTIMESTAMP AS COLLECTION_TIMESTAMP,
			s.username,
			s.sid,
			s.serial#,
			s.sql_id AS query_id,
			s.SQL_CHILD_NUMBER,
			s.SQL_EXEC_START,
			s.SQL_EXEC_ID,
			s.SECONDS_IN_WAIT
		FROM
			v$session s
		WHERE
			s.sql_id IN (%s)
			AND s.status = 'ACTIVE'
			AND s.wait_class <> 'Idle'
		ORDER BY
			s.SECONDS_IN_WAIT DESC`, sqlIDs)
}
