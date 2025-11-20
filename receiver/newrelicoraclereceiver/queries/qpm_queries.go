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
			s2.sid AS blocked_sid,
			s2.serial# AS blocked_serial,
			s2.username AS blocked_user,
			s2.seconds_in_wait AS blocked_wait_sec,
			s2.sql_id AS query_id,
			s2.sql_exec_start AS blocked_sql_exec_start,
			blocked_sql.sql_text AS blocked_query_text,
			s1.sid AS blocking_sid,
			s1.serial# AS blocking_serial,
			s1.username AS blocking_user,
			d.name AS database_name
		FROM
			v$session s2
		JOIN
			v$session s1 ON s2.blocking_session = s1.sid
		LEFT JOIN
			v$sql blocked_sql ON s2.sql_id = blocked_sql.sql_id
		CROSS JOIN
			v$database d
		WHERE
			s2.blocking_session IS NOT NULL
		ORDER BY
			s2.seconds_in_wait DESC
		FETCH FIRST %d ROWS ONLY`, rowLimit)
}

// GetWaitEventQueriesSQL returns SQL for wait event queries with configurable row limit
func GetWaitEventQueriesSQL(rowLimit int) string {
	return fmt.Sprintf(`
		SELECT 
			s.username,
			s.sid,
			s.status,
			s.sql_id,
			s.wait_class,
			s.event,
			s.SECONDS_IN_WAIT,
			s.SQL_EXEC_START,
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
		SQL_ID IN (%s)`, sqlIDs)
}

// GetActiveSessionQueriesSQL returns SQL to fetch active session queries by SQL IDs
func GetActiveSessionQueriesSQL(sqlIDs string) string {
	return fmt.Sprintf(`
		SELECT
			s.username,
			s.sid,
			s.serial#,
			s.sql_id AS query_id,
			s.SQL_CHILD_NUMBER,
			s.SQL_EXEC_START,
			s.SQL_EXEC_ID
		FROM
			v$session s
		WHERE
			s.sql_id IN (%s)
			AND s.status = 'ACTIVE'
			AND s.wait_class <> 'Idle'`, sqlIDs)
}
