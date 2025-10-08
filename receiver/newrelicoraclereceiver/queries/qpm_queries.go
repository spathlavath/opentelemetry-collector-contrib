// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL queries for performance metrics
const (
	SlowQueriesSQL = `
		WITH full_scans AS (
			-- Get a distinct list of SQL_IDs that have a full table scan in their plan
			SELECT DISTINCT sql_id
			FROM   v$sql_plan
			WHERE  operation = 'TABLE ACCESS' AND options = 'FULL'
		)
		SELECT
			d.name AS database_name,
			sa.sql_id AS query_id,
			sa.parsing_schema_name AS schema_name,
			COALESCE(sa.module,
				CASE
					WHEN UPPER(LTRIM(sa.sql_text)) LIKE 'SELECT%' THEN 'SELECT'
					WHEN UPPER(LTRIM(sa.sql_text)) LIKE 'INSERT%' THEN 'INSERT'
					WHEN UPPER(LTRIM(sa.sql_text)) LIKE 'UPDATE%' THEN 'UPDATE'
					WHEN UPPER(LTRIM(sa.sql_text)) LIKE 'DELETE%' THEN 'DELETE'
					ELSE 'OTHER'
				END
			) AS statement_type,
			sa.executions AS execution_count,
			sa.sql_text AS query_text,
			sa.cpu_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000 AS avg_cpu_time_ms,
			sa.disk_reads / DECODE(sa.executions, 0, 1, sa.executions) AS avg_disk_reads,
			sa.direct_writes / DECODE(sa.executions, 0, 1, sa.executions) AS avg_disk_writes,
			sa.elapsed_time / DECODE(sa.executions, 0, 1, sa.executions) / 1000 AS avg_elapsed_time_ms,
			CASE WHEN fs.sql_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_full_table_scan
		FROM
			v$sqlarea sa
		CROSS JOIN
			v$database d
		LEFT JOIN
			full_scans fs ON sa.sql_id = fs.sql_id
		WHERE
		    sa.parsing_schema_name NOT IN ('SYS', 'SYSTEM', 'MDSYS', 'DVSYS', 'LBACSYS', 'DBSNMP', 'SYSMAN', 'AUDSYS','WMSYS', 'XDB', 'OJVMSYS', 'CTXSYS')
			AND sa.executions > 0
			AND sa.sql_text NOT LIKE '%full_scans AS%'
		ORDER BY
			avg_elapsed_time_ms DESC
		FETCH FIRST 10 ROWS ONLY`

	BlockingQueriesSQL = `
		SELECT
			s2.sid AS blocked_sid,
			s2.serial# AS blocked_serial,
			s2.username AS blocked_user,
			s2.seconds_in_wait AS blocked_wait_sec,
			s2.sql_id AS blocked_sql_id,
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
			s2.seconds_in_wait DESC`

	IndividualQueriesSQL = `
		SELECT
			s.sid,
			s.serial#,
			s.username,
			s.status,
			a.sql_id as query_id,
			a.plan_hash_value,
			a.elapsed_time / 1000 AS elapsed_time_ms,
			a.cpu_time / 1000 as cpu_time_ms,
			s.osuser,
			s.machine as hostname,
			a.sql_text as query_text
		FROM
			v$session s
		JOIN
			v$sql a ON s.sql_id = a.sql_id AND s.sql_child_number = a.child_number
		WHERE
			s.status = 'ACTIVE'
			AND s.username IS NOT NULL
			AND a.sql_id in (%s)
		FETCH FIRST 10 ROWS ONLY`
)
