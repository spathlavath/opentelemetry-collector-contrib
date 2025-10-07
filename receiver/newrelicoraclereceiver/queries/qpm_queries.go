// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL query for slow queries metrics
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

	// Oracle SQL query for individual queries metrics
	IndividualQueriesSQL = `
		SELECT
		    sql_id as query_id,
		    sql_text as query_text,
		    cpu_time/1000 as cpu_time_ms,
		    elapsed_time / 1000 AS elapsed_time_ms
		FROM
		    v$sql
		WHERE
		    sql_id is not null
		ORDER BY
		    elapsed_time_ms DESC
		FETCH FIRST 10 ROWS ONLY`

	// Oracle SQL query for individual queries metrics filtered by query IDs
	// This query uses placeholders for query IDs that will be replaced programmatically
	IndividualQueriesFilteredSQL = `
		SELECT
		    sql_id as query_id,
		    sql_text as query_text,
		    cpu_time/1000 as cpu_time_ms,
		    elapsed_time / 1000 AS elapsed_time_ms
		FROM
		    v$sql
		WHERE
		    sql_id IN (%s)
		    AND sql_id is not null
		ORDER BY
		    elapsed_time_ms DESC`
)
