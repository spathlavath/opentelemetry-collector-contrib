// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL query for slow queries metrics
const (
	SlowQueriesSQL = `
		SELECT
			d.name AS database_name,
			sql_id AS query_id,
			parsing_schema_name AS schema_name,
			module AS statement_type,
			executions AS execution_count,
			sql_fulltext AS query_text,
			cpu_time / DECODE(executions, 0, 1, executions) / 1000 AS avg_cpu_time_ms,
			disk_reads / DECODE(executions, 0, 1, executions) AS avg_disk_reads,
			elapsed_time / DECODE(executions, 0, 1, executions) / 1000 AS avg_elapsed_time_ms
		FROM
			v$sqlarea,
			v$database d
		WHERE
			parsing_schema_name NOT IN ('SYS', 'SYSTEM')
			AND executions > 0
		ORDER BY
			avg_elapsed_time_ms DESC
		FETCH FIRST 10 ROWS ONLY`
)
