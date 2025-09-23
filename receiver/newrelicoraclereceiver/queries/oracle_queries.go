// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL query constants
const (
	SessionCountSQL = "SELECT COUNT(*) as SESSION_COUNT FROM v$session WHERE type = 'USER'"

	// SlowQuerySQL retrieves top slow queries sorted by average elapsed time
	SlowQuerySQL = `
        SELECT
            d.name AS database_name,
            sql_id AS query_id,
            parsing_schema_name AS schema_name,
            COALESCE(module, 'UNKNOWN') AS statement_type,
            executions AS execution_count,
            last_load_time AS collection_timestamp,
            last_active_time AS last_execution_timestamp,
            sql_fulltext AS query_text,
            cpu_time / DECODE(executions, 0, 1, executions) / 1000 AS avg_cpu_time_ms,
            disk_reads / DECODE(executions, 0, 1, executions) AS avg_disk_reads,
            elapsed_time / DECODE(executions, 0, 1, executions) / 1000 AS avg_elapsed_time_ms,
            CASE WHEN full_plan_hash_value != 0 THEN 'YES' ELSE 'NO' END AS has_full_table_scan
        FROM
            v$sqlarea,
            v$database d
        WHERE
            parsing_schema_name NOT IN ('SYS', 'SYSTEM', 'DBSNMP', 'OUTLN', 'DVSYS', 'AUDSYS', 'LABCSYS','DVSYS')
            AND executions > 0
        ORDER BY
            avg_elapsed_time_ms DESC,
            query_id
        FETCH FIRST 10 ROWS ONLY`
)
