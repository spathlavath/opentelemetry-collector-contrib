// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"fmt"
	"strings"
)

// Oracle SQL query constants
const (
	SessionCountSQL = "SELECT COUNT(*) as SESSION_COUNT FROM v$session WHERE type = 'USER'"

	// Base slow query SQL template
	slowQuerySQLBase = `
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
            executions > 0
            %s
        ORDER BY
            avg_elapsed_time_ms DESC,
            query_id
        FETCH FIRST %d ROWS ONLY`
)

// SlowQueryConfig represents slow query filtering configuration
type SlowQueryConfig struct {
	Enabled              bool
	ExcludeSchemas       []string
	MinExecutionTimeMs   int
	ExcludeQueryPatterns []string
	MaxQueries           int
}

// BuildSlowQuerySQL builds a dynamic slow query SQL based on configuration
func BuildSlowQuerySQL(config SlowQueryConfig) string {
	var conditions []string

	// Add schema exclusion
	if len(config.ExcludeSchemas) > 0 {
		schemaList := "'" + strings.Join(config.ExcludeSchemas, "', '") + "'"
		conditions = append(conditions, fmt.Sprintf("AND parsing_schema_name NOT IN (%s)", schemaList))
	}

	// Add minimum execution time filter
	if config.MinExecutionTimeMs > 0 {
		conditions = append(conditions, fmt.Sprintf("AND elapsed_time / DECODE(executions, 0, 1, executions) > %d", config.MinExecutionTimeMs*1000))
	}

	// Add query pattern exclusions
	for _, pattern := range config.ExcludeQueryPatterns {
		conditions = append(conditions, fmt.Sprintf("AND UPPER(sql_fulltext) NOT LIKE '%%%s%%'", strings.ToUpper(pattern)))
	}

	// Join all conditions
	whereClause := strings.Join(conditions, " ")

	// Set max queries limit
	maxQueries := config.MaxQueries
	if maxQueries <= 0 {
		maxQueries = 10 // Default
	}

	return fmt.Sprintf(slowQuerySQLBase, whereClause, maxQueries)
}
