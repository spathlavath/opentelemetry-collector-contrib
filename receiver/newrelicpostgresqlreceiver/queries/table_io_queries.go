// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"fmt"
	"strings"
)

// PgStatUserTablesSQL returns per-table statistics from pg_stat_user_tables (PostgreSQL 9.6+)
// This query retrieves vacuum/analyze statistics and row-level activity per table
// The WHERE clause is dynamically built based on schema and table filters
func PgStatUserTablesSQL(schemas, tables []string) string {
	whereClause := buildTableFilterClause(schemas, tables)

	return fmt.Sprintf(`
		SELECT
			current_database() as database,
			schemaname,
			relname as table_name,
			seq_scan,
			seq_tup_read,
			idx_scan,
			idx_tup_fetch,
			n_tup_ins,
			n_tup_upd,
			n_tup_del,
			n_tup_hot_upd,
			n_live_tup,
			n_dead_tup,
			n_mod_since_analyze,
			EXTRACT(EPOCH FROM (now() - last_vacuum)) as last_vacuum_age,
			EXTRACT(EPOCH FROM (now() - last_autovacuum)) as last_autovacuum_age,
			EXTRACT(EPOCH FROM (now() - last_analyze)) as last_analyze_age,
			EXTRACT(EPOCH FROM (now() - last_autoanalyze)) as last_autoanalyze_age,
			vacuum_count,
			autovacuum_count,
			analyze_count,
			autoanalyze_count
		FROM pg_stat_user_tables
		WHERE %s
		ORDER BY schemaname, relname`, whereClause)
}

// buildTableFilterClause builds the WHERE clause for filtering tables by schema and name
// schemas: list of schema names to include (defaults to ["public"] if empty)
// tables: list of table names to include (required, returns "1=0" if empty)
func buildTableFilterClause(schemas, tables []string) string {
	// Default to public schema if not specified
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	// If no tables specified, return impossible condition (no metrics collected)
	if len(tables) == 0 {
		return "1=0"
	}

	// Build schema filter
	schemaList := make([]string, len(schemas))
	for i, schema := range schemas {
		schemaList[i] = fmt.Sprintf("'%s'", schema)
	}
	schemaFilter := fmt.Sprintf("schemaname IN (%s)", strings.Join(schemaList, ", "))

	// Build table filter
	tableList := make([]string, len(tables))
	for i, table := range tables {
		tableList[i] = fmt.Sprintf("'%s'", table)
	}
	tableFilter := fmt.Sprintf("relname IN (%s)", strings.Join(tableList, ", "))

	return fmt.Sprintf("%s AND %s", schemaFilter, tableFilter)
}

// PgStatIOUserTablesSQL returns per-table disk IO statistics from pg_statio_user_tables (PostgreSQL 9.6+)
// This query retrieves heap, index, and TOAST block reads from disk vs buffer cache
// The WHERE clause is dynamically built based on schema and table filters
func PgStatIOUserTablesSQL(schemas, tables []string) string {
	whereClause := buildTableFilterClause(schemas, tables)

	return fmt.Sprintf(`
		SELECT
			current_database() as database,
			schemaname,
			relname as table_name,
			heap_blks_read,
			heap_blks_hit,
			idx_blks_read,
			idx_blks_hit,
			toast_blks_read,
			toast_blks_hit,
			tidx_blks_read,
			tidx_blks_hit
		FROM pg_statio_user_tables
		WHERE %s
		ORDER BY schemaname, relname`, whereClause)
}

// PgStatUserIndexesSQL returns per-index statistics from pg_stat_user_indexes (PostgreSQL 9.6+)
// This query retrieves index usage statistics for individual indexes
// The WHERE clause is dynamically built based on schema and table filters
// Also calculates index size using pg_relation_size function
func PgStatUserIndexesSQL(schemas, tables []string) string {
	whereClause := buildTableFilterClause(schemas, tables)

	return fmt.Sprintf(`
		SELECT
			current_database() as database,
			schemaname,
			relname as table_name,
			indexrelname as index_name,
			indexrelid,
			idx_scan,
			idx_tup_read,
			idx_tup_fetch
		FROM pg_stat_user_indexes
		WHERE %s
		ORDER BY schemaname, relname, indexrelname`, whereClause)
}

// PgStatToastTablesSQL returns TOAST table vacuum statistics (PostgreSQL 9.6+)
// This query retrieves vacuum/autovacuum statistics for TOAST tables
// TOAST tables are automatically created for tables with large column values
// The WHERE clause is dynamically built based on schema and table filters (applied to base table)
// Note: TOAST tables are system tables in pg_toast schema, so we use pg_stat_all_tables
func PgStatToastTablesSQL(schemas, tables []string) string {
	whereClause := buildTableFilterClause(schemas, tables)

	// Replace the table-level filters to use "stat." prefix since we're joining
	// This ensures the filter applies to the base table, not the TOAST table
	whereClause = strings.Replace(whereClause, "schemaname", "stat.schemaname", -1)
	whereClause = strings.Replace(whereClause, "relname", "stat.relname", -1)

	return fmt.Sprintf(`
		SELECT
			current_database() as database,
			stat.schemaname,
			stat.relname as table_name,
			toast.vacuum_count as toast_vacuumed,
			toast.autovacuum_count as toast_autovacuumed,
			EXTRACT(EPOCH FROM (now() - toast.last_vacuum)) as toast_last_vacuum_age,
			EXTRACT(EPOCH FROM (now() - toast.last_autovacuum)) as toast_last_autovacuum_age
		FROM pg_stat_user_tables stat
		JOIN pg_class c ON stat.relid = c.oid
		JOIN pg_stat_all_tables toast ON toast.relid = c.reltoastrelid
		WHERE %s AND c.reltoastrelid != 0
		ORDER BY stat.schemaname, stat.relname`, whereClause)
}
