// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// PgStatProgressAnalyzeSQL returns ANALYZE operation progress from pg_stat_progress_analyze (PostgreSQL 13+)
// This query retrieves real-time progress of running ANALYZE operations
// The query returns data only for actively running ANALYZE operations
const PgStatProgressAnalyzeSQL = `
	SELECT
		datname as database,
		COALESCE(n.nspname, '') as schemaname,
		COALESCE(c.relname, '') as table_name,
		phase,
		sample_blks_total,
		sample_blks_scanned,
		ext_stats_total,
		ext_stats_computed,
		child_tables_total,
		child_tables_done
	FROM pg_stat_progress_analyze ppa
	LEFT JOIN pg_class c ON ppa.relid = c.oid
	LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE ppa.datname = current_database()
	ORDER BY n.nspname, c.relname`

// PgStatProgressClusterSQL returns CLUSTER/VACUUM FULL operation progress from pg_stat_progress_cluster (PostgreSQL 12+)
// This query retrieves real-time progress of running CLUSTER or VACUUM FULL operations
// The query returns data only for actively running operations
const PgStatProgressClusterSQL = `
	SELECT
		datname as database,
		COALESCE(n.nspname, '') as schemaname,
		COALESCE(c.relname, '') as table_name,
		command,
		phase,
		heap_blks_total,
		heap_blks_scanned,
		heap_tuples_scanned,
		heap_tuples_written,
		index_rebuild_count
	FROM pg_stat_progress_cluster ppc
	LEFT JOIN pg_class c ON ppc.relid = c.oid
	LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE ppc.datname = current_database()
	ORDER BY n.nspname, c.relname`

// PgStatProgressCreateIndexSQL returns CREATE INDEX operation progress from pg_stat_progress_create_index (PostgreSQL 12+)
// This query retrieves real-time progress of running CREATE INDEX operations
// The query returns data only for actively running CREATE INDEX operations
const PgStatProgressCreateIndexSQL = `
	SELECT
		datname as database,
		COALESCE(n.nspname, '') as schemaname,
		COALESCE(c.relname, '') as table_name,
		COALESCE(ci.relname, '') as index_name,
		command,
		phase,
		lockers_total,
		lockers_done,
		blocks_total,
		blocks_done,
		tuples_total,
		tuples_done,
		partitions_total,
		partitions_done
	FROM pg_stat_progress_create_index ppci
	LEFT JOIN pg_class c ON ppci.relid = c.oid
	LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
	LEFT JOIN pg_class ci ON ppci.index_relid = ci.oid
	WHERE ppci.datname = current_database()
	ORDER BY n.nspname, c.relname`

// PgStatProgressVacuumSQL returns VACUUM operation progress from pg_stat_progress_vacuum (PostgreSQL 12+)
// This query retrieves real-time progress of running VACUUM operations
// The query returns data only for actively running VACUUM operations
const PgStatProgressVacuumSQL = `
	SELECT
		datname as database,
		COALESCE(n.nspname, '') as schemaname,
		COALESCE(c.relname, '') as table_name,
		phase,
		heap_blks_total,
		heap_blks_scanned,
		heap_blks_vacuumed,
		index_vacuum_count,
		max_dead_tuples,
		num_dead_tuples
	FROM pg_stat_progress_vacuum ppv
	LEFT JOIN pg_class c ON ppv.relid = c.oid
	LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE ppv.datname = current_database()
	ORDER BY n.nspname, c.relname`
