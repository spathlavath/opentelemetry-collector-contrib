// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL queries for metrics collection
const (
	// SessionCountSQL retrieves the count of user sessions
	SessionCountSQL = "SELECT COUNT(*) as SESSION_COUNT FROM v$session WHERE type = 'USER'"

	// PDBSysMetricsSQL retrieves all system metrics from gv$con_sysmetric
	// This matches the approach used in nri-oracledb for PDB container metrics
	PDBSysMetricsSQL = "SELECT INST_ID, METRIC_NAME, VALUE FROM gv$con_sysmetric"

	// RedoLogWaitsSQL retrieves redo log and system event waits from gv$system_event
	// This matches the approach used in nri-oracledb oracleRedoLogWaits metric group
	RedoLogWaitsSQL = `
		SELECT
			sysevent.total_waits,
			inst.inst_id,
			sysevent.event
		FROM
			GV$SYSTEM_EVENT sysevent,
			GV$INSTANCE inst
		WHERE sysevent.inst_id=inst.inst_id`

	// RollbackSegmentsSQL retrieves rollback segment statistics from gv$rollstat
	// This matches the approach used in nri-oracledb oracleRollbackSegments metric group
	RollbackSegmentsSQL = `
		SELECT
			SUM(stat.gets) AS gets,
			sum(stat.waits) AS waits,
			sum(stat.waits)/sum(stat.gets) AS ratio,
			inst.inst_id
		FROM GV$ROLLSTAT stat, GV$INSTANCE inst
		WHERE stat.inst_id=inst.inst_id
		GROUP BY inst.inst_id`

	// SGAMetricsSQL retrieves SGA metrics from gv$sga
	// This matches the approach used in nri-oracledb oracleSGA metric group
	SGAMetricsSQL = `
		SELECT inst.inst_id, sga.name, sga.value
		FROM GV$SGA sga, GV$INSTANCE inst
		WHERE sga.inst_id=inst.inst_id 
		AND sga.name IN ('Fixed Size', 'Redo Buffers')`
)
