// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle system metrics and performance monitoring queries

// System Performance Metrics
const (
	// SystemSysMetricsSQL returns system metrics from gv$sysmetric
	SystemSysMetricsSQL = "SELECT INST_ID, METRIC_NAME, VALUE FROM gv$sysmetric"

	// PDBSysMetricsSQL returns PDB-specific system metrics
	PDBSysMetricsSQL = `
		SELECT
			csm.INST_ID,
			pd.NAME AS PDB_NAME,
			csm.METRIC_NAME,
			csm.VALUE
		FROM gv$con_sysmetric csm
		JOIN gv$pdbs pd ON csm.CON_ID = pd.CON_ID AND csm.INST_ID = pd.INST_ID`

	// ReadWriteMetricsSQL returns database I/O statistics
	ReadWriteMetricsSQL = `
		SELECT
			INST_ID,
			SUM(PHYRDS) AS PhysicalReads,
			SUM(PHYWRTS) AS PhysicalWrites,
			SUM(PHYBLKRD) AS PhysicalBlockReads,
			SUM(PHYBLKWRT) AS PhysicalBlockWrites,
			SUM(READTIM) * 10 AS ReadTime,
			SUM(WRITETIM) * 10 AS WriteTime
		FROM gv$filestat
		GROUP BY INST_ID`
)

// System Statistics Queries
const (
	// SysstatSQL returns specific system statistics
	SysstatSQL = `
		SELECT 
			inst.inst_id, 
			sysstat.name, 
			sysstat.value
		FROM GV$SYSSTAT sysstat, GV$INSTANCE inst
		WHERE sysstat.inst_id = inst.inst_id 
			AND sysstat.name IN (
				'redo buffer allocation retries', 
				'redo entries', 
				'sorts (memory)', 
				'sorts (disk)'
			)`
)

// Wait Events and Lock Queries
const (
	// RedoLogWaitsSQL returns redo log related wait events
	RedoLogWaitsSQL = `
		SELECT
			sysevent.total_waits,
			inst.inst_id,
			sysevent.event
		FROM GV$SYSTEM_EVENT sysevent, GV$INSTANCE inst
		WHERE sysevent.inst_id = inst.inst_id
			AND (sysevent.event LIKE '%log file parallel write%'
				OR sysevent.event LIKE '%log file switch completion%'
				OR sysevent.event LIKE '%log file switch (check%'
				OR sysevent.event LIKE '%log file switch (arch%'
				OR sysevent.event LIKE '%buffer busy waits%'
				OR sysevent.event LIKE '%freeBufferWaits%'
				OR sysevent.event LIKE '%free buffer inspected%')`

	// RollbackSegmentsSQL returns rollback segment statistics
	RollbackSegmentsSQL = `
		SELECT
			SUM(stat.gets) AS gets,
			SUM(stat.waits) AS waits,
			SUM(stat.waits)/SUM(stat.gets) AS ratio,
			inst.inst_id
		FROM GV$ROLLSTAT stat, GV$INSTANCE inst
		WHERE stat.inst_id = inst.inst_id
		GROUP BY inst.inst_id`
)
