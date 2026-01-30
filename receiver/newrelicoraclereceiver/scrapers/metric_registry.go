// Copyright 2025 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

// MetricRecorderFunc defines a function type for recording metrics
type MetricRecorderFunc func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string)

// PdbMetricRecorderFunc defines a function type for recording PDB metrics
type PdbMetricRecorderFunc func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string)

// SystemMetricRegistry maps metric names to their recorder functions
type SystemMetricRegistry struct {
	recorders map[string]MetricRecorderFunc
}

// PdbMetricRegistry maps PDB metric names to their recorder functions
type PdbMetricRegistry struct {
	recorders map[string]PdbMetricRecorderFunc
}

// NewSystemMetricRegistry creates and initializes a system metric registry
func NewSystemMetricRegistry() *SystemMetricRegistry {
	registry := &SystemMetricRegistry{
		recorders: make(map[string]MetricRecorderFunc, 150),
	}
	registry.registerAll()
	return registry
}

// RecordMetric records a metric using the registered recorder function
func (r *SystemMetricRegistry) RecordMetric(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, metricName string, value float64, instanceID string) bool {
	recorder, exists := r.recorders[metricName]
	if !exists {
		return false
	}
	recorder(mb, ts, value, instanceID)
	return true
}

// NewPdbMetricRegistry creates and initializes a PDB metric registry
func NewPdbMetricRegistry() *PdbMetricRegistry {
	registry := &PdbMetricRegistry{
		recorders: make(map[string]PdbMetricRecorderFunc, 55),
	}
	registry.registerAll()
	return registry
}

// RecordMetric records a PDB metric using the registered recorder function
func (r *PdbMetricRegistry) RecordMetric(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, metricName string, value float64, instanceID, pdbName string) bool {
	recorder, exists := r.recorders[metricName]
	if !exists {
		return false
	}
	recorder(mb, ts, value, instanceID, pdbName)
	return true
}

// registerAll registers all system metric recorder functions
func (r *SystemMetricRegistry) registerAll() {
	// Cache and performance ratios
	r.recorders["Buffer Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemBufferCacheHitRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Memory Sorts Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemMemorySortsRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Redo Allocation Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoAllocationHitRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Cursor Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCursorCacheHitRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Soft Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemSoftParseRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["User Calls Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCallsRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Row Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRowCacheHitRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Row Cache Miss Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRowCacheMissRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Library Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLibraryCacheHitRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Library Cache Miss Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLibraryCacheMissRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Database Wait Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDatabaseWaitTimeRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Database CPU Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDatabaseCPUTimeRatioDataPoint(ts, value, instanceID)
	}
	r.recorders["Execute Without Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecuteWithoutParseRatioDataPoint(ts, value, instanceID)
	}

	// Transaction metrics
	r.recorders["User Transaction Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTransactionsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Writes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Reads Direct Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsDirectPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Writes Direct Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesDirectPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Reads Direct Lobs Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsReadsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Writes Direct Lobs Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsWritesPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Redo Generated Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoGeneratedBytesPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Logons Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogonsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Open Cursors Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemOpenCursorsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["User Calls Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCallsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Recursive Calls Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRecursiveCallsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Logical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogicalReadsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Redo Writes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoWritesPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Long Table Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLongTableScansPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Table Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalTableScansPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Full Index Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemFullIndexScansPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Index Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalIndexScansPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalParseCountPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Hard Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemHardParseCountPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Parse Failure Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemParseFailureCountPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Disk Sort Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDiskSortPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Timeouts Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueTimeoutsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Waits Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueWaitsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Deadlocks Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueDeadlocksPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Requests Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueRequestsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["DB Block Gets Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockGetsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Consistent Read Gets Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadGetsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["DB Block Changes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockChangesPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Consistent Read Changes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadChangesPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["CPU Usage Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCPUUsagePerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["CR Blocks Created Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrBlocksCreatedPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["CR Undo Records Applied Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrUndoRecordsAppliedPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["User Rollback Undo Records Applied Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbackUndoRecordsAppliedPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Leaf Node Splits Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLeafNodeSplitsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Branch Node Splits Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemBranchNodeSplitsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["GC CR Block Received Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCrBlockReceivedPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["GC Current Block Received Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCurrentBlockReceivedPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Response Time Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemResponseTimePerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Executions Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecutionsPerTransactionDataPoint(ts, value, instanceID)
	}
	r.recorders["Txns Per Logon"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTransactionsPerLogonDataPoint(ts, value, instanceID)
	}

	// Per second metrics
	r.recorders["Physical Reads Direct Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsDirectPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Writes Direct Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesDirectPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Reads Direct Lobs Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsReadsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Writes Direct Lobs Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsWritesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Redo Generated Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoGeneratedBytesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Open Cursors Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemOpenCursorsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["User Commits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCommitsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["User Commits Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCommitsPercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["User Rollbacks Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbacksPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["User Rollbacks Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbacksPercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["User Calls Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCallsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Recursive Calls Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRecursiveCallsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Logical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogicalReadsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["DBWR Checkpoints Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbwrCheckpointsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Background Checkpoints Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemBackgroundCheckpointsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Redo Writes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoWritesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Long Table Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLongTableScansPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Table Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalTableScansPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Full Index Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemFullIndexScansPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Index Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalIndexScansPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalParseCountPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Hard Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemHardParseCountPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Parse Failure Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemParseFailureCountPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Disk Sort Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDiskSortPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Timeouts Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueTimeoutsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Waits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueWaitsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Deadlocks Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueDeadlocksPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Enqueue Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueRequestsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["DB Block Gets Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockGetsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Consistent Read Gets Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadGetsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["DB Block Changes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockChangesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Consistent Read Changes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadChangesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCPUUsagePerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["CR Blocks Created Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrBlocksCreatedPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["CR Undo Records Applied Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrUndoRecordsAppliedPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["User Rollback UndoRec Applied Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbackUndoRecordsAppliedPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Leaf Node Splits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLeafNodeSplitsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Branch Node Splits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemBranchNodeSplitsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Read Total IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadTotalIoRequestsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Read Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadTotalBytesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["GC CR Block Received Per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCrBlockReceivedPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["GC Current Block Received Per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCurrentBlockReceivedPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Write Total IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteTotalIoRequestsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Write Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteTotalBytesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Write IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteIoRequestsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Database Time Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDatabaseTimePerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Network Traffic Volume Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemNetworkTrafficVolumePerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Executions Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecutionsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Logons Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogonsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Read Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadBytesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Read IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadIoRequestsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Write Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteBytesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Physical Writes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Background CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemBackgroundCPUUsagePerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Background Time Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemBackgroundTimePerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Host CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemHostCPUUsagePerSecondDataPoint(ts, value, instanceID)
	}

	// Single value metrics
	r.recorders["Rows Per Sort"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemRowsPerSortDataPoint(ts, value, instanceID)
	}
	r.recorders["Host CPU Utilization (%)"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemHostCPUUtilizationDataPoint(ts, value, instanceID)
	}
	r.recorders["Global Cache Average CR Get Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheAverageCrGetTimeDataPoint(ts, value, instanceID)
	}
	r.recorders["Global Cache Average Current Get Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheAverageCurrentGetTimeDataPoint(ts, value, instanceID)
	}
	r.recorders["Global Cache Blocks Corrupted"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheBlocksCorruptedDataPoint(ts, value, instanceID)
	}
	r.recorders["Global Cache Blocks Lost"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheBlocksLostDataPoint(ts, value, instanceID)
	}
	r.recorders["Current Logons Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCurrentLogonsCountDataPoint(ts, value, instanceID)
	}
	r.recorders["Current Open Cursors Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCurrentOpenCursorsCountDataPoint(ts, value, instanceID)
	}
	r.recorders["User Limit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserLimitPercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["SQL Service Response Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemSQLServiceResponseTimeDataPoint(ts, value, instanceID)
	}
	r.recorders["Shared Pool Free %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemSharedPoolFreePercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["PGA Cache Hit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemPgaCacheHitPercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["Process Limit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemProcessLimitPercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["Session Limit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemSessionLimitPercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["Temp Space Used"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTempSpaceUsedDataPoint(ts, value, instanceID)
	}
	r.recorders["Session Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemSessionCountDataPoint(ts, value, instanceID)
	}
	r.recorders["Captured user calls"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCapturedUserCallsDataPoint(ts, value, instanceID)
	}
	r.recorders["Current OS Load"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemCurrentOsLoadDataPoint(ts, value, instanceID)
	}
	r.recorders["Streams Pool Usage Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemStreamsPoolUsagePercentageDataPoint(ts, value, instanceID)
	}
	r.recorders["I/O Megabytes per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemIoMegabytesPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["I/O Requests per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemIoRequestsPerSecondDataPoint(ts, value, instanceID)
	}
	r.recorders["Average Active Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemAverageActiveSessionsDataPoint(ts, value, instanceID)
	}
	r.recorders["Active Serial Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemActiveSerialSessionsDataPoint(ts, value, instanceID)
	}
	r.recorders["Active Parallel Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemActiveParallelSessionsDataPoint(ts, value, instanceID)
	}

	// Per user call metrics
	r.recorders["DB Block Changes Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockChangesPerUserCallDataPoint(ts, value, instanceID)
	}
	r.recorders["DB Block Gets Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockGetsPerUserCallDataPoint(ts, value, instanceID)
	}
	r.recorders["Executions Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecutionsPerUserCallDataPoint(ts, value, instanceID)
	}
	r.recorders["Logical Reads Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogicalReadsPerUserCallDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Sorts Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalSortsPerUserCallDataPoint(ts, value, instanceID)
	}
	r.recorders["Total Table Scans Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalTableScansPerUserCallDataPoint(ts, value, instanceID)
	}
}

// registerAll registers all PDB metric recorder functions
func (r *PdbMetricRegistry) registerAll() {
	r.recorders["Active Parallel Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbActiveParallelSessionsDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Active Serial Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbActiveSerialSessionsDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Average Active Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbAverageActiveSessionsDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Background CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBackgroundCPUUsagePerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Background Time Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBackgroundTimePerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCPUUsagePerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["CPU Usage Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCPUUsagePerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Current Logons Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCurrentLogonsDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Current Open Cursors Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCurrentOpenCursorsDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Database CPU Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCPUTimeRatioDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Database Wait Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbWaitTimeRatioDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["DB Block Changes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBlockChangesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["DB Block Changes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBlockChangesPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Executions Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbExecutionsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Executions Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbExecutionsPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Hard Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbHardParseCountPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Hard Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbHardParseCountPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Logical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogicalReadsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Logical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogicalReadsPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Logons Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogonsPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Network Traffic Volume Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbNetworkTrafficBytePerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Open Cursors Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbOpenCursorsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Open Cursors Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbOpenCursorsPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Parse Failure Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbParseFailureCountPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Read Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalReadBytesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalReadsPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Write Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalWriteBytesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Writes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalWritesPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Redo Generated Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbRedoGeneratedBytesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Redo Generated Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbRedoGeneratedBytesPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Response Time Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbResponseTimePerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Session Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbSessionCountDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Soft Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbSoftParseRatioDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["SQL Service Response Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbSQLServiceResponseTimeDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Total Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbTotalParseCountPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Total Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbTotalParseCountPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Calls Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCallsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Calls Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCallsPerTransactionDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Commits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCommitsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Commits Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCommitsPercentageDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Rollbacks Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserRollbacksPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Rollbacks Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserRollbacksPercentageDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["User Transaction Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbTransactionsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Execute Without Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbExecuteWithoutParseRatioDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Logons Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogonsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Read Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalReadBytesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalReadsPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Write Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalWriteBytesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
	r.recorders["Physical Writes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalWritesPerSecondDataPoint(ts, value, instanceID, pdbName)
	}
}
