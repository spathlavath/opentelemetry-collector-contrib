// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

// MetricRecorderFunc defines a function type for recording metrics
type MetricRecorderFunc func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string)

// PdbMetricRecorderFunc defines a function type for recording PDB metrics
type PdbMetricRecorderFunc func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string)

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
func (r *SystemMetricRegistry) RecordMetric(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, metricName string, value float64, instanceName, instanceID string) bool {
	recorder, exists := r.recorders[metricName]
	if !exists {
		return false
	}
	recorder(mb, ts, value, instanceName, instanceID)
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
func (r *PdbMetricRegistry) RecordMetric(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, metricName string, value float64, instanceName, instanceID, pdbName string) bool {
	recorder, exists := r.recorders[metricName]
	if !exists {
		return false
	}
	recorder(mb, ts, value, instanceName, instanceID, pdbName)
	return true
}

// registerAll registers all system metric recorder functions
func (r *SystemMetricRegistry) registerAll() {
	// Cache and performance ratios
	r.recorders["Buffer Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemBufferCacheHitRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Memory Sorts Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemMemorySortsRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Redo Allocation Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoAllocationHitRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Cursor Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCursorCacheHitRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Soft Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemSoftParseRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Calls Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCallsRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Row Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRowCacheHitRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Row Cache Miss Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRowCacheMissRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Library Cache Hit Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLibraryCacheHitRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Library Cache Miss Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLibraryCacheMissRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Database Wait Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDatabaseWaitTimeRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Database CPU Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDatabaseCPUTimeRatioDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Execute Without Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecuteWithoutParseRatioDataPoint(ts, value, instanceName, instanceID)
	}

	// Transaction metrics
	r.recorders["User Transaction Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTransactionsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Writes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Reads Direct Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsDirectPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Writes Direct Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesDirectPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Reads Direct Lobs Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsReadsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Writes Direct Lobs Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsWritesPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Redo Generated Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoGeneratedBytesPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Logons Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogonsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Open Cursors Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemOpenCursorsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Calls Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCallsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Recursive Calls Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRecursiveCallsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Logical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogicalReadsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Redo Writes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoWritesPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Long Table Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLongTableScansPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Table Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalTableScansPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Full Index Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemFullIndexScansPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Index Scans Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalIndexScansPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalParseCountPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Hard Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemHardParseCountPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Parse Failure Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemParseFailureCountPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Disk Sort Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDiskSortPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Timeouts Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueTimeoutsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Waits Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueWaitsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Deadlocks Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueDeadlocksPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Requests Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueRequestsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["DB Block Gets Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockGetsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Consistent Read Gets Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadGetsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["DB Block Changes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockChangesPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Consistent Read Changes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadChangesPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["CPU Usage Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCPUUsagePerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["CR Blocks Created Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrBlocksCreatedPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["CR Undo Records Applied Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrUndoRecordsAppliedPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Rollback Undo Records Applied Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbackUndoRecordsAppliedPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Leaf Node Splits Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLeafNodeSplitsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Branch Node Splits Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemBranchNodeSplitsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["GC CR Block Received Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCrBlockReceivedPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["GC Current Block Received Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCurrentBlockReceivedPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Response Time Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemResponseTimePerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Executions Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecutionsPerTransactionDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Txns Per Logon"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTransactionsPerLogonDataPoint(ts, value, instanceName, instanceID)
	}

	// Per second metrics
	r.recorders["Physical Reads Direct Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsDirectPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Writes Direct Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesDirectPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Reads Direct Lobs Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsReadsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Writes Direct Lobs Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalLobsWritesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Redo Generated Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoGeneratedBytesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Open Cursors Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemOpenCursorsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Commits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCommitsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Commits Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCommitsPercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Rollbacks Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbacksPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Rollbacks Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbacksPercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Calls Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserCallsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Recursive Calls Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRecursiveCallsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Logical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogicalReadsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["DBWR Checkpoints Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbwrCheckpointsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Background Checkpoints Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemBackgroundCheckpointsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Redo Writes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRedoWritesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Long Table Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLongTableScansPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Table Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalTableScansPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Full Index Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemFullIndexScansPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Index Scans Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalIndexScansPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalParseCountPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Hard Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemHardParseCountPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Parse Failure Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemParseFailureCountPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Disk Sort Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDiskSortPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Timeouts Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueTimeoutsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Waits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueWaitsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Deadlocks Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueDeadlocksPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Enqueue Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemEnqueueRequestsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["DB Block Gets Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockGetsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Consistent Read Gets Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadGetsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["DB Block Changes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockChangesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Consistent Read Changes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemConsistentReadChangesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCPUUsagePerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["CR Blocks Created Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrBlocksCreatedPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["CR Undo Records Applied Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCrUndoRecordsAppliedPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Rollback UndoRec Applied Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserRollbackUndoRecordsAppliedPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Leaf Node Splits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLeafNodeSplitsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Branch Node Splits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemBranchNodeSplitsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Read Total IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadTotalIoRequestsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Read Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadTotalBytesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["GC CR Block Received Per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCrBlockReceivedPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["GC Current Block Received Per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGcCurrentBlockReceivedPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Write Total IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteTotalIoRequestsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Write Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteTotalBytesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Write IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteIoRequestsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Database Time Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDatabaseTimePerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Network Traffic Volume Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemNetworkTrafficVolumePerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Executions Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecutionsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Logons Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogonsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Read Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadBytesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Read IO Requests Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadIoRequestsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalReadsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Write Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWriteBytesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Physical Writes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPhysicalWritesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Background CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemBackgroundCPUUsagePerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Background Time Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemBackgroundTimePerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Host CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemHostCPUUsagePerSecondDataPoint(ts, value, instanceName, instanceID)
	}

	// Single value metrics
	r.recorders["Rows Per Sort"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemRowsPerSortDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Host CPU Utilization (%)"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemHostCPUUtilizationDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Global Cache Average CR Get Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheAverageCrGetTimeDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Global Cache Average Current Get Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheAverageCurrentGetTimeDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Global Cache Blocks Corrupted"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheBlocksCorruptedDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Global Cache Blocks Lost"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemGlobalCacheBlocksLostDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Current Logons Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCurrentLogonsCountDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Current Open Cursors Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCurrentOpenCursorsCountDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["User Limit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemUserLimitPercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["SQL Service Response Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemSQLServiceResponseTimeDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Shared Pool Free %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemSharedPoolFreePercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["PGA Cache Hit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemPgaCacheHitPercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Process Limit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemProcessLimitPercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Session Limit %"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemSessionLimitPercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Temp Space Used"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTempSpaceUsedDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Session Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemSessionCountDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Captured user calls"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCapturedUserCallsDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Current OS Load"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemCurrentOsLoadDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Streams Pool Usage Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemStreamsPoolUsagePercentageDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["I/O Megabytes per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemIoMegabytesPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["I/O Requests per Second"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemIoRequestsPerSecondDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Average Active Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemAverageActiveSessionsDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Active Serial Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemActiveSerialSessionsDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Active Parallel Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemActiveParallelSessionsDataPoint(ts, value, instanceName, instanceID)
	}

	// Per user call metrics
	r.recorders["DB Block Changes Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockChangesPerUserCallDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["DB Block Gets Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemDbBlockGetsPerUserCallDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Executions Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemExecutionsPerUserCallDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Logical Reads Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemLogicalReadsPerUserCallDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Sorts Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalSortsPerUserCallDataPoint(ts, value, instanceName, instanceID)
	}
	r.recorders["Total Table Scans Per User Call"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID string) {
		mb.RecordNewrelicoracledbSystemTotalTableScansPerUserCallDataPoint(ts, value, instanceName, instanceID)
	}
}

// registerAll registers all PDB metric recorder functions
func (r *PdbMetricRegistry) registerAll() {
	r.recorders["Active Parallel Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbActiveParallelSessionsDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Active Serial Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbActiveSerialSessionsDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Average Active Sessions"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbAverageActiveSessionsDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Background CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBackgroundCPUUsagePerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Background Time Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBackgroundTimePerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["CPU Usage Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCPUUsagePerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["CPU Usage Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCPUUsagePerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Current Logons Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCurrentLogonsDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Current Open Cursors Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCurrentOpenCursorsDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Database CPU Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbCPUTimeRatioDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Database Wait Time Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbWaitTimeRatioDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["DB Block Changes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBlockChangesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["DB Block Changes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbBlockChangesPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Executions Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbExecutionsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Executions Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbExecutionsPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Hard Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbHardParseCountPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Hard Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbHardParseCountPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Logical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogicalReadsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Logical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogicalReadsPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Logons Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogonsPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Network Traffic Volume Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbNetworkTrafficBytePerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Open Cursors Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbOpenCursorsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Open Cursors Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbOpenCursorsPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Parse Failure Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbParseFailureCountPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Read Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalReadBytesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Reads Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalReadsPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Write Total Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalWriteBytesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Writes Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbPhysicalWritesPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Redo Generated Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbRedoGeneratedBytesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Redo Generated Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbRedoGeneratedBytesPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Response Time Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbResponseTimePerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Session Count"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbSessionCountDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Soft Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbSoftParseRatioDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["SQL Service Response Time"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbSQLServiceResponseTimeDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Total Parse Count Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbTotalParseCountPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Total Parse Count Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbTotalParseCountPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Calls Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCallsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Calls Per Txn"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCallsPerTransactionDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Commits Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCommitsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Commits Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserCommitsPercentageDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Rollbacks Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserRollbacksPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Rollbacks Percentage"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbUserRollbacksPercentageDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["User Transaction Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbTransactionsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Execute Without Parse Ratio"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbExecuteWithoutParseRatioDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Logons Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbLogonsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Read Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalReadBytesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Reads Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalReadsPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Write Bytes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalWriteBytesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
	r.recorders["Physical Writes Per Sec"] = func(mb *metadata.MetricsBuilder, ts pcommon.Timestamp, value float64, instanceName, instanceID, pdbName string) {
		mb.RecordNewrelicoracledbPdbDbPhysicalWritesPerSecondDataPoint(ts, value, instanceName, instanceID, pdbName)
	}
}
