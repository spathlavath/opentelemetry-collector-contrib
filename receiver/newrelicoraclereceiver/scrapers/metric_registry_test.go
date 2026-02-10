// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/internal/metadata"
)

func TestNewSystemMetricRegistry(t *testing.T) {
	registry := NewSystemMetricRegistry()

	assert.NotNil(t, registry)
	assert.NotNil(t, registry.recorders)
	assert.NotEmpty(t, registry.recorders)
}

func TestSystemMetricRegistry_RecordMetric_Success(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemBufferCacheHitRatio.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))
	success := registry.RecordMetric(mb, ts, "Buffer Cache Hit Ratio", 95.5, "instance1")

	assert.True(t, success)
}

func TestSystemMetricRegistry_RecordMetric_UnknownMetric(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))
	success := registry.RecordMetric(mb, ts, "Unknown Metric Name", 100.0, "instance1")

	assert.False(t, success)
}

func TestSystemMetricRegistry_RecordMultipleMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemBufferCacheHitRatio.Enabled = true
	config.Metrics.NewrelicoracledbSystemMemorySortsRatio.Enabled = true
	config.Metrics.NewrelicoracledbSystemRedoAllocationHitRatio.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	metrics := []struct {
		name  string
		value float64
	}{
		{"Buffer Cache Hit Ratio", 95.5},
		{"Memory Sorts Ratio", 98.2},
		{"Redo Allocation Hit Ratio", 99.9},
	}

	for _, metric := range metrics {
		success := registry.RecordMetric(mb, ts, metric.name, metric.value, "instance1")
		assert.True(t, success, "Failed to record metric: %s", metric.name)
	}
}

func TestSystemMetricRegistry_CacheRatioMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemCursorCacheHitRatio.Enabled = true
	config.Metrics.NewrelicoracledbSystemSoftParseRatio.Enabled = true
	config.Metrics.NewrelicoracledbSystemRowCacheHitRatio.Enabled = true
	config.Metrics.NewrelicoracledbSystemLibraryCacheHitRatio.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Cursor Cache Hit Ratio",
		"Soft Parse Ratio",
		"Row Cache Hit Ratio",
		"Library Cache Hit Ratio",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 90.0, "instance1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestSystemMetricRegistry_TransactionMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemTransactionsPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbSystemPhysicalReadsPerTransaction.Enabled = true
	config.Metrics.NewrelicoracledbSystemPhysicalWritesPerTransaction.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"User Transaction Per Sec",
		"Physical Reads Per Txn",
		"Physical Writes Per Txn",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 50.0, "instance1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestSystemMetricRegistry_PerSecondMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemPhysicalReadsDirectPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbSystemOpenCursorsPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbSystemUserCommitsPerSecond.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Physical Reads Direct Per Sec",
		"Open Cursors Per Sec",
		"User Commits Per Sec",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 100.0, "instance1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestSystemMetricRegistry_SingleValueMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemRowsPerSort.Enabled = true
	config.Metrics.NewrelicoracledbSystemHostCPUUtilization.Enabled = true
	config.Metrics.NewrelicoracledbSystemCurrentLogonsCount.Enabled = true
	config.Metrics.NewrelicoracledbSystemSessionCount.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Rows Per Sort",
		"Host CPU Utilization (%)",
		"Current Logons Count",
		"Session Count",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 50.0, "instance1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestNewPdbMetricRegistry(t *testing.T) {
	registry := NewPdbMetricRegistry()

	assert.NotNil(t, registry)
	assert.NotNil(t, registry.recorders)
	assert.NotEmpty(t, registry.recorders)
}

func TestPdbMetricRegistry_RecordMetric_Success(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbActiveParallelSessions.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))
	success := registry.RecordMetric(mb, ts, "Active Parallel Sessions", 10.0, "instance1", "pdb1")

	assert.True(t, success)
}

func TestPdbMetricRegistry_RecordMetric_UnknownMetric(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))
	success := registry.RecordMetric(mb, ts, "Unknown PDB Metric", 100.0, "instance1", "pdb1")

	assert.False(t, success)
}

func TestPdbMetricRegistry_RecordMultipleMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbActiveParallelSessions.Enabled = true
	config.Metrics.NewrelicoracledbPdbActiveSerialSessions.Enabled = true
	config.Metrics.NewrelicoracledbPdbAverageActiveSessions.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	metrics := []struct {
		name  string
		value float64
	}{
		{"Active Parallel Sessions", 5.0},
		{"Active Serial Sessions", 15.0},
		{"Average Active Sessions", 20.0},
	}

	for _, metric := range metrics {
		success := registry.RecordMetric(mb, ts, metric.name, metric.value, "instance1", "pdb1")
		assert.True(t, success, "Failed to record metric: %s", metric.name)
	}
}

func TestPdbMetricRegistry_CPUMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbBackgroundCPUUsagePerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbCPUUsagePerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbCPUUsagePerTransaction.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Background CPU Usage Per Sec",
		"CPU Usage Per Sec",
		"CPU Usage Per Txn",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 75.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_DatabaseMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbCPUTimeRatio.Enabled = true
	config.Metrics.NewrelicoracledbPdbWaitTimeRatio.Enabled = true
	config.Metrics.NewrelicoracledbPdbBlockChangesPerSecond.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Database CPU Time Ratio",
		"Database Wait Time Ratio",
		"DB Block Changes Per Sec",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 80.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_TransactionMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbExecutionsPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbExecutionsPerTransaction.Enabled = true
	config.Metrics.NewrelicoracledbPdbTransactionsPerSecond.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Executions Per Sec",
		"Executions Per Txn",
		"User Transaction Per Sec",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 100.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_ParseMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbHardParseCountPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbHardParseCountPerTransaction.Enabled = true
	config.Metrics.NewrelicoracledbPdbSoftParseRatio.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Hard Parse Count Per Sec",
		"Hard Parse Count Per Txn",
		"Soft Parse Ratio",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 25.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_IOMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbPhysicalReadBytesPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbPhysicalReadsPerTransaction.Enabled = true
	config.Metrics.NewrelicoracledbPdbPhysicalWriteBytesPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbPhysicalWritesPerTransaction.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Physical Read Total Bytes Per Sec",
		"Physical Reads Per Txn",
		"Physical Write Total Bytes Per Sec",
		"Physical Writes Per Txn",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 1024.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_UserMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbUserCommitsPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbUserCommitsPercentage.Enabled = true
	config.Metrics.NewrelicoracledbPdbUserRollbacksPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbUserRollbacksPercentage.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"User Commits Per Sec",
		"User Commits Percentage",
		"User Rollbacks Per Sec",
		"User Rollbacks Percentage",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 50.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestSystemMetricRegistry_AllCacheMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()

	cacheMetrics := []string{
		"Buffer Cache Hit Ratio",
		"Cursor Cache Hit Ratio",
		"Row Cache Hit Ratio",
		"Row Cache Miss Ratio",
		"Library Cache Hit Ratio",
		"Library Cache Miss Ratio",
	}

	for _, metricName := range cacheMetrics {
		_, exists := registry.recorders[metricName]
		assert.True(t, exists, "Metric not registered: %s", metricName)
	}
}

func TestSystemMetricRegistry_AllEnqueueMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()

	enqueueMetrics := []string{
		"Enqueue Timeouts Per Txn",
		"Enqueue Waits Per Txn",
		"Enqueue Deadlocks Per Txn",
		"Enqueue Requests Per Txn",
		"Enqueue Timeouts Per Sec",
		"Enqueue Waits Per Sec",
		"Enqueue Deadlocks Per Sec",
		"Enqueue Requests Per Sec",
	}

	for _, metricName := range enqueueMetrics {
		_, exists := registry.recorders[metricName]
		assert.True(t, exists, "Metric not registered: %s", metricName)
	}
}

func TestPdbMetricRegistry_AllRegisteredMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()

	// Verify we have the expected number of PDB metrics registered
	assert.Greater(t, len(registry.recorders), 40, "Expected more than 40 PDB metrics registered")
}

func TestSystemMetricRegistry_AllRegisteredMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()

	// Verify we have the expected number of system metrics registered
	assert.Greater(t, len(registry.recorders), 100, "Expected more than 100 system metrics registered")
}

func TestSystemMetricRegistry_PerUserCallMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemDbBlockChangesPerUserCall.Enabled = true
	config.Metrics.NewrelicoracledbSystemDbBlockGetsPerUserCall.Enabled = true
	config.Metrics.NewrelicoracledbSystemExecutionsPerUserCall.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"DB Block Changes Per User Call",
		"DB Block Gets Per User Call",
		"Executions Per User Call",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 10.0, "instance1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_SessionAndCursorMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbCurrentLogons.Enabled = true
	config.Metrics.NewrelicoracledbPdbCurrentOpenCursors.Enabled = true
	config.Metrics.NewrelicoracledbPdbSessionCount.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Current Logons Count",
		"Current Open Cursors Count",
		"Session Count",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 50.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestSystemMetricRegistry_GlobalCacheMetrics(t *testing.T) {
	registry := NewSystemMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbSystemGlobalCacheAverageCrGetTime.Enabled = true
	config.Metrics.NewrelicoracledbSystemGlobalCacheAverageCurrentGetTime.Enabled = true
	config.Metrics.NewrelicoracledbSystemGlobalCacheBlocksCorrupted.Enabled = true
	config.Metrics.NewrelicoracledbSystemGlobalCacheBlocksLost.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Global Cache Average CR Get Time",
		"Global Cache Average Current Get Time",
		"Global Cache Blocks Corrupted",
		"Global Cache Blocks Lost",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 5.0, "instance1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}

func TestPdbMetricRegistry_RedoAndLogMetrics(t *testing.T) {
	registry := NewPdbMetricRegistry()
	config := metadata.DefaultMetricsBuilderConfig()
	config.Metrics.NewrelicoracledbPdbRedoGeneratedBytesPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbRedoGeneratedBytesPerTransaction.Enabled = true
	config.Metrics.NewrelicoracledbPdbLogonsPerSecond.Enabled = true
	config.Metrics.NewrelicoracledbPdbLogonsPerTransaction.Enabled = true
	settings := receivertest.NewNopSettings(metadata.Type)
	mb := metadata.NewMetricsBuilder(config, settings)

	ts := pcommon.NewTimestampFromTime(time.Unix(1234567890, 0))

	tests := []string{
		"Redo Generated Per Sec",
		"Redo Generated Per Txn",
		"Logons Per Sec",
		"Logons Per Txn",
	}

	for _, metricName := range tests {
		success := registry.RecordMetric(mb, ts, metricName, 2048.0, "instance1", "pdb1")
		assert.True(t, success, "Failed to record: %s", metricName)
	}
}
