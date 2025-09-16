// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

// oracleScraper scrapes Oracle database metrics
type oracleScraper struct {
	logger   *zap.Logger
	config   *Config
	dbClient DBClient
	mb       *metadata.MetricsBuilder
}

// newOracleScraper creates a new Oracle scraper
func newOracleScraper(params receiver.Settings, cfg *Config) scraper.Metrics {
	return &oracleScraper{
		logger: params.Logger,
		config: cfg,
		mb:     metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, params),
	}
}

// Start initializes the scraper
func (s *oracleScraper) Start(ctx context.Context, host component.Host) error {
	client, err := newOracleDBClient(s.config)
	if err != nil {
		return fmt.Errorf("failed to create Oracle DB client: %w", err)
	}
	s.dbClient = client
	return nil
}

// ScrapeMetrics scrapes Oracle database metrics and returns them
func (s *oracleScraper) ScrapeMetrics(ctx context.Context) (pmetric.Metrics, error) {
	return s.Scrape(ctx)
}

// Shutdown closes the database connection
func (s *oracleScraper) Shutdown(ctx context.Context) error {
	if s.dbClient != nil {
		return s.dbClient.Close()
	}
	return nil
}

// Scrape collects metrics from Oracle database
func (s *oracleScraper) Scrape(ctx context.Context) (pmetric.Metrics, error) {
	if s.dbClient == nil {
		return pmetric.NewMetrics(), fmt.Errorf("database client not initialized")
	}

	// Collect instance information first
	instanceInfo, err := s.getInstanceInfo()
	if err != nil {
		s.logger.Error("Failed to get instance information", zap.Error(err))
		return pmetric.NewMetrics(), err
	}

	var wg sync.WaitGroup
	errors := make(chan error, 25) // Buffer for potential errors

	// Collect all metric groups concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectSGAMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("SGA metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectMemoryMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("memory metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectDiskMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("disk metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectQueryMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("query metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectDatabaseMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("database metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectNetworkMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("network metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectTablespaceMetrics(); err != nil {
			errors <- fmt.Errorf("tablespace metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectMiscMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("misc metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectSortsMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("sorts metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectRollbackSegmentsMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("rollback segments metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectRedoLogMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("redo log metrics: %w", err)
		}
	}()

	// Wait for all collections to complete
	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		s.logger.Warn("Error collecting metrics", zap.Error(err))
	}

	return s.mb.Emit(), nil
}

// Instance information structure
type instanceInfo struct {
	instanceID   string
	instanceName string
	globalName   string
	dbID         string
}

// getInstanceInfo retrieves basic instance information
func (s *oracleScraper) getInstanceInfo() (*instanceInfo, error) {
	query := `
		SELECT 
			i.INST_ID,
			i.INSTANCE_NAME,
			g.GLOBAL_NAME,
			d.DBID
		FROM 
			gv$instance i,
			global_name g,
			v$database d
		WHERE i.INST_ID = 1`

	row := s.dbClient.QueryRow(query)

	var info instanceInfo
	err := row.Scan(&info.instanceID, &info.instanceName, &info.globalName, &info.dbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance info: %w", err)
	}

	return &info, nil
}

// collectSGAMetrics collects SGA-related metrics
func (s *oracleScraper) collectSGAMetrics(instanceInfo *instanceInfo) error {
	// Comprehensive SGA metrics from gv$sysmetric
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Buffer Cache Hit Ratio',
			'Shared Pool Library Cache Hit Ratio',
			'Shared Pool Library Cache Reload Ratio',
			'Shared Pool Dictionary Cache Miss Ratio',
			'Log Buffer Allocation Retries Ratio'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		switch metricName {
		case "Buffer Cache Hit Ratio":
			s.mb.RecordNewrelicOracleSgaHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "Shared Pool Library Cache Hit Ratio":
			s.mb.RecordNewrelicOracleSgaSharedPoolLibraryCacheHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "Shared Pool Library Cache Reload Ratio":
			s.mb.RecordNewrelicOracleSgaSharedPoolLibraryCacheReloadRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "Shared Pool Dictionary Cache Miss Ratio":
			s.mb.RecordNewrelicOracleSgaSharedPoolDictCacheMissRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "Log Buffer Allocation Retries Ratio":
			s.mb.RecordNewrelicOracleSgaLogBufferAllocationRetriesRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		}
	}

	// SGA component sizes
	sgaQuery := `
		SELECT name, value 
		FROM v$sga 
		WHERE name IN ('Fixed Size', 'Redo Buffers')`

	rows, err = s.dbClient.Query(sgaQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var value int64
		if err := rows.Scan(&name, &value); err != nil {
			continue
		}

		switch name {
		case "Fixed Size":
			s.mb.RecordNewrelicOracleSgaFixedSizeBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Redo Buffers":
			s.mb.RecordNewrelicOracleSgaRedoBuffersBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	// Additional SGA metrics from gv$sysstat
	sysstatQuery := `
		SELECT INST_ID, NAME, VALUE
		FROM gv$sysstat
		WHERE NAME IN (
			'log buffer space waits',
			'redo log space wait time',
			'redo entries',
			'buffer busy waits',
			'free buffer waits',
			'free buffer inspected'
		)`

	rows, err = s.dbClient.Query(sysstatQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var name string
		var value int64
		if err := rows.Scan(&instID, &name, &value); err != nil {
			continue
		}

		switch name {
		case "log buffer space waits":
			s.mb.RecordNewrelicOracleSgaLogBufferSpaceWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "redo entries":
			s.mb.RecordNewrelicOracleSgaLogBufferRedoEntriesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "buffer busy waits":
			s.mb.RecordNewrelicOracleSgaBufferBusyWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "free buffer waits":
			s.mb.RecordNewrelicOracleSgaFreeBufferWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "free buffer inspected":
			s.mb.RecordNewrelicOracleSgaFreeBufferInspectedDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	// Redo allocation retries
	redoQuery := `
		SELECT INST_ID, NAME, VALUE
		FROM gv$sysstat
		WHERE NAME = 'redo log space requests'`

	rows, err = s.dbClient.Query(redoQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var name string
		var value int64
		if err := rows.Scan(&instID, &name, &value); err != nil {
			continue
		}
		s.mb.RecordNewrelicOracleSgaLogBufferRedoAllocationRetriesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
	}

	// UGA total memory from gv$sesstat
	ugaQuery := `
		SELECT SUM(s.value)
		FROM gv$sesstat s, gv$statname n
		WHERE s.statistic# = n.statistic#
		AND n.name = 'session uga memory'`

	rows, err = s.dbClient.Query(ugaQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var value sql.NullInt64
		if err := rows.Scan(&value); err != nil {
			continue
		}
		if value.Valid {
			s.mb.RecordNewrelicOracleSgaUgaTotalMemoryBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value.Int64)
		}
	}

	// Shared pool library cache memory metrics
	libCacheQuery := `
		SELECT 
			SUM(SHARABLE_MEM) / COUNT(DISTINCT SQL_ID) as avg_per_statement,
			SUM(SHARABLE_MEM) / COUNT(DISTINCT PARSING_USER_ID) as avg_per_user
		FROM gv$sql
		WHERE SHARABLE_MEM > 0`

	rows, err = s.dbClient.Query(libCacheQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var perStatement, perUser sql.NullFloat64
		if err := rows.Scan(&perStatement, &perUser); err != nil {
			continue
		}
		if perStatement.Valid {
			s.mb.RecordNewrelicOracleSgaSharedPoolLibraryCacheShareableMemoryPerStatementBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(perStatement.Float64))
		}
		if perUser.Valid {
			s.mb.RecordNewrelicOracleSgaSharedPoolLibraryCacheShareableMemoryPerUserBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(perUser.Float64))
		}
	}

	return nil
}

// collectMemoryMetrics collects memory-related metrics
func (s *oracleScraper) collectMemoryMetrics(instanceInfo *instanceInfo) error {
	// PGA metrics from gv$pgastat
	pgaQuery := `
		SELECT INST_ID, NAME, VALUE 
		FROM gv$pgastat 
		WHERE NAME IN ('total PGA inuse', 'total PGA allocated', 'total freeable PGA memory', 'global memory bound')`

	rows, err := s.dbClient.Query(pgaQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var name string
		var value int64
		if err := rows.Scan(&instID, &name, &value); err != nil {
			continue
		}

		switch name {
		case "total PGA inuse":
			s.mb.RecordNewrelicOracleMemoryPgaInUseBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "total PGA allocated":
			s.mb.RecordNewrelicOracleMemoryPgaAllocatedBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "total freeable PGA memory":
			s.mb.RecordNewrelicOracleMemoryPgaFreeableBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "global memory bound":
			s.mb.RecordNewrelicOracleMemoryPgaMaxSizeBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	// Comprehensive memory metrics from gv$sysmetric
	memoryMetricsQuery := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Buffer Cache Hit Ratio',
			'In-memory Sort Ratio',
			'Redo Allocation Hit Ratio',
			'Redo Generated Per Sec',
			'Redo Generated Per Txn',
			'Global Cache Blocks Corrupted',
			'Global Cache Blocks Lost'
		)`

	rows, err = s.dbClient.Query(memoryMetricsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		switch metricName {
		case "Buffer Cache Hit Ratio":
			s.mb.RecordNewrelicOracleMemoryBufferCacheHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "In-memory Sort Ratio":
			s.mb.RecordNewrelicOracleMemorySortsRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "Redo Allocation Hit Ratio":
			s.mb.RecordNewrelicOracleMemoryRedoAllocationHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/100)
		case "Redo Generated Per Sec":
			s.mb.RecordNewrelicOracleMemoryRedoGeneratedBytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Redo Generated Per Txn":
			s.mb.RecordNewrelicOracleMemoryRedoGeneratedBytesPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Global Cache Blocks Corrupted":
			s.mb.RecordNewrelicOracleMemoryGlobalCacheBlocksCorruptedDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "Global Cache Blocks Lost":
			s.mb.RecordNewrelicOracleMemoryGlobalCacheBlocksLostDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		}
	}

	// Alternative metric names that might be available
	altMemoryMetricsQuery := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'PGA Allocated in Bytes',
			'PGA Freeable in Bytes',
			'PGA In Use in Bytes',
			'PGA Max Size in Bytes'
		)`

	rows, err = s.dbClient.Query(altMemoryMetricsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		switch metricName {
		case "PGA Allocated in Bytes":
			s.mb.RecordNewrelicOracleMemoryPgaAllocatedBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "PGA Freeable in Bytes":
			s.mb.RecordNewrelicOracleMemoryPgaFreeableBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "PGA In Use in Bytes":
			s.mb.RecordNewrelicOracleMemoryPgaInUseBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "PGA Max Size in Bytes":
			s.mb.RecordNewrelicOracleMemoryPgaMaxSizeBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		}
	}

	return nil
}

// collectDiskMetrics collects disk I/O metrics
func (s *oracleScraper) collectDiskMetrics(instanceInfo *instanceInfo) error {
	// Physical reads/writes metrics from gv$filestat
	query := `
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

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var physReads, physWrites, physBlkReads, physBlkWrites, readTime, writeTime int64
		if err := rows.Scan(&instID, &physReads, &physWrites, &physBlkReads, &physBlkWrites, &readTime, &writeTime); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleDiskReadsDataPoint(pcommon.NewTimestampFromTime(time.Now()), physReads)
		s.mb.RecordNewrelicOracleDiskWritesDataPoint(pcommon.NewTimestampFromTime(time.Now()), physWrites)
		s.mb.RecordNewrelicOracleDiskBlocksReadDataPoint(pcommon.NewTimestampFromTime(time.Now()), physBlkReads)
		s.mb.RecordNewrelicOracleDiskBlocksWrittenDataPoint(pcommon.NewTimestampFromTime(time.Now()), physBlkWrites)
		s.mb.RecordNewrelicOracleDiskReadTimeMillisecondsDataPoint(pcommon.NewTimestampFromTime(time.Now()), readTime)
		s.mb.RecordNewrelicOracleDiskWriteTimeMillisecondsDataPoint(pcommon.NewTimestampFromTime(time.Now()), writeTime)
	}

	// Comprehensive disk metrics from gv$sysmetric
	diskMetricsQuery := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Physical Reads Per Sec',
			'Physical Writes Per Sec',
			'Physical Read IO Requests Per Sec',
			'Physical Write Total IO Requests Per Sec',
			'Physical Read Bytes Per Sec',
			'Physical Write Bytes Per Sec',
			'Logical Reads Per User Call',
			'Sorts Per Sec',
			'Sorts Per Txn',
			'Temp Space Used',
			'Physical LOBs reads per second',
			'Physical LOBs writes per second',
			'I/O Requests per Second',
			'I/O Megabytes per Second'
		)`

	rows, err = s.dbClient.Query(diskMetricsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		switch metricName {
		case "Physical Reads Per Sec":
			s.mb.RecordNewrelicOracleDiskPhysicalReadsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Writes Per Sec":
			s.mb.RecordNewrelicOracleDiskPhysicalWritesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Read IO Requests Per Sec":
			s.mb.RecordNewrelicOracleDiskPhysicalReadIoRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Write Total IO Requests Per Sec":
			s.mb.RecordNewrelicOracleDiskPhysicalWriteTotalIoRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Read Bytes Per Sec":
			s.mb.RecordNewrelicOracleDiskPhysicalReadBytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Write Bytes Per Sec":
			s.mb.RecordNewrelicOracleDiskPhysicalWriteBytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Logical Reads Per User Call":
			s.mb.RecordNewrelicOracleDiskLogicalReadsPerUserCallDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Sorts Per Sec":
			s.mb.RecordNewrelicOracleDiskSortPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Sorts Per Txn":
			s.mb.RecordNewrelicOracleDiskSortPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Temp Space Used":
			s.mb.RecordNewrelicOracleDiskTempSpaceUsedBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "Physical LOBs reads per second":
			s.mb.RecordNewrelicOracleDiskPhysicalLobsReadsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical LOBs writes per second":
			s.mb.RecordNewrelicOracleDiskPhysicalLobsWritesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	// Alternative names for disk metrics that might be available - only use available methods
	// Skip the alternative section since we have the main metrics covered

	return nil
}

// collectQueryMetrics collects query performance metrics
func (s *oracleScraper) collectQueryMetrics(instanceInfo *instanceInfo) error {
	// System metrics from gv$sysmetric
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'User Transaction Per Sec',
			'Physical Reads Per Txn',
			'Physical Writes Per Txn'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		switch metricName {
		case "User Transaction Per Sec":
			s.mb.RecordNewrelicOracleQueryTransactionsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Reads Per Txn":
			s.mb.RecordNewrelicOracleQueryPhysicalReadsPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Physical Writes Per Txn":
			s.mb.RecordNewrelicOracleQueryPhysicalWritesPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectDatabaseMetrics collects general database metrics
func (s *oracleScraper) collectDatabaseMetrics(instanceInfo *instanceInfo) error {
	// Comprehensive database metrics from v$sysstat (which actually has data in Oracle Free 23c)
	query := `
		SELECT INST_ID, NAME, VALUE
		FROM gv$sysstat
		WHERE NAME IN (
			'user commits',
			'user rollbacks', 
			'execute count',
			'parse count (total)',
			'parse count (hard)',
			'session logical reads',
			'physical reads',
			'physical writes',
			'physical reads direct',
			'physical writes direct',
			'redo writes',
			'redo blocks written',
			'redo entries',
			'redo size',
			'db block gets',
			'consistent gets',
			'db block changes',
			'consistent changes',
			'CPU used by this session',
			'recursive calls',
			'user calls',
			'sorts (memory)',
			'sorts (disk)',
			'sorts (rows)',
			'table scans (short tables)',
			'table scans (long tables)',
			'index fast full scans (full)',
			'opened cursors cumulative',
			'opened cursors current',
			'Enqueue Waits Per Sec',
			'Enqueue Deadlocks Per Sec',
			'Enqueue Requests Per Sec',
			'Block Gets Per Sec',
			'Consistent Read Gets Per Sec',
			'Block Changes Per Sec',
			'Consistent Read Changes Per Sec',
			'CPU Usage Per Txn',
			'CR Blocks Created Per Sec',
			'CR Undo Records Applied Per Sec',
			'User Rollback UndoRec Applied Per Sec',
			'Leaf Node Splits Per Sec',
			'Branch Node Splits Per Sec',
			'GC CR Block Received Per Sec',
			'GC Current Block Received Per Sec',
			'Global Cache Average CR Get Time',
			'Global Cache Average Current Get Time',
			'Current Logons Count',
			'Current Open Cursors Count',
			'User Limit %',
			'Logons Per Txn',
			'Open Cursors Per Txn',
			'User Commits Percentage',
			'User Rollbacks Percentage',
			'User Calls Per Txn',
			'Recursive Calls Per Txn',
			'Logical Reads Per Txn',
			'Redo Writes Per Txn',
			'Long Table Scans Per Txn',
			'Total Table Scans Per Txn',
			'session uga memory',
			'session pga memory',
			'session uga memory max',
			'session pga memory max',
			'logons cumulative',
			'logons current',
			'enqueue requests',
			'enqueue waits',
			'enqueue timeouts',
			'enqueue deadlocks',
			'buffer is not pinned count',
			'buffer is pinned count',
			'free buffer requested',
			'dirty buffers inspected',
			'summed dirty queue length',
			'write requests',
			'bytes sent via SQL*Net to client',
			'bytes received via SQL*Net from client',
			'SQL*Net roundtrips to/from client',
			'bytes sent via SQL*Net to dblink',
			'bytes received via SQL*Net from dblink',
			'SQL*Net roundtrips to/from dblink'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		// Map v$sysstat metrics to record functions based on metric name
		switch metricName {
		case "user commits":
			s.mb.RecordNewrelicOracleDbUserCommitsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "user rollbacks":
			s.mb.RecordNewrelicOracleDbUserRollbacksPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "execute count":
			s.mb.RecordNewrelicOracleDbExecutionsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "parse count (total)":
			s.mb.RecordNewrelicOracleDbTotalParseCountPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "parse count (hard)":
			s.mb.RecordNewrelicOracleDbHardParseCountPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "session logical reads":
			s.mb.RecordNewrelicOracleDbLogicalReadsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "physical reads":
			s.mb.RecordNewrelicOracleDbPhysicalReadsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "physical writes":
			s.mb.RecordNewrelicOracleDbPhysicalWritesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "physical reads direct":
			// Use existing disk metrics for direct reads
			s.mb.RecordNewrelicOracleDiskReadsDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "physical writes direct":
			// Use existing disk metrics for direct writes
			s.mb.RecordNewrelicOracleDiskWritesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "redo writes":
			s.mb.RecordNewrelicOracleDbRedoWritesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "redo size":
			// Map to existing redo log file switch metric as a general redo activity indicator
			s.mb.RecordNewrelicOracleRedoLogFileSwitchDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value/1024/1024)) // Convert to MB
		case "db block gets":
			s.mb.RecordNewrelicOracleDbBlockGetsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "consistent gets":
			s.mb.RecordNewrelicOracleDbConsistentReadGetsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "db block changes":
			s.mb.RecordNewrelicOracleDbBlockChangesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "CPU used by this session":
			s.mb.RecordNewrelicOracleDbCPUUsagePerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "recursive calls":
			s.mb.RecordNewrelicOracleDbRecursiveCallsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "user calls":
			s.mb.RecordNewrelicOracleDbUserCallsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "sorts (memory)":
			s.mb.RecordNewrelicOracleSortsMemoryBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "sorts (disk)":
			s.mb.RecordNewrelicOracleSortsDiskBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "opened cursors current":
			s.mb.RecordNewrelicOracleDbCurrentOpenCursorsDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "logons current":
			s.mb.RecordNewrelicOracleDbCurrentLogonsDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "enqueue requests":
			s.mb.RecordNewrelicOracleDbEnqueueRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "enqueue waits":
			s.mb.RecordNewrelicOracleDbEnqueueWaitsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "enqueue timeouts":
			s.mb.RecordNewrelicOracleDbEnqueueTimeoutsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "enqueue deadlocks":
			s.mb.RecordNewrelicOracleDbEnqueueDeadlocksPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "bytes sent via SQL*Net to client", "bytes received via SQL*Net from client":
			// Use existing network traffic metric
			s.mb.RecordNewrelicOracleNetworkTrafficBytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "SQL*Net roundtrips to/from client":
			// Use existing network IO requests metric
			s.mb.RecordNewrelicOracleNetworkIoRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "SQL*Net roundtrips to/from dblink":
			// Also use network IO requests metric for dblink roundtrips
			s.mb.RecordNewrelicOracleNetworkIoRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectNetworkMetrics collects network-related metrics from v$sysstat
func (s *oracleScraper) collectNetworkMetrics(instanceInfo *instanceInfo) error {
	// Use v$sysstat for network metrics since gv$sysmetric is empty in Oracle Free 23c
	query := `
		SELECT INST_ID, NAME, VALUE
		FROM gv$sysstat
		WHERE NAME IN (
			'bytes sent via SQL*Net to client',
			'bytes received via SQL*Net from client',
			'SQL*Net roundtrips to/from client',
			'bytes sent via SQL*Net to dblink',
			'bytes received via SQL*Net from dblink',
			'SQL*Net roundtrips to/from dblink'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var metricName string
		var value float64
		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			continue
		}

		switch metricName {
		case "bytes sent via SQL*Net to client", "bytes received via SQL*Net from client":
			s.mb.RecordNewrelicOracleNetworkTrafficBytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "SQL*Net roundtrips to/from client", "SQL*Net roundtrips to/from dblink":
			s.mb.RecordNewrelicOracleNetworkIoRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "bytes sent via SQL*Net to dblink", "bytes received via SQL*Net from dblink":
			s.mb.RecordNewrelicOracleNetworkIoMegabytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value/1024/1024) // Convert to MB
		}
	}

	return nil
}

// collectTablespaceMetrics collects tablespace usage metrics
func (s *oracleScraper) collectTablespaceMetrics() error {
	// Build whitelist condition
	whereClause := ""
	if len(s.config.TablespaceWhitelist) > 0 {
		quotedTablespaces := make([]string, len(s.config.TablespaceWhitelist))
		for i, ts := range s.config.TablespaceWhitelist {
			quotedTablespaces[i] = fmt.Sprintf("'%s'", ts)
		}
		whereClause = fmt.Sprintf(" AND dt.TABLESPACE_NAME IN (%s)", strings.Join(quotedTablespaces, ","))
	}

	query := fmt.Sprintf(`
		SELECT 
			dt.TABLESPACE_NAME,
			ROUND((dt.BYTES - NVL(fs.BYTES, 0)) / dt.BYTES * 100, 2) AS USED_PERCENT,
			(dt.BYTES - NVL(fs.BYTES, 0)) AS USED,
			dt.BYTES AS SIZE,
			0 AS OFFLINE
		FROM 
			(SELECT TABLESPACE_NAME, SUM(BYTES) AS BYTES 
			 FROM DBA_DATA_FILES 
			 GROUP BY TABLESPACE_NAME) dt
		LEFT JOIN 
			(SELECT TABLESPACE_NAME, SUM(BYTES) AS BYTES 
			 FROM DBA_FREE_SPACE 
			 GROUP BY TABLESPACE_NAME) fs
		ON dt.TABLESPACE_NAME = fs.TABLESPACE_NAME
		WHERE 1=1%s`, whereClause)

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tablespaceName string
		var usedPercent float64
		var used, size int64
		var offline int
		if err := rows.Scan(&tablespaceName, &usedPercent, &used, &size, &offline); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleTablespaceSpaceConsumedBytesDataPoint(
			pcommon.NewTimestampFromTime(time.Now()), used, tablespaceName)
		s.mb.RecordNewrelicOracleTablespaceSpaceReservedBytesDataPoint(
			pcommon.NewTimestampFromTime(time.Now()), size, tablespaceName)
		s.mb.RecordNewrelicOracleTablespaceSpaceUsedPercentageDataPoint(
			pcommon.NewTimestampFromTime(time.Now()), usedPercent, tablespaceName)
		s.mb.RecordNewrelicOracleTablespaceIsOfflineDataPoint(
			pcommon.NewTimestampFromTime(time.Now()), int64(offline), tablespaceName)
	}

	return nil
}

// collectMiscMetrics collects miscellaneous metrics
func (s *oracleScraper) collectMiscMetrics(instanceInfo *instanceInfo) error {
	// Long running queries
	longQueryQuery := `
		SELECT inst_id, sum(num) AS total 
		FROM ((
			SELECT i.inst_id, 1 AS num
			FROM gv$session s, gv$instance i
			WHERE i.inst_id=s.inst_id
			AND s.status='ACTIVE'
			AND s.type <>'BACKGROUND'
			AND s.last_call_et > 60
			GROUP BY i.inst_id
		) UNION (
			SELECT i.inst_id, 0 AS num
			FROM gv$session s, gv$instance i
			WHERE i.inst_id=s.inst_id
		))
		GROUP BY inst_id`

	rows, err := s.dbClient.Query(longQueryQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var total int64
		if err := rows.Scan(&instID, &total); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleLongRunningQueriesDataPoint(
			pcommon.NewTimestampFromTime(time.Now()), total, strconv.Itoa(instID))
	}

	// Locked accounts
	lockedAccountsQuery := `
		SELECT INST_ID, LOCKED_ACCOUNTS
		FROM (
			SELECT
				INST_ID,
				(SELECT COUNT(1) 
				FROM dba_users
				WHERE account_status != 'OPEN') AS LOCKED_ACCOUNTS
			FROM gv$instance i
		)`

	rows, err = s.dbClient.Query(lockedAccountsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var lockedAccounts int64
		if err := rows.Scan(&instID, &lockedAccounts); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleLockedAccountsDataPoint(
			pcommon.NewTimestampFromTime(time.Now()), lockedAccounts, strconv.Itoa(instID))
	}

	return nil
}

// collectSortsMetrics collects sort-related metrics
func (s *oracleScraper) collectSortsMetrics(instanceInfo *instanceInfo) error {
	// Collect sort metrics from gv$sysstat
	query := `
		SELECT INST_ID, NAME, VALUE 
		FROM gv$sysstat 
		WHERE NAME IN (
			'sorts (memory)',
			'sorts (disk)',
			'sorts (rows)'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var name string
		var value int64
		if err := rows.Scan(&instID, &name, &value); err != nil {
			continue
		}

		switch name {
		case "sorts (memory)":
			s.mb.RecordNewrelicOracleSortsMemoryBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "sorts (disk)":
			s.mb.RecordNewrelicOracleSortsDiskBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectRollbackSegmentsMetrics collects rollback segment metrics
func (s *oracleScraper) collectRollbackSegmentsMetrics(instanceInfo *instanceInfo) error {
	// Collect rollback segment metrics from gv$sysstat
	query := `
		SELECT INST_ID, NAME, VALUE 
		FROM gv$sysstat 
		WHERE NAME IN (
			'rollback seg gets',
			'rollback seg waits'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var gets, waits int64
	for rows.Next() {
		var instID int
		var name string
		var value int64
		if err := rows.Scan(&instID, &name, &value); err != nil {
			continue
		}

		switch name {
		case "rollback seg gets":
			gets = value
			s.mb.RecordNewrelicOracleRollbackSegmentsGetsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "rollback seg waits":
			waits = value
			s.mb.RecordNewrelicOracleRollbackSegmentsWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	// Calculate wait ratio
	if gets > 0 {
		waitRatio := float64(waits) / float64(gets) * 100
		s.mb.RecordNewrelicOracleRollbackSegmentsRatioWaitDataPoint(pcommon.NewTimestampFromTime(time.Now()), waitRatio)
	}

	return nil
}

// collectRedoLogMetrics collects redo log metrics
func (s *oracleScraper) collectRedoLogMetrics(instanceInfo *instanceInfo) error {
	// Collect redo log metrics from gv$sysstat
	query := `
		SELECT INST_ID, NAME, VALUE 
		FROM gv$sysstat 
		WHERE NAME IN (
			'redo log space requests',
			'redo log space wait time',
			'log file parallel write',
			'redo log parallel write',
			'redo log sequential writes',
			'redo log single file switch'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var name string
		var value int64
		if err := rows.Scan(&instID, &name, &value); err != nil {
			continue
		}

		switch name {
		case "redo log space requests":
			s.mb.RecordNewrelicOracleRedoLogWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "redo log single file switch":
			s.mb.RecordNewrelicOracleRedoLogFileSwitchDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	// Collect additional redo log metrics from gv$log_history for file switches
	switchQuery := `
		SELECT COUNT(*) as switches_today
		FROM gv$log_history
		WHERE first_time >= TRUNC(SYSDATE)`

	switchRows, err := s.dbClient.Query(switchQuery)
	if err == nil {
		defer switchRows.Close()
		if switchRows.Next() {
			var switchesToday int64
			if err := switchRows.Scan(&switchesToday); err == nil {
				s.mb.RecordNewrelicOracleRedoLogFileSwitchDataPoint(pcommon.NewTimestampFromTime(time.Now()), switchesToday)
			}
		}
	}

	return nil
}
