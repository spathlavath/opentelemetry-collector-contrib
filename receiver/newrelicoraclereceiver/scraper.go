// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

import (
	"context"
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
	errors := make(chan error, 20) // Buffer for potential errors

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
		if err := s.collectParseMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("parse metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectScanMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("scan metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectEnqueueMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("enqueue metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectGlobalCacheMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("global cache metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectResourceLimitMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("resource limit metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectCacheHitMissMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("cache hit/miss metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectSessionMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("session metrics: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.collectUserCallMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("user call metrics: %w", err)
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
		if err := s.collectRollbackSegmentMetrics(instanceInfo); err != nil {
			errors <- fmt.Errorf("rollback segment metrics: %w", err)
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
	// SGA Hit Ratio
	query := `
		SELECT inst.inst_id, (1 - (phy.value - lob.value - dir.value)/ses.value) as ratio
		FROM GV$SYSSTAT ses, GV$SYSSTAT lob, GV$SYSSTAT dir, GV$SYSSTAT phy, GV$INSTANCE inst
		WHERE ses.name='session logical reads'
		AND dir.name='physical reads direct'
		AND lob.name='physical reads direct (lob)'
		AND phy.name='physical reads'
		AND ses.inst_id=inst.inst_id
		AND lob.inst_id=inst.inst_id
		AND dir.inst_id=inst.inst_id
		AND phy.inst_id=inst.inst_id`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var ratio float64
		if err := rows.Scan(&instID, &ratio); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleSgaHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), ratio)
	}

	// SGA Fixed Size
	query = `
		SELECT inst.inst_id, sga.value
		FROM GV$SGA sga, GV$INSTANCE inst
		WHERE sga.inst_id=inst.inst_id AND sga.name='Fixed Size'`

	rows, err = s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var instID int
		var value int64
		if err := rows.Scan(&instID, &value); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleSgaFixedSizeBytesDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
	}

	// Shared Pool Library Cache Hit Ratio
	query = `
		SELECT libcache.gethitratio as ratio, inst.inst_id
		FROM GV$librarycache libcache, GV$INSTANCE inst
		WHERE namespace='SQL AREA'
		AND inst.inst_id=libcache.inst_id`

	rows, err = s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ratio float64
		var instID int
		if err := rows.Scan(&ratio, &instID); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleSgaSharedPoolLibraryCacheHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), ratio)
	}

	return nil
}

// collectMemoryMetrics collects memory-related metrics
func (s *oracleScraper) collectMemoryMetrics(instanceInfo *instanceInfo) error {
	// PGA metrics
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

	return nil
}

// collectDiskMetrics collects disk I/O metrics
func (s *oracleScraper) collectDiskMetrics(instanceInfo *instanceInfo) error {
	// Physical reads/writes metrics
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
	// Database metrics from gv$sysmetric
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'SQL Service Response Time',
			'Database Wait Time Ratio',
			'Database CPU Time Ratio',
			'Session Count',
			'CPU Usage Per Sec',
			'Executions Per Sec'
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
		case "SQL Service Response Time":
			s.mb.RecordNewrelicOracleQuerySQLServiceResponseTimeDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Database Wait Time Ratio":
			s.mb.RecordNewrelicOracleDbWaitTimeRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Database CPU Time Ratio":
			s.mb.RecordNewrelicOracleDbCPUTimeRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Session Count":
			s.mb.RecordNewrelicOracleDbSessionCountDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "CPU Usage Per Sec":
			s.mb.RecordNewrelicOracleDbCPUUsagePerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Executions Per Sec":
			s.mb.RecordNewrelicOracleDbExecutionsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectNetworkMetrics collects network-related metrics
func (s *oracleScraper) collectNetworkMetrics(instanceInfo *instanceInfo) error {
	// Network metrics from gv$sysmetric
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Network Traffic Volume Per Sec',
			'I/O Megabytes per Second',
			'I/O Requests per Second'
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
		case "Network Traffic Volume Per Sec":
			s.mb.RecordNewrelicOracleNetworkTrafficBytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "I/O Megabytes per Second":
			s.mb.RecordNewrelicOracleNetworkIoMegabytesPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "I/O Requests per Second":
			s.mb.RecordNewrelicOracleNetworkIoRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
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
		whereClause = fmt.Sprintf(" WHERE dt.TABLESPACE_NAME IN (%s)", strings.Join(quotedTablespaces, ","))
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
		ON dt.TABLESPACE_NAME = fs.TABLESPACE_NAME%s`, whereClause)

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

// collectParseMetrics collects parsing-related metrics
func (s *oracleScraper) collectParseMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Total Parse Count Per Sec',
			'Total Parse Count Per Txn',
			'Hard Parse Count Per Sec',
			'Hard Parse Count Per Txn',
			'Parse Failure Count Per Sec',
			'Parse Failure Count Per Txn'
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
		case "Total Parse Count Per Sec":
			s.mb.RecordNewrelicOracleDbTotalParseCountPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Parse Count Per Txn":
			s.mb.RecordNewrelicOracleDbTotalParseCountPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Hard Parse Count Per Sec":
			s.mb.RecordNewrelicOracleDbHardParseCountPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Hard Parse Count Per Txn":
			s.mb.RecordNewrelicOracleDbHardParseCountPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Parse Failure Count Per Sec":
			s.mb.RecordNewrelicOracleDbParseFailureCountPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Parse Failure Count Per Txn":
			s.mb.RecordNewrelicOracleDbParseFailureCountPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectScanMetrics collects scan-related metrics
func (s *oracleScraper) collectScanMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Full Index Scans Per Sec',
			'Full Index Scans Per Txn',
			'Total Index Scans Per Sec',
			'Total Index Scans Per Txn',
			'Long Table Scans Per Sec',
			'Long Table Scans Per Txn',
			'Total Table Scans Per Sec',
			'Total Table Scans Per Txn'
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
		case "Full Index Scans Per Sec":
			s.mb.RecordNewrelicOracleDbFullIndexScansPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Full Index Scans Per Txn":
			s.mb.RecordNewrelicOracleDbFullIndexScansPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Index Scans Per Sec":
			s.mb.RecordNewrelicOracleDbTotalIndexScansPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Index Scans Per Txn":
			s.mb.RecordNewrelicOracleDbTotalIndexScansPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Long Table Scans Per Sec":
			s.mb.RecordNewrelicOracleDbLongTableScansPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Long Table Scans Per Txn":
			s.mb.RecordNewrelicOracleDbLongTableScansPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Table Scans Per Sec":
			s.mb.RecordNewrelicOracleDbTotalTableScansPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Table Scans Per Txn":
			s.mb.RecordNewrelicOracleDbTotalTableScansPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectEnqueueMetrics collects enqueue-related metrics
func (s *oracleScraper) collectEnqueueMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Enqueue Timeouts Per Sec',
			'Enqueue Timeouts Per Txn',
			'Enqueue Waits Per Sec',
			'Enqueue Waits Per Txn',
			'Enqueue Deadlocks Per Sec',
			'Enqueue Deadlocks Per Txn',
			'Enqueue Requests Per Sec',
			'Enqueue Requests Per Txn'
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
		case "Enqueue Timeouts Per Sec":
			s.mb.RecordNewrelicOracleDbEnqueueTimeoutsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Timeouts Per Txn":
			s.mb.RecordNewrelicOracleDbEnqueueTimeoutsPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Waits Per Sec":
			s.mb.RecordNewrelicOracleDbEnqueueWaitsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Waits Per Txn":
			s.mb.RecordNewrelicOracleDbEnqueueWaitsPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Deadlocks Per Sec":
			s.mb.RecordNewrelicOracleDbEnqueueDeadlocksPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Deadlocks Per Txn":
			s.mb.RecordNewrelicOracleDbEnqueueDeadlocksPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Requests Per Sec":
			s.mb.RecordNewrelicOracleDbEnqueueRequestsPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Enqueue Requests Per Txn":
			s.mb.RecordNewrelicOracleDbEnqueueRequestsPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectGlobalCacheMetrics collects RAC global cache metrics
func (s *oracleScraper) collectGlobalCacheMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'GC CR Block Received Per Second',
			'GC CR Block Received Per Txn',
			'GC Current Block Received Per Second',
			'GC Current Block Received Per Txn',
			'Global Cache Average CR Get Time',
			'Global Cache Average Current Get Time'
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
		case "GC CR Block Received Per Second":
			s.mb.RecordNewrelicOracleDbGcCrBlockReceivedPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "GC CR Block Received Per Txn":
			s.mb.RecordNewrelicOracleDbGcCrBlockReceivedPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "GC Current Block Received Per Second":
			s.mb.RecordNewrelicOracleDbGcCurrentBlockReceivedPerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "GC Current Block Received Per Txn":
			s.mb.RecordNewrelicOracleDbGcCurrentBlockReceivedPerTransactionDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Global Cache Average CR Get Time":
			s.mb.RecordNewrelicOracleDbGlobalCacheAverageCrGetTimeDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Global Cache Average Current Get Time":
			s.mb.RecordNewrelicOracleDbGlobalCacheAverageCurrentGetTimeDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectResourceLimitMetrics collects resource limit metrics
func (s *oracleScraper) collectResourceLimitMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Process Limit %',
			'Session Limit %',
			'User Limit %',
			'Shared Pool Free %',
			'PGA Cache Hit %',
			'Streams Pool Usage Percentage'
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
		case "Process Limit %":
			s.mb.RecordNewrelicOracleDbProcessLimitPercentageDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Session Limit %":
			s.mb.RecordNewrelicOracleDbSessionLimitPercentageDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "User Limit %":
			s.mb.RecordNewrelicOracleDbUserLimitPercentageDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Shared Pool Free %":
			s.mb.RecordNewrelicOracleDbSharedPoolFreePercentageDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "PGA Cache Hit %":
			s.mb.RecordNewrelicOracleDbPgaCacheHitPercentageDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Streams Pool Usage Percentage":
			s.mb.RecordNewrelicOracleDbStreamsPoolUsagePercentageDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectCacheHitMissMetrics collects cache hit/miss metrics
func (s *oracleScraper) collectCacheHitMissMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Row Cache Hit Ratio',
			'Row Cache Miss Ratio',
			'Library Cache Hit Ratio',
			'Library Cache Miss Ratio',
			'Cursor Cache Hit Ratio'
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
		case "Row Cache Hit Ratio":
			s.mb.RecordNewrelicOracleDbRowCacheHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Row Cache Miss Ratio":
			s.mb.RecordNewrelicOracleDbRowCacheMissRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Library Cache Hit Ratio":
			s.mb.RecordNewrelicOracleDbLibraryCacheHitRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Library Cache Miss Ratio":
			s.mb.RecordNewrelicOracleDbLibraryCacheMissRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Cursor Cache Hit Ratio":
			s.mb.RecordNewrelicOracleDbCursorCacheHitsPerAttemptsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectSessionMetrics collects session-related metrics
func (s *oracleScraper) collectSessionMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'Average Active Sessions',
			'Active Serial Sessions',
			'Active Parallel Sessions',
			'Current OS Load',
			'Host CPU Usage Per Sec',
			'Background CPU Usage Per Sec',
			'Background Time Per Sec'
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
		case "Average Active Sessions":
			s.mb.RecordNewrelicOracleDbAverageActiveSessionsDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Active Serial Sessions":
			s.mb.RecordNewrelicOracleDbActiveSerialSessionsDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "Active Parallel Sessions":
			s.mb.RecordNewrelicOracleDbActiveParallelSessionsDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "Current OS Load":
			s.mb.RecordNewrelicOracleDbOsLoadDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Host CPU Usage Per Sec":
			s.mb.RecordNewrelicOracleDbHostCPUUsagePerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Background CPU Usage Per Sec":
			s.mb.RecordNewrelicOracleDbBackgroundCPUUsagePerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Background Time Per Sec":
			s.mb.RecordNewrelicOracleDbBackgroundTimePerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectUserCallMetrics collects user call related metrics
func (s *oracleScraper) collectUserCallMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT INST_ID, METRIC_NAME, VALUE
		FROM gv$sysmetric
		WHERE METRIC_NAME IN (
			'DB Block Changes Per User Call',
			'DB Block Gets Per User Call',
			'Executions Per User Call',
			'Logical Reads Per User Call',
			'Total Sorts Per User Call',
			'Total Table Scans Per User Call',
			'Execute Without Parse Ratio',
			'Captured user calls',
			'Txns Per Logon',
			'Database Time Per Sec'
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
		case "DB Block Changes Per User Call":
			s.mb.RecordNewrelicOracleDbBlockChangesPerUserCallDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "DB Block Gets Per User Call":
			s.mb.RecordNewrelicOracleDbBlockGetsPerUserCallDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Executions Per User Call":
			s.mb.RecordNewrelicOracleDbExecutionsPerUserCallDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Sorts Per User Call":
			s.mb.RecordNewrelicOracleDbSortsPerUserCallDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Total Table Scans Per User Call":
			s.mb.RecordNewrelicOracleDbTableScansPerUserCallDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Execute Without Parse Ratio":
			s.mb.RecordNewrelicOracleDbExecuteWithoutParseRatioDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Captured user calls":
			s.mb.RecordNewrelicOracleDbCapturedUserCallsDataPoint(pcommon.NewTimestampFromTime(time.Now()), int64(value))
		case "Txns Per Logon":
			s.mb.RecordNewrelicOracleDbTransactionsPerLogonDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		case "Database Time Per Sec":
			s.mb.RecordNewrelicOracleDbDatabaseCPUTimePerSecondDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
		}
	}

	return nil
}

// collectSortsMetrics collects sorts metrics
func (s *oracleScraper) collectSortsMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT inst.inst_id, sysstat.name, sysstat.value
		FROM GV$SYSSTAT sysstat, GV$INSTANCE inst
		WHERE sysstat.inst_id=inst.inst_id 
		AND sysstat.name IN ('sorts (memory)', 'sorts (disk)')`

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

// collectRollbackSegmentMetrics collects rollback segment metrics
func (s *oracleScraper) collectRollbackSegmentMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT
			SUM(stat.gets) AS gets,
			sum(stat.waits) AS waits,
			sum(stat.waits)/sum(stat.gets) AS ratio,
			inst.inst_id
		FROM GV$ROLLSTAT stat, GV$INSTANCE inst
		WHERE stat.inst_id=inst.inst_id
		GROUP BY inst.inst_id`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var gets, waits int64
		var ratio float64
		var instID int
		if err := rows.Scan(&gets, &waits, &ratio, &instID); err != nil {
			continue
		}

		s.mb.RecordNewrelicOracleRollbackSegmentsGetsDataPoint(pcommon.NewTimestampFromTime(time.Now()), gets)
		s.mb.RecordNewrelicOracleRollbackSegmentsWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), waits)
		s.mb.RecordNewrelicOracleRollbackSegmentsRatioWaitDataPoint(pcommon.NewTimestampFromTime(time.Now()), ratio)
	}

	return nil
}

// collectRedoLogMetrics collects redo log metrics
func (s *oracleScraper) collectRedoLogMetrics(instanceInfo *instanceInfo) error {
	query := `
		SELECT
			sysevent.total_waits,
			inst.inst_id,
			sysevent.event
		FROM
			GV$SYSTEM_EVENT sysevent,
			GV$INSTANCE inst
		WHERE sysevent.inst_id=inst.inst_id
		AND sysevent.event IN (
			'log file parallel write',
			'log file switch completion',
			'log file switch (check',
			'log file switch (arch'
		)`

	rows, err := s.dbClient.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var totalWaits int64
		var instID int
		var event string
		if err := rows.Scan(&totalWaits, &instID, &event); err != nil {
			continue
		}

		if strings.Contains(event, "log file parallel write") {
			s.mb.RecordNewrelicOracleRedoLogWaitsDataPoint(pcommon.NewTimestampFromTime(time.Now()), totalWaits)
		} else if strings.Contains(event, "log file switch completion") {
			s.mb.RecordNewrelicOracleRedoLogFileSwitchDataPoint(pcommon.NewTimestampFromTime(time.Now()), totalWaits)
		} else if strings.Contains(event, "log file switch (check") {
			s.mb.RecordNewrelicOracleRedoLogFileSwitchCheckpointIncompleteDataPoint(pcommon.NewTimestampFromTime(time.Now()), totalWaits)
		} else if strings.Contains(event, "log file switch (arch") {
			s.mb.RecordNewrelicOracleRedoLogFileSwitchArchivingNeededDataPoint(pcommon.NewTimestampFromTime(time.Now()), totalWaits)
		}
	}

	return nil
}
