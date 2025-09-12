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
			s.mb.RecordNewrelicOracleDbSQLServiceResponseTimeDataPoint(pcommon.NewTimestampFromTime(time.Now()), value)
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
		whereClause = fmt.Sprintf(" WHERE a.TABLESPACE_NAME IN (%s)", strings.Join(quotedTablespaces, ","))
	}

	query := fmt.Sprintf(`
		SELECT a.TABLESPACE_NAME,
			a.USED_PERCENT,
			a.USED_SPACE * b.BLOCK_SIZE AS USED,
			a.TABLESPACE_SIZE * b.BLOCK_SIZE AS SIZE,
			b.TABLESPACE_OFFLINE AS OFFLINE
		FROM DBA_TABLESPACE_USAGE_METRICS a
		JOIN (
			SELECT
				TABLESPACE_NAME,
				BLOCK_SIZE,
				MAX(CASE WHEN status = 'OFFLINE' THEN 1 ELSE 0 END) AS TABLESPACE_OFFLINE
			FROM DBA_TABLESPACES
			GROUP BY TABLESPACE_NAME, BLOCK_SIZE
		) b ON a.TABLESPACE_NAME = b.TABLESPACE_NAME%s`, whereClause)

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
