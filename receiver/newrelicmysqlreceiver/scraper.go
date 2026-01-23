// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

type newRelicMySQLScraper struct {
	sqlclient client
	logger    *zap.Logger
	config    *Config
	mb        *metadata.MetricsBuilder
}

func newNewRelicMySQLScraper(
	settings receiver.Settings,
	config *Config,
) *newRelicMySQLScraper {
	return &newRelicMySQLScraper{
		logger: settings.Logger,
		config: config,
		mb:     metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings),
	}
}

// start starts the scraper by initializing the database client connection.
func (n *newRelicMySQLScraper) start(_ context.Context, _ component.Host) error {
	sqlclient, err := newMySQLClient(n.config)
	if err != nil {
		return err
	}

	err = sqlclient.Connect()
	if err != nil {
		return err
	}
	n.sqlclient = sqlclient

	return nil
}

// shutdown closes the database connection.
func (n *newRelicMySQLScraper) shutdown(context.Context) error {
	if n.sqlclient == nil {
		return nil
	}
	return n.sqlclient.Close()
}

// scrape scrapes the MySQL database metrics and transforms them.
func (n *newRelicMySQLScraper) scrape(_ context.Context) (pmetric.Metrics, error) {
	if n.sqlclient == nil {
		return pmetric.Metrics{}, errors.New("failed to connect to database client")
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	// collect global status metrics.
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver started")
	n.scrapeGlobalStats(now, errs)
	n.scrapeVersion(now, errs)
	n.scrapeReplication(now, errs)
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver completed")

	return n.mb.Emit(), nil
}

func (n *newRelicMySQLScraper) scrapeGlobalStats(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	globalStats, err := n.sqlclient.getGlobalStats()
	if err != nil {
		n.logger.Error("Failed to fetch global stats", zap.Error(err))
		errs.AddPartial(66, err)
		return
	}

	globalVars, err := n.sqlclient.getGlobalVariables()
	if err != nil {
		n.logger.Error("Failed to fetch global variables", zap.Error(err))
		errs.AddPartial(66, err)
		return
	}

	// Merge global variables into stats for unified processing
	for k, v := range globalVars {
		globalStats[k] = v
	}

	// Track values needed for calculated metrics
	var (
		maxConnections        int64
		threadsConnected      int64
		keyBufferSize         int64
		keyBlocksUsed         int64
		innodbBufferPoolTotal int64
		innodbBufferPoolUsed  int64
	)

	for k, v := range globalStats {
		switch k {
		// Original newrelicmysql metrics
		case "Connections":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlConnectionCountDataPoint(now, strconv.FormatInt(v, 10)))
			n.mb.RecordNewrelicmysqlNetConnectionsDataPoint(now, v)
		case "Queries":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlQueryCountDataPoint(now, strconv.FormatInt(v, 10)))
		case "Uptime":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlUptimeDataPoint(now, strconv.FormatInt(v, 10)))

		// Commands
		case "Com_commit":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandCommit))
		case "Com_delete":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandDelete))
		case "Com_delete_multi":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandDeleteMulti))
		case "Com_insert":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandInsert))
		case "Com_insert_select":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandInsertSelect))
		case "Com_load":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandLoad))
		case "Com_replace":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandReplace))
		case "Com_replace_select":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandReplaceSelect))
		case "Com_rollback":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandRollback))
		case "Com_select":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandSelect))
		case "Com_update":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandUpdate))
		case "Com_update_multi":
			addPartialIfError(errs, n.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandUpdateMulti))

		// Performance metrics
		case "Handler_rollback":
			n.mb.RecordNewrelicmysqlDbHandlerRollbackDataPoint(now, v)
		case "Opened_tables":
			n.mb.RecordNewrelicmysqlDbOpenedTablesDataPoint(now, v)
		case "Questions":
			n.mb.RecordNewrelicmysqlPerformanceQuestionsDataPoint(now, v)
		case "Slow_queries":
			n.mb.RecordNewrelicmysqlPerformanceSlowQueriesDataPoint(now, v)
		case "Threads_connected":
			threadsConnected = v
			n.mb.RecordNewrelicmysqlPerformanceThreadsConnectedDataPoint(now, v)
		case "Threads_running":
			n.mb.RecordNewrelicmysqlPerformanceThreadsRunningDataPoint(now, v)
		case "Qcache_hits":
			n.mb.RecordNewrelicmysqlPerformanceQcacheHitsDataPoint(now, v)
		case "Qcache_inserts":
			n.mb.RecordNewrelicmysqlPerformanceQcacheInsertsDataPoint(now, v)
		case "Qcache_lowmem_prunes":
			n.mb.RecordNewrelicmysqlPerformanceQcacheLowmemPrunesDataPoint(now, v)
		case "Qcache_not_cached":
			n.mb.RecordNewrelicmysqlPerformanceQcacheNotCachedDataPoint(now, v)
		case "Qcache_free_memory":
			n.mb.RecordNewrelicmysqlPerformanceQcacheSizeDataPoint(now, v)
		case "Open_files":
			n.mb.RecordNewrelicmysqlPerformanceOpenFilesDataPoint(now, v)
		case "Open_tables":
			n.mb.RecordNewrelicmysqlPerformanceOpenTablesDataPoint(now, v)
		case "Table_locks_waited":
			n.mb.RecordNewrelicmysqlPerformanceTableLocksWaitedDataPoint(now, v)
		case "Table_open_cache":
			n.mb.RecordNewrelicmysqlPerformanceTableOpenCacheDataPoint(now, v)
		case "Created_tmp_disk_tables":
			n.mb.RecordNewrelicmysqlPerformanceCreatedTmpDiskTablesDataPoint(now, v)
		case "Created_tmp_files":
			n.mb.RecordNewrelicmysqlPerformanceCreatedTmpFilesDataPoint(now, v)
		case "Created_tmp_tables":
			n.mb.RecordNewrelicmysqlPerformanceCreatedTmpTablesDataPoint(now, v)
		case "Bytes_received":
			n.mb.RecordNewrelicmysqlPerformanceBytesReceivedDataPoint(now, v)
		case "Bytes_sent":
			n.mb.RecordNewrelicmysqlPerformanceBytesSentDataPoint(now, v)
		case "max_prepared_stmt_count":
			n.mb.RecordNewrelicmysqlPerformanceMaxPreparedStmtCountDataPoint(now, v)
		case "Prepared_stmt_count":
			n.mb.RecordNewrelicmysqlPerformancePreparedStmtCountDataPoint(now, v)
		case "Performance_schema_digest_lost":
			n.mb.RecordNewrelicmysqlPerformancePerformanceSchemaDigestLostDataPoint(now, v)
		case "thread_cache_size":
			n.mb.RecordNewrelicmysqlPerformanceThreadCacheSizeDataPoint(now, v)

		// Network metrics
		case "Aborted_clients":
			n.mb.RecordNewrelicmysqlNetAbortedClientsDataPoint(now, v)
		case "Aborted_connects":
			n.mb.RecordNewrelicmysqlNetAbortedConnectsDataPoint(now, v)
		case "Max_used_connections":
			n.mb.RecordNewrelicmysqlNetMaxUsedConnectionsDataPoint(now, v)
		case "max_connections":
			maxConnections = v
			n.mb.RecordNewrelicmysqlNetMaxConnectionsDataPoint(now, v)

		// MyISAM metrics
		case "Key_blocks_not_flushed":
			n.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUnflushedDataPoint(now, v*1024)
		case "Key_blocks_used":
			keyBlocksUsed = v
			n.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUsedDataPoint(now, v*1024)
		case "key_buffer_size":
			keyBufferSize = v
			n.mb.RecordNewrelicmysqlMyisamKeyBufferSizeDataPoint(now, v)
		case "Key_read_requests":
			n.mb.RecordNewrelicmysqlMyisamKeyReadRequestsDataPoint(now, v)
		case "Key_reads":
			n.mb.RecordNewrelicmysqlMyisamKeyReadsDataPoint(now, v)
		case "Key_write_requests":
			n.mb.RecordNewrelicmysqlMyisamKeyWriteRequestsDataPoint(now, v)
		case "Key_writes":
			n.mb.RecordNewrelicmysqlMyisamKeyWritesDataPoint(now, v)

		// InnoDB buffer pool metrics
		case "Innodb_buffer_pool_pages_dirty":
			n.mb.RecordNewrelicmysqlInnodbBufferPoolDirtyDataPoint(now, v)
		case "Innodb_buffer_pool_pages_free":
			n.mb.RecordNewrelicmysqlInnodbBufferPoolFreeDataPoint(now, v)
		case "Innodb_buffer_pool_read_requests":
			n.mb.RecordNewrelicmysqlInnodbBufferPoolReadRequestsDataPoint(now, v)
		case "Innodb_buffer_pool_reads":
			n.mb.RecordNewrelicmysqlInnodbBufferPoolReadsDataPoint(now, v)
		case "Innodb_buffer_pool_pages_total":
			innodbBufferPoolTotal = v
			n.mb.RecordNewrelicmysqlInnodbBufferPoolTotalDataPoint(now, v)
		case "Innodb_buffer_pool_pages_data":
			innodbBufferPoolUsed = v
			n.mb.RecordNewrelicmysqlInnodbBufferPoolUsedDataPoint(now, v)

		// InnoDB row lock metrics
		case "Innodb_row_lock_current_waits":
			n.mb.RecordNewrelicmysqlInnodbRowLockCurrentWaitsDataPoint(now, v)
		case "Innodb_row_lock_time":
			n.mb.RecordNewrelicmysqlInnodbRowLockTimeDataPoint(now, v)
		case "Innodb_row_lock_waits":
			n.mb.RecordNewrelicmysqlInnodbRowLockWaitsDataPoint(now, v)

		// InnoDB data metrics
		case "Innodb_data_reads":
			n.mb.RecordNewrelicmysqlInnodbDataReadsDataPoint(now, v)
		case "Innodb_data_writes":
			n.mb.RecordNewrelicmysqlInnodbDataWritesDataPoint(now, v)
		case "Innodb_data_written":
			n.mb.RecordNewrelicmysqlInnodbDataWrittenDataPoint(now, v)
		case "Innodb_log_waits":
			n.mb.RecordNewrelicmysqlInnodbLogWaitsDataPoint(now, v)

		// InnoDB mutex metrics
		case "Innodb_mutex_os_waits":
			n.mb.RecordNewrelicmysqlInnodbMutexOsWaitsDataPoint(now, v)
		case "Innodb_mutex_spin_rounds":
			n.mb.RecordNewrelicmysqlInnodbMutexSpinRoundsDataPoint(now, v)
		case "Innodb_mutex_spin_waits":
			n.mb.RecordNewrelicmysqlInnodbMutexSpinWaitsDataPoint(now, v)
		case "Innodb_os_log_fsyncs":
			n.mb.RecordNewrelicmysqlInnodbOsLogFsyncsDataPoint(now, v)

		// InnoDB current row locks (deprecated in MySQL 5.7+, but kept for compatibility)
		case "Innodb_current_row_locks":
			n.mb.RecordNewrelicmysqlInnodbCurrentRowLocksDataPoint(now, v)
		}
	}

	// Calculate derived metrics
	// max_connections_available = max_connections - threads_connected
	if maxConnections > 0 {
		available := maxConnections - threadsConnected
		if available < 0 {
			available = 0
		}
		n.mb.RecordNewrelicmysqlNetMaxConnectionsAvailableDataPoint(now, available)
	}

	// key_cache_utilization = (key_blocks_used * key_cache_block_size) / key_buffer_size * 100
	if keyBufferSize > 0 && keyBlocksUsed > 0 {
		utilization := float64(keyBlocksUsed*1024) / float64(keyBufferSize) * 100
		n.mb.RecordNewrelicmysqlPerformanceKeyCacheUtilizationDataPoint(now, utilization)
	}

	// innodb_buffer_pool_utilization = innodb_buffer_pool_used / innodb_buffer_pool_total * 100
	if innodbBufferPoolTotal > 0 {
		utilization := float64(innodbBufferPoolUsed) / float64(innodbBufferPoolTotal) * 100
		n.mb.RecordNewrelicmysqlInnodbBufferPoolUtilizationDataPoint(now, utilization)
	}
}

func (n *newRelicMySQLScraper) scrapeVersion(_ pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	version, err := n.sqlclient.getVersion()
	if err != nil {
		n.logger.Error("Failed to fetch version", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	// Log version info (string metrics not supported in OpenTelemetry)
	n.logger.Info("MySQL version detected", zap.String("version", version))
}

func (n *newRelicMySQLScraper) scrapeReplication(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	replicationStatus, err := n.sqlclient.getReplicationStatus()
	if err != nil {
		n.logger.Error("Failed to fetch replication status", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	// If replication status is empty, this is not a replica
	if len(replicationStatus) == 0 {
		n.logger.Info("Node is a master (no replication status)")
		return
	}

	// This is a replica - log status info
	n.logger.Info("Node is a replica")

	// Parse and record replication metrics
	for key, value := range replicationStatus {
		switch key {
		case "Seconds_Behind_Master", "Seconds_Behind_Source":
			if value != "" && value != "NULL" {
				if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
					n.mb.RecordNewrelicmysqlReplicationSecondsBehindMasterDataPoint(now, intVal)
				}
			}
		case "Read_Master_Log_Pos", "Read_Source_Log_Pos":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				n.mb.RecordNewrelicmysqlReplicationReadMasterLogPosDataPoint(now, intVal)
			}
		case "Exec_Master_Log_Pos", "Exec_Source_Log_Pos":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				n.mb.RecordNewrelicmysqlReplicationExecMasterLogPosDataPoint(now, intVal)
			}
		case "Last_IO_Errno":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				n.mb.RecordNewrelicmysqlReplicationLastIoErrnoDataPoint(now, intVal)
			}
		case "Last_SQL_Errno", "Last_Errno":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				n.mb.RecordNewrelicmysqlReplicationLastSQLErrnoDataPoint(now, intVal)
			}
		case "Relay_Log_Space":
			if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
				n.mb.RecordNewrelicmysqlReplicationRelayLogSpaceDataPoint(now, intVal)
			}
		}
	}

	// Get IO and SQL thread status (compatible with MySQL 5.7 and 8.0+)
	ioRunning := replicationStatus["Slave_IO_Running"]
	if ioRunning == "" {
		ioRunning = replicationStatus["Replica_IO_Running"]
	}
	sqlRunning := replicationStatus["Slave_SQL_Running"]
	if sqlRunning == "" {
		sqlRunning = replicationStatus["Replica_SQL_Running"]
	}

	// Convert IO thread status to numeric: 0=No, 1=Yes, 2=Connecting
	ioStatus := convertReplicationThreadStatus(ioRunning)
	n.mb.RecordNewrelicmysqlReplicationSlaveIoRunningDataPoint(now, ioStatus)

	// Convert SQL thread status to numeric: 0=No, 1=Yes
	sqlStatus := convertReplicationThreadStatus(sqlRunning)
	n.mb.RecordNewrelicmysqlReplicationSlaveSQLRunningDataPoint(now, sqlStatus)

	// Calculate composite slave_running (1 if both IO and SQL threads are running, 0 otherwise)
	slaveRunning := int64(0)
	if ioRunning == "Yes" && sqlRunning == "Yes" {
		slaveRunning = 1
	}
	n.mb.RecordNewrelicmysqlReplicationSlaveRunningDataPoint(now, slaveRunning)
}

// convertReplicationThreadStatus converts replication thread status string to numeric code
// Returns: 0 = No/Stopped, 1 = Yes/Running, 2 = Connecting (IO thread only)
func convertReplicationThreadStatus(status string) int64 {
	switch status {
	case "Yes":
		return 1
	case "Connecting":
		return 2
	case "No", "":
		return 0
	default:
		return 0
	}
}

func addPartialIfError(errors *scrapererror.ScrapeErrors, err error) {
	if err != nil {
		errors.AddPartial(1, err)
	}
}
