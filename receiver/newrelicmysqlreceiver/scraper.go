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
func (n *newRelicMySQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	if n.sqlclient == nil {
		return pmetric.Metrics{}, errors.New("failed to connect to database client")
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	// collect global status metrics.
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver started")
	n.scrapeGlobalStats(now, errs)
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver completed")

	return n.mb.Emit(), nil
}

func (m *newRelicMySQLScraper) scrapeGlobalStats(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	globalStats, err := m.sqlclient.getGlobalStats()
	if err != nil {
		m.logger.Error("Failed to fetch global stats", zap.Error(err))
		errs.AddPartial(66, err)
		return
	}

	globalVars, err := m.sqlclient.getGlobalVariables()
	if err != nil {
		m.logger.Error("Failed to fetch global variables", zap.Error(err))
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
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlConnectionCountDataPoint(now, strconv.FormatInt(v, 10)))
			m.mb.RecordNewrelicmysqlNetConnectionsDataPoint(now, v)
		case "Queries":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlQueryCountDataPoint(now, strconv.FormatInt(v, 10)))
		case "Uptime":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlUptimeDataPoint(now, strconv.FormatInt(v, 10)))

		// Commands
		case "Com_delete":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandDelete))
		case "Com_delete_multi":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandDeleteMulti))
		case "Com_insert":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandInsert))
		case "Com_insert_select":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandInsertSelect))
		case "Com_load":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandLoad))
		case "Com_replace":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandReplace))
		case "Com_replace_select":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandReplaceSelect))
		case "Com_select":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandSelect))
		case "Com_update":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandUpdate))
		case "Com_update_multi":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandUpdateMulti))

		// Performance metrics
		case "Questions":
			m.mb.RecordNewrelicmysqlPerformanceQuestionsDataPoint(now, v)
		case "Slow_queries":
			m.mb.RecordNewrelicmysqlPerformanceSlowQueriesDataPoint(now, v)
		case "Threads_connected":
			threadsConnected = v
			m.mb.RecordNewrelicmysqlPerformanceThreadsConnectedDataPoint(now, v)
		case "Threads_running":
			m.mb.RecordNewrelicmysqlPerformanceThreadsRunningDataPoint(now, v)
		case "Qcache_hits":
			m.mb.RecordNewrelicmysqlPerformanceQcacheHitsDataPoint(now, v)
		case "Qcache_inserts":
			m.mb.RecordNewrelicmysqlPerformanceQcacheInsertsDataPoint(now, v)
		case "Qcache_lowmem_prunes":
			m.mb.RecordNewrelicmysqlPerformanceQcacheLowmemPrunesDataPoint(now, v)
		case "Qcache_free_memory":
			m.mb.RecordNewrelicmysqlPerformanceQcacheSizeDataPoint(now, v)
		case "Open_files":
			m.mb.RecordNewrelicmysqlPerformanceOpenFilesDataPoint(now, v)
		case "Open_tables":
			m.mb.RecordNewrelicmysqlPerformanceOpenTablesDataPoint(now, v)
		case "Table_locks_waited":
			m.mb.RecordNewrelicmysqlPerformanceTableLocksWaitedDataPoint(now, v)
		case "Table_open_cache":
			m.mb.RecordNewrelicmysqlPerformanceTableOpenCacheDataPoint(now, v)
		case "Created_tmp_disk_tables":
			m.mb.RecordNewrelicmysqlPerformanceCreatedTmpDiskTablesDataPoint(now, v)
		case "Created_tmp_files":
			m.mb.RecordNewrelicmysqlPerformanceCreatedTmpFilesDataPoint(now, v)
		case "Created_tmp_tables":
			m.mb.RecordNewrelicmysqlPerformanceCreatedTmpTablesDataPoint(now, v)
		case "Bytes_received":
			m.mb.RecordNewrelicmysqlPerformanceBytesReceivedDataPoint(now, v)
		case "Bytes_sent":
			m.mb.RecordNewrelicmysqlPerformanceBytesSentDataPoint(now, v)
		case "max_prepared_stmt_count":
			m.mb.RecordNewrelicmysqlPerformanceMaxPreparedStmtCountDataPoint(now, v)
		case "Prepared_stmt_count":
			m.mb.RecordNewrelicmysqlPerformancePreparedStmtCountDataPoint(now, v)
		case "Performance_schema_digest_lost":
			m.mb.RecordNewrelicmysqlPerformancePerformanceSchemaDigestLostDataPoint(now, v)
		case "thread_cache_size":
			m.mb.RecordNewrelicmysqlPerformanceThreadCacheSizeDataPoint(now, v)

		// Network metrics
		case "Aborted_clients":
			m.mb.RecordNewrelicmysqlNetAbortedClientsDataPoint(now, v)
		case "Aborted_connects":
			m.mb.RecordNewrelicmysqlNetAbortedConnectsDataPoint(now, v)
		case "max_connections":
			maxConnections = v
			m.mb.RecordNewrelicmysqlNetMaxConnectionsDataPoint(now, v)

		// MyISAM metrics
		case "Key_blocks_not_flushed":
			m.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUnflushedDataPoint(now, v*1024)
		case "Key_blocks_used":
			keyBlocksUsed = v
			m.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUsedDataPoint(now, v*1024)
		case "key_buffer_size":
			keyBufferSize = v
			m.mb.RecordNewrelicmysqlMyisamKeyBufferSizeDataPoint(now, v)
		case "Key_read_requests":
			m.mb.RecordNewrelicmysqlMyisamKeyReadRequestsDataPoint(now, v)
		case "Key_reads":
			m.mb.RecordNewrelicmysqlMyisamKeyReadsDataPoint(now, v)
		case "Key_write_requests":
			m.mb.RecordNewrelicmysqlMyisamKeyWriteRequestsDataPoint(now, v)
		case "Key_writes":
			m.mb.RecordNewrelicmysqlMyisamKeyWritesDataPoint(now, v)

		// InnoDB buffer pool metrics
		case "Innodb_buffer_pool_pages_dirty":
			m.mb.RecordNewrelicmysqlInnodbBufferPoolDirtyDataPoint(now, v)
		case "Innodb_buffer_pool_pages_free":
			m.mb.RecordNewrelicmysqlInnodbBufferPoolFreeDataPoint(now, v)
		case "Innodb_buffer_pool_read_requests":
			m.mb.RecordNewrelicmysqlInnodbBufferPoolReadRequestsDataPoint(now, v)
		case "Innodb_buffer_pool_reads":
			m.mb.RecordNewrelicmysqlInnodbBufferPoolReadsDataPoint(now, v)
		case "Innodb_buffer_pool_pages_total":
			innodbBufferPoolTotal = v
			m.mb.RecordNewrelicmysqlInnodbBufferPoolTotalDataPoint(now, v)
		case "Innodb_buffer_pool_pages_data":
			innodbBufferPoolUsed = v
			m.mb.RecordNewrelicmysqlInnodbBufferPoolUsedDataPoint(now, v)

		// InnoDB row lock metrics
		case "Innodb_row_lock_current_waits":
			m.mb.RecordNewrelicmysqlInnodbRowLockCurrentWaitsDataPoint(now, v)
		case "Innodb_row_lock_time":
			m.mb.RecordNewrelicmysqlInnodbRowLockTimeDataPoint(now, v)
		case "Innodb_row_lock_waits":
			m.mb.RecordNewrelicmysqlInnodbRowLockWaitsDataPoint(now, v)

		// InnoDB data metrics
		case "Innodb_data_reads":
			m.mb.RecordNewrelicmysqlInnodbDataReadsDataPoint(now, v)
		case "Innodb_data_writes":
			m.mb.RecordNewrelicmysqlInnodbDataWritesDataPoint(now, v)

		// InnoDB mutex metrics
		case "Innodb_mutex_os_waits":
			m.mb.RecordNewrelicmysqlInnodbMutexOsWaitsDataPoint(now, v)
		case "Innodb_mutex_spin_rounds":
			m.mb.RecordNewrelicmysqlInnodbMutexSpinRoundsDataPoint(now, v)
		case "Innodb_mutex_spin_waits":
			m.mb.RecordNewrelicmysqlInnodbMutexSpinWaitsDataPoint(now, v)
		case "Innodb_os_log_fsyncs":
			m.mb.RecordNewrelicmysqlInnodbOsLogFsyncsDataPoint(now, v)

		// InnoDB current row locks (deprecated in MySQL 5.7+, but kept for compatibility)
		case "Innodb_current_row_locks":
			m.mb.RecordNewrelicmysqlInnodbCurrentRowLocksDataPoint(now, v)
		}
	}

	// Calculate derived metrics
	// max_connections_available = max_connections - threads_connected
	if maxConnections > 0 {
		available := maxConnections - threadsConnected
		if available < 0 {
			available = 0
		}
		m.mb.RecordNewrelicmysqlNetMaxConnectionsAvailableDataPoint(now, available)
	}

	// key_cache_utilization = (key_blocks_used * key_cache_block_size) / key_buffer_size * 100
	if keyBufferSize > 0 && keyBlocksUsed > 0 {
		utilization := float64(keyBlocksUsed*1024) / float64(keyBufferSize) * 100
		m.mb.RecordNewrelicmysqlPerformanceKeyCacheUtilizationDataPoint(now, utilization)
	}

	// innodb_buffer_pool_utilization = innodb_buffer_pool_used / innodb_buffer_pool_total * 100
	if innodbBufferPoolTotal > 0 {
		utilization := float64(innodbBufferPoolUsed) / float64(innodbBufferPoolTotal) * 100
		m.mb.RecordNewrelicmysqlInnodbBufferPoolUtilizationDataPoint(now, utilization)
	}
}

func addPartialIfError(errors *scrapererror.ScrapeErrors, err error) {
	if err != nil {
		errors.AddPartial(1, err)
	}
}
