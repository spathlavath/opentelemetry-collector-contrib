// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// CoreScraper handles MySQL core metrics collection.
// These metrics are based on the Datadog MySQL integration's always-collected metrics.
type CoreScraper struct {
	client client.MySQLClient
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

// NewCoreScraper creates a new core metrics scraper.
func NewCoreScraper(c client.MySQLClient, mb *metadata.MetricsBuilder, logger *zap.Logger) (*CoreScraper, error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &CoreScraper{
		client: c,
		mb:     mb,
		logger: logger,
	}, nil
}

// ScrapeMetrics collects MySQL core database metrics.
func (s *CoreScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL core metrics")

	globalStats, err := s.client.GetGlobalStats()
	if err != nil {
		s.logger.Error("Failed to fetch global stats", zap.Error(err))
		errs.AddPartial(66, err)
		return
	}

	globalVars, err := s.client.GetGlobalVariables()
	if err != nil {
		s.logger.Error("Failed to fetch global variables", zap.Error(err))
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
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlConnectionCountDataPoint(now, strconv.FormatInt(v, 10)))
			s.mb.RecordNewrelicmysqlNetConnectionsDataPoint(now, v)
		case "Queries":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlQueryCountDataPoint(now, strconv.FormatInt(v, 10)))
		case "Uptime":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlUptimeDataPoint(now, strconv.FormatInt(v, 10)))

		// Commands
		case "Com_commit":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandCommit))
		case "Com_delete":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandDelete))
		case "Com_delete_multi":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandDeleteMulti))
		case "Com_insert":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandInsert))
		case "Com_insert_select":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandInsertSelect))
		case "Com_load":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandLoad))
		case "Com_replace":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandReplace))
		case "Com_replace_select":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandReplaceSelect))
		case "Com_rollback":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandRollback))
		case "Com_select":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandSelect))
		case "Com_update":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandUpdate))
		case "Com_update_multi":
			AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, strconv.FormatInt(v, 10), metadata.AttributeCommandUpdateMulti))

		// Performance metrics
		case "Handler_rollback":
			s.mb.RecordNewrelicmysqlDbHandlerRollbackDataPoint(now, v)
		case "Opened_tables":
			s.mb.RecordNewrelicmysqlDbOpenedTablesDataPoint(now, v)
		case "Questions":
			s.mb.RecordNewrelicmysqlPerformanceQuestionsDataPoint(now, v)
		case "Slow_queries":
			s.mb.RecordNewrelicmysqlPerformanceSlowQueriesDataPoint(now, v)
		case "Threads_connected":
			threadsConnected = v
			s.mb.RecordNewrelicmysqlPerformanceThreadsConnectedDataPoint(now, v)
		case "Threads_running":
			s.mb.RecordNewrelicmysqlPerformanceThreadsRunningDataPoint(now, v)
		case "Qcache_hits":
			s.mb.RecordNewrelicmysqlPerformanceQcacheHitsDataPoint(now, v)
		case "Qcache_inserts":
			s.mb.RecordNewrelicmysqlPerformanceQcacheInsertsDataPoint(now, v)
		case "Qcache_lowmem_prunes":
			s.mb.RecordNewrelicmysqlPerformanceQcacheLowmemPrunesDataPoint(now, v)
		case "Qcache_not_cached":
			s.mb.RecordNewrelicmysqlPerformanceQcacheNotCachedDataPoint(now, v)
		case "Qcache_free_memory":
			s.mb.RecordNewrelicmysqlPerformanceQcacheSizeDataPoint(now, v)
		case "Open_files":
			s.mb.RecordNewrelicmysqlPerformanceOpenFilesDataPoint(now, v)
		case "Open_tables":
			s.mb.RecordNewrelicmysqlPerformanceOpenTablesDataPoint(now, v)
		case "Table_locks_waited":
			s.mb.RecordNewrelicmysqlPerformanceTableLocksWaitedDataPoint(now, v)
		case "Table_open_cache":
			s.mb.RecordNewrelicmysqlPerformanceTableOpenCacheDataPoint(now, v)
		case "Created_tmp_disk_tables":
			s.mb.RecordNewrelicmysqlPerformanceCreatedTmpDiskTablesDataPoint(now, v)
		case "Created_tmp_files":
			s.mb.RecordNewrelicmysqlPerformanceCreatedTmpFilesDataPoint(now, v)
		case "Created_tmp_tables":
			s.mb.RecordNewrelicmysqlPerformanceCreatedTmpTablesDataPoint(now, v)
		case "Bytes_received":
			s.mb.RecordNewrelicmysqlPerformanceBytesReceivedDataPoint(now, v)
		case "Bytes_sent":
			s.mb.RecordNewrelicmysqlPerformanceBytesSentDataPoint(now, v)
		case "max_prepared_stmt_count":
			s.mb.RecordNewrelicmysqlPerformanceMaxPreparedStmtCountDataPoint(now, v)
		case "Prepared_stmt_count":
			s.mb.RecordNewrelicmysqlPerformancePreparedStmtCountDataPoint(now, v)
		case "Performance_schema_digest_lost":
			s.mb.RecordNewrelicmysqlPerformancePerformanceSchemaDigestLostDataPoint(now, v)
		case "thread_cache_size":
			s.mb.RecordNewrelicmysqlPerformanceThreadCacheSizeDataPoint(now, v)

		// Network metrics
		case "Aborted_clients":
			s.mb.RecordNewrelicmysqlNetAbortedClientsDataPoint(now, v)
		case "Aborted_connects":
			s.mb.RecordNewrelicmysqlNetAbortedConnectsDataPoint(now, v)
		case "Max_used_connections":
			s.mb.RecordNewrelicmysqlNetMaxUsedConnectionsDataPoint(now, v)
		case "max_connections":
			maxConnections = v
			s.mb.RecordNewrelicmysqlNetMaxConnectionsDataPoint(now, v)

		// MyISAM metrics
		case "Key_blocks_not_flushed":
			s.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUnflushedDataPoint(now, v*1024)
		case "Key_blocks_used":
			keyBlocksUsed = v
			s.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUsedDataPoint(now, v*1024)
		case "key_buffer_size":
			keyBufferSize = v
			s.mb.RecordNewrelicmysqlMyisamKeyBufferSizeDataPoint(now, v)
		case "Key_read_requests":
			s.mb.RecordNewrelicmysqlMyisamKeyReadRequestsDataPoint(now, v)
		case "Key_reads":
			s.mb.RecordNewrelicmysqlMyisamKeyReadsDataPoint(now, v)
		case "Key_write_requests":
			s.mb.RecordNewrelicmysqlMyisamKeyWriteRequestsDataPoint(now, v)
		case "Key_writes":
			s.mb.RecordNewrelicmysqlMyisamKeyWritesDataPoint(now, v)

		// InnoDB buffer pool metrics
		case "Innodb_buffer_pool_pages_dirty":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolDirtyDataPoint(now, v)
		case "Innodb_buffer_pool_pages_free":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolFreeDataPoint(now, v)
		case "Innodb_buffer_pool_read_requests":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadRequestsDataPoint(now, v)
		case "Innodb_buffer_pool_reads":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadsDataPoint(now, v)
		case "Innodb_buffer_pool_pages_total":
			innodbBufferPoolTotal = v
			s.mb.RecordNewrelicmysqlInnodbBufferPoolTotalDataPoint(now, v)
		case "Innodb_buffer_pool_pages_data":
			innodbBufferPoolUsed = v
			s.mb.RecordNewrelicmysqlInnodbBufferPoolUsedDataPoint(now, v)

		// InnoDB row lock metrics
		case "Innodb_row_lock_current_waits":
			s.mb.RecordNewrelicmysqlInnodbRowLockCurrentWaitsDataPoint(now, v)
		case "Innodb_row_lock_time":
			s.mb.RecordNewrelicmysqlInnodbRowLockTimeDataPoint(now, v)
		case "Innodb_row_lock_waits":
			s.mb.RecordNewrelicmysqlInnodbRowLockWaitsDataPoint(now, v)

		// InnoDB data metrics
		case "Innodb_data_reads":
			s.mb.RecordNewrelicmysqlInnodbDataReadsDataPoint(now, v)
		case "Innodb_data_writes":
			s.mb.RecordNewrelicmysqlInnodbDataWritesDataPoint(now, v)
		case "Innodb_data_written":
			s.mb.RecordNewrelicmysqlInnodbDataWrittenDataPoint(now, v)
		case "Innodb_log_waits":
			s.mb.RecordNewrelicmysqlInnodbLogWaitsDataPoint(now, v)

		// InnoDB mutex metrics
		case "Innodb_mutex_os_waits":
			s.mb.RecordNewrelicmysqlInnodbMutexOsWaitsDataPoint(now, v)
		case "Innodb_mutex_spin_rounds":
			s.mb.RecordNewrelicmysqlInnodbMutexSpinRoundsDataPoint(now, v)
		case "Innodb_mutex_spin_waits":
			s.mb.RecordNewrelicmysqlInnodbMutexSpinWaitsDataPoint(now, v)
		case "Innodb_os_log_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbOsLogFsyncsDataPoint(now, v)

		// InnoDB current row locks (deprecated in MySQL 5.7+, but kept for compatibility)
		case "Innodb_current_row_locks":
			s.mb.RecordNewrelicmysqlInnodbCurrentRowLocksDataPoint(now, v)
		}
	}

	// Calculate derived metrics
	s.calculateDerivedMetrics(now, maxConnections, threadsConnected, keyBufferSize, keyBlocksUsed, innodbBufferPoolTotal, innodbBufferPoolUsed)
}

// calculateDerivedMetrics calculates and records derived metrics.
func (s *CoreScraper) calculateDerivedMetrics(
	now pcommon.Timestamp,
	maxConnections, threadsConnected, keyBufferSize, keyBlocksUsed, innodbBufferPoolTotal, innodbBufferPoolUsed int64,
) {
	// max_connections_available = max_connections - threads_connected
	if maxConnections > 0 {
		available := maxConnections - threadsConnected
		if available < 0 {
			available = 0
		}
		s.mb.RecordNewrelicmysqlNetMaxConnectionsAvailableDataPoint(now, available)
	}

	// key_cache_utilization = (key_blocks_used * key_cache_block_size) / key_buffer_size * 100
	if keyBufferSize > 0 && keyBlocksUsed > 0 {
		utilization := float64(keyBlocksUsed*1024) / float64(keyBufferSize) * 100
		s.mb.RecordNewrelicmysqlPerformanceKeyCacheUtilizationDataPoint(now, utilization)
	}

	// innodb_buffer_pool_utilization = innodb_buffer_pool_used / innodb_buffer_pool_total * 100
	if innodbBufferPoolTotal > 0 {
		utilization := float64(innodbBufferPoolUsed) / float64(innodbBufferPoolTotal) * 100
		s.mb.RecordNewrelicmysqlInnodbBufferPoolUtilizationDataPoint(now, utilization)
	}
}
