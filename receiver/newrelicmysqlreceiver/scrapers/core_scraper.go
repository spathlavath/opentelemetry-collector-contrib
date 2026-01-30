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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

type CoreScraper struct {
	client common.Client
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

// NewCoreScraper creates a new core metrics scraper.
func NewCoreScraper(c common.Client, mb *metadata.MetricsBuilder, logger *zap.Logger) (*CoreScraper, error) {
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

// parseInt64 parses a string to int64, returning 0 and logging an error if parsing fails.
func parseInt64(s string, logger *zap.Logger, field string) int64 {
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		logger.Debug("Failed to parse int64", zap.String("field", field), zap.String("value", s), zap.Error(err))
		return 0
	}
	return val
}

// ScrapeMetrics collects MySQL core database metrics.
func (s *CoreScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL core metrics")

	// Fetch and log MySQL version information
	version, err := s.client.GetVersion()
	if err != nil {
		s.logger.Error("Failed to fetch version", zap.Error(err))
		errs.AddPartial(1, err)
	} else {
		// Log version info (string metrics not supported in OpenTelemetry)
		s.logger.Info("MySQL version detected", zap.String("version", version))
	}

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
		val := parseInt64(v, s.logger, k)
		switch k {
		// Original newrelicmysql metrics
		case "Connections":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlConnectionCountDataPoint(now, v))
			s.mb.RecordNewrelicmysqlNetConnectionsDataPoint(now, val)
		case "Queries":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlQueryCountDataPoint(now, v))
		case "Uptime":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlUptimeDataPoint(now, v))

		// Commands
		case "Com_commit":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandCommit))
		case "Com_delete":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandDelete))
		case "Com_delete_multi":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandDeleteMulti))
		case "Com_insert":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandInsert))
		case "Com_insert_select":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandInsertSelect))
		case "Com_load":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandLoad))
		case "Com_replace":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandReplace))
		case "Com_replace_select":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandReplaceSelect))
		case "Com_rollback":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandRollback))
		case "Com_select":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandSelect))
		case "Com_update":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandUpdate))
		case "Com_update_multi":
			common.AddPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandUpdateMulti))

		// Performance metrics
		case "Handler_rollback":
			s.mb.RecordNewrelicmysqlDbHandlerRollbackDataPoint(now, val)
		case "Opened_tables":
			s.mb.RecordNewrelicmysqlDbOpenedTablesDataPoint(now, val)
		case "Questions":
			s.mb.RecordNewrelicmysqlPerformanceQuestionsDataPoint(now, val)
		case "Slow_queries":
			s.mb.RecordNewrelicmysqlPerformanceSlowQueriesDataPoint(now, val)
		case "Threads_connected":
			threadsConnected = val
			s.mb.RecordNewrelicmysqlPerformanceThreadsConnectedDataPoint(now, val)
		case "Threads_running":
			s.mb.RecordNewrelicmysqlPerformanceThreadsRunningDataPoint(now, val)
		case "Qcache_hits":
			s.mb.RecordNewrelicmysqlPerformanceQcacheHitsDataPoint(now, val)
		case "Qcache_inserts":
			s.mb.RecordNewrelicmysqlPerformanceQcacheInsertsDataPoint(now, val)
		case "Qcache_lowmem_prunes":
			s.mb.RecordNewrelicmysqlPerformanceQcacheLowmemPrunesDataPoint(now, val)
		case "Qcache_not_cached":
			s.mb.RecordNewrelicmysqlPerformanceQcacheNotCachedDataPoint(now, val)
		case "Qcache_free_memory":
			s.mb.RecordNewrelicmysqlPerformanceQcacheSizeDataPoint(now, val)
		case "Open_files":
			s.mb.RecordNewrelicmysqlPerformanceOpenFilesDataPoint(now, val)
		case "Open_tables":
			s.mb.RecordNewrelicmysqlPerformanceOpenTablesDataPoint(now, val)
		case "Table_locks_waited":
			s.mb.RecordNewrelicmysqlPerformanceTableLocksWaitedDataPoint(now, val)
		case "Table_open_cache":
			s.mb.RecordNewrelicmysqlPerformanceTableOpenCacheDataPoint(now, val)
		case "Created_tmp_disk_tables":
			s.mb.RecordNewrelicmysqlPerformanceCreatedTmpDiskTablesDataPoint(now, val)
		case "Created_tmp_files":
			s.mb.RecordNewrelicmysqlPerformanceCreatedTmpFilesDataPoint(now, val)
		case "Created_tmp_tables":
			s.mb.RecordNewrelicmysqlPerformanceCreatedTmpTablesDataPoint(now, val)
		case "Bytes_received":
			s.mb.RecordNewrelicmysqlPerformanceBytesReceivedDataPoint(now, val)
		case "Bytes_sent":
			s.mb.RecordNewrelicmysqlPerformanceBytesSentDataPoint(now, val)
		case "max_prepared_stmt_count":
			s.mb.RecordNewrelicmysqlPerformanceMaxPreparedStmtCountDataPoint(now, val)
		case "Prepared_stmt_count":
			s.mb.RecordNewrelicmysqlPerformancePreparedStmtCountDataPoint(now, val)
		case "Performance_schema_digest_lost":
			s.mb.RecordNewrelicmysqlPerformancePerformanceSchemaDigestLostDataPoint(now, val)
		case "thread_cache_size":
			s.mb.RecordNewrelicmysqlPerformanceThreadCacheSizeDataPoint(now, val)

		// Network metrics
		case "Aborted_clients":
			s.mb.RecordNewrelicmysqlNetAbortedClientsDataPoint(now, val)
		case "Aborted_connects":
			s.mb.RecordNewrelicmysqlNetAbortedConnectsDataPoint(now, val)
		case "Max_used_connections":
			s.mb.RecordNewrelicmysqlNetMaxUsedConnectionsDataPoint(now, val)
		case "max_connections":
			maxConnections = val
			s.mb.RecordNewrelicmysqlNetMaxConnectionsDataPoint(now, val)

		// MyISAM metrics
		case "Key_blocks_not_flushed":
			s.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUnflushedDataPoint(now, val*1024)
		case "Key_blocks_used":
			keyBlocksUsed = val
			s.mb.RecordNewrelicmysqlMyisamKeyBufferBytesUsedDataPoint(now, val*1024)
		case "key_buffer_size":
			keyBufferSize = val
			s.mb.RecordNewrelicmysqlMyisamKeyBufferSizeDataPoint(now, val)
		case "Key_read_requests":
			s.mb.RecordNewrelicmysqlMyisamKeyReadRequestsDataPoint(now, val)
		case "Key_reads":
			s.mb.RecordNewrelicmysqlMyisamKeyReadsDataPoint(now, val)
		case "Key_write_requests":
			s.mb.RecordNewrelicmysqlMyisamKeyWriteRequestsDataPoint(now, val)
		case "Key_writes":
			s.mb.RecordNewrelicmysqlMyisamKeyWritesDataPoint(now, val)

		// InnoDB buffer pool metrics
		case "Innodb_buffer_pool_pages_dirty":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolDirtyDataPoint(now, val)
		case "Innodb_buffer_pool_pages_free":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolFreeDataPoint(now, val)
		case "Innodb_buffer_pool_read_requests":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadRequestsDataPoint(now, val)
		case "Innodb_buffer_pool_reads":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadsDataPoint(now, val)
		case "Innodb_buffer_pool_pages_total":
			innodbBufferPoolTotal = val
			s.mb.RecordNewrelicmysqlInnodbBufferPoolTotalDataPoint(now, val)
		case "Innodb_buffer_pool_pages_data":
			innodbBufferPoolUsed = val
			s.mb.RecordNewrelicmysqlInnodbBufferPoolUsedDataPoint(now, val)

		// InnoDB row lock metrics
		case "Innodb_row_lock_current_waits":
			s.mb.RecordNewrelicmysqlInnodbRowLockCurrentWaitsDataPoint(now, val)
		case "Innodb_row_lock_time":
			s.mb.RecordNewrelicmysqlInnodbRowLockTimeDataPoint(now, val)
		case "Innodb_row_lock_waits":
			s.mb.RecordNewrelicmysqlInnodbRowLockWaitsDataPoint(now, val)

		// InnoDB data metrics
		case "Innodb_data_reads":
			s.mb.RecordNewrelicmysqlInnodbDataReadsDataPoint(now, val)
		case "Innodb_data_writes":
			s.mb.RecordNewrelicmysqlInnodbDataWritesDataPoint(now, val)
		case "Innodb_data_written":
			s.mb.RecordNewrelicmysqlInnodbDataWrittenDataPoint(now, val)
		case "Innodb_log_waits":
			s.mb.RecordNewrelicmysqlInnodbLogWaitsDataPoint(now, val)

		// InnoDB mutex metrics
		case "Innodb_mutex_os_waits":
			s.mb.RecordNewrelicmysqlInnodbMutexOsWaitsDataPoint(now, val)
		case "Innodb_mutex_spin_rounds":
			s.mb.RecordNewrelicmysqlInnodbMutexSpinRoundsDataPoint(now, val)
		case "Innodb_mutex_spin_waits":
			s.mb.RecordNewrelicmysqlInnodbMutexSpinWaitsDataPoint(now, val)
		case "Innodb_os_log_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbOsLogFsyncsDataPoint(now, val)

		// InnoDB current row locks (deprecated in MySQL 5.7+, but kept for compatibility)
		case "Innodb_current_row_locks":
			s.mb.RecordNewrelicmysqlInnodbCurrentRowLocksDataPoint(now, val)
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
