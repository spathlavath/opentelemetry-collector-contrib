// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// InnoDBExtendedScraper handles optional InnoDB extended metrics collection (125 metrics).
// These metrics provide comprehensive insights into InnoDB internals including:
// active transactions, adaptive hash index, buffer pool operations, change buffer, doublewrite buffer,
// data/log operations, LSN tracking, master thread, memory subsystems, mutex/locks, pending operations,
// purge operations, queries, row operations, semaphores, and undo tablespaces.
// This scraper is only initialized when extra_innodb_metrics flag is enabled.
type InnoDBExtendedScraper struct {
	client common.Client
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

// NewInnoDBExtendedScraper creates a new InnoDB extended metrics scraper.
func NewInnoDBExtendedScraper(c common.Client, mb *metadata.MetricsBuilder, logger *zap.Logger) (*InnoDBExtendedScraper, error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &InnoDBExtendedScraper{
		client: c,
		mb:     mb,
		logger: logger,
	}, nil
}

// ScrapeMetrics collects MySQL InnoDB extended metrics.
func (s *InnoDBExtendedScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL InnoDB extended metrics")

	globalStats, err := s.client.GetGlobalStats()
	if err != nil {
		s.logger.Error("Failed to fetch global stats for InnoDB extended metrics", zap.Error(err))
		errs.AddPartial(125, err)
		return
	}

	// Process InnoDB extended metrics
	for k, v := range globalStats {
		val := parseInt64(v, s.logger, k)

		switch k {
		// Active Transactions
		case "Innodb_active_transactions":
			s.mb.RecordNewrelicmysqlInnodbActiveTransactionsDataPoint(now, val)

		// Adaptive Hash Index
		case "Innodb_adaptive_hash_hash_searches":
			s.mb.RecordNewrelicmysqlInnodbAdaptiveHashHashSearchesDataPoint(now, val)
		case "Innodb_adaptive_hash_non_hash_searches":
			s.mb.RecordNewrelicmysqlInnodbAdaptiveHashNonHashSearchesDataPoint(now, val)
		case "Innodb_adaptive_hash_pages_added":
			s.mb.RecordNewrelicmysqlInnodbAdaptiveHashPagesAddedDataPoint(now, val)
		case "Innodb_adaptive_hash_pages_removed":
			s.mb.RecordNewrelicmysqlInnodbAdaptiveHashPagesRemovedDataPoint(now, val)

		// Undo Logs
		case "Innodb_available_undo_logs":
			s.mb.RecordNewrelicmysqlInnodbAvailableUndoLogsDataPoint(now, val)

		// Buffer Pool Extended metrics
		case "Innodb_buffer_pool_bytes_data":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolBytesDataDataPoint(now, val)
		case "Innodb_buffer_pool_bytes_dirty":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolBytesDirtyDataPoint(now, val)
		case "Innodb_buffer_pool_dirty":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolDirtyDataPoint(now, val)
		case "Innodb_buffer_pool_free":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolFreeDataPoint(now, val)
		case "Innodb_buffer_pool_pages_data":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesDataDataPoint(now, val)
		case "Innodb_buffer_pool_pages_free":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesFreeDataPoint(now, val)
		case "Innodb_buffer_pool_pages_flushed":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesFlushedDataPoint(now, val)
		case "Innodb_buffer_pool_pages_LRU_flushed":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesLruFlushedDataPoint(now, val)
		case "Innodb_buffer_pool_pages_made_not_young":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesMadeNotYoungDataPoint(now, val)
		case "Innodb_buffer_pool_pages_made_young":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesMadeYoungDataPoint(now, val)
		case "Innodb_buffer_pool_pages_total":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesTotalDataPoint(now, val)
		case "Innodb_buffer_pool_pages_misc":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesMiscDataPoint(now, val)
		case "Innodb_buffer_pool_pages_old":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolPagesOldDataPoint(now, val)
		case "Innodb_buffer_pool_read_ahead":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadAheadDataPoint(now, val)
		case "Innodb_buffer_pool_read_ahead_evicted":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadAheadEvictedDataPoint(now, val)
		case "Innodb_buffer_pool_read_ahead_rnd":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadAheadRndDataPoint(now, val)
		case "Innodb_buffer_pool_read_requests":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadRequestsDataPoint(now, val)
		case "Innodb_buffer_pool_reads":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolReadsDataPoint(now, val)
		case "Innodb_buffer_pool_total":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolTotalDataPoint(now, val)
		case "Innodb_buffer_pool_used":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolUsedDataPoint(now, val)
		case "Innodb_buffer_pool_utilization":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolUtilizationDataPoint(now, float64(val))
		case "Innodb_buffer_pool_wait_free":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolWaitFreeDataPoint(now, val)
		case "Innodb_buffer_pool_write_requests":
			s.mb.RecordNewrelicmysqlInnodbBufferPoolWriteRequestsDataPoint(now, val)

		// Checkpoint
		case "Innodb_checkpoint_age":
			s.mb.RecordNewrelicmysqlInnodbCheckpointAgeDataPoint(now, val)

		// Current Row Locks
		case "Innodb_current_row_locks":
			s.mb.RecordNewrelicmysqlInnodbCurrentRowLocksDataPoint(now, val)

		// Current Transactions
		case "Innodb_current_transactions":
			s.mb.RecordNewrelicmysqlInnodbCurrentTransactionsDataPoint(now, val)

		// Data Operations
		case "Innodb_data_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbDataFsyncsDataPoint(now, val)
		case "Innodb_data_pending_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbDataPendingFsyncsDataPoint(now, val)
		case "Innodb_data_pending_reads":
			s.mb.RecordNewrelicmysqlInnodbDataPendingReadsDataPoint(now, val)
		case "Innodb_data_pending_writes":
			s.mb.RecordNewrelicmysqlInnodbDataPendingWritesDataPoint(now, val)
		case "Innodb_data_read":
			s.mb.RecordNewrelicmysqlInnodbDataReadDataPoint(now, val)
		case "Innodb_data_reads":
			s.mb.RecordNewrelicmysqlInnodbDataReadsDataPoint(now, val)
		case "Innodb_data_writes":
			s.mb.RecordNewrelicmysqlInnodbDataWritesDataPoint(now, val)
		case "Innodb_data_written":
			s.mb.RecordNewrelicmysqlInnodbDataWrittenDataPoint(now, val)

		// Doublewrite Buffer
		case "Innodb_dblwr_pages_written":
			s.mb.RecordNewrelicmysqlInnodbDblwrPagesWrittenDataPoint(now, val)
		case "Innodb_dblwr_writes":
			s.mb.RecordNewrelicmysqlInnodbDblwrWritesDataPoint(now, val)

		// Hash Index Cells
		case "Innodb_hash_index_cells_total":
			s.mb.RecordNewrelicmysqlInnodbHashIndexCellsTotalDataPoint(now, val)
		case "Innodb_hash_index_cells_used":
			s.mb.RecordNewrelicmysqlInnodbHashIndexCellsUsedDataPoint(now, val)

		// History List
		case "Innodb_history_list_length":
			s.mb.RecordNewrelicmysqlInnodbHistoryListLengthDataPoint(now, val)

		// Change Buffer (Insert Buffer)
		case "Innodb_ibuf_free_list":
			s.mb.RecordNewrelicmysqlInnodbIbufFreeListDataPoint(now, val)
		case "Innodb_ibuf_merged_delete_marks":
			s.mb.RecordNewrelicmysqlInnodbIbufMergedDeleteMarksDataPoint(now, val)
		case "Innodb_ibuf_merged_deletes":
			s.mb.RecordNewrelicmysqlInnodbIbufMergedDeletesDataPoint(now, val)
		case "Innodb_ibuf_merged_inserts":
			s.mb.RecordNewrelicmysqlInnodbIbufMergedInsertsDataPoint(now, val)
		case "Innodb_ibuf_merges":
			s.mb.RecordNewrelicmysqlInnodbIbufMergesDataPoint(now, val)
		case "Innodb_ibuf_segment_size":
			s.mb.RecordNewrelicmysqlInnodbIbufSegmentSizeDataPoint(now, val)
		case "Innodb_ibuf_size":
			s.mb.RecordNewrelicmysqlInnodbIbufSizeDataPoint(now, val)

		// Lock Structs
		case "Innodb_lock_structs":
			s.mb.RecordNewrelicmysqlInnodbLockStructsDataPoint(now, val)

		// Locked Tables
		case "Innodb_locked_tables":
			s.mb.RecordNewrelicmysqlInnodbLockedTablesDataPoint(now, val)

		// Locked Transactions
		case "Innodb_locked_transactions":
			s.mb.RecordNewrelicmysqlInnodbLockedTransactionsDataPoint(now, val)

		// Log Operations
		case "Innodb_log_waits":
			s.mb.RecordNewrelicmysqlInnodbLogWaitsDataPoint(now, val)
		case "Innodb_log_write_requests":
			s.mb.RecordNewrelicmysqlInnodbLogWriteRequestsDataPoint(now, val)
		case "Innodb_log_writes":
			s.mb.RecordNewrelicmysqlInnodbLogWritesDataPoint(now, val)

		// LSN (Log Sequence Number)
		case "Innodb_lsn_current":
			s.mb.RecordNewrelicmysqlInnodbLsnCurrentDataPoint(now, val)
		case "Innodb_lsn_flushed":
			s.mb.RecordNewrelicmysqlInnodbLsnFlushedDataPoint(now, val)
		case "Innodb_lsn_last_checkpoint":
			s.mb.RecordNewrelicmysqlInnodbLsnLastCheckpointDataPoint(now, val)

		// Master Thread
		case "Innodb_master_thread_active_loops":
			s.mb.RecordNewrelicmysqlInnodbMasterThreadActiveLoopsDataPoint(now, val)
		case "Innodb_master_thread_idle_loops":
			s.mb.RecordNewrelicmysqlInnodbMasterThreadIdleLoopsDataPoint(now, val)

		// Memory
		case "Innodb_mem_adaptive_hash":
			s.mb.RecordNewrelicmysqlInnodbMemAdaptiveHashDataPoint(now, val)
		case "Innodb_mem_additional_pool":
			s.mb.RecordNewrelicmysqlInnodbMemAdditionalPoolDataPoint(now, val)
		case "Innodb_mem_dictionary":
			s.mb.RecordNewrelicmysqlInnodbMemDictionaryDataPoint(now, val)
		case "Innodb_mem_file_system":
			s.mb.RecordNewrelicmysqlInnodbMemFileSystemDataPoint(now, val)
		case "Innodb_mem_lock_system":
			s.mb.RecordNewrelicmysqlInnodbMemLockSystemDataPoint(now, val)
		case "Innodb_mem_page_hash":
			s.mb.RecordNewrelicmysqlInnodbMemPageHashDataPoint(now, val)
		case "Innodb_mem_recovery_system":
			s.mb.RecordNewrelicmysqlInnodbMemRecoverySystemDataPoint(now, val)
		case "Innodb_mem_thread_hash":
			s.mb.RecordNewrelicmysqlInnodbMemThreadHashDataPoint(now, val)
		case "Innodb_mem_total":
			s.mb.RecordNewrelicmysqlInnodbMemTotalDataPoint(now, val)

		// Mutex Operations
		case "Innodb_mutex_os_waits":
			s.mb.RecordNewrelicmysqlInnodbMutexOsWaitsDataPoint(now, val)
		case "Innodb_mutex_spin_rounds":
			s.mb.RecordNewrelicmysqlInnodbMutexSpinRoundsDataPoint(now, val)
		case "Innodb_mutex_spin_waits":
			s.mb.RecordNewrelicmysqlInnodbMutexSpinWaitsDataPoint(now, val)

		// Open Files
		case "Innodb_num_open_files":
			s.mb.RecordNewrelicmysqlInnodbNumOpenFilesDataPoint(now, val)

		// I/O Operations
		case "Innodb_os_file_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbOsFileFsyncsDataPoint(now, val)
		case "Innodb_os_file_reads":
			s.mb.RecordNewrelicmysqlInnodbOsFileReadsDataPoint(now, val)
		case "Innodb_os_file_writes":
			s.mb.RecordNewrelicmysqlInnodbOsFileWritesDataPoint(now, val)

		// OS Log Operations
		case "Innodb_os_log_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbOsLogFsyncsDataPoint(now, val)
		case "Innodb_os_log_pending_fsyncs":
			s.mb.RecordNewrelicmysqlInnodbOsLogPendingFsyncsDataPoint(now, val)
		case "Innodb_os_log_pending_writes":
			s.mb.RecordNewrelicmysqlInnodbOsLogPendingWritesDataPoint(now, val)
		case "Innodb_os_log_written":
			s.mb.RecordNewrelicmysqlInnodbOsLogWrittenDataPoint(now, val)

		// Page Operations
		case "Innodb_page_size":
			s.mb.RecordNewrelicmysqlInnodbPageSizeDataPoint(now, val)
		case "Innodb_pages_created":
			s.mb.RecordNewrelicmysqlInnodbPagesCreatedDataPoint(now, val)
		case "Innodb_pages_read":
			s.mb.RecordNewrelicmysqlInnodbPagesReadDataPoint(now, val)
		case "Innodb_pages_written":
			s.mb.RecordNewrelicmysqlInnodbPagesWrittenDataPoint(now, val)

		// Pending Operations
		case "Innodb_pending_aio_log_ios":
			s.mb.RecordNewrelicmysqlInnodbPendingAioLogIosDataPoint(now, val)
		case "Innodb_pending_aio_sync_ios":
			s.mb.RecordNewrelicmysqlInnodbPendingAioSyncIosDataPoint(now, val)
		case "Innodb_pending_buffer_pool_flushes":
			s.mb.RecordNewrelicmysqlInnodbPendingBufferPoolFlushesDataPoint(now, val)
		case "Innodb_pending_checkpoint_writes":
			s.mb.RecordNewrelicmysqlInnodbPendingCheckpointWritesDataPoint(now, val)
		case "Innodb_pending_ibuf_aio_reads":
			s.mb.RecordNewrelicmysqlInnodbPendingIbufAioReadsDataPoint(now, val)
		case "Innodb_pending_log_flushes":
			s.mb.RecordNewrelicmysqlInnodbPendingLogFlushesDataPoint(now, val)
		case "Innodb_pending_log_writes":
			s.mb.RecordNewrelicmysqlInnodbPendingLogWritesDataPoint(now, val)
		case "Innodb_pending_normal_aio_reads":
			s.mb.RecordNewrelicmysqlInnodbPendingNormalAioReadsDataPoint(now, val)
		case "Innodb_pending_normal_aio_writes":
			s.mb.RecordNewrelicmysqlInnodbPendingNormalAioWritesDataPoint(now, val)

		// Purge Operations
		case "Innodb_purge_trx_id":
			s.mb.RecordNewrelicmysqlInnodbPurgeTrxIDDataPoint(now, val)
		case "Innodb_purge_undo_no":
			s.mb.RecordNewrelicmysqlInnodbPurgeUndoNoDataPoint(now, val)

		// Queries
		case "Innodb_queries_inside":
			s.mb.RecordNewrelicmysqlInnodbQueriesInsideDataPoint(now, val)
		case "Innodb_queries_queued":
			s.mb.RecordNewrelicmysqlInnodbQueriesQueuedDataPoint(now, val)

		// Read Views
		case "Innodb_read_views":
			s.mb.RecordNewrelicmysqlInnodbReadViewsDataPoint(now, val)

		// Redo Log
		case "Innodb_redo_log_enabled":
			s.mb.RecordNewrelicmysqlInnodbRedoLogEnabledDataPoint(now, val)

		// Row Lock Extended
		case "Innodb_row_lock_current_waits":
			s.mb.RecordNewrelicmysqlInnodbRowLockCurrentWaitsDataPoint(now, val)
		case "Innodb_row_lock_time":
			s.mb.RecordNewrelicmysqlInnodbRowLockTimeDataPoint(now, val)
		case "Innodb_row_lock_time_avg":
			s.mb.RecordNewrelicmysqlInnodbRowLockTimeAvgDataPoint(now, val)
		case "Innodb_row_lock_time_max":
			s.mb.RecordNewrelicmysqlInnodbRowLockTimeMaxDataPoint(now, val)
		case "Innodb_row_lock_waits":
			s.mb.RecordNewrelicmysqlInnodbRowLockWaitsDataPoint(now, val)

		// Row Operations
		case "Innodb_rows_inserted":
			s.mb.RecordNewrelicmysqlInnodbRowsInsertedDataPoint(now, val)
		case "Innodb_rows_deleted":
			s.mb.RecordNewrelicmysqlInnodbRowsDeletedDataPoint(now, val)
		case "Innodb_rows_updated":
			s.mb.RecordNewrelicmysqlInnodbRowsUpdatedDataPoint(now, val)
		case "Innodb_rows_read":
			s.mb.RecordNewrelicmysqlInnodbRowsReadDataPoint(now, val)

		// S-Lock/Semaphore
		case "Innodb_s_lock_os_waits":
			s.mb.RecordNewrelicmysqlInnodbSLockOsWaitsDataPoint(now, val)
		case "Innodb_s_lock_spin_rounds":
			s.mb.RecordNewrelicmysqlInnodbSLockSpinRoundsDataPoint(now, val)
		case "Innodb_s_lock_spin_waits":
			s.mb.RecordNewrelicmysqlInnodbSLockSpinWaitsDataPoint(now, val)

		// Semaphore Operations
		case "Innodb_semaphore_wait_time":
			s.mb.RecordNewrelicmysqlInnodbSemaphoreWaitTimeDataPoint(now, val)
		case "Innodb_semaphore_waits":
			s.mb.RecordNewrelicmysqlInnodbSemaphoreWaitsDataPoint(now, val)

		// System
		case "Innodb_tables_in_use":
			s.mb.RecordNewrelicmysqlInnodbTablesInUseDataPoint(now, val)
		case "Innodb_truncated_status_writes":
			s.mb.RecordNewrelicmysqlInnodbTruncatedStatusWritesDataPoint(now, val)

		// Undo Tablespaces
		case "Innodb_undo_tablespaces_active":
			s.mb.RecordNewrelicmysqlInnodbUndoTablespacesActiveDataPoint(now, val)
		case "Innodb_undo_tablespaces_explicit":
			s.mb.RecordNewrelicmysqlInnodbUndoTablespacesExplicitDataPoint(now, val)
		case "Innodb_undo_tablespaces_implicit":
			s.mb.RecordNewrelicmysqlInnodbUndoTablespacesImplicitDataPoint(now, val)
		case "Innodb_undo_tablespaces_total":
			s.mb.RecordNewrelicmysqlInnodbUndoTablespacesTotalDataPoint(now, val)

		// X-Lock Operations
		case "Innodb_x_lock_os_waits":
			s.mb.RecordNewrelicmysqlInnodbXLockOsWaitsDataPoint(now, val)
		case "Innodb_x_lock_spin_rounds":
			s.mb.RecordNewrelicmysqlInnodbXLockSpinRoundsDataPoint(now, val)
		case "Innodb_x_lock_spin_waits":
			s.mb.RecordNewrelicmysqlInnodbXLockSpinWaitsDataPoint(now, val)
		}
	}

	s.logger.Debug("Completed scraping InnoDB extended metrics")
}
