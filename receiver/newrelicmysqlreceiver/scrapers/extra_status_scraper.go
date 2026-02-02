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

type ExtraStatusScraper struct {
	client common.Client
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

func NewExtraStatusScraper(client common.Client, mb *metadata.MetricsBuilder, logger *zap.Logger) (*ExtraStatusScraper, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &ExtraStatusScraper{
		client: client,
		mb:     mb,
		logger: logger,
	}, nil
}

func (s *ExtraStatusScraper) ScrapeMetrics(_ context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL extra status metrics")

	globalStats, err := s.client.GetGlobalStats()
	if err != nil {
		s.logger.Error("Failed to fetch global stats for extra status metrics", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	if val, ok := globalStats["Binlog_cache_disk_use"]; ok {
		s.mb.RecordNewrelicmysqlBinlogCacheDiskUseDataPoint(now, parseInt64(val, s.logger, "Binlog_cache_disk_use"))
	}

	if val, ok := globalStats["Binlog_cache_use"]; ok {
		s.mb.RecordNewrelicmysqlBinlogCacheUseDataPoint(now, parseInt64(val, s.logger, "Binlog_cache_use"))
	}

	if val, ok := globalStats["Handler_commit"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerCommitDataPoint(now, parseInt64(val, s.logger, "Handler_commit"))
	}

	if val, ok := globalStats["Handler_delete"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerDeleteDataPoint(now, parseInt64(val, s.logger, "Handler_delete"))
	}

	if val, ok := globalStats["Handler_prepare"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerPrepareDataPoint(now, parseInt64(val, s.logger, "Handler_prepare"))
	}

	if val, ok := globalStats["Handler_read_first"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerReadFirstDataPoint(now, parseInt64(val, s.logger, "Handler_read_first"))
	}

	if val, ok := globalStats["Handler_read_key"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerReadKeyDataPoint(now, parseInt64(val, s.logger, "Handler_read_key"))
	}

	if val, ok := globalStats["Handler_read_next"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerReadNextDataPoint(now, parseInt64(val, s.logger, "Handler_read_next"))
	}

	if val, ok := globalStats["Handler_read_prev"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerReadPrevDataPoint(now, parseInt64(val, s.logger, "Handler_read_prev"))
	}

	if val, ok := globalStats["Handler_read_rnd"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerReadRndDataPoint(now, parseInt64(val, s.logger, "Handler_read_rnd"))
	}

	if val, ok := globalStats["Handler_read_rnd_next"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerReadRndNextDataPoint(now, parseInt64(val, s.logger, "Handler_read_rnd_next"))
	}

	if val, ok := globalStats["Handler_rollback"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerRollbackDataPoint(now, parseInt64(val, s.logger, "Handler_rollback"))
	}

	if val, ok := globalStats["Handler_update"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerUpdateDataPoint(now, parseInt64(val, s.logger, "Handler_update"))
	}

	if val, ok := globalStats["Handler_write"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceHandlerWriteDataPoint(now, parseInt64(val, s.logger, "Handler_write"))
	}

	if val, ok := globalStats["Opened_tables"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceOpenedTablesDataPoint(now, parseInt64(val, s.logger, "Opened_tables"))
	}

	if val, ok := globalStats["Qcache_total_blocks"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceQcacheTotalBlocksDataPoint(now, parseInt64(val, s.logger, "Qcache_total_blocks"))
	}

	if val, ok := globalStats["Qcache_free_blocks"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceQcacheFreeBlocksDataPoint(now, parseInt64(val, s.logger, "Qcache_free_blocks"))
	}

	if val, ok := globalStats["Qcache_free_memory"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceQcacheFreeMemoryDataPoint(now, parseInt64(val, s.logger, "Qcache_free_memory"))
	}

	if val, ok := globalStats["Qcache_not_cached"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceQcacheNotCachedDataPoint(now, parseInt64(val, s.logger, "Qcache_not_cached"))
	}

	if val, ok := globalStats["Qcache_queries_in_cache"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceQcacheQueriesInCacheDataPoint(now, parseInt64(val, s.logger, "Qcache_queries_in_cache"))
	}

	if val, ok := globalStats["Select_full_join"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSelectFullJoinDataPoint(now, parseInt64(val, s.logger, "Select_full_join"))
	}

	if val, ok := globalStats["Select_full_range_join"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSelectFullRangeJoinDataPoint(now, parseInt64(val, s.logger, "Select_full_range_join"))
	}

	if val, ok := globalStats["Select_range"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSelectRangeDataPoint(now, parseInt64(val, s.logger, "Select_range"))
	}

	if val, ok := globalStats["Select_range_check"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSelectRangeCheckDataPoint(now, parseInt64(val, s.logger, "Select_range_check"))
	}

	if val, ok := globalStats["Select_scan"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSelectScanDataPoint(now, parseInt64(val, s.logger, "Select_scan"))
	}

	if val, ok := globalStats["Sort_merge_passes"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSortMergePassesDataPoint(now, parseInt64(val, s.logger, "Sort_merge_passes"))
	}

	if val, ok := globalStats["Sort_range"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSortRangeDataPoint(now, parseInt64(val, s.logger, "Sort_range"))
	}

	if val, ok := globalStats["Sort_rows"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSortRowsDataPoint(now, parseInt64(val, s.logger, "Sort_rows"))
	}

	if val, ok := globalStats["Sort_scan"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceSortScanDataPoint(now, parseInt64(val, s.logger, "Sort_scan"))
	}

	if val, ok := globalStats["Table_locks_immediate"]; ok {
		intVal := parseInt64(val, s.logger, "Table_locks_immediate")
		s.mb.RecordNewrelicmysqlPerformanceTableLocksImmediateDataPoint(now, intVal)
		s.mb.RecordNewrelicmysqlPerformanceTableLocksImmediateRateDataPoint(now, intVal)
	}

	if val, ok := globalStats["Threads_cached"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceThreadsCachedDataPoint(now, parseInt64(val, s.logger, "Threads_cached"))
	}

	if val, ok := globalStats["Threads_created"]; ok {
		s.mb.RecordNewrelicmysqlPerformanceThreadsCreatedDataPoint(now, parseInt64(val, s.logger, "Threads_created"))
	}

	s.logger.Debug("Completed scraping MySQL extra status metrics",
		zap.Int("metrics_collected", len(globalStats)))
}
