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

	for key, val := range globalStats {
		switch key {
		case "Binlog_cache_disk_use":
			s.mb.RecordNewrelicmysqlBinlogCacheDiskUseDataPoint(now, parseInt64(val, s.logger, key))
		case "Binlog_cache_use":
			s.mb.RecordNewrelicmysqlBinlogCacheUseDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_commit":
			s.mb.RecordNewrelicmysqlPerformanceHandlerCommitDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_delete":
			s.mb.RecordNewrelicmysqlPerformanceHandlerDeleteDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_prepare":
			s.mb.RecordNewrelicmysqlPerformanceHandlerPrepareDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_read_first":
			s.mb.RecordNewrelicmysqlPerformanceHandlerReadFirstDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_read_key":
			s.mb.RecordNewrelicmysqlPerformanceHandlerReadKeyDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_read_next":
			s.mb.RecordNewrelicmysqlPerformanceHandlerReadNextDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_read_prev":
			s.mb.RecordNewrelicmysqlPerformanceHandlerReadPrevDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_read_rnd":
			s.mb.RecordNewrelicmysqlPerformanceHandlerReadRndDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_read_rnd_next":
			s.mb.RecordNewrelicmysqlPerformanceHandlerReadRndNextDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_rollback":
			s.mb.RecordNewrelicmysqlPerformanceHandlerRollbackDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_update":
			s.mb.RecordNewrelicmysqlPerformanceHandlerUpdateDataPoint(now, parseInt64(val, s.logger, key))
		case "Handler_write":
			s.mb.RecordNewrelicmysqlPerformanceHandlerWriteDataPoint(now, parseInt64(val, s.logger, key))
		case "Opened_tables":
			s.mb.RecordNewrelicmysqlPerformanceOpenedTablesDataPoint(now, parseInt64(val, s.logger, key))
		case "Qcache_total_blocks":
			s.mb.RecordNewrelicmysqlPerformanceQcacheTotalBlocksDataPoint(now, parseInt64(val, s.logger, key))
		case "Qcache_free_blocks":
			s.mb.RecordNewrelicmysqlPerformanceQcacheFreeBlocksDataPoint(now, parseInt64(val, s.logger, key))
		case "Qcache_free_memory":
			s.mb.RecordNewrelicmysqlPerformanceQcacheFreeMemoryDataPoint(now, parseInt64(val, s.logger, key))
		case "Qcache_not_cached":
			s.mb.RecordNewrelicmysqlPerformanceQcacheNotCachedDataPoint(now, parseInt64(val, s.logger, key))
		case "Qcache_queries_in_cache":
			s.mb.RecordNewrelicmysqlPerformanceQcacheQueriesInCacheDataPoint(now, parseInt64(val, s.logger, key))
		case "Select_full_join":
			s.mb.RecordNewrelicmysqlPerformanceSelectFullJoinDataPoint(now, parseInt64(val, s.logger, key))
		case "Select_full_range_join":
			s.mb.RecordNewrelicmysqlPerformanceSelectFullRangeJoinDataPoint(now, parseInt64(val, s.logger, key))
		case "Select_range":
			s.mb.RecordNewrelicmysqlPerformanceSelectRangeDataPoint(now, parseInt64(val, s.logger, key))
		case "Select_range_check":
			s.mb.RecordNewrelicmysqlPerformanceSelectRangeCheckDataPoint(now, parseInt64(val, s.logger, key))
		case "Select_scan":
			s.mb.RecordNewrelicmysqlPerformanceSelectScanDataPoint(now, parseInt64(val, s.logger, key))
		case "Sort_merge_passes":
			s.mb.RecordNewrelicmysqlPerformanceSortMergePassesDataPoint(now, parseInt64(val, s.logger, key))
		case "Sort_range":
			s.mb.RecordNewrelicmysqlPerformanceSortRangeDataPoint(now, parseInt64(val, s.logger, key))
		case "Sort_rows":
			s.mb.RecordNewrelicmysqlPerformanceSortRowsDataPoint(now, parseInt64(val, s.logger, key))
		case "Sort_scan":
			s.mb.RecordNewrelicmysqlPerformanceSortScanDataPoint(now, parseInt64(val, s.logger, key))
		case "Table_locks_immediate":
			intVal := parseInt64(val, s.logger, key)
			// record gauge value
			s.mb.RecordNewrelicmysqlPerformanceTableLocksImmediateDataPoint(now, intVal)
			// record cumulative value
			s.mb.RecordNewrelicmysqlPerformanceTableLocksImmediateRateDataPoint(now, intVal)
		case "Threads_cached":
			s.mb.RecordNewrelicmysqlPerformanceThreadsCachedDataPoint(now, parseInt64(val, s.logger, key))
		case "Threads_created":
			s.mb.RecordNewrelicmysqlPerformanceThreadsCreatedDataPoint(now, parseInt64(val, s.logger, key))
		}
	}

	s.logger.Debug("Completed scraping MySQL extra status metrics",
		zap.Int("metrics_collected", len(globalStats)))
}
