// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoracledbreceiver

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoracledbreceiver/internal/metadata"
)

// oracledbScraper scrapes Oracle database metrics
type oracledbScraper struct {
	dbClient  dbClient
	mb        *metadata.MetricsBuilder
	logger    *zap.Logger
	cfg       *Config
	startTime pcommon.Timestamp
}

// newScraper creates a new Oracle database scraper
func newScraper(cfg *Config, settings receiver.Settings) (*oracledbScraper, error) {
	mb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), settings)

	return &oracledbScraper{
		mb:        mb,
		logger:    settings.Logger,
		cfg:       cfg,
		startTime: pcommon.NewTimestampFromTime(time.Now()),
	}, nil
}

// Start initializes the scraper
func (s *oracledbScraper) Start(_ context.Context, _ component.Host) error {
	client, err := newDBClient(s.cfg, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create database client: %w", err)
	}

	s.dbClient = client
	return nil
}

// scrape collects Oracle database metrics
func (s *oracledbScraper) ScrapeMetrics(ctx context.Context) (pmetric.Metrics, error) {
	now := pcommon.NewTimestampFromTime(time.Now())
	var errs error

	// Scrape system statistics with basic metrics
	if err := s.scrapeBasicStats(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape basic stats: %w", err))
	}

	return s.mb.Emit(), errs
}

// scrapeBasicStats collects basic Oracle metrics
func (s *oracledbScraper) scrapeBasicStats(ctx context.Context, now pcommon.Timestamp) error {
	var errs error

	// Scrape core category metrics
	if err := s.scrapeCPUMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape CPU metrics: %w", err))
	}

	if err := s.scrapePhysicalIOMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape physical I/O metrics: %w", err))
	}

	if err := s.scrapeSessionMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape session metrics: %w", err))
	}

	if err := s.scrapeTablespaceMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape tablespace metrics: %w", err))
	}

	// Scrape additional requested metrics
	if err := s.scrapePGAMemoryMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape PGA memory metrics: %w", err))
	}

	if err := s.scrapeParseMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape parse metrics: %w", err))
	}

	if err := s.scrapeConnectionMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape connection metrics: %w", err))
	}

	// Scrape enhanced metrics for New Relic parity with verified functions
	if err := s.scrapeSGAMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape SGA metrics: %w", err))
	}

	if err := s.scrapeBufferCacheMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape buffer cache metrics: %w", err))
	}

	if err := s.scrapeParallelMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape parallel processing metrics: %w", err))
	}

	// Enhanced comprehensive metrics using available functions
	if err := s.scrapeSimpleMemoryMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape memory metrics: %w", err))
	}

	if err := s.scrapeSimplePhysicalIOMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape physical I/O metrics: %w", err))
	}

	if err := s.scrapeSimpleSystemMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape system metrics: %w", err))
	}

	// Additional enhanced metrics for comprehensive coverage
	if err := s.scrapeAdditionalSGAMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape additional SGA metrics: %w", err))
	}

	if err := s.scrapeAdditionalNetworkMetrics(ctx, now); err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to scrape additional network metrics: %w", err))
	}

	// Query basic system statistics for additional metrics
	query := "SELECT name, value FROM v$sysstat WHERE name IN ('execute count', 'user commits', 'user rollbacks')"
	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		errs = multierr.Append(errs, fmt.Errorf("failed to fetch basic stats: %w", err))
		return errs
	}

	for _, row := range rows {
		statName := row["NAME"]
		value := row["VALUE"]

		switch statName {
		case "execute count":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbExecutionsDataPoint(now, val)
			}
		case "user commits":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbUserCommitsDataPoint(now, val)
			}
		case "user rollbacks":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbUserRollbacksDataPoint(now, val)
			}
		}
	}

	return errs
}

// scrapeCPUMetrics collects CPU time and usage metrics
func (s *oracledbScraper) scrapeCPUMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT name, value 
		FROM v$sysstat 
		WHERE name IN ('CPU used by this session', 'CPU used when call started')
		UNION ALL
		SELECT 'cpu_time_total', ROUND(value/100, 2) 
		FROM v$sysstat 
		WHERE name = 'CPU used by this session'`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch CPU metrics: %w", err)
	}

	for _, row := range rows {
		statName := row["NAME"]
		value := row["VALUE"]

		switch statName {
		case "CPU used by this session", "cpu_time_total":
			if val, err := strconv.ParseFloat(value, 64); err == nil {
				s.mb.RecordOracledbCPUTimeDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapePhysicalIOMetrics collects physical reads and writes metrics
func (s *oracledbScraper) scrapePhysicalIOMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT name, value 
		FROM v$sysstat 
		WHERE name IN (
			'physical reads',
			'physical writes', 
			'physical read IO requests',
			'physical write IO requests',
			'physical reads direct',
			'physical writes direct',
			'session logical reads'
		)`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch physical I/O metrics: %w", err)
	}

	for _, row := range rows {
		statName := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch statName {
			case "physical reads":
				s.mb.RecordOracledbPhysicalReadsDataPoint(now, val)
			case "physical writes":
				s.mb.RecordOracledbPhysicalWritesDataPoint(now, val)
			case "physical read IO requests":
				s.mb.RecordOracledbPhysicalReadIoRequestsDataPoint(now, val)
			case "physical write IO requests":
				s.mb.RecordOracledbPhysicalWriteIoRequestsDataPoint(now, val)
			case "physical reads direct":
				s.mb.RecordOracledbPhysicalReadsDirectDataPoint(now, val)
			case "physical writes direct":
				s.mb.RecordOracledbPhysicalWritesDirectDataPoint(now, val)
			case "session logical reads":
				s.mb.RecordOracledbLogicalReadsDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeSessionMetrics collects session count and status metrics
func (s *oracledbScraper) scrapeSessionMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT 
			status as session_status,
			type as session_type,
			COUNT(*) as session_count
		FROM v$session 
		WHERE status IN ('ACTIVE', 'INACTIVE', 'KILLED', 'CACHED', 'SNIPED')
		  AND type IN ('USER', 'BACKGROUND', 'RECURSIVE')
		GROUP BY status, type`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch session metrics: %w", err)
	}

	for _, row := range rows {
		sessionStatus := row["SESSION_STATUS"]
		sessionType := row["SESSION_TYPE"]
		countStr := row["SESSION_COUNT"]

		if count, err := strconv.ParseInt(countStr, 10, 64); err == nil {
			s.mb.RecordOracledbSessionsDataPoint(now, count, sessionStatus, sessionType)
		}
	}

	return nil
}

// scrapeTablespaceMetrics collects tablespace usage and limit metrics
func (s *oracledbScraper) scrapeTablespaceMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT 
			ts.tablespace_name,
			NVL(df.bytes, 0) as max_bytes,
			NVL(df.bytes - NVL(fs.bytes, 0), 0) as used_bytes,
			CASE 
				WHEN df.bytes > 0 THEN ROUND((df.bytes - NVL(fs.bytes, 0)) / df.bytes * 100, 2)
				ELSE 0 
			END as usage_percentage,
			CASE 
				WHEN ts.status = 'OFFLINE' THEN 1 
				ELSE 0 
			END as offline_status
		FROM dba_tablespaces ts
		LEFT JOIN (
			SELECT tablespace_name, SUM(bytes) as bytes
			FROM dba_data_files 
			GROUP BY tablespace_name
		) df ON ts.tablespace_name = df.tablespace_name
		LEFT JOIN (
			SELECT tablespace_name, SUM(bytes) as bytes
			FROM dba_free_space 
			GROUP BY tablespace_name
		) fs ON ts.tablespace_name = fs.tablespace_name
		WHERE ts.contents != 'TEMPORARY'`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch tablespace metrics: %w", err)
	}

	for _, row := range rows {
		tablespaceName := row["TABLESPACE_NAME"]

		if maxBytes, err := strconv.ParseInt(row["MAX_BYTES"], 10, 64); err == nil {
			s.mb.RecordOracledbTablespaceSizeLimitDataPoint(now, maxBytes, tablespaceName)
		}

		if usedBytes, err := strconv.ParseInt(row["USED_BYTES"], 10, 64); err == nil {
			s.mb.RecordOracledbTablespaceSizeUsageDataPoint(now, usedBytes, tablespaceName)
		}

		if usagePercentage, err := strconv.ParseFloat(row["USAGE_PERCENTAGE"], 64); err == nil {
			s.mb.RecordOracledbTablespaceUsagePercentageDataPoint(now, usagePercentage, tablespaceName)
		}

		if offlineStatus, err := strconv.ParseInt(row["OFFLINE_STATUS"], 10, 64); err == nil {
			s.mb.RecordOracledbTablespaceOfflineDataPoint(now, offlineStatus, tablespaceName)
		}
	}

	return nil
}

// scrapePGAMemoryMetrics collects PGA memory metrics
func (s *oracledbScraper) scrapePGAMemoryMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT 
			name,
			TO_CHAR(value) as value 
		FROM v$pgastat 
		WHERE name IN (
			'total PGA inuse',
			'total PGA allocated', 
			'total freeable PGA memory',
			'maximum PGA allocated'
		)
		UNION ALL
		SELECT 
			'aggregate PGA target parameter' as name,
			TO_CHAR(value) as value
		FROM v$parameter 
		WHERE name = 'pga_aggregate_target'`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch PGA memory metrics: %w", err)
	}

	for _, row := range rows {
		statName := row["NAME"]
		value := row["VALUE"]

		switch statName {
		case "total PGA inuse":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbPgaMemoryDataPoint(now, val)
				s.mb.RecordOracledbPgaUsedMemoryDataPoint(now, val)
			}
		case "total PGA allocated":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbPgaAllocatedMemoryDataPoint(now, val)
			}
		case "total freeable PGA memory":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbPgaFreeableMemoryDataPoint(now, val)
			}
		case "maximum PGA allocated":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbPgaMaximumMemoryDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeParseMetrics collects parse operation metrics
func (s *oracledbScraper) scrapeParseMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT name, value 
		FROM v$sysstat 
		WHERE name IN (
			'parse count (total)',
			'parse count (hard)'
		)`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch parse metrics: %w", err)
	}

	for _, row := range rows {
		statName := row["NAME"]
		value := row["VALUE"]

		switch statName {
		case "parse count (total)":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbParseCallsDataPoint(now, val)
			}
		case "parse count (hard)":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbHardParsesDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeConnectionMetrics collects connection and session metrics
func (s *oracledbScraper) scrapeConnectionMetrics(ctx context.Context, now pcommon.Timestamp) error {
	// Get session limit and logon statistics
	query := `
		SELECT 
			'sessions_limit' as metric_name,
			TO_CHAR(value) as metric_value
		FROM v$parameter 
		WHERE name = 'sessions'
		UNION ALL
		SELECT 
			'logons_cumulative' as metric_name,
			TO_CHAR(value) as metric_value
		FROM v$sysstat 
		WHERE name = 'logons cumulative'`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch connection metrics: %w", err)
	}

	for _, row := range rows {
		metricName := row["METRIC_NAME"]
		value := row["METRIC_VALUE"]

		switch metricName {
		case "sessions_limit":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbSessionsLimitDataPoint(now, val)
			}
		case "logons_cumulative":
			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				s.mb.RecordOracledbLogonsDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeSGAMetrics collects SGA memory metrics
func (s *oracledbScraper) scrapeSGAMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT 
			name,
			TO_CHAR(value) as value 
		FROM v$sga 
		WHERE name IN (
			'Fixed Size',
			'Database Buffers'
		)`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch SGA metrics: %w", err)
	}

	for _, row := range rows {
		name := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch name {
			case "Fixed Size":
				s.mb.RecordOracledbSgaFixedSizeDataPoint(now, val)
			case "Database Buffers":
				s.mb.RecordOracledbSgaDatabaseBufferCacheDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeBufferCacheMetrics collects buffer cache efficiency metrics
func (s *oracledbScraper) scrapeBufferCacheMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT 
			name,
			TO_CHAR(value) as value 
		FROM v$sysstat 
		WHERE name IN (
			'consistent gets',
			'db block gets',
			'physical reads direct',
			'physical writes direct',
			'physical read IO requests',
			'physical write IO requests'
		)`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch buffer cache metrics: %w", err)
	}

	for _, row := range rows {
		name := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch name {
			case "consistent gets":
				s.mb.RecordOracledbConsistentGetsDataPoint(now, val)
			case "db block gets":
				s.mb.RecordOracledbDbBlockGetsDataPoint(now, val)
			case "physical reads direct":
				s.mb.RecordOracledbPhysicalReadsDirectDataPoint(now, val)
			case "physical writes direct":
				s.mb.RecordOracledbPhysicalWritesDirectDataPoint(now, val)
			case "physical read IO requests":
				s.mb.RecordOracledbPhysicalReadIoRequestsDataPoint(now, val)
			case "physical write IO requests":
				s.mb.RecordOracledbPhysicalWriteIoRequestsDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeParallelMetrics collects parallel processing metrics
func (s *oracledbScraper) scrapeParallelMetrics(ctx context.Context, now pcommon.Timestamp) error {
	query := `
		SELECT 
			name,
			TO_CHAR(value) as value 
		FROM v$sysstat 
		WHERE name IN (
			'queries parallelized',
			'DDL statements parallelized',
			'DML statements parallelized',
			'Parallel operations not downgraded',
			'Parallel operations downgraded to serial',
			'Parallel operations downgraded 1 to 25 pct',
			'Parallel operations downgraded 25 to 50 pct',
			'Parallel operations downgraded 50 to 75 pct',
			'Parallel operations downgraded 75 to 99 pct'
		)`

	rows, err := s.dbClient.metricRows(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to fetch parallel processing metrics: %w", err)
	}

	for _, row := range rows {
		name := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch name {
			case "queries parallelized":
				s.mb.RecordOracledbQueriesParallelizedDataPoint(now, val)
			case "DDL statements parallelized":
				s.mb.RecordOracledbDdlStatementsParallelizedDataPoint(now, val)
			case "DML statements parallelized":
				s.mb.RecordOracledbDmlStatementsParallelizedDataPoint(now, val)
			case "Parallel operations not downgraded":
				s.mb.RecordOracledbParallelOperationsNotDowngradedDataPoint(now, val)
			case "Parallel operations downgraded to serial":
				s.mb.RecordOracledbParallelOperationsDowngradedToSerialDataPoint(now, val)
			case "Parallel operations downgraded 1 to 25 pct":
				s.mb.RecordOracledbParallelOperationsDowngraded1To25PctDataPoint(now, val)
			case "Parallel operations downgraded 25 to 50 pct":
				s.mb.RecordOracledbParallelOperationsDowngraded25To50PctDataPoint(now, val)
			case "Parallel operations downgraded 50 to 75 pct":
				s.mb.RecordOracledbParallelOperationsDowngraded50To75PctDataPoint(now, val)
			case "Parallel operations downgraded 75 to 99 pct":
				s.mb.RecordOracledbParallelOperationsDowngraded75To99PctDataPoint(now, val)
			}
		}
	}

	return nil
}

// scrapeSimpleMemoryMetrics collects available PGA and memory metrics
func (s *oracledbScraper) scrapeSimpleMemoryMetrics(ctx context.Context, now pcommon.Timestamp) error {
	// PGA metrics using existing functions
	pgaQuery := `
		SELECT name, TO_CHAR(value) as value 
		FROM v$pgastat
		WHERE name IN (
			'aggregate PGA target parameter',
			'aggregate PGA auto target',
			'global memory bound',
			'total PGA allocated',
			'total PGA used by SQL workareas',
			'total freeable PGA memory',
			'maximum PGA allocated',
			'over allocation count'
		)`

	rows, err := s.dbClient.metricRows(ctx, pgaQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch PGA metrics: %w", err)
	}

	for _, row := range rows {
		name := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch name {
			case "aggregate PGA target parameter":
				s.mb.RecordOracledbPgaAggregateTargetDataPoint(now, val)
			case "aggregate PGA auto target":
				s.mb.RecordOracledbPgaAggregateLimitDataPoint(now, val)
			case "global memory bound":
				s.mb.RecordOracledbPgaGlobalMemoryBoundDataPoint(now, val)
			case "total PGA allocated":
				s.mb.RecordOracledbPgaAllocatedMemoryDataPoint(now, val)
			case "total PGA used by SQL workareas":
				s.mb.RecordOracledbPgaUsedMemoryDataPoint(now, val)
			case "total freeable PGA memory":
				s.mb.RecordOracledbPgaFreeableMemoryDataPoint(now, val)
			case "maximum PGA allocated":
				s.mb.RecordOracledbPgaMaximumMemoryDataPoint(now, val)
			case "over allocation count":
				s.mb.RecordOracledbPgaOverAllocationCountDataPoint(now, val)
			}
		}
	}

	// Additional SGA metrics
	sgaQuery := `
		SELECT name, TO_CHAR(value) as value 
		FROM v$sga
		WHERE name IN (
			'Fixed Size',
			'Variable Size',
			'Database Buffers',
			'Redo Buffers'
		)
		UNION ALL
		SELECT component, TO_CHAR(current_size) as value
		FROM v$sga_dynamic_components
		WHERE component IN (
			'buffer cache',
			'shared pool',
			'large pool',
			'java pool',
			'DEFAULT buffer cache'
		)`

	sgaRows, err := s.dbClient.metricRows(ctx, sgaQuery)
	if err == nil {
		for _, row := range sgaRows {
			name := row["NAME"]
			if name == "" {
				name = row["COMPONENT"] // For dynamic components
			}
			value := row["VALUE"]

			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				switch name {
				case "buffer cache", "DEFAULT buffer cache":
					s.mb.RecordOracledbSgaBufferCacheSizeDataPoint(now, val)
				case "shared pool":
					s.mb.RecordOracledbSgaSharedPoolFreeMemoryDataPoint(now, val)
				case "large pool":
					s.mb.RecordOracledbSgaLargePoolSizeDataPoint(now, val)
				case "java pool":
					s.mb.RecordOracledbSgaJavaPoolSizeDataPoint(now, val)
				case "Redo Buffers":
					s.mb.RecordOracledbSgaLogBufferDataPoint(now, val)
				}
			}
		}
	}

	return nil
}

// scrapeSimplePhysicalIOMetrics collects available physical I/O and disk metrics
func (s *oracledbScraper) scrapeSimplePhysicalIOMetrics(ctx context.Context, now pcommon.Timestamp) error {
	// Physical I/O metrics using existing functions
	ioQuery := `
		SELECT name, TO_CHAR(value) as value 
		FROM v$sysstat
		WHERE name IN (
			'physical reads',
			'physical writes',
			'physical reads direct',
			'physical writes direct',
			'physical read IO requests',
			'physical write IO requests'
		)`

	rows, err := s.dbClient.metricRows(ctx, ioQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch physical I/O metrics: %w", err)
	}

	for _, row := range rows {
		name := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch name {
			case "physical reads":
				s.mb.RecordOracledbPhysicalReadsDataPoint(now, val)
			case "physical writes":
				s.mb.RecordOracledbPhysicalWritesDataPoint(now, val)
			case "physical reads direct":
				s.mb.RecordOracledbPhysicalReadsDirectDataPoint(now, val)
			case "physical writes direct":
				s.mb.RecordOracledbPhysicalWritesDirectDataPoint(now, val)
			case "physical read IO requests":
				s.mb.RecordOracledbPhysicalReadIoRequestsDataPoint(now, val)
			case "physical write IO requests":
				s.mb.RecordOracledbPhysicalWriteIoRequestsDataPoint(now, val)
			}
		}
	}

	// Disk space metrics from tablespace information
	diskQuery := `
		SELECT 
			tablespace_name,
			TO_CHAR(SUM(bytes)) as total_bytes,
			TO_CHAR(SUM(bytes) - SUM(NVL(user_bytes, 0))) as used_bytes,
			TO_CHAR(SUM(NVL(user_bytes, 0))) as free_bytes
		FROM dba_data_files 
		WHERE tablespace_name IN ('SYSTEM', 'SYSAUX', 'USERS', 'UNDOTBS1')
		GROUP BY tablespace_name`

	diskRows, err := s.dbClient.metricRows(ctx, diskQuery)
	if err == nil {
		for _, row := range diskRows {
			tablespaceName := row["TABLESPACE_NAME"]

			if totalBytes, err := strconv.ParseInt(row["TOTAL_BYTES"], 10, 64); err == nil {
				s.mb.RecordOracledbDiskTotalDataPoint(now, totalBytes, tablespaceName)
			}

			if usedBytes, err := strconv.ParseInt(row["USED_BYTES"], 10, 64); err == nil {
				s.mb.RecordOracledbDiskUsedDataPoint(now, usedBytes, tablespaceName)
			}

			if freeBytes, err := strconv.ParseInt(row["FREE_BYTES"], 10, 64); err == nil {
				s.mb.RecordOracledbDiskFreeDataPoint(now, freeBytes, tablespaceName)
			}

			// Calculate utilization percentage
			if totalBytes, err1 := strconv.ParseInt(row["TOTAL_BYTES"], 10, 64); err1 == nil {
				if usedBytes, err2 := strconv.ParseInt(row["USED_BYTES"], 10, 64); err2 == nil && totalBytes > 0 {
					utilization := float64(usedBytes) / float64(totalBytes) * 100
					s.mb.RecordOracledbDiskUtilizationDataPoint(now, utilization, tablespaceName)
				}
			}
		}
	}

	// Disk I/O operations with tablespace attributes
	diskIOQuery := `
		SELECT 
			df.tablespace_name,
			'reads' as operation_type,
			TO_CHAR(SUM(fs.phyrds)) as value
		FROM v$filestat fs, dba_data_files df
		WHERE fs.file# = df.file_id
		GROUP BY df.tablespace_name
		UNION ALL
		SELECT 
			df.tablespace_name,
			'writes' as operation_type,
			TO_CHAR(SUM(fs.phywrts)) as value
		FROM v$filestat fs, dba_data_files df
		WHERE fs.file# = df.file_id
		GROUP BY df.tablespace_name`

	diskIORows, err := s.dbClient.metricRows(ctx, diskIOQuery)
	if err == nil {
		for _, row := range diskIORows {
			tablespaceName := row["TABLESPACE_NAME"]
			opType := row["OPERATION_TYPE"]
			value := row["VALUE"]

			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				switch opType {
				case "reads":
					s.mb.RecordOracledbDiskReadsDataPoint(now, val, tablespaceName)
				case "writes":
					s.mb.RecordOracledbDiskWritesDataPoint(now, val, tablespaceName)
				}
			}
		}
	}

	return nil
}

// scrapeSimpleSystemMetrics collects available system, network, and enqueue metrics
func (s *oracledbScraper) scrapeSimpleSystemMetrics(ctx context.Context, now pcommon.Timestamp) error {
	// System and enqueue metrics
	systemQuery := `
		SELECT name, TO_CHAR(value) as value 
		FROM v$sysstat
		WHERE name IN (
			'logons cumulative',
			'execute count',
			'user commits',
			'user rollbacks',
			'enqueue deadlocks',
			'bytes sent via SQL*Net to client',
			'bytes received via SQL*Net from client'
		)`

	rows, err := s.dbClient.metricRows(ctx, systemQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch system metrics: %w", err)
	}

	for _, row := range rows {
		name := row["NAME"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch name {
			case "bytes sent via SQL*Net to client":
				s.mb.RecordOracledbNetworkBytesSentDataPoint(now, val)
			case "bytes received via SQL*Net from client":
				s.mb.RecordOracledbNetworkBytesReceivedDataPoint(now, val)
			case "enqueue deadlocks":
				s.mb.RecordOracledbEnqueueDeadlocksDataPoint(now, val)
			}
		}
	}

	// CPU Usage Percentage calculation
	cpuQuery := `
		SELECT 
			ROUND((SELECT value FROM v$sysstat WHERE name = 'CPU used by this session') / 
				  (SELECT value FROM v$parameter WHERE name = 'cpu_count') / 
				  (SELECT EXTRACT(DAY FROM systimestamp - startup_time) * 24 * 3600 + 
						  EXTRACT(HOUR FROM systimestamp - startup_time) * 3600 + 
						  EXTRACT(MINUTE FROM systimestamp - startup_time) * 60 + 
						  EXTRACT(SECOND FROM systimestamp - startup_time) 
				   FROM v$instance) * 100, 2) as cpu_usage_pct
		FROM dual`

	cpuRows, err := s.dbClient.metricRows(ctx, cpuQuery)
	if err == nil && len(cpuRows) > 0 {
		if cpuUsage, err := strconv.ParseFloat(cpuRows[0]["CPU_USAGE_PCT"], 64); err == nil {
			s.mb.RecordOracledbCPUUsagePercentageDataPoint(now, cpuUsage)
		}
	}

	// Enqueue locks and resources metrics
	enqueueQuery := `
		SELECT 'locks_usage' as metric_type, TO_CHAR(COUNT(*)) as value
		FROM v$lock WHERE type = 'TX'
		UNION ALL
		SELECT 'locks_limit' as metric_type, TO_CHAR(value) as value
		FROM v$parameter WHERE name = 'enqueue_resources'
		UNION ALL
		SELECT 'resources_usage' as metric_type, TO_CHAR(COUNT(*)) as value
		FROM v$enqueue_lock
		UNION ALL
		SELECT 'resources_limit' as metric_type, TO_CHAR(value) as value
		FROM v$parameter WHERE name = 'processes'`

	enqRows, err := s.dbClient.metricRows(ctx, enqueueQuery)
	if err == nil {
		for _, row := range enqRows {
			metricType := row["METRIC_TYPE"]
			value := row["VALUE"]

			if val, err := strconv.ParseInt(value, 10, 64); err == nil {
				switch metricType {
				case "locks_usage":
					s.mb.RecordOracledbEnqueueLocksUsageDataPoint(now, val)
				case "locks_limit":
					s.mb.RecordOracledbEnqueueLocksLimitDataPoint(now, val)
				case "resources_usage":
					s.mb.RecordOracledbEnqueueResourcesUsageDataPoint(now, val)
				case "resources_limit":
					s.mb.RecordOracledbEnqueueResourcesLimitDataPoint(now, val)
				}
			}
		}
	}

	// Network traffic volume (total bytes)
	netQuery := `
		SELECT TO_CHAR(
			(SELECT NVL(value, 0) FROM v$sysstat WHERE name = 'bytes sent via SQL*Net to client') +
			(SELECT NVL(value, 0) FROM v$sysstat WHERE name = 'bytes received via SQL*Net from client')
		) as total_traffic FROM dual`

	netRows, err := s.dbClient.metricRows(ctx, netQuery)
	if err == nil && len(netRows) > 0 {
		if totalTraffic, err := strconv.ParseInt(netRows[0]["TOTAL_TRAFFIC"], 10, 64); err == nil {
			s.mb.RecordOracledbNetworkTrafficVolumeDataPoint(now, totalTraffic)
		}
	}

	return nil
}

// scrapeAdditionalSGAMetrics collects additional SGA component metrics
func (s *oracledbScraper) scrapeAdditionalSGAMetrics(ctx context.Context, now pcommon.Timestamp) error {
	// Additional SGA component metrics
	sgaComponentQuery := `
		SELECT component, TO_CHAR(current_size) as current_size, TO_CHAR(max_size) as max_size
		FROM v$sga_dynamic_components
		WHERE component IN (
			'shared pool',
			'large pool',
			'java pool',
			'DEFAULT buffer cache',
			'buffer_cache'
		)`

	rows, err := s.dbClient.metricRows(ctx, sgaComponentQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch additional SGA metrics: %w", err)
	}

	for _, row := range rows {
		component := row["COMPONENT"]
		currentSize := row["CURRENT_SIZE"]
		maxSize := row["MAX_SIZE"]

		if currentVal, err := strconv.ParseInt(currentSize, 10, 64); err == nil {
			switch component {
			case "shared pool":
				s.mb.RecordOracledbSgaSharedPoolFreeMemoryDataPoint(now, currentVal)
			case "large pool":
				s.mb.RecordOracledbSgaLargePoolSizeDataPoint(now, currentVal)
			case "java pool":
				s.mb.RecordOracledbSgaJavaPoolSizeDataPoint(now, currentVal)
			case "DEFAULT buffer cache", "buffer_cache":
				s.mb.RecordOracledbSgaDataBufferCacheSizeDataPoint(now, currentVal)
			}
		}

		if maxVal, err := strconv.ParseInt(maxSize, 10, 64); err == nil {
			switch component {
			case "DEFAULT buffer cache", "buffer_cache":
				s.mb.RecordOracledbSgaMaximumSizeDataPoint(now, maxVal)
			}
		}
	}

	return nil
}

// scrapeAdditionalNetworkMetrics collects network packet metrics
func (s *oracledbScraper) scrapeAdditionalNetworkMetrics(ctx context.Context, now pcommon.Timestamp) error {
	// Network packet statistics
	packetQuery := `
		SELECT 
			'packets_sent' as metric_type,
			TO_CHAR(value) as value
		FROM v$sysstat 
		WHERE name = 'SQL*Net roundtrips to/from client'
		UNION ALL
		SELECT 
			'packets_received' as metric_type,
			TO_CHAR(value) as value
		FROM v$sysstat 
		WHERE name = 'SQL*Net roundtrips to/from client'`

	rows, err := s.dbClient.metricRows(ctx, packetQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch network packet metrics: %w", err)
	}

	for _, row := range rows {
		metricType := row["METRIC_TYPE"]
		value := row["VALUE"]

		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			switch metricType {
			case "packets_sent":
				s.mb.RecordOracledbNetworkPacketsSentDataPoint(now, val)
			case "packets_received":
				s.mb.RecordOracledbNetworkPacketsReceivedDataPoint(now, val)
			}
		}
	}

	return nil
}

// Shutdown closes database connections
func (*oracledbScraper) Shutdown(_ context.Context) error {
	return nil
}
