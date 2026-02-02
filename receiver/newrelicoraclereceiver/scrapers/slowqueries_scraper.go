package scrapers

import (
	"context"
	"fmt"
	"sort"
	"time"

	commonutils "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/common-utils"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// SlowQueriesScraper contains the scraper for slow queries metrics
type SlowQueriesScraper struct {
	client                               client.OracleClient
	mb                                   *metadata.MetricsBuilder
	logger                               *zap.Logger
	metricsBuilderConfig                 metadata.MetricsBuilderConfig
	queryMonitoringResponseTimeThreshold int
	queryMonitoringCountThreshold        int
	queryMonitoringIntervalSeconds       int                       // Time window for fetching queries (in seconds)
	intervalCalculator                   *OracleIntervalCalculator // Delta-based interval calculator
}

func NewSlowQueriesScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, metricsBuilderConfig metadata.MetricsBuilderConfig, responseTimeThreshold, countThreshold, intervalSeconds int, enableIntervalCalculator bool, intervalCalculatorCacheTTLMinutes int) *SlowQueriesScraper {
	var intervalCalc *OracleIntervalCalculator

	// Initialize interval-based calculator if enabled
	if enableIntervalCalculator {
		cacheTTL := time.Duration(intervalCalculatorCacheTTLMinutes) * time.Minute
		intervalCalc = NewOracleIntervalCalculator(logger, cacheTTL)
		logger.Info("Oracle interval-based delta calculator enabled")
	} else {
		logger.Info("Oracle interval-based delta calculator disabled - using cumulative averages")
	}

	return &SlowQueriesScraper{
		client:                               oracleClient,
		mb:                                   mb,
		logger:                               logger,
		metricsBuilderConfig:                 metricsBuilderConfig,
		queryMonitoringResponseTimeThreshold: responseTimeThreshold,
		queryMonitoringCountThreshold:        countThreshold,
		queryMonitoringIntervalSeconds:       intervalSeconds,
		intervalCalculator:                   intervalCalc,
	}
}

func (s *SlowQueriesScraper) ScrapeSlowQueries(ctx context.Context) ([]models.SQLIdentifier, []error) {
	var scrapeErrors []error
	var sqlIdentifiers []models.SQLIdentifier

	// Fetch queries from the database with time window filter
	// Note: intervalSeconds determines the time window (e.g., 60 = last 60 seconds)
	//       This is critical for delta calculation to work correctly
	// The value comes from config and is typically set to collection_interval
	slowQueries, err := s.client.QuerySlowQueries(ctx, s.queryMonitoringIntervalSeconds, s.queryMonitoringResponseTimeThreshold, s.queryMonitoringCountThreshold)
	if err != nil {
		return nil, []error{err}
	}

	// Apply interval-based delta calculation if enabled
	var queriesToProcess []models.SlowQuery
	if s.intervalCalculator != nil {
		now := time.Now()

		// Pre-allocate slice capacity to avoid reallocations
		queriesToProcess = make([]models.SlowQuery, 0, len(slowQueries))

		// Calculate interval metrics for each query
		for _, slowQuery := range slowQueries {
			metrics := s.intervalCalculator.CalculateMetrics(&slowQuery, now)

			if metrics == nil {
				s.logger.Debug("Skipping query with nil metrics")
				continue
			}

			// Skip queries with no new executions
			if !metrics.HasNewExecutions {
				s.logger.Debug("Skipping query with no new executions",
					zap.String("query_id", slowQuery.GetQueryID()))
				continue
			}

			// Apply interval-based threshold filtering
			// First scrape: interval = historical (no baseline), filter still applies
			// Subsequent scrapes: interval = delta, filter on delta performance
			if metrics.IntervalAvgElapsedTimeMs < float64(s.queryMonitoringResponseTimeThreshold) {
				continue
			}

			// Store interval metrics in new fields (preserve historical values)
			// Historical values remain unchanged from DB
			slowQuery.IntervalAvgElapsedTimeMS = &metrics.IntervalAvgElapsedTimeMs
			slowQuery.IntervalExecutionCount = &metrics.IntervalExecutionCount
			slowQuery.IntervalAvgCPUTimeMS = &metrics.IntervalAvgCPUTimeMs
			slowQuery.IntervalAvgDiskReads = &metrics.IntervalAvgDiskReads
			slowQuery.IntervalAvgDiskWrites = &metrics.IntervalAvgDiskWrites
			slowQuery.IntervalAvgBufferGets = &metrics.IntervalAvgBufferGets
			slowQuery.IntervalAvgRowsProcessed = &metrics.IntervalAvgRowsProcessed

			queriesToProcess = append(queriesToProcess, slowQuery)
		}

		// Cleanup stale entries periodically (TTL-based only)
		s.intervalCalculator.CleanupStaleEntries(now)

		// Log calculator statistics
		stats := s.intervalCalculator.GetCacheStats()
		s.logger.Debug("Interval calculator statistics", zap.Any("stats", stats))

		// Apply Go-level sorting and top-N selection AFTER delta calculation and filtering
		// This ensures we get the slowest queries based on interval (delta) averages, not historical
		if len(queriesToProcess) > 0 {
			// Sort by interval average elapsed time (delta) - slowest first
			sort.Slice(queriesToProcess, func(i, j int) bool {
				// Handle nil cases - queries without interval metrics go to the end
				if queriesToProcess[i].IntervalAvgElapsedTimeMS == nil {
					return false
				}
				if queriesToProcess[j].IntervalAvgElapsedTimeMS == nil {
					return true
				}
				// Sort descending (highest delta average first)
				return *queriesToProcess[i].IntervalAvgElapsedTimeMS > *queriesToProcess[j].IntervalAvgElapsedTimeMS
			})

			// Take top N queries after sorting
			if len(queriesToProcess) > s.queryMonitoringCountThreshold {
				queriesToProcess = queriesToProcess[:s.queryMonitoringCountThreshold]
			}
		}
	} else {
		// No interval calculator - use raw results
		queriesToProcess = slowQueries
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, slowQuery := range queriesToProcess {
		if !slowQuery.IsValidForMetrics() {
			continue
		}

		collectionTimestamp := slowQuery.GetCollectionTimestamp()
		dbName := slowQuery.GetDatabaseName()
		qID := slowQuery.GetQueryID()
		userName := slowQuery.GetUserName()
		schName := slowQuery.GetSchemaName()
		lastActiveTime := slowQuery.GetLastActiveTime()

		// Extract New Relic metadata (nr_service and optionally nr_txn) from sql_fulltext BEFORE normalization
		// Generate normalised SQL hash from sql_fulltext using New Relic Java agent normalization logic
		// The anonymized query text is derived from the normalized SQL (for attribute display)
		var queryHash, nrService, nrTxn, qText string
		if slowQuery.QueryText.Valid && slowQuery.QueryText.String != "" {
			// Extract New Relic metadata from comment
			nrService, nrTxn = commonutils.ExtractNewRelicMetadata(slowQuery.QueryText.String)
			// Generate normalized SQL and hash
			normalizedSQL, hash := commonutils.NormalizeSqlAndHash(slowQuery.QueryText.String)
			queryHash = hash
			// Use normalized SQL as the query text attribute (anonymized)
			qText = normalizedSQL
		}

		if err := s.recordMetrics(now, &slowQuery, collectionTimestamp, dbName, qID, qText, userName, schName, lastActiveTime, queryHash, nrService, nrTxn); err != nil {
			s.logger.Warn("Failed to record metrics for slow query",
				zap.String("sql_id", qID),
				zap.Error(err))
			scrapeErrors = append(scrapeErrors, err)
			continue
		}

		if slowQuery.QueryID.Valid {
			sqlIdentifiers = append(sqlIdentifiers, models.SQLIdentifier{
				SQLID:             slowQuery.QueryID.String,
				ChildNumber:       0, // Will be populated later by child cursors scraper
				Timestamp:         time.Now(),
				ClientName:        nrService,        // Empty string if not present
				TransactionName:   nrTxn,            // Empty string if not present
				NormalisedSQLHash: queryHash,        // Empty string if no query text
			})
		}
	}

	s.logger.Debug("Slow queries scrape completed",
		zap.Int("errors", len(scrapeErrors)))

	return sqlIdentifiers, scrapeErrors
}

func (s *SlowQueriesScraper) recordMetrics(now pcommon.Timestamp, slowQuery *models.SlowQuery, collectionTimestamp, dbName, qID, qText, userName, schName, lastActiveTime, queryHash, nrService, nrTxn string) error {
	if slowQuery == nil {
		s.logger.Warn("Attempted to record metrics for nil slow query")
		return fmt.Errorf("slow query is nil")
	}
	if slowQuery.ExecutionCount.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesExecutionCountDataPoint(
			now,
			float64(slowQuery.ExecutionCount.Int64),
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total elapsed time - raw value from V$SQLAREA
	if slowQuery.TotalElapsedTimeMS.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalElapsedTimeDataPoint(
			now,
			slowQuery.TotalElapsedTimeMS.Float64,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total CPU time - raw value from V$SQLAREA
	if slowQuery.TotalCPUTimeMS.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalCPUTimeDataPoint(
			now,
			slowQuery.TotalCPUTimeMS.Float64,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total disk reads - raw value from V$SQLAREA
	if slowQuery.TotalDiskReads.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalDiskReadsDataPoint(
			now,
			float64(slowQuery.TotalDiskReads.Int64),
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total direct writes - raw value from V$SQLAREA
	if slowQuery.TotalDiskWrites.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalDiskWritesDataPoint(
			now,
			float64(slowQuery.TotalDiskWrites.Int64),
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total rows examined (buffer gets) - raw value from V$SQLAREA
	if slowQuery.TotalBufferGets.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalRowsExaminedDataPoint(
			now,
			float64(slowQuery.TotalBufferGets.Int64),
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total rows returned (rows processed) - raw value from V$SQLAREA
	if slowQuery.TotalRowsProcessed.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalRowsReturnedDataPoint(
			now,
			float64(slowQuery.TotalRowsProcessed.Int64),
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval-based average elapsed time if available (delta metric)
	if slowQuery.IntervalAvgElapsedTimeMS != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalAvgElapsedTimeDataPoint(
			now,
			*slowQuery.IntervalAvgElapsedTimeMS,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval execution count if available (delta metric)
	if slowQuery.IntervalExecutionCount != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalExecutionCountDataPoint(
			now,
			float64(*slowQuery.IntervalExecutionCount),
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval CPU time if available (delta metric)
	if slowQuery.IntervalAvgCPUTimeMS != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalAvgCPUTimeDataPoint(
			now,
			*slowQuery.IntervalAvgCPUTimeMS,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval disk reads if available (delta metric)
	if slowQuery.IntervalAvgDiskReads != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalAvgDiskReadsDataPoint(
			now,
			*slowQuery.IntervalAvgDiskReads,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval direct writes if available (delta metric)
	if slowQuery.IntervalAvgDiskWrites != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalAvgDiskWritesDataPoint(
			now,
			*slowQuery.IntervalAvgDiskWrites,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval buffer gets if available (delta metric)
	if slowQuery.IntervalAvgBufferGets != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalAvgBufferGetsDataPoint(
			now,
			*slowQuery.IntervalAvgBufferGets,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record interval rows processed if available (delta metric)
	if slowQuery.IntervalAvgRowsProcessed != nil {
		s.mb.RecordNewrelicoracledbSlowQueriesIntervalAvgRowsProcessedDataPoint(
			now,
			*slowQuery.IntervalAvgRowsProcessed,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	// Record historical (cumulative) total wait time - raw value from V$SQLAREA
	if slowQuery.TotalWaitTimeMS.Valid {
		s.mb.RecordNewrelicoracledbSlowQueriesTotalWaitTimeDataPoint(
			now,
			slowQuery.TotalWaitTimeMS.Float64,
			collectionTimestamp,
			dbName,
			qID,
			userName,
			queryHash,
			nrService,
			nrTxn,
		)
	}

	s.mb.RecordNewrelicoracledbSlowQueriesQueryDetailsDataPoint(
		now,
		1,
		"OracleSlowQuery",
		collectionTimestamp,
		dbName,
		qID,
		qText,
		schName,
		userName,
		lastActiveTime,
		queryHash,
		nrService,
		nrTxn,
	)

	return nil
}

// GetSlowQueryIDs returns only the query IDs without emitting metrics
// This is used by the logs scraper to get query IDs for execution plans
// without duplicating the slow query metrics already emitted by the metrics scraper
func (s *SlowQueriesScraper) GetSlowQueryIDs(ctx context.Context) ([]string, []error) {
	var queryIDs []string

	slowQueries, err := s.client.QuerySlowQueries(ctx, s.queryMonitoringIntervalSeconds, s.queryMonitoringResponseTimeThreshold, s.queryMonitoringCountThreshold)
	if err != nil {
		return nil, []error{err}
	}

	for _, slowQuery := range slowQueries {
		if !slowQuery.IsValidForMetrics() {
			continue
		}

		if slowQuery.QueryID.Valid {
			queryIDs = append(queryIDs, slowQuery.QueryID.String)
		}
	}

	s.logger.Debug("Fetched slow query IDs")

	return queryIDs, nil
}
