package newrelicmysqlreceiver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// mySQLScraper implements the MySQL metrics scraper
type mySQLScraper struct {
	config *Config
	logger *zap.Logger
	client *MySQLClient
	mb     *metadata.MetricsBuilder
	rb     *metadata.ResourceBuilder
}

// start initializes the scraper
func (s *mySQLScraper) start(ctx context.Context, host component.Host) error {
	s.logger.Info("Starting New Relic MySQL receiver")

	// Create MySQL client
	var err error
	s.client, err = NewMySQLClient(s.config, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create MySQL client: %w", err)
	}

	// Test connection
	if err := s.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	s.logger.Info("New Relic MySQL receiver started successfully")
	return nil
}

// scrape collects metrics from MySQL
func (s *mySQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	s.logger.Debug("Scraping MySQL metrics")

	// Get MySQL version and set resource attributes
	version, err := s.client.GetVersion(ctx)
	if err != nil {
		s.logger.Error("Failed to get MySQL version", zap.Error(err))
		return pmetric.NewMetrics(), err
	}

	// Set resource attributes
	s.rb.SetMysqlInstanceEndpoint(s.config.Endpoint)
	s.rb.SetMysqlInstanceVersion(version.Version)

	// Collect core MySQL metrics
	if err := s.collectCoreMetrics(ctx); err != nil {
		s.logger.Error("Failed to collect core metrics", zap.Error(err))
	}

	// Collect query performance metrics
	if s.config.QueryPerformanceConfig.Enabled && s.config.IsFeatureEnabled("query_performance") {
		if err := s.collectQueryPerformanceMetrics(ctx); err != nil {
			s.logger.Error("Failed to collect query performance metrics", zap.Error(err))
		}
	}

	// Collect wait events metrics
	if s.config.WaitEventsConfig.Enabled && s.config.IsFeatureEnabled("wait_events") {
		if err := s.collectWaitEventsMetrics(ctx); err != nil {
			s.logger.Error("Failed to collect wait events metrics", zap.Error(err))
		}
	}

	// Collect blocking sessions metrics
	if s.config.BlockingSessionsConfig.Enabled && s.config.IsFeatureEnabled("blocking_sessions") {
		if err := s.collectBlockingSessionsMetrics(ctx); err != nil {
			s.logger.Error("Failed to collect blocking sessions metrics", zap.Error(err))
		}
	}

	// Collect replication metrics
	if s.config.ReplicationConfig.Enabled && s.config.IsFeatureEnabled("replication") {
		if err := s.collectReplicationMetrics(ctx); err != nil {
			s.logger.Error("Failed to collect replication metrics", zap.Error(err))
		}
	}

	// Collect slow query metrics
	if s.config.SlowQueryConfig.Enabled && s.config.IsFeatureEnabled("slow_query") {
		if err := s.collectSlowQueryMetrics(ctx); err != nil {
			s.logger.Error("Failed to collect slow query metrics", zap.Error(err))
		}
	}

	// Build and return metrics
	metrics := s.mb.Emit(metadata.WithResource(s.rb.Emit()))
	return metrics, nil
}

// collectCoreMetrics collects core MySQL metrics using new methods
func (s *mySQLScraper) collectCoreMetrics(ctx context.Context) error {
	// Get core metrics using the new aggregated method
	coreMetrics, err := s.client.GetCoreMetrics(ctx)
	if err != nil {
		return fmt.Errorf("failed to get core metrics: %w", err)
	}

	// Log core metrics for observability (since we don't have specific data points yet)
	s.logger.Info("Core MySQL metrics collected",
		zap.Int64("connections", coreMetrics.Connections),
		zap.Int64("max_used_connections", coreMetrics.MaxUsedConnections),
		zap.Int64("threads_connected", coreMetrics.ThreadsConnected),
		zap.Int64("threads_running", coreMetrics.ThreadsRunning),
		zap.Int64("buffer_pool_size", coreMetrics.BufferPoolSize),
		zap.Int64("buffer_pool_pages_data", coreMetrics.BufferPoolPagesData),
		zap.Int64("buffer_pool_pages_free", coreMetrics.BufferPoolPagesFree),
		zap.Int64("innodb_data_reads", coreMetrics.InnoDBDataReads),
		zap.Int64("innodb_data_writes", coreMetrics.InnoDBDataWrites),
		zap.Int64("queries", coreMetrics.Queries),
		zap.Int64("slow_queries", coreMetrics.SlowQueries),
		zap.Int64("handler_read_total", coreMetrics.HandlerRead),
		zap.Int64("uptime", coreMetrics.Uptime))

	// Get and log network I/O metrics
	networkStats, err := s.client.GetNetworkIOStats(ctx)
	if err != nil {
		s.logger.Warn("Failed to get network I/O stats", zap.Error(err))
	} else {
		s.logger.Debug("Network I/O stats",
			zap.Int64("bytes_received", networkStats.BytesReceived),
			zap.Int64("bytes_sent", networkStats.BytesSent))
	}

	// Get and log command statistics
	commandStats, err := s.client.GetCommandStats(ctx)
	if err != nil {
		s.logger.Warn("Failed to get command stats", zap.Error(err))
	} else {
		s.logger.Debug("Command stats",
			zap.Int64("select", commandStats.Select),
			zap.Int64("insert", commandStats.Insert),
			zap.Int64("update", commandStats.Update),
			zap.Int64("delete", commandStats.Delete))
	}

	return nil
}

// collectQueryPerformanceMetrics collects query performance metrics using New Relic patterns
func (s *mySQLScraper) collectQueryPerformanceMetrics(ctx context.Context) error {
	// Check if performance_schema is available
	hasPS, err := s.client.HasPerformanceSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to check performance_schema: %w", err)
	}
	if !hasPS {
		s.logger.Warn("Performance schema not available, skipping query performance metrics")
		return nil
	}

	// Basic query performance data
	queryData, err := s.client.GetQueryPerformanceData(ctx)
	if err != nil {
		return fmt.Errorf("failed to get query performance data: %w", err)
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, query := range queryData {
		// Determine query type from digest text
		queryType := s.getQueryType(query.DigestText)

		// Record query execution metrics
		s.mb.RecordMysqlQueryExecutionCountDataPoint(now, query.CountStar, query.Digest, queryType, query.Schema)
		s.mb.RecordMysqlQueryLockTimeDataPoint(now, float64(query.SumLockTime)/1000000, query.Digest, queryType, query.Schema) // Convert to microseconds
		s.mb.RecordMysqlQueryRowsSentDataPoint(now, query.SumRowsSent, query.Digest, queryType, query.Schema)
		s.mb.RecordMysqlQueryRowsExaminedDataPoint(now, query.SumRowsExamined, query.Digest, queryType, query.Schema)
	}

	// Enhanced query monitoring using New Relic patterns
	if s.config.QueryPerformanceConfig.Enabled {
		// CurrentRunningQueriesSearch pattern - check for currently running queries
		for _, query := range queryData {
			if query.CountStar > 0 { // Active queries
				currentQueries, err := s.client.GetCurrentRunningQueries(ctx,
					query.Digest,
					int(s.config.QueryPerformanceConfig.MinQueryTime.Seconds()),
					10) // Limit current running queries
				if err != nil {
					s.logger.Error("Failed to get current running queries using New Relic pattern",
						zap.Error(err), zap.String("digest", query.Digest))
				} else if len(currentQueries) > 0 {
					s.logger.Debug("Found current running queries",
						zap.String("digest", query.Digest), zap.Int("count", len(currentQueries)))
				}

				// RecentQueriesSearch pattern - check recent query history
				recentQueries, err := s.client.GetRecentQueries(ctx,
					query.Digest,
					int(s.config.QueryPerformanceConfig.MinQueryTime.Seconds()),
					10) // Limit recent queries
				if err != nil {
					s.logger.Error("Failed to get recent queries using New Relic pattern",
						zap.Error(err), zap.String("digest", query.Digest))
				} else if len(recentQueries) > 0 {
					s.logger.Debug("Found recent queries",
						zap.String("digest", query.Digest), zap.Int("count", len(recentQueries)))
				}

				// PastQueriesSearch pattern - check historical query data
				pastQueries, err := s.client.GetPastQueries(ctx,
					query.Digest,
					int(s.config.QueryPerformanceConfig.MinQueryTime.Seconds()),
					10) // Limit past queries
				if err != nil {
					s.logger.Error("Failed to get past queries using New Relic pattern",
						zap.Error(err), zap.String("digest", query.Digest))
				} else if len(pastQueries) > 0 {
					s.logger.Debug("Found past queries",
						zap.String("digest", query.Digest), zap.Int("count", len(pastQueries)))
				}
			}
		}
	}

	return nil
}

// collectWaitEventsMetrics collects wait events metrics using New Relic patterns
func (s *mySQLScraper) collectWaitEventsMetrics(ctx context.Context) error {
	// Check if performance_schema is available
	hasPS, err := s.client.HasPerformanceSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to check performance_schema: %w", err)
	}
	if !hasPS {
		s.logger.Warn("Performance schema not available, skipping wait events metrics")
		return nil
	}

	// Basic wait events data
	waitEvents, err := s.client.GetWaitEventData(ctx)
	if err != nil {
		return fmt.Errorf("failed to get wait events data: %w", err)
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, event := range waitEvents {
		eventType := s.getWaitEventType(event.EventName)
		s.mb.RecordMysqlWaitEventsTotalTimeDataPoint(now, float64(event.SumTimerWait)/1000000, eventType, event.EventName) // Convert to microseconds
		s.mb.RecordMysqlWaitEventsCountDataPoint(now, event.CountStar, eventType, event.EventName)
	}

	// Enhanced wait events using New Relic WaitEventsQuery pattern
	if s.config.WaitEventsConfig.Enabled {
		advancedWaitEvents, err := s.client.GetAdvancedWaitEvents(ctx,
			[]string{"information_schema", "performance_schema", "sys"},
			s.config.WaitEventsConfig.MaxEvents)
		if err != nil {
			s.logger.Error("Failed to get advanced wait events using New Relic pattern", zap.Error(err))
		} else {
			s.logger.Debug("Collected advanced wait events using New Relic pattern", zap.Int("count", len(advancedWaitEvents)))
			// TODO: Add advanced wait event metrics to metadata.yaml for detailed tracking
		}
	}

	return nil
}

// collectBlockingSessionsMetrics collects blocking sessions metrics using New Relic patterns
func (s *mySQLScraper) collectBlockingSessionsMetrics(ctx context.Context) error {
	// Check if sys schema is available
	hasSys, err := s.client.HasSysSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to check sys schema: %w", err)
	}
	if !hasSys {
		s.logger.Warn("Sys schema not available, skipping blocking sessions metrics")
		return nil
	}

	// Basic blocking sessions data
	blockingSessions, err := s.client.GetBlockingSessionData(ctx)
	if err != nil {
		return fmt.Errorf("failed to get blocking sessions data: %w", err)
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, session := range blockingSessions {
		blockingSessionID := strconv.FormatInt(session.BlockingSessionID, 10)
		blockedSessionID := strconv.FormatInt(session.BlockedSessionID, 10)

		s.mb.RecordMysqlBlockingSessionsCountDataPoint(now, 1, blockingSessionID)
		s.mb.RecordMysqlBlockedSessionsCountDataPoint(now, 1, blockedSessionID)
		s.mb.RecordMysqlBlockedSessionsWaitTimeDataPoint(now, float64(session.WaitTime), blockedSessionID, blockingSessionID)
	}

	// Enhanced blocking sessions using New Relic BlockingSessionsQuery pattern
	if s.config.BlockingSessionsConfig.Enabled {
		advancedBlockingSessions, err := s.client.GetAdvancedBlockingSessions(ctx,
			[]string{"information_schema", "performance_schema", "sys"},
			s.config.BlockingSessionsConfig.MaxSessions)
		if err != nil {
			s.logger.Error("Failed to get advanced blocking sessions using New Relic pattern", zap.Error(err))
		} else {
			s.logger.Debug("Collected advanced blocking sessions using New Relic pattern", zap.Int("count", len(advancedBlockingSessions)))
			// TODO: Add advanced blocking session metrics to metadata.yaml for detailed tracking
		}
	}

	return nil
}

// collectReplicationMetrics collects replication metrics
func (s *mySQLScraper) collectReplicationMetrics(ctx context.Context) error {
	slaveStatus, err := s.client.GetSlaveStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get slave status: %w", err)
	}

	if slaveStatus == nil {
		// Not a slave, skip replication metrics
		return nil
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	// Slave lag
	if slaveStatus.SecondsBehindMaster.Valid {
		s.mb.RecordMysqlSlaveLagDataPoint(now, slaveStatus.SecondsBehindMaster.Int64)
	}

	// Slave status
	ioRunning := 0
	if slaveStatus.SlaveIORunning == "Yes" {
		ioRunning = 1
	}
	s.mb.RecordMysqlSlaveIoRunningDataPoint(now, int64(ioRunning))

	sqlRunning := 0
	if slaveStatus.SlaveSQLRunning == "Yes" {
		sqlRunning = 1
	}
	s.mb.RecordMysqlSlaveSQLRunningDataPoint(now, int64(sqlRunning))

	return nil
}

// collectSlowQueryMetrics collects slow query metrics using New Relic patterns
func (s *mySQLScraper) collectSlowQueryMetrics(ctx context.Context) error {
	status, err := s.client.GetGlobalStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get global status: %w", err)
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	// Basic slow queries count from global status
	s.mb.RecordMysqlSlowQueriesCountDataPoint(now, s.client.GetStatusInt64(status, "Slow_queries"))

	// Enhanced slow query analysis using New Relic patterns
	if s.config.QueryPerformanceConfig.Enabled {
		// Get slow queries using New Relic SlowQueries pattern
		slowQueries, err := s.client.GetSlowQueries(ctx,
			int(s.config.QueryPerformanceConfig.CollectionInterval.Seconds()),
			[]string{"information_schema", "performance_schema", "sys"},
			s.config.QueryPerformanceConfig.MaxDigests)
		if err != nil {
			s.logger.Error("Failed to get slow queries using New Relic pattern", zap.Error(err))
		} else {
			s.logger.Debug("Collected slow queries using New Relic pattern", zap.Int("count", len(slowQueries)))
			// TODO: Add slow query metrics to metadata.yaml for detailed tracking
		}
	}

	return nil
}

// shutdown closes the scraper
func (s *mySQLScraper) shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down New Relic MySQL receiver")

	if s.client != nil {
		if err := s.client.Close(); err != nil {
			s.logger.Error("Failed to close MySQL client", zap.Error(err))
			return err
		}
	}

	return nil
}

// Helper functions

// getQueryType determines the query type from digest text
func (s *mySQLScraper) getQueryType(digestText string) metadata.AttributeQueryType {
	if digestText == "" {
		return metadata.AttributeQueryTypeSelect // Default fallback
	}

	digestUpper := strings.ToUpper(strings.TrimSpace(digestText))

	switch {
	case strings.HasPrefix(digestUpper, "SELECT"):
		return metadata.AttributeQueryTypeSelect
	case strings.HasPrefix(digestUpper, "INSERT"):
		return metadata.AttributeQueryTypeInsert
	case strings.HasPrefix(digestUpper, "UPDATE"):
		return metadata.AttributeQueryTypeUpdate
	case strings.HasPrefix(digestUpper, "DELETE"):
		return metadata.AttributeQueryTypeDelete
	case strings.HasPrefix(digestUpper, "CREATE"):
		return metadata.AttributeQueryTypeCreate
	case strings.HasPrefix(digestUpper, "DROP"):
		return metadata.AttributeQueryTypeDrop
	case strings.HasPrefix(digestUpper, "ALTER"):
		return metadata.AttributeQueryTypeAlter
	case strings.HasPrefix(digestUpper, "SHOW"):
		return metadata.AttributeQueryTypeShow
	case strings.HasPrefix(digestUpper, "SET"):
		return metadata.AttributeQueryTypeSet
	case strings.HasPrefix(digestUpper, "BEGIN"):
		return metadata.AttributeQueryTypeBegin
	case strings.HasPrefix(digestUpper, "COMMIT"):
		return metadata.AttributeQueryTypeCommit
	case strings.HasPrefix(digestUpper, "ROLLBACK"):
		return metadata.AttributeQueryTypeRollback
	default:
		return metadata.AttributeQueryTypeSelect // Default fallback
	}
}

// getWaitEventType categorizes wait events into types
func (s *mySQLScraper) getWaitEventType(eventName string) metadata.AttributeWaitEventType {
	switch {
	case strings.Contains(eventName, "io"):
		return metadata.AttributeWaitEventTypeIo
	case strings.Contains(eventName, "lock"):
		return metadata.AttributeWaitEventTypeLock
	case strings.Contains(eventName, "cpu"):
		return metadata.AttributeWaitEventTypeCpu
	case strings.Contains(eventName, "network"):
		return metadata.AttributeWaitEventTypeNetwork
	case strings.Contains(eventName, "memory"):
		return metadata.AttributeWaitEventTypeMemory
	default:
		return metadata.AttributeWaitEventTypeOther
	}
}
