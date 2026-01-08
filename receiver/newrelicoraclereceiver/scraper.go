// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"
)

// Constants for scraper configuration
const (
	// keepAlive defines the connection keepalive duration
	keepAlive = 30 * time.Second
	// maxErrors defines the buffer size for error channel to prevent blocking
	maxErrors = 100
)

// Type definitions
type (
	// ScraperFunc represents a function that scrapes metrics and returns errors
	ScraperFunc func(context.Context) []error

	// dbProviderFunc represents a function that provides database connections
	dbProviderFunc func() (*sql.DB, error)
)

// SQLIdentifierCache holds cached SQL identifiers with expiration
type SQLIdentifierCache struct {
	identifiers []models.SQLIdentifier
	timestamp   time.Time
	mutex       sync.RWMutex
}

// IsExpired checks if the cache is expired (older than 30 seconds)
func (c *SQLIdentifierCache) IsExpired() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return time.Since(c.timestamp) > 30*time.Second
}

// Get returns the cached SQL identifiers if not expired
func (c *SQLIdentifierCache) Get() ([]models.SQLIdentifier, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.IsExpired() {
		return nil, false
	}
	return c.identifiers, true
}

// Set merges new SQL identifiers with existing cached identifiers, removing duplicates
func (c *SQLIdentifierCache) Set(identifiers []models.SQLIdentifier) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Create a map to track existing identifiers for deduplication
	existingMap := make(map[string]bool)
	for _, existing := range c.identifiers {
		// Create a unique key based on SQL_ID and CHILD_NUMBER
		key := fmt.Sprintf("%s_%d", existing.SQLID, existing.ChildNumber)
		existingMap[key] = true
	}

	// Add new identifiers that don't already exist
	for _, newID := range identifiers {
		key := fmt.Sprintf("%s_%d", newID.SQLID, newID.ChildNumber)
		if !existingMap[key] {
			c.identifiers = append(c.identifiers, newID)
			existingMap[key] = true
		}
	}

	c.timestamp = time.Now()
}

// Global cache for sharing SQL identifiers between metrics and logs pipelines
var globalSQLIdentifierCache = &SQLIdentifierCache{}

// newRelicOracleScraper orchestrates all metric collection scrapers for Oracle database monitoring
type newRelicOracleScraper struct {
	// Core scrapers for basic database metrics
	sessionScraper      *scrapers.SessionScraper
	tablespaceScraper   *scrapers.TablespaceScraper
	coreScraper         *scrapers.CoreScraper
	pdbScraper          *scrapers.PdbScraper
	systemScraper       *scrapers.SystemScraper
	connectionScraper   *scrapers.ConnectionScraper
	containerScraper    *scrapers.ContainerScraper
	racScraper          *scrapers.RacScraper
	databaseInfoScraper *scrapers.DatabaseInfoScraper

	// Query Performance Monitoring (QPM) scrapers
	slowQueriesScraper       *scrapers.SlowQueriesScraper
	executionPlanScraper     *scrapers.ExecutionPlanScraper
	waitEventBlockingScraper *scrapers.WaitEventBlockingScraper
	childCursorsScraper      *scrapers.ChildCursorsScraper

	// Database and configuration
	db             *sql.DB
	client         client.OracleClient
	dbProviderFunc dbProviderFunc
	config         *Config

	// Metrics building and logging
	mb                   *metadata.MetricsBuilder
	metricsBuilderConfig metadata.MetricsBuilderConfig
	lb                   *metadata.LogsBuilder
	logsBuilderConfig    metadata.LogsBuilderConfig
	logger               *zap.Logger

	// Runtime configuration
	instanceName string
	hostName     string
	scrapeCfg    scraperhelper.ControllerConfig
	startTime    pcommon.Timestamp
}

// newScraper creates a new Oracle database metrics scraper
func newScraper(
	metricsBuilder *metadata.MetricsBuilder,
	metricsBuilderConfig metadata.MetricsBuilderConfig,
	logsBuilder *metadata.LogsBuilder,
	logsBuilderConfig metadata.LogsBuilderConfig,
	scrapeCfg scraperhelper.ControllerConfig,
	config *Config,
	logger *zap.Logger,
	providerFunc dbProviderFunc,
	instanceName, hostName string,
) (scraper.Metrics, error) {
	s := &newRelicOracleScraper{
		// Metrics and configuration
		mb:                   metricsBuilder,
		metricsBuilderConfig: metricsBuilderConfig,
		lb:                   logsBuilder,
		logsBuilderConfig:    logsBuilderConfig,
		config:               config,
		scrapeCfg:            scrapeCfg,

		// Database connection
		dbProviderFunc: providerFunc,

		// Runtime info
		logger:       logger,
		instanceName: instanceName,
		hostName:     hostName,
	}

	return scraper.NewMetrics(
		s.scrape,
		scraper.WithShutdown(s.shutdown),
		scraper.WithStart(s.start),
	)
}

// newLogsScraper creates a new logs scraper for Oracle execution plans
func newLogsScraper(
	logsBuilder *metadata.LogsBuilder,
	logsBuilderConfig metadata.LogsBuilderConfig,
	scrapeCfg scraperhelper.ControllerConfig,
	config *Config,
	logger *zap.Logger,
	providerFunc dbProviderFunc,
	instanceName, hostName string,
) (scraper.Logs, error) {
	s := &newRelicOracleScraper{
		// Logs and configuration
		lb:                logsBuilder,
		logsBuilderConfig: logsBuilderConfig,
		config:            config,
		scrapeCfg:         scrapeCfg,

		// Database connection
		dbProviderFunc: providerFunc,

		// Runtime info
		logger:       logger,
		instanceName: instanceName,
		hostName:     hostName,
	}

	return scraper.NewLogs(
		s.scrapeLogs,
		scraper.WithShutdown(s.shutdown),
		scraper.WithStart(s.startLogs),
	)
}

// start initializes the scraper and establishes database connections
func (s *newRelicOracleScraper) start(context.Context, component.Host) error {
	s.startTime = pcommon.NewTimestampFromTime(time.Now())

	// Establish database connection
	if err := s.initializeDatabase(); err != nil {
		return err
	}

	if err := s.initializeScrapers(); err != nil {
		return err
	}

	return nil
}

// initializeDatabase establishes the database connection
func (s *newRelicOracleScraper) initializeDatabase() error {
	db, err := s.dbProviderFunc()
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	s.db = db
	s.client = client.NewSQLClient(db)
	return nil
}

// initializeScrapers initializes all metric scrapers
func (s *newRelicOracleScraper) initializeScrapers() error {
	if err := s.initializeCoreScrapers(); err != nil {
		return err
	}

	if err := s.initializeQPMScrapers(); err != nil {
		return err
	}

	return nil
}

func (s *newRelicOracleScraper) initializeCoreScrapers() error {
	s.sessionScraper = scrapers.NewSessionScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	s.tablespaceScraper = scrapers.NewTablespaceScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig, s.config.TablespaceFilter.IncludeTablespaces, s.config.TablespaceFilter.ExcludeTablespaces)

	var err error
	s.coreScraper, err = scrapers.NewCoreScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	if err != nil {
		return fmt.Errorf("failed to create core scraper: %w", err)
	}

	s.pdbScraper = scrapers.NewPdbScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	s.systemScraper = scrapers.NewSystemScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

	s.connectionScraper, err = scrapers.NewConnectionScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection scraper: %w", err)
	}

	s.containerScraper, err = scrapers.NewContainerScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig, s.config.TablespaceFilter.IncludeTablespaces, s.config.TablespaceFilter.ExcludeTablespaces)
	if err != nil {
		return fmt.Errorf("failed to create container scraper: %w", err)
	}

	s.racScraper = scrapers.NewRacScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	s.databaseInfoScraper = scrapers.NewDatabaseInfoScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

	return nil
}

func (s *newRelicOracleScraper) initializeQPMScrapers() error {
	s.slowQueriesScraper = scrapers.NewSlowQueriesScraper(
		s.client, s.mb, s.logger, s.metricsBuilderConfig,
		s.config.QueryMonitoringResponseTimeThreshold,
		s.config.QueryMonitoringCountThreshold,
		s.config.QueryMonitoringIntervalSeconds,
		s.config.EnableIntervalBasedAveraging,
		s.config.IntervalCalculatorCacheTTLMinutes,
	)

	s.executionPlanScraper = scrapers.NewExecutionPlanScraper(s.client, s.lb, s.logger, s.logsBuilderConfig)

	var err error
	s.waitEventBlockingScraper, err = scrapers.NewWaitEventBlockingScraper(
		s.client, s.mb, s.logger, s.metricsBuilderConfig,
		s.config.QueryMonitoringCountThreshold,
	)
	if err != nil {
		return fmt.Errorf("failed to create wait event blocking scraper: %w", err)
	}

	s.childCursorsScraper = scrapers.NewChildCursorsScraper(s.client, s.mb, s.logger, s.metricsBuilderConfig)

	return nil
}

// scrape orchestrates the collection of all Oracle database metrics
func (s *newRelicOracleScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	startTime := time.Now()
	s.logger.Info("Begin New Relic Oracle scrape", zap.Time("start_time", startTime))

	scrapeCtx := s.createScrapeContext(ctx)

	errChan := make(chan error, maxErrors)
	var wg sync.WaitGroup

	s.executeQPMScrapers(scrapeCtx, errChan)

	s.executeIndependentScrapers(scrapeCtx, errChan, &wg)

	scrapeErrors := s.collectScrapingErrors(scrapeCtx, errChan, &wg)

	metrics := s.buildMetrics()

	s.logScrapeCompletion(scrapeErrors)

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	s.logger.Info("Completed New Relic Oracle scrape",
		zap.Time("end_time", endTime),
		zap.Duration("total_duration", duration),
		zap.Int("total_errors", len(scrapeErrors)))

	if len(scrapeErrors) > 0 {
		return metrics, scrapererror.NewPartialScrapeError(multierr.Combine(scrapeErrors...), len(scrapeErrors))
	}
	return metrics, nil
}

// scrapeLogs collects execution plan logs by reusing SQL identifiers from metrics pipeline cache
// If no cached SQL identifiers are available, the logs scrape is skipped
func (s *newRelicOracleScraper) scrapeLogs(ctx context.Context) (plog.Logs, error) {
	startTime := time.Now()
	s.logger.Info("Begin New Relic Oracle logs scrape", zap.Time("start_time", startTime))

	// Check if execution plan logs are enabled
	if !s.logsBuilderConfig.Events.NewrelicoracledbExecutionPlan.Enabled {
		s.logger.Debug("Execution plan event disabled, skipping logs scrape")
		return plog.NewLogs(), nil
	}

	// Check if QPM is enabled
	if !s.config.EnableQueryMonitoring {
		s.logger.Debug("Query Performance Monitoring disabled, skipping logs scrape")
		return plog.NewLogs(), nil
	}

	scrapeCtx := s.createScrapeContext(ctx)

	var sqlIdentifiers []models.SQLIdentifier
	var totalErrors []error

	// Try to get SQL identifiers from cache first
	if cachedIdentifiers, found := globalSQLIdentifierCache.Get(); found {
		sqlIdentifiers = cachedIdentifiers
		s.logger.Info("Using cached SQL identifiers from metrics pipeline",
			zap.Int("cached_identifiers", len(sqlIdentifiers)))
	} else {
		s.logger.Info("No cached SQL identifiers found, skipping logs scrape - logs pipeline depends on metrics pipeline")
	}

	// Execute execution plan scraper with the SQL identifiers
	if len(sqlIdentifiers) > 0 {
		s.logger.Debug("Starting execution plan scraping for logs",
			zap.Int("sql_identifiers", len(sqlIdentifiers)))

		executionPlanErrs := s.executionPlanScraper.ScrapeExecutionPlans(scrapeCtx, sqlIdentifiers)
		totalErrors = append(totalErrors, executionPlanErrs...)

		s.logger.Info("Execution plan scraping completed for logs",
			zap.Int("sql_identifiers_processed", len(sqlIdentifiers)),
			zap.Int("errors", len(executionPlanErrs)))
	} else {
		s.logger.Debug("No SQL identifiers available, skipping execution plan scraping for logs")
	}

	logs := s.lb.Emit()

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	s.logger.Info("Completed New Relic Oracle logs scrape",
		zap.Time("end_time", endTime),
		zap.Duration("total_duration", duration),
		zap.Int("total_errors", len(totalErrors)))

	return logs, nil
}

// startLogs initializes the logs scraper (execution plans only)
func (s *newRelicOracleScraper) startLogs(_ context.Context, _ component.Host) error {
	s.startTime = pcommon.NewTimestampFromTime(time.Now())

	if err := s.initializeDatabase(); err != nil {
		return err
	}

	s.logger.Info("Initializing logs scraper for execution plans")

	s.client = client.NewSQLClient(s.db)

	executionPlanScraper := scrapers.NewExecutionPlanScraper(
		s.client,
		s.lb,
		s.logger,
		s.logsBuilderConfig,
	)
	s.executionPlanScraper = executionPlanScraper

	return nil
}

// createScrapeContext creates a context with timeout for scraping operations
func (s *newRelicOracleScraper) createScrapeContext(ctx context.Context) context.Context {
	if s.scrapeCfg.Timeout > 0 {
		scrapeCtx, cancel := context.WithTimeout(ctx, s.scrapeCfg.Timeout)
		_ = cancel
		return scrapeCtx
	}
	return ctx
}

// executeQPMScrapers executes Query Performance Monitoring scrapers sequentially
func (s *newRelicOracleScraper) executeQPMScrapers(ctx context.Context, errChan chan<- error) {
	if !s.config.EnableQueryMonitoring {
		s.logger.Debug("Query Performance Monitoring disabled, skipping QPM scrapers")
		return
	}

	s.logger.Debug("Starting slow queries scraper")
	queryIDs, slowQueryErrs := s.slowQueriesScraper.ScrapeSlowQueries(ctx)

	s.logger.Info("Slow queries scraper completed",
		zap.Int("query_ids_found", len(queryIDs)),
		zap.Strings("query_ids", queryIDs),
		zap.Int("slow_query_errors", len(slowQueryErrs)))

	s.sendErrorsToChannel(errChan, slowQueryErrs, "slow query")

	s.logger.Debug("Starting wait events & blocking scraper for slow query SQL_IDs",
		zap.Int("slow_query_ids", len(queryIDs)))
	waitEventSQLIdentifiers, waitEventErrs := s.waitEventBlockingScraper.ScrapeWaitEventsAndBlocking(ctx, queryIDs)

	s.logger.Info("Wait events & blocking scraper completed",
		zap.Int("unique_sql_identifiers", len(waitEventSQLIdentifiers)),
		zap.Int("errors", len(waitEventErrs)))

	s.sendErrorsToChannel(errChan, waitEventErrs, "wait events & blocking")

	// Cache SQL identifiers for logs pipeline to reuse
	if len(waitEventSQLIdentifiers) > 0 {
		globalSQLIdentifierCache.Set(waitEventSQLIdentifiers)
		s.logger.Debug("Cached SQL identifiers for logs pipeline reuse",
			zap.Int("cached_identifiers", len(waitEventSQLIdentifiers)))

		s.executeChildCursorsAndExecutionPlans(ctx, errChan, waitEventSQLIdentifiers)
	} else {
		s.logger.Debug("No SQL identifiers from wait events, skipping child cursor and execution plan scraping")
	}
}

// executeChildCursorsAndExecutionPlans executes both child cursor and execution plan scrapers in parallel
func (s *newRelicOracleScraper) executeChildCursorsAndExecutionPlans(ctx context.Context, errChan chan<- error, sqlIdentifiers []models.SQLIdentifier) {
	s.logger.Debug("Starting parallel execution of child cursor and execution plan scrapers",
		zap.Int("sql_identifiers", len(sqlIdentifiers)))

	if len(sqlIdentifiers) == 0 {
		s.logger.Debug("No SQL identifiers from wait events")
		return
	}

	var wg sync.WaitGroup

	// Execute child cursors in parallel
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.logger.Debug("Starting child cursor scraping from wait events",
			zap.Int("sql_identifiers", len(sqlIdentifiers)))

		// Create a separate context with extended timeout for child cursors since they can be slow
		childCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		childCursorErrs := s.childCursorsScraper.ScrapeChildCursorsForIdentifiers(childCtx, sqlIdentifiers, s.config.ChildCursorsPerSQLID)
		if len(childCursorErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping child cursor metrics",
				zap.Int("error_count", len(childCursorErrs)),
				zap.Int("sql_identifiers_attempted", len(sqlIdentifiers)))
			s.sendErrorsToChannelWithCancellation(ctx, errChan, childCursorErrs, -1)
		} else {
			s.logger.Debug("Child cursor scraping completed successfully with no errors")
		}

		s.logger.Info("Child cursor scraping completed",
			zap.Int("sql_identifiers_processed", len(sqlIdentifiers)),
			zap.Int("errors", len(childCursorErrs)))
	}()

	// Execute execution plans in parallel (only if logs scraper is enabled and execution plan scraper exists)
	if s.executionPlanScraper != nil && s.logsBuilderConfig.Events.NewrelicoracledbExecutionPlan.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Debug("Starting execution plan scraping from wait events",
				zap.Int("sql_identifiers", len(sqlIdentifiers)))

			// Create a separate context with timeout for execution plans
			planCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			executionPlanErrs := s.executionPlanScraper.ScrapeExecutionPlans(planCtx, sqlIdentifiers)
			if len(executionPlanErrs) > 0 {
				s.logger.Warn("Errors occurred while scraping execution plans",
					zap.Int("error_count", len(executionPlanErrs)),
					zap.Int("sql_identifiers_attempted", len(sqlIdentifiers)))
				s.sendErrorsToChannelWithCancellation(ctx, errChan, executionPlanErrs, -2)
			} else {
				s.logger.Debug("Execution plan scraping completed successfully with no errors")
			}

			s.logger.Info("Execution plan scraping completed",
				zap.Int("sql_identifiers_processed", len(sqlIdentifiers)),
				zap.Int("errors", len(executionPlanErrs)))
		}()
	} else {
		s.logger.Debug("Execution plan scraper not available or disabled, skipping parallel execution plan scraping")
	}

	wg.Wait()
	s.logger.Debug("Parallel child cursor and execution plan scraping completed")
}

// executeChildCursors executes child cursor scraper for SQL identifiers from wait events
func (s *newRelicOracleScraper) executeChildCursors(ctx context.Context, errChan chan<- error, sqlIdentifiers []models.SQLIdentifier) {
	s.logger.Debug("Starting child cursor scraping from wait events",
		zap.Int("sql_identifiers", len(sqlIdentifiers)))

	if len(sqlIdentifiers) == 0 {
		s.logger.Debug("No SQL identifiers from wait events")
		return
	}

	childCursorErrs := s.childCursorsScraper.ScrapeChildCursorsForIdentifiers(ctx, sqlIdentifiers, s.config.ChildCursorsPerSQLID)
	if len(childCursorErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping child cursor metrics",
			zap.Int("error_count", len(childCursorErrs)))
		s.sendErrorsToChannel(errChan, childCursorErrs, "child cursors")
	}

	s.logger.Info("Child cursor scraping completed",
		zap.Int("sql_identifiers_processed", len(sqlIdentifiers)),
		zap.Int("errors", len(childCursorErrs)))
}

// executeIndependentScrapers launches concurrent scrapers for independent metrics
func (s *newRelicOracleScraper) executeIndependentScrapers(ctx context.Context, errChan chan<- error, wg *sync.WaitGroup) {
	scraperFuncs := s.getIndependentScraperFunctions()

	for i, scraperFunc := range scraperFuncs {
		wg.Add(1)
		go s.runScraperWithErrorHandling(ctx, errChan, wg, i, scraperFunc)
	}
}

// getIndependentScraperFunctions returns the list of scraper functions that can run independently
func (s *newRelicOracleScraper) getIndependentScraperFunctions() []ScraperFunc {
	scraperFuncs := []ScraperFunc{
		s.sessionScraper.ScrapeSessionCount,
		s.tablespaceScraper.ScrapeTablespaceMetrics,
		s.coreScraper.ScrapeCoreMetrics,
		s.pdbScraper.ScrapePdbMetrics,
		s.systemScraper.ScrapeSystemMetrics,
		s.connectionScraper.ScrapeConnectionMetrics,
		s.containerScraper.ScrapeContainerMetrics,
		s.racScraper.ScrapeRacMetrics,
		s.databaseInfoScraper.ScrapeDatabaseInfo,
		s.databaseInfoScraper.ScrapeHostingInfo,
		s.databaseInfoScraper.ScrapeDatabaseRole,
	}

	return scraperFuncs
}

// runScraperWithErrorHandling executes a single scraper function with proper error handling
func (s *newRelicOracleScraper) runScraperWithErrorHandling(ctx context.Context, errChan chan<- error, wg *sync.WaitGroup, index int, scraperFunc ScraperFunc) {
	defer wg.Done()

	select {
	case <-ctx.Done():
		s.logger.Debug("Context cancelled before starting scraper",
			zap.Int("scraper_index", index),
			zap.Error(ctx.Err()))
		return
	default:
	}

	s.logger.Debug("Starting scraper", zap.Int("scraper_index", index))
	startTime := time.Now()

	errs := scraperFunc(ctx)

	duration := time.Since(startTime)
	s.logger.Debug("Completed scraper",
		zap.Int("scraper_index", index),
		zap.Duration("duration", duration),
		zap.Int("error_count", len(errs)))

	s.sendErrorsToChannelWithCancellation(ctx, errChan, errs, index)
}

// collectScrapingErrors collects all errors from concurrent scrapers
func (s *newRelicOracleScraper) collectScrapingErrors(ctx context.Context, errChan chan error, wg *sync.WaitGroup) []error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
		close(errChan)
	}()

	var scrapeErrors []error
	collecting := true

	for collecting {
		select {
		case err, ok := <-errChan:
			if !ok {
				collecting = false
				break
			}
			scrapeErrors = append(scrapeErrors, err)
		case <-ctx.Done():
			s.logger.Warn("Context cancelled while collecting errors", zap.Error(ctx.Err()))
			scrapeErrors = append(scrapeErrors, fmt.Errorf("scrape operation cancelled: %w", ctx.Err()))
			collecting = false
		case <-done:
			for err := range errChan {
				scrapeErrors = append(scrapeErrors, err)
			}
			collecting = false
		}
	}

	return scrapeErrors
}

// buildMetrics constructs the final metrics output
func (s *newRelicOracleScraper) buildMetrics() pmetric.Metrics {
	rb := s.mb.NewResourceBuilder()
	rb.SetNewrelicoracledbInstanceName(s.instanceName)
	rb.SetHostName(s.hostName)
	return s.mb.Emit(metadata.WithResource(rb.Emit()))
}

// logScrapeCompletion logs the completion of the scraping operation
func (s *newRelicOracleScraper) logScrapeCompletion(scrapeErrors []error) {
	scraperCount := len(s.getIndependentScraperFunctions())
	s.logger.Debug("Done New Relic Oracle scraping",
		zap.Int("total_errors", len(scrapeErrors)),
		zap.Int("scrapers_executed", scraperCount),
	)
}

// sendErrorsToChannel sends a slice of errors to the error channel
func (s *newRelicOracleScraper) sendErrorsToChannel(errChan chan<- error, errs []error, scraperType string) {
	for _, err := range errs {
		select {
		case errChan <- err:
		default:
			s.logger.Warn("Error channel full, dropping error",
				zap.Error(err),
				zap.String("scraper_type", scraperType))
		}
	}
}

// sendErrorsToChannelWithCancellation sends errors to channel with context cancellation check
func (s *newRelicOracleScraper) sendErrorsToChannelWithCancellation(ctx context.Context, errChan chan<- error, errs []error, scraperIndex int) {
	for _, err := range errs {
		select {
		case errChan <- err:
		case <-ctx.Done():
			s.logger.Debug("Context cancelled while sending errors",
				zap.Int("scraper_index", scraperIndex),
				zap.Error(ctx.Err()))
			return
		default:
			s.logger.Warn("Error channel full, dropping error",
				zap.Error(err),
				zap.Int("scraper_index", scraperIndex))
		}
	}
}

// shutdown closes the database connection
func (s *newRelicOracleScraper) shutdown(_ context.Context) error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
