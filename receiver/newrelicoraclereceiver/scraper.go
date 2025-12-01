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
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
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
	slowQueriesScraper         *scrapers.SlowQueriesScraper
	executionPlanScraper       *scrapers.ExecutionPlanScraper
	waitEventBlockingScraper   *scrapers.WaitEventBlockingScraper
	lockScraper                *scrapers.LockScraper
	childCursorsScraper        *scrapers.ChildCursorsScraper

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

	// Initialize all scrapers
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
	// Initialize core database scrapers
	if err := s.initializeCoreScrapers(); err != nil {
		return err
	}

	// Initialize Query Performance Monitoring scrapers (with error handling)
	if err := s.initializeQPMScrapers(); err != nil {
		return err
	}

	return nil
}

// initializeCoreScrapers initializes basic database metric scrapers
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

// initializeQPMScrapers initializes Query Performance Monitoring scrapers
func (s *newRelicOracleScraper) initializeQPMScrapers() error {
	// Initialize slow queries scraper with interval calculator configuration
	s.slowQueriesScraper = scrapers.NewSlowQueriesScraper(
		s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig,
		s.config.QueryMonitoringResponseTimeThreshold,
		s.config.QueryMonitoringCountThreshold,
		s.config.QueryMonitoringIntervalSeconds,
		s.config.EnableIntervalBasedAveraging,
		s.config.IntervalCalculatorCacheTTLMinutes,
	)

	// Initialize execution plan scraper (uses LogsBuilder for log events)
	s.executionPlanScraper = scrapers.NewExecutionPlanScraper(s.client, s.lb, s.logger, s.instanceName, s.logsBuilderConfig)

	// Initialize combined wait events and blocking scraper (replaces separate BlockingScraper and WaitEventsScraper)
	var err error
	s.waitEventBlockingScraper, err = scrapers.NewWaitEventBlockingScraper(
		s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig,
		s.config.QueryMonitoringCountThreshold,
	)
	if err != nil {
		return fmt.Errorf("failed to create wait event blocking scraper: %w", err)
	}

	// Initialize child cursors scraper (fetches V$SQL child cursors)
	s.childCursorsScraper = scrapers.NewChildCursorsScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

	// Initialize lock scraper
	s.lockScraper = scrapers.NewLockScraper(s.logger, s.client, s.mb, s.instanceName)

	return nil
}

// scrape orchestrates the collection of all Oracle database metrics
func (s *newRelicOracleScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	s.logger.Debug("Begin New Relic Oracle scrape")

	// Setup scrape context with timeout
	scrapeCtx := s.createScrapeContext(ctx)

	// Initialize error collection
	errChan := make(chan error, maxErrors)
	var wg sync.WaitGroup

	// Execute QPM scrapers sequentially (if enabled)
	s.executeQPMScrapers(scrapeCtx, errChan)

	// Execute independent scrapers concurrently
	s.executeIndependentScrapers(scrapeCtx, errChan, &wg)

	// Collect all errors
	scrapeErrors := s.collectScrapingErrors(scrapeCtx, errChan, &wg)

	// Build and return metrics
	metrics := s.buildMetrics()

	s.logScrapeCompletion(scrapeErrors)

	if len(scrapeErrors) > 0 {
		return metrics, scrapererror.NewPartialScrapeError(multierr.Combine(scrapeErrors...), len(scrapeErrors))
	}
	return metrics, nil
}

// scrapeLogs orchestrates the collection of Oracle execution plans as logs
func (s *newRelicOracleScraper) scrapeLogs(ctx context.Context) (plog.Logs, error) {
	s.logger.Debug("Begin New Relic Oracle logs scrape for execution plans")

	logs := plog.NewLogs()

	// Only scrape execution plans if event is enabled
	if !s.logsBuilderConfig.Events.NewrelicoracledbExecutionPlan.Enabled {
		s.logger.Debug("Execution plan event disabled, skipping logs scrape")
		return logs, nil
	}

	// Check if QPM is enabled
	if !s.config.EnableQueryMonitoring {
		s.logger.Debug("Query Performance Monitoring disabled, skipping execution plan scrape")
		return logs, nil
	}

	// Setup scrape context with timeout
	scrapeCtx := s.createScrapeContext(ctx)

	// Get query IDs without emitting metrics (metrics already emitted by metrics scraper)
	s.logger.Debug("Fetching query IDs for execution plans (no metrics)")
	queryIDs, slowQueryErrs := s.slowQueriesScraper.GetSlowQueryIDs(scrapeCtx)

	if len(slowQueryErrs) > 0 {
		s.logger.Warn("Errors occurred while getting query IDs for execution plans",
			zap.Int("error_count", len(slowQueryErrs)))
	}

	s.logger.Info("Query IDs fetched for execution plans",
		zap.Int("query_ids_found", len(queryIDs)),
		zap.Strings("query_ids", queryIDs))

	if len(queryIDs) == 0 {
		s.logger.Debug("No query IDs found, skipping execution plan scrape")
		return logs, nil
	}

	// Scrape execution plans (which now emits logs)
	s.logger.Debug("Starting execution plan scraper for logs", zap.Strings("query_ids", queryIDs))

	// Safety check: ensure waitEventBlockingScraper is initialized
	if s.waitEventBlockingScraper == nil {
		err := fmt.Errorf("waitEventBlockingScraper is not initialized")
		s.logger.Error("Cannot get SQL identifiers for execution plans", zap.Error(err))
		return logs, err
	}

	// Get SQL identifiers from V$SQL child cursors + wait events (optimized method)
	// We only need the identifiers for execution plans in logs, don't need metrics
	_, _, sqlIdentifiers, err := s.waitEventBlockingScraper.GetChildCursorsWithMetrics(scrapeCtx, queryIDs, s.config.ChildCursorsPerSQLID)
	if err != nil {
		s.logger.Warn("Failed to get SQL identifiers for execution plan logs", zap.Error(err))
		return logs, err
	}

	if len(sqlIdentifiers) == 0 {
		s.logger.Info("No SQL identifiers found for execution plan logs")
		return logs, nil
	}

	executionPlanErrs := s.executionPlanScraper.ScrapeExecutionPlans(scrapeCtx, sqlIdentifiers)

	s.logger.Info("Execution plan scraper completed for logs",
		zap.Int("sql_identifiers_processed", len(sqlIdentifiers)),
		zap.Int("execution_plan_errors", len(executionPlanErrs)))

	// Emit logs
	logs = s.lb.Emit()

	if len(executionPlanErrs) > 0 {
		return logs, scrapererror.NewPartialScrapeError(multierr.Combine(executionPlanErrs...), len(executionPlanErrs))
	}

	return logs, nil
}

// startLogs initializes the logs scraper (execution plans only)
func (s *newRelicOracleScraper) startLogs(_ context.Context, _ component.Host) error {
	s.startTime = pcommon.NewTimestampFromTime(time.Now())

	// Establish database connection
	if err := s.initializeDatabase(); err != nil {
		return err
	}

	// Initialize only the scrapers needed for logs (slow queries + execution plans)
	s.logger.Info("Initializing logs scrapers for execution plans")

	// Initialize Oracle client
	s.client = client.NewSQLClient(s.db)

	// Initialize temporary metrics builder for slow queries scraper (needed to get query IDs)
	// We need a metrics builder because slow queries scraper uses it, even though we only need the IDs
	tempSettings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: s.logger,
		},
	}
	tempMb := metadata.NewMetricsBuilder(metadata.DefaultMetricsBuilderConfig(), tempSettings)

	// Initialize slow queries scraper with interval calculator configuration
	slowQueriesScraper := scrapers.NewSlowQueriesScraper(
		s.client,
		tempMb,
		s.logger,
		s.instanceName,
		metadata.DefaultMetricsBuilderConfig(),
		s.config.QueryMonitoringResponseTimeThreshold,
		s.config.QueryMonitoringCountThreshold,
		s.config.QueryMonitoringIntervalSeconds,
		s.config.EnableIntervalBasedAveraging,
		s.config.IntervalCalculatorCacheTTLMinutes,
	)
	s.slowQueriesScraper = slowQueriesScraper

	// Initialize execution plan scraper
	executionPlanScraper := scrapers.NewExecutionPlanScraper(
		s.client,
		s.lb,
		s.logger,
		s.instanceName,
		s.logsBuilderConfig,
	)
	s.executionPlanScraper = executionPlanScraper

	// Initialize combined wait events and blocking scraper (needed for execution plan SQL identifier fetching)
	waitEventBlockingScraper, err := scrapers.NewWaitEventBlockingScraper(
		s.client,
		tempMb,
		s.logger,
		s.instanceName,
		metadata.DefaultMetricsBuilderConfig(),
		s.config.QueryMonitoringCountThreshold,
	)
	if err != nil {
		return fmt.Errorf("failed to create wait event blocking scraper for logs: %w", err)
	}
	s.waitEventBlockingScraper = waitEventBlockingScraper

	return nil
}

// createScrapeContext creates a context with timeout for scraping operations
func (s *newRelicOracleScraper) createScrapeContext(ctx context.Context) context.Context {
	if s.scrapeCfg.Timeout > 0 {
		scrapeCtx, cancel := context.WithTimeout(ctx, s.scrapeCfg.Timeout)
		// Note: cancel is intentionally not deferred here as it's managed by the caller
		_ = cancel
		return scrapeCtx
	}
	return ctx
}

// executeQPMScrapers executes Query Performance Monitoring scrapers sequentially
// Flow:
// 1. Slow queries scraper → get query IDs
// 2. Wait events & blocking scraper → capture current wait states
// 3. Child cursors scraper → fetch latest child cursor metrics (after wait events)
// 4. Execution plan scraper → fetch plans for identified SQL identifiers
func (s *newRelicOracleScraper) executeQPMScrapers(ctx context.Context, errChan chan<- error) {
	if !s.config.EnableQueryMonitoring {
		s.logger.Debug("Query Performance Monitoring disabled, skipping QPM scrapers")
		return
	}

	// STEP 1: Execute slow queries scraper to get query IDs
	s.logger.Debug("Step 1: Starting slow queries scraper")
	queryIDs, slowQueryErrs := s.slowQueriesScraper.ScrapeSlowQueries(ctx)

	s.logger.Info("Slow queries scraper completed",
		zap.Int("query_ids_found", len(queryIDs)),
		zap.Strings("query_ids", queryIDs),
		zap.Int("slow_query_errors", len(slowQueryErrs)))

	s.sendErrorsToChannel(errChan, slowQueryErrs, "slow query")

	// STEP 2: Execute wait events & blocking scraper
	// This captures current wait states and blocking information
	// Must run BEFORE child cursors to ensure we get latest active child numbers
	s.logger.Debug("Step 2: Starting wait events & blocking scraper")
	waitEventErrs := s.waitEventBlockingScraper.ScrapeWaitEventsAndBlocking(ctx)

	s.logger.Info("Wait events & blocking scraper completed",
		zap.Int("errors", len(waitEventErrs)))

	s.sendErrorsToChannel(errChan, waitEventErrs, "wait events & blocking")

	// STEP 3 & 4: Execute child cursors and execution plans with query IDs
	if len(queryIDs) > 0 {
		s.executeChildCursorsAndExecutionPlans(ctx, errChan, queryIDs)
	} else {
		s.logger.Debug("No query IDs available for child cursor and execution plan scraping")
	}
}

// executeChildCursorsAndExecutionPlans executes child cursor and execution plan scrapers with given query IDs
// Flow:
// 1. Slow Query IDs → V$SQL top 5 child cursors
// 2. Check wait events for NEW child numbers not in V$SQL top N
// 3. Merge both lists to get complete SQL identifiers
// 4. Scrape child cursor metrics for ALL identified SQL_IDs (including new ones from wait events)
// 5. Fetch execution plans for merged SQL identifiers
func (s *newRelicOracleScraper) executeChildCursorsAndExecutionPlans(ctx context.Context, errChan chan<- error, queryIDs []string) {
	s.logger.Debug("Starting execution plan scraper with query IDs", zap.Strings("query_ids", queryIDs))

	// Safety check: ensure waitEventBlockingScraper is initialized
	if s.waitEventBlockingScraper == nil {
		err := fmt.Errorf("waitEventBlockingScraper is not initialized")
		s.logger.Error("Cannot get SQL identifiers for execution plans", zap.Error(err))
		s.sendErrorsToChannel(errChan, []error{err}, "execution plan - scraper not initialized")
		return
	}

	// OPTIMIZED FLOW: Get child cursors WITH metrics and SQL identifiers in ONE call
	// This avoids duplicate V$SQL queries
	// Returns:
	// 1. cachedChildCursors: Full child cursor objects with metrics from V$SQL top N
	// 2. newIdentifiers: NEW child numbers found in wait events (not in V$SQL top N)
	// 3. allSQLIdentifiers: Merged list for execution plans
	cachedChildCursors, newIdentifiers, allSQLIdentifiers, err := s.waitEventBlockingScraper.GetChildCursorsWithMetrics(ctx, queryIDs, s.config.ChildCursorsPerSQLID)
	if err != nil {
		s.logger.Warn("Failed to get child cursors and SQL identifiers", zap.Error(err))
		s.sendErrorsToChannel(errChan, []error{err}, "execution plan - get child cursors")
		return
	}

	if len(allSQLIdentifiers) == 0 {
		s.logger.Info("No SQL identifiers found for execution plans",
			zap.Int("slow_query_ids_checked", len(queryIDs)))
		return
	}

	s.logger.Info("Fetching child cursor metrics and execution plans",
		zap.Int("cached_child_cursors", len(cachedChildCursors)),
		zap.Int("new_identifiers_from_wait_events", len(newIdentifiers)),
		zap.Int("total_sql_identifiers", len(allSQLIdentifiers)),
		zap.Int("slow_query_ids", len(queryIDs)))

	// OPTIMIZED: Scrape child cursor metrics using cached data
	// Only queries V$SQL for NEW child numbers found in wait events
	childCursorErrs := s.childCursorsScraper.ScrapeChildCursorsWithCache(ctx, cachedChildCursors, newIdentifiers, s.config.ChildCursorsPerSQLID)
	if len(childCursorErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping child cursor metrics",
			zap.Int("error_count", len(childCursorErrs)))
		s.sendErrorsToChannel(errChan, childCursorErrs, "child cursors")
	}

	// Fetch execution plans for the merged SQL identifiers
	executionPlanErrs := s.executionPlanScraper.ScrapeExecutionPlans(ctx, allSQLIdentifiers)

	s.logger.Info("Execution plan scraper completed",
		zap.Int("sql_identifiers_processed", len(allSQLIdentifiers)),
		zap.Int("execution_plan_errors", len(executionPlanErrs)))

	s.sendErrorsToChannel(errChan, executionPlanErrs, "execution plan")
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

	// Note: Wait events & blocking scraper removed from parallel execution
	// It now runs sequentially in executeQPMScrapers() BEFORE child cursors
	// This ensures we capture latest active child numbers before fetching metrics

	// Add lock scraper (always enabled for lock monitoring)
	scraperFuncs = append(scraperFuncs,
		s.lockScraper.ScrapeLocks,
	)

	return scraperFuncs
}

// runScraperWithErrorHandling executes a single scraper function with proper error handling
func (s *newRelicOracleScraper) runScraperWithErrorHandling(ctx context.Context, errChan chan<- error, wg *sync.WaitGroup, index int, scraperFunc ScraperFunc) {
	defer wg.Done()

	// Check for context cancellation before starting
	select {
	case <-ctx.Done():
		s.logger.Debug("Context cancelled before starting scraper",
			zap.Int("scraper_index", index),
			zap.Error(ctx.Err()))
		return
	default:
	}

	// Execute scraper with timing
	s.logger.Debug("Starting scraper", zap.Int("scraper_index", index))
	startTime := time.Now()

	errs := scraperFunc(ctx)

	duration := time.Since(startTime)
	s.logger.Debug("Completed scraper",
		zap.Int("scraper_index", index),
		zap.Duration("duration", duration),
		zap.Int("error_count", len(errs)))

	// Send errors to channel with cancellation check
	s.sendErrorsToChannelWithCancellation(ctx, errChan, errs, index)
}

// collectScrapingErrors collects all errors from concurrent scrapers
func (s *newRelicOracleScraper) collectScrapingErrors(ctx context.Context, errChan chan error, wg *sync.WaitGroup) []error {
	// Setup cleanup
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
			// Collect remaining errors
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
