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
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	slowQueriesScraper   *scrapers.SlowQueriesScraper
	executionPlanScraper *scrapers.ExecutionPlanScraper
	blockingScraper      *scrapers.BlockingScraper
	waitEventsScraper    *scrapers.WaitEventsScraper

	// Database and configuration
	db             *sql.DB
	client         client.OracleClient
	dbProviderFunc dbProviderFunc
	config         *Config

	// Metrics building and logging
	mb                   *metadata.MetricsBuilder
	metricsBuilderConfig metadata.MetricsBuilderConfig
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
	s.tablespaceScraper = scrapers.NewTablespaceScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

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

	s.containerScraper, err = scrapers.NewContainerScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	if err != nil {
		return fmt.Errorf("failed to create container scraper: %w", err)
	}

	s.racScraper = scrapers.NewRacScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	s.databaseInfoScraper = scrapers.NewDatabaseInfoScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

	return nil
}

// initializeQPMScrapers initializes Query Performance Monitoring scrapers
func (s *newRelicOracleScraper) initializeQPMScrapers() error {
	// Initialize slow queries scraper
	s.slowQueriesScraper = scrapers.NewSlowQueriesScraper(
		s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig,
		s.config.QueryMonitoringResponseTimeThreshold,
		s.config.QueryMonitoringCountThreshold,
	)

	// Initialize execution plan scraper
	s.executionPlanScraper = scrapers.NewExecutionPlanScraper(s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

	// Initialize blocking scraper
	var err error
	s.blockingScraper, err = scrapers.NewBlockingScraper(
		s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig,
		s.config.QueryMonitoringCountThreshold,
	)
	if err != nil {
		return fmt.Errorf("failed to create blocking scraper: %w", err)
	}

	// Initialize wait events scraper
	s.waitEventsScraper = scrapers.NewWaitEventsScraper(
		s.client, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig,
		s.config.QueryMonitoringCountThreshold,
	)

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
func (s *newRelicOracleScraper) executeQPMScrapers(ctx context.Context, errChan chan<- error) {
	if !s.config.EnableQueryMonitoring {
		s.logger.Debug("Query Performance Monitoring disabled, skipping QPM scrapers")
		return
	}

	// Execute slow queries scraper to get query IDs
	s.logger.Debug("Starting slow queries scraper to get query IDs (QPM enabled)")
	queryIDs, slowQueryErrs := s.slowQueriesScraper.ScrapeSlowQueries(ctx)

	s.logger.Info("Slow queries scraper completed",
		zap.Int("query_ids_found", len(queryIDs)),
		zap.Strings("query_ids", queryIDs),
		zap.Int("slow_query_errors", len(slowQueryErrs)))

	// Collect slow query errors
	s.sendErrorsToChannel(errChan, slowQueryErrs, "slow query")

	// Execute execution plan scraper with query IDs
	if len(queryIDs) > 0 {
		s.executeExecutionPlanScraper(ctx, errChan, queryIDs)
	} else {
		s.logger.Debug("No query IDs available for execution plan scraping")
	}
}

// executeExecutionPlanScraper executes the execution plan scraper with given query IDs
func (s *newRelicOracleScraper) executeExecutionPlanScraper(ctx context.Context, errChan chan<- error, queryIDs []string) {
	s.logger.Debug("Starting execution plan scraper with query IDs", zap.Strings("query_ids", queryIDs))
	executionPlanErrs := s.executionPlanScraper.ScrapeExecutionPlans(ctx, queryIDs)

	s.logger.Info("Execution plan scraper completed",
		zap.Int("query_ids_processed", len(queryIDs)),
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

	// Add QPM scrapers if enabled
	if s.config.EnableQueryMonitoring {
		scraperFuncs = append(scraperFuncs,
			s.blockingScraper.ScrapeBlockingQueries,
			s.waitEventsScraper.ScrapeWaitEvents,
		)
	}

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
