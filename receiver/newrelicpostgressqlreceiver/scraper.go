// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgressqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgressqlreceiver/scrapers"
)

// Type definitions
type (
	dbProviderFunc func() (*sql.DB, error)
)

// newRelicPostgreSQLScraper orchestrates all metric collection scrapers for PostgreSQL database monitoring
type newRelicPostgreSQLScraper struct {
	// Scrapers
	databaseMetricsScraper *scrapers.DatabaseMetricsScraper

	// Database and configuration
	db             *sql.DB
	client         client.PostgreSQLClient
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

// newScraper creates a new PostgreSQL database metrics scraper
func newScraper(
	metricsBuilder *metadata.MetricsBuilder,
	metricsBuilderConfig metadata.MetricsBuilderConfig,
	scrapeCfg scraperhelper.ControllerConfig,
	config *Config,
	logger *zap.Logger,
	providerFunc dbProviderFunc,
	instanceName, hostName string,
) (scraper.Metrics, error) {
	s := &newRelicPostgreSQLScraper{
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
func (s *newRelicPostgreSQLScraper) start(context.Context, component.Host) error {
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
func (s *newRelicPostgreSQLScraper) initializeDatabase() error {
	db, err := s.dbProviderFunc()
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	s.db = db
	s.client = client.NewSQLClient(db)
	return nil
}

// initializeScrapers initializes all metric scrapers
func (s *newRelicPostgreSQLScraper) initializeScrapers() error {
	s.databaseMetricsScraper = scrapers.NewDatabaseMetricsScraper(
		s.client,
		s.mb,
		s.logger,
		s.instanceName,
		s.metricsBuilderConfig,
	)

	return nil
}

// scrape orchestrates the collection of all PostgreSQL database metrics
func (s *newRelicPostgreSQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	startTime := time.Now()
	s.logger.Info("Begin New Relic PostgreSQL scrape", zap.Time("start_time", startTime))

	scrapeCtx, cancel := s.createScrapeContext(ctx)
	defer cancel() // Ensure context resources are released

	var scrapeErrors []error

	// Scrape database metrics
	dbErrs := s.databaseMetricsScraper.ScrapeDatabaseMetrics(scrapeCtx)
	if len(dbErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping database metrics",
			zap.Int("error_count", len(dbErrs)))
		scrapeErrors = append(scrapeErrors, dbErrs...)
	}

	metrics := s.buildMetrics()

	s.logScrapeCompletion(scrapeErrors)

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	s.logger.Info("Completed New Relic PostgreSQL scrape",
		zap.Time("end_time", endTime),
		zap.Duration("total_duration", duration),
		zap.Int("total_errors", len(scrapeErrors)))

	if len(scrapeErrors) > 0 {
		return metrics, scrapererror.NewPartialScrapeError(multierr.Combine(scrapeErrors...), len(scrapeErrors))
	}
	return metrics, nil
}

// createScrapeContext creates a context with timeout for scraping operations
// Returns the context and a cancel function that must be called to release resources
func (s *newRelicPostgreSQLScraper) createScrapeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.scrapeCfg.Timeout > 0 {
		return context.WithTimeout(ctx, s.scrapeCfg.Timeout)
	}
	return ctx, func() {} // Return no-op cancel function if no timeout
}

// buildMetrics constructs the final metrics output
func (s *newRelicPostgreSQLScraper) buildMetrics() pmetric.Metrics {
	rb := s.mb.NewResourceBuilder()
	rb.SetNewrelicpostgresqlInstanceName(s.instanceName)
	return s.mb.Emit(metadata.WithResource(rb.Emit()))
}

// logScrapeCompletion logs the completion of the scraping operation
func (s *newRelicPostgreSQLScraper) logScrapeCompletion(scrapeErrors []error) {
	s.logger.Debug("Done New Relic PostgreSQL scraping",
		zap.Int("total_errors", len(scrapeErrors)))
}

// shutdown closes the database connection
func (s *newRelicPostgreSQLScraper) shutdown(_ context.Context) error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
