// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicpostgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicpostgresqlreceiver/scrapers"
)

// Type definitions
type (
	dbProviderFunc func() (*sql.DB, error)
)

// PostgreSQL version constants
const (
	PG96Version = 90600  // PostgreSQL 9.6
	PG10Version = 100000 // PostgreSQL 10.0
	PG12Version = 120000 // PostgreSQL 12.0
	PG14Version = 140000 // PostgreSQL 14.0
)

// newRelicPostgreSQLScraper orchestrates all metric collection scrapers for PostgreSQL database monitoring
type newRelicPostgreSQLScraper struct {
	// Scrapers
	databaseMetricsScraper    *scrapers.DatabaseMetricsScraper
	replicationMetricsScraper *scrapers.ReplicationScraper
	bgwriterScraper           *scrapers.BgwriterScraper

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
	pgVersion    int  // PostgreSQL version number
	supportsPG96 bool // Whether PostgreSQL version is 9.6 or higher
	supportsPG12 bool // Whether PostgreSQL version is 12 or higher
	supportsPG14 bool // Whether PostgreSQL version is 14 or higher
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
	// Detect PostgreSQL version
	version, err := s.client.GetVersion(context.Background())
	if err != nil {
		s.logger.Warn("Failed to detect PostgreSQL version, version-specific features will be disabled", zap.Error(err))
		s.pgVersion = 0
		s.supportsPG96 = false
		s.supportsPG12 = false
		s.supportsPG14 = false
	} else {
		s.pgVersion = version
		// Version format: Major * 10000 + Minor * 100 + Patch
		// PostgreSQL 9.6 = 90600, PostgreSQL 12.0 = 120000, PostgreSQL 14.0 = 140000
		s.supportsPG96 = version >= PG96Version
		s.supportsPG12 = version >= PG12Version
		s.supportsPG14 = version >= PG14Version
		s.logger.Info("PostgreSQL version detected",
			zap.Int("version", version),
			zap.Bool("supports_pg96_features", s.supportsPG96),
			zap.Bool("supports_pg12_features", s.supportsPG12),
			zap.Bool("supports_pg14_features", s.supportsPG14))
	}

	// Initialize database metrics scraper (always available)
	s.databaseMetricsScraper = scrapers.NewDatabaseMetricsScraper(
		s.client,
		s.mb,
		s.logger,
		s.instanceName,
		s.metricsBuilderConfig,
		s.supportsPG12,
	)

	// Initialize replication metrics scraper only if PostgreSQL 9.6+
	if s.supportsPG96 {
		s.replicationMetricsScraper = scrapers.NewReplicationScraper(
			s.client,
			s.mb,
			s.logger,
			s.instanceName,
			s.metricsBuilderConfig,
			s.pgVersion,
		)
		s.logger.Info("Replication metrics scraper enabled (PostgreSQL 9.6+)")
	} else {
		s.logger.Info("Replication metrics scraper disabled (requires PostgreSQL 9.6+)")
	}

	// Initialize background writer scraper only if PostgreSQL 9.6+
	if s.supportsPG96 {
		s.bgwriterScraper = scrapers.NewBgwriterScraper(
			s.client,
			s.mb,
			s.logger,
			s.instanceName,
			s.metricsBuilderConfig,
			s.pgVersion,
		)
		s.logger.Info("Background writer scraper enabled (PostgreSQL 9.6+)")
	} else {
		s.logger.Info("Background writer scraper disabled (requires PostgreSQL 9.6+)")
	}

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

	// Scrape server uptime (PostgreSQL 9.6+, available on all versions we support)
	uptimeErrs := s.databaseMetricsScraper.ScrapeServerUptime(scrapeCtx)
	if len(uptimeErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping server uptime",
			zap.Int("error_count", len(uptimeErrs)))
		scrapeErrors = append(scrapeErrors, uptimeErrs...)
	}

	// Scrape database count (PostgreSQL 9.6+, available on all versions we support)
	dbCountErrs := s.databaseMetricsScraper.ScrapeDatabaseCount(scrapeCtx)
	if len(dbCountErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping database count",
			zap.Int("error_count", len(dbCountErrs)))
		scrapeErrors = append(scrapeErrors, dbCountErrs...)
	}

	// Scrape running status health check (PostgreSQL 9.6+, available on all versions we support)
	runningErrs := s.databaseMetricsScraper.ScrapeRunningStatus(scrapeCtx)
	if len(runningErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping running status",
			zap.Int("error_count", len(runningErrs)))
		scrapeErrors = append(scrapeErrors, runningErrs...)
	}

	// Scrape conflict metrics (all supported versions)
	conflictErrs := s.databaseMetricsScraper.ScrapeConflictMetrics(scrapeCtx)
	if len(conflictErrs) > 0 {
		s.logger.Warn("Errors occurred while scraping conflict metrics",
			zap.Int("error_count", len(conflictErrs)))
		scrapeErrors = append(scrapeErrors, conflictErrs...)
	}

	// Scrape session metrics (PostgreSQL 14+ only)
	if s.supportsPG14 {
		sessionErrs := s.databaseMetricsScraper.ScrapeSessionMetrics(scrapeCtx)
		if len(sessionErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping session metrics",
				zap.Int("error_count", len(sessionErrs)))
			scrapeErrors = append(scrapeErrors, sessionErrs...)
		}
	}

	// Scrape replication metrics (PostgreSQL 9.6+ only)
	if s.supportsPG96 && s.replicationMetricsScraper != nil {
		replicationErrs := s.replicationMetricsScraper.ScrapeReplicationMetrics(scrapeCtx)
		if len(replicationErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping replication metrics",
				zap.Int("error_count", len(replicationErrs)))
			scrapeErrors = append(scrapeErrors, replicationErrs...)
		}

		// Scrape replication slot metrics (PostgreSQL 9.4+ only)
		slotErrs := s.replicationMetricsScraper.ScrapeReplicationSlots(scrapeCtx)
		if len(slotErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping replication slot metrics",
				zap.Int("error_count", len(slotErrs)))
			scrapeErrors = append(scrapeErrors, slotErrs...)
		}

		// Scrape replication delay metrics (PostgreSQL 9.6+ only, standby-side)
		delayErrs := s.replicationMetricsScraper.ScrapeReplicationDelay(scrapeCtx)
		if len(delayErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping replication delay metrics",
				zap.Int("error_count", len(delayErrs)))
			scrapeErrors = append(scrapeErrors, delayErrs...)
		}

		// Scrape WAL receiver metrics (PostgreSQL 9.6+ only, standby-side)
		walReceiverErrs := s.replicationMetricsScraper.ScrapeWalReceiverMetrics(scrapeCtx)
		if len(walReceiverErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping WAL receiver metrics",
				zap.Int("error_count", len(walReceiverErrs)))
			scrapeErrors = append(scrapeErrors, walReceiverErrs...)
		}
	}

	// Scrape WAL statistics (PostgreSQL 14+ only, both primary and standby)
	if s.supportsPG14 && s.replicationMetricsScraper != nil {
		walStatsErrs := s.replicationMetricsScraper.ScrapeWalStatistics(scrapeCtx)
		if len(walStatsErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping WAL statistics",
				zap.Int("error_count", len(walStatsErrs)))
			scrapeErrors = append(scrapeErrors, walStatsErrs...)
		}
	}

	// Scrape WAL files (PostgreSQL 10+ only, both primary and standby)
	if s.pgVersion >= PG10Version && s.replicationMetricsScraper != nil {
		walFilesErrs := s.replicationMetricsScraper.ScrapeWalFiles(scrapeCtx)
		if len(walFilesErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping WAL files statistics",
				zap.Int("error_count", len(walFilesErrs)))
			scrapeErrors = append(scrapeErrors, walFilesErrs...)
		}
	}

	// Scrape replication slot stats (PostgreSQL 14+ only)
	if s.supportsPG14 && s.replicationMetricsScraper != nil {
		slotStatsErrs := s.replicationMetricsScraper.ScrapeReplicationSlotStats(scrapeCtx)
		if len(slotStatsErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping replication slot stats",
				zap.Int("error_count", len(slotStatsErrs)))
			scrapeErrors = append(scrapeErrors, slotStatsErrs...)
		}
	}

	// Scrape subscription stats (PostgreSQL 15+ only, subscriber-side)
	if s.pgVersion >= 150000 && s.replicationMetricsScraper != nil {
		subscriptionErrs := s.replicationMetricsScraper.ScrapeSubscriptionStats(scrapeCtx)
		if len(subscriptionErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping subscription statistics",
				zap.Int("error_count", len(subscriptionErrs)))
			scrapeErrors = append(scrapeErrors, subscriptionErrs...)
		}
	}

	// Scrape background writer metrics (PostgreSQL 9.6+ only)
	if s.supportsPG96 && s.bgwriterScraper != nil {
		bgwriterErrs := s.bgwriterScraper.ScrapeBgwriterMetrics(scrapeCtx)
		if len(bgwriterErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping background writer metrics",
				zap.Int("error_count", len(bgwriterErrs)))
			scrapeErrors = append(scrapeErrors, bgwriterErrs...)
		}
	}

	// Scrape checkpoint control metrics (PostgreSQL 10+ only)
	if s.pgVersion >= PG10Version && s.bgwriterScraper != nil {
		checkpointErrs := s.bgwriterScraper.ScrapeControlCheckpoint(scrapeCtx)
		if len(checkpointErrs) > 0 {
			s.logger.Warn("Errors occurred while scraping checkpoint control metrics",
				zap.Int("error_count", len(checkpointErrs)))
			scrapeErrors = append(scrapeErrors, checkpointErrs...)
		}
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
