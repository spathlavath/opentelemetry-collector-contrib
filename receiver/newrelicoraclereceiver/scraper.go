// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicoraclereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver"

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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"
)

const (
	// keepalive for connection
	keepAlive = 30 * time.Second
)

type dbProviderFunc func() (*sql.DB, error)

type newRelicOracleScraper struct {
	// Session scraper for basic session metrics
	sessionScraper *scrapers.SessionScraper
	// PDB system metrics scraper for comprehensive system metrics
	pdbSysMetricsScraper *scrapers.PDBSysMetricsScraper
	// Redo log waits scraper for redo log and system event wait metrics
	redoLogWaitsScraper *scrapers.RedoLogWaitsScraper

	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	dbProviderFunc       dbProviderFunc
	logger               *zap.Logger
	instanceName         string
	hostName             string
	scrapeCfg            scraperhelper.ControllerConfig
	startTime            pcommon.Timestamp
	metricsBuilderConfig metadata.MetricsBuilderConfig
	enablePDBSysMetrics  bool
}

func newScraper(metricsBuilder *metadata.MetricsBuilder, metricsBuilderConfig metadata.MetricsBuilderConfig, scrapeCfg scraperhelper.ControllerConfig, logger *zap.Logger, providerFunc dbProviderFunc, instanceName, hostName string, enablePDBSysMetrics bool) (scraper.Metrics, error) {
	s := &newRelicOracleScraper{
		mb:                   metricsBuilder,
		dbProviderFunc:       providerFunc,
		logger:               logger,
		scrapeCfg:            scrapeCfg,
		metricsBuilderConfig: metricsBuilderConfig,
		instanceName:         instanceName,
		hostName:             hostName,
		enablePDBSysMetrics:  enablePDBSysMetrics,
	}
	return scraper.NewMetrics(s.scrape, scraper.WithShutdown(s.shutdown), scraper.WithStart(s.start))
}

func (s *newRelicOracleScraper) start(context.Context, component.Host) error {
	s.startTime = pcommon.NewTimestampFromTime(time.Now())
	var err error
	s.db, err = s.dbProviderFunc()
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}

	// Initialize session scraper with direct DB connection
	s.sessionScraper = scrapers.NewSessionScraper(s.db, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)

	// Initialize PDB system metrics scraper only if enabled
	if s.enablePDBSysMetrics {
		s.pdbSysMetricsScraper = scrapers.NewPDBSysMetricsScraper(s.db, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
		s.logger.Info("Oracle scrapers initialized with PDB metrics enabled", zap.String("instance", s.instanceName))
	} else {
		s.logger.Info("Oracle scrapers initialized with PDB metrics disabled", zap.String("instance", s.instanceName))
	}

	// Initialize redo log waits scraper (always enabled as the metrics have individual enable/disable flags)
	s.redoLogWaitsScraper = scrapers.NewRedoLogWaitsScraper(s.db, s.logger, s.mb, s, s.instanceName)
	s.logger.Info("Oracle redo log waits scraper initialized", zap.String("instance", s.instanceName))

	return nil
}

// GetMetrics implements the scrapers.Config interface
func (s *newRelicOracleScraper) GetMetrics() metadata.MetricsConfig {
	return s.metricsBuilderConfig.Metrics
}

func (s *newRelicOracleScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	s.logger.Debug("Begin New Relic Oracle scrape")

	var scrapeErrors []error

	// Scrape session count metric
	scrapeErrors = append(scrapeErrors, s.sessionScraper.ScrapeSessionCount(ctx)...)

	// Scrape PDB system metrics only if enabled
	if s.enablePDBSysMetrics && s.pdbSysMetricsScraper != nil {
		scrapeErrors = append(scrapeErrors, s.pdbSysMetricsScraper.ScrapePDBSysMetrics(ctx)...)
	}

	// Scrape redo log waits metrics (always enabled as individual metrics have enable/disable flags)
	if s.redoLogWaitsScraper != nil {
		scrapeErrors = append(scrapeErrors, s.redoLogWaitsScraper.ScrapeRedoLogWaits(ctx)...)
	}

	// Build the resource with instance and host information
	rb := s.mb.NewResourceBuilder()
	rb.SetNewrelicoracledbInstanceName(s.instanceName)
	rb.SetHostName(s.hostName)
	out := s.mb.Emit(metadata.WithResource(rb.Emit()))

	s.logger.Debug("Done New Relic Oracle scraping", zap.Int("total_errors", len(scrapeErrors)))
	if len(scrapeErrors) > 0 {
		return out, scrapererror.NewPartialScrapeError(multierr.Combine(scrapeErrors...), len(scrapeErrors))
	}
	return out, nil
}

func (s *newRelicOracleScraper) shutdown(_ context.Context) error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
