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
	// Scrapers
	slowQueryScraper *scrapers.SlowQueryScraper
	config           *Config

	// Database and configuration
	db *sql.DB
	// Only keep session scraper for simplicity
	sessionScraper       *scrapers.SessionScraper
	mb                   *metadata.MetricsBuilder
	dbProviderFunc       dbProviderFunc
	logger               *zap.Logger
	instanceName         string
	hostName             string
	scrapeCfg            scraperhelper.ControllerConfig
	startTime            pcommon.Timestamp
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

func newScraper(metricsBuilder *metadata.MetricsBuilder, metricsBuilderConfig metadata.MetricsBuilderConfig, scrapeCfg scraperhelper.ControllerConfig, logger *zap.Logger, providerFunc dbProviderFunc, instanceName, hostName string, config *Config) (scraper.Metrics, error) {
	s := &newRelicOracleScraper{
		mb:                   metricsBuilder,
		dbProviderFunc:       providerFunc,
		logger:               logger,
		scrapeCfg:            scrapeCfg,
		metricsBuilderConfig: metricsBuilderConfig,
		instanceName:         instanceName,
		hostName:             hostName,
		config:               config,
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

	// Initialize scrapers
	s.sessionScraper = scrapers.NewSessionScraper(s.db, s.mb, s.logger, s.instanceName, s.metricsBuilderConfig)
	s.slowQueryScraper = scrapers.NewSlowQueryScraper(s.logger)

	return nil
}

func (s *newRelicOracleScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	s.logger.Debug("Begin New Relic Oracle scrape")

	var scrapeErrors []error

	// Scrape session count metric
	scrapeErrors = append(scrapeErrors, s.sessionScraper.ScrapeSessionCount(ctx)...)

	// Collect instance information for slow queries
	instanceInfo, err := s.getInstanceInfo()
	if err != nil {
		s.logger.Warn("Failed to get instance information for slow queries", zap.Error(err))
		// Continue without instance info
		instanceInfo = nil
	}

	// Collect slow query metrics
	slowQueryMetrics, err := s.slowQueryScraper.CollectSlowQueryMetrics(s.db, s.config.SkipMetricsGroups, instanceInfo)
	if err != nil {
		s.logger.Warn("Error collecting slow query metrics", zap.Error(err))
		scrapeErrors = append(scrapeErrors, err)
	}

	// Build the resource with instance and host information
	rb := s.mb.NewResourceBuilder()
	rb.SetNewrelicoracledbInstanceName(s.instanceName)
	rb.SetHostName(s.hostName)
	out := s.mb.Emit(metadata.WithResource(rb.Emit()))

	// Merge slow query metrics with standard metrics
	if slowQueryMetrics.ResourceMetrics().Len() > 0 {
		s.logger.Debug("Merging slow query metrics",
			zap.Int("slow_query_resource_metrics", slowQueryMetrics.ResourceMetrics().Len()))
		slowQueryMetrics.ResourceMetrics().MoveAndAppendTo(out.ResourceMetrics())
	}

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

// getInstanceInfo retrieves basic instance information for slow query resource attributes
func (s *newRelicOracleScraper) getInstanceInfo() (*scrapers.InstanceInfo, error) {
	query := `
		SELECT 
			i.INST_ID,
			i.INSTANCE_NAME,
			g.GLOBAL_NAME,
			d.DBID
		FROM 
			gv$instance i,
			global_name g,
			v$database d
		WHERE i.INST_ID = 1`

	row := s.db.QueryRow(query)

	var info scrapers.InstanceInfo
	err := row.Scan(&info.InstanceID, &info.InstanceName, &info.GlobalName, &info.DbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance info: %w", err)
	}

	return &info, nil
}
