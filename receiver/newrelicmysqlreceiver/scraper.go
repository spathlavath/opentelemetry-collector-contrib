// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/scrapers"
)

type newRelicMySQLScraper struct {
	sqlclient         client
	logger            *zap.Logger
	config            *Config
	mb                *metadata.MetricsBuilder
	globalStatsScraper *scrapers.GlobalStatsScraper // Scraper for global MySQL stats
	slowQueryScraper   *scrapers.SlowQueryScraper   // Scraper for slow query monitoring
}

func newNewRelicMySQLScraper(
	settings receiver.Settings,
	config *Config,
) *newRelicMySQLScraper {
	mb := metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings)

	// Initialize global stats scraper
	globalStatsScraper := scrapers.NewGlobalStatsScraper(settings.Logger, mb)

	// Initialize slow query scraper if enabled
	var slowQueryScraper *scrapers.SlowQueryScraper
	if config.QueryMonitoringEnabled {
		slowQueryConfig := &scrapers.SlowQueryScraperConfig{
			IntervalSeconds:      config.QueryMonitoringIntervalSeconds,
			TopN:                 config.QueryMonitoringTopN,
			ElapsedTimeThreshold: config.QueryMonitoringElapsedTimeThreshold,
			EnableIntervalCalc:   config.QueryMonitoringEnableIntervalCalc,
		}
		// Note: client will be set later in start() method
		slowQueryScraper = scrapers.NewSlowQueryScraper(settings.Logger, mb, slowQueryConfig, nil)
	}

	return &newRelicMySQLScraper{
		logger:             settings.Logger,
		config:             config,
		mb:                 mb,
		globalStatsScraper: globalStatsScraper,
		slowQueryScraper:   slowQueryScraper,
	}
}

// start starts the scraper by initializing the database client connection.
func (n *newRelicMySQLScraper) start(_ context.Context, _ component.Host) error {
	sqlclient, err := newMySQLClient(n.config)
	if err != nil {
		return err
	}

	err = sqlclient.Connect()
	if err != nil {
		return err
	}
	n.sqlclient = sqlclient

	// Set the client for slow query scraper if enabled
	if n.slowQueryScraper != nil {
		n.slowQueryScraper = scrapers.NewSlowQueryScraper(
			n.logger,
			n.mb,
			&scrapers.SlowQueryScraperConfig{
				IntervalSeconds:      n.config.QueryMonitoringIntervalSeconds,
				TopN:                 n.config.QueryMonitoringTopN,
				ElapsedTimeThreshold: n.config.QueryMonitoringElapsedTimeThreshold,
				EnableIntervalCalc:   n.config.QueryMonitoringEnableIntervalCalc,
			},
			sqlclient,
		)
	}

	return nil
}

// shutdown closes the database connection.
func (n *newRelicMySQLScraper) shutdown(context.Context) error {
	if n.sqlclient == nil {
		return nil
	}
	return n.sqlclient.Close()
}

// scrape scrapes the MySQL database metrics and transforms them.
func (n *newRelicMySQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	if n.sqlclient == nil {
		return pmetric.Metrics{}, errors.New("failed to connect to database client")
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	errs := &scrapererror.ScrapeErrors{}

	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver started")

	// Collect global status metrics using dedicated scraper
	globalStats, err := n.sqlclient.GetGlobalStats()
	if err != nil {
		n.logger.Error("Failed to fetch global stats", zap.Error(err))
		errs.AddPartial(66, err)
	} else {
		n.globalStatsScraper.Scrape(now, globalStats, errs)
	}

	// Collect slow query metrics if enabled using dedicated scraper
	if n.config.QueryMonitoringEnabled && n.slowQueryScraper != nil {
		if err := n.slowQueryScraper.Scrape(ctx, now); err != nil {
			n.logger.Error("Failed to scrape slow queries", zap.Error(err))
			errs.AddPartial(1, err)
		}
	}

	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver completed")

	return n.mb.Emit(), nil
}

