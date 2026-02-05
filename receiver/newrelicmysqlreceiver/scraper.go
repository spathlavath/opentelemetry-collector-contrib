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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/scrapers"
)

// newRelicMySQLScraper orchestrates all metric collection scrapers for MySQL database monitoring.
type newRelicMySQLScraper struct {
	// Slice of all metric scrapers
	scrapers []scrapers.Scraper

	// Database client and configuration
	sqlclient common.Client
	logger    *zap.Logger
	config    *Config
	mb        *metadata.MetricsBuilder
}

// newNewRelicMySQLScraper creates a new MySQL metrics scraper.
func newNewRelicMySQLScraper(
	settings receiver.Settings,
	config *Config,
) *newRelicMySQLScraper {
	return &newRelicMySQLScraper{
		logger: settings.Logger,
		config: config,
		mb:     metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings),
	}
}

// start starts the scraper by initializing the database client connection and scrapers.
func (n *newRelicMySQLScraper) start(_ context.Context, _ component.Host) error {
	// Create MySQL client
	sqlclient, err := NewMySQLClient(n.config)
	if err != nil {
		return err
	}

	err = sqlclient.Connect()
	if err != nil {
		return err
	}
	n.sqlclient = sqlclient

	// Initialize all scrapers
	// Adding new scrapers requires only adding one line here
	coreScraper, err := scrapers.NewCoreScraper(n.sqlclient, n.mb, n.logger)
	if err != nil {
		return err
	}

	// Initialize replication scraper (always enabled for core metrics)
	// Additional metrics are controlled by the Replication flag
	replicationScraper, err := scrapers.NewReplicationScraper(n.sqlclient, n.mb, n.logger, n.config.Replication)
	if err != nil {
		return err
	}

	n.scrapers = []scrapers.Scraper{
		coreScraper,
		replicationScraper,
	}

	if n.config.Replication {
		n.logger.Info("Additional replication metrics collection enabled")
	}

	if n.config.ExtraStatusMetrics {
		extraStatusScraper, err := scrapers.NewExtraStatusScraper(n.sqlclient, n.mb, n.logger)
		if err != nil {
			return err
		}
		n.scrapers = append(n.scrapers, extraStatusScraper)
		n.logger.Info("Extra status metrics collection enabled")
	}

	// Conditionally initialize InnoDB extended scraper if enabled
	if n.config.ExtraInnoDBMetrics {
		innodbExtendedScraper, err := scrapers.NewInnoDBExtendedScraper(n.sqlclient, n.mb, n.logger)
		if err != nil {
			return err
		}
		n.scrapers = append(n.scrapers, innodbExtendedScraper)
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

	// Delegate to all registered scrapers for metric collection
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver started")
	for _, scraper := range n.scrapers {
		scraper.ScrapeMetrics(ctx, now, errs)
	}
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver completed")

	return n.mb.Emit(), nil
}
