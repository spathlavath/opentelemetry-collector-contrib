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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/scrapers"
)

// newRelicMySQLScraper orchestrates all metric collection scrapers for MySQL database monitoring.
type newRelicMySQLScraper struct {
	// Specialized scrapers for different metric categories
	coreScraper        *scrapers.CoreScraper
	replicationScraper *scrapers.ReplicationScraper
	versionScraper     *scrapers.VersionScraper

	// Database client and configuration
	sqlclient client.MySQLClient
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
	clientCfg := client.Config{
		Username:             n.config.Username,
		Password:             string(n.config.Password),
		Endpoint:             n.config.Endpoint,
		Database:             n.config.Database,
		Transport:            string(n.config.Transport),
		AllowNativePasswords: n.config.AllowNativePasswords,
		TLSConfig: client.TLSConfig{
			Insecure: n.config.TLS.Insecure,
			LoadFunc: func(ctx context.Context) (interface{}, error) {
				return n.config.TLS.LoadTLSConfig(ctx)
			},
		},
	}

	sqlclient, err := client.NewMySQLClient(clientCfg)
	if err != nil {
		return err
	}

	err = sqlclient.Connect()
	if err != nil {
		return err
	}
	n.sqlclient = sqlclient

	// Initialize scrapers
	n.coreScraper, err = scrapers.NewCoreScraper(n.sqlclient, n.mb, n.logger)
	if err != nil {
		return err
	}

	n.replicationScraper, err = scrapers.NewReplicationScraper(n.sqlclient, n.mb, n.logger)
	if err != nil {
		return err
	}

	n.versionScraper, err = scrapers.NewVersionScraper(n.sqlclient, n.logger)
	if err != nil {
		return err
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

	// Delegate to specialized scrapers for metric collection
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver started")
	n.coreScraper.ScrapeMetrics(ctx, now, errs)
	n.versionScraper.ScrapeMetrics(ctx, now, errs)
	n.replicationScraper.ScrapeMetrics(ctx, now, errs)
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver completed")

	return n.mb.Emit(), nil
}
