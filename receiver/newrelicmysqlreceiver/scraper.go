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
)

type newRelicMySQLScraper struct {
	sqlclient client
	logger    *zap.Logger
	config    *Config
	mb        *metadata.MetricsBuilder
}

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

	// collect global status metrics.
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver started")
	n.scrapeGlobalStats(now, errs)
	n.logger.Info("Scraping MySQL metrics using newrelicmysql receiver completed")

	return n.mb.Emit(), nil
}

func (m *newRelicMySQLScraper) scrapeGlobalStats(now pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	globalStats, err := m.sqlclient.getGlobalStats()
	if err != nil {
		m.logger.Error("Failed to fetch global stats", zap.Error(err))
		errs.AddPartial(66, err)
		return
	}

	for k, v := range globalStats {
		switch k {

		// connection
		case "Connections":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlConnectionCountDataPoint(now, v))

		// commands
		case "Com_delete":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandDelete))
		case "Com_delete_multi":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandDeleteMulti))
		case "Com_insert":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandInsert))
		case "Com_select":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandSelect))
		case "Com_update":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandUpdate))
		case "Com_update_multi":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandUpdateMulti))

		// queries
		case "Queries":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlQueryCountDataPoint(now, v))

		// uptime
		case "Uptime":
			addPartialIfError(errs, m.mb.RecordNewrelicmysqlUptimeDataPoint(now, v))
		}
	}
}

func addPartialIfError(errors *scrapererror.ScrapeErrors, err error) {
	if err != nil {
		errors.AddPartial(1, err)
	}
}
