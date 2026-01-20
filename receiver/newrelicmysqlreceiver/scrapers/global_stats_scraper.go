// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// GlobalStatsScraper handles scraping of MySQL global status metrics
type GlobalStatsScraper struct {
	logger *zap.Logger
	mb     *metadata.MetricsBuilder
}

// NewGlobalStatsScraper creates a new instance of GlobalStatsScraper
func NewGlobalStatsScraper(logger *zap.Logger, mb *metadata.MetricsBuilder) *GlobalStatsScraper {
	return &GlobalStatsScraper{
		logger: logger,
		mb:     mb,
	}
}

// Scrape collects global MySQL status metrics
// This includes:
// - Connection counts
// - Command counts (INSERT, SELECT, UPDATE, DELETE, etc.)
// - Query counts
// - Uptime
func (s *GlobalStatsScraper) Scrape(now pcommon.Timestamp, globalStats map[string]string, errs *scrapererror.ScrapeErrors) {
	if globalStats == nil {
		s.logger.Warn("Global stats is nil, skipping scrape")
		return
	}

	s.logger.Debug("Scraping global MySQL stats", zap.Int("stat_count", len(globalStats)))

	for k, v := range globalStats {
		switch k {
		// Connection metrics
		case "Connections":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlConnectionCountDataPoint(now, v))

		// Command metrics - DML operations
		case "Com_delete":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandDelete))
		case "Com_delete_multi":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandDeleteMulti))
		case "Com_insert":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandInsert))
		case "Com_select":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandSelect))
		case "Com_update":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandUpdate))
		case "Com_update_multi":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlCommandsDataPoint(now, v, metadata.AttributeCommandUpdateMulti))

		// Query metrics
		case "Queries":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlQueryCountDataPoint(now, v))

		// Uptime metrics
		case "Uptime":
			addPartialIfError(errs, s.mb.RecordNewrelicmysqlUptimeDataPoint(now, v))
		}
	}

	s.logger.Debug("Global MySQL stats scraping completed")
}

// addPartialIfError is a helper function to add partial errors to the scrape error collection
func addPartialIfError(errors *scrapererror.ScrapeErrors, err error) {
	if err != nil {
		errors.AddPartial(1, err)
	}
}
