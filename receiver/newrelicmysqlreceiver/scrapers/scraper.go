// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
)

// Scraper defines the interface for all MySQL metric scrapers.
// This interface allows for easy addition of new scrapers without modifying the main scraper orchestration logic.
type Scraper interface {
	// ScrapeMetrics collects metrics from MySQL and records them using the metrics builder.
	ScrapeMetrics(ctx context.Context, now pcommon.Timestamp, errs *scrapererror.ScrapeErrors)
}
