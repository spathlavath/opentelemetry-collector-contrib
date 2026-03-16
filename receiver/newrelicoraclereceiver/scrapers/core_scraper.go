// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"errors"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

// CoreScraper handles Oracle core database metrics
type CoreScraper struct {
	client                client.OracleClient
	mb                    *metadata.MetricsBuilder
	logger                *zap.Logger
	config                metadata.MetricsBuilderConfig
	enableAdvancedMetrics bool
}

// NewCoreScraper creates a new core scraper
func NewCoreScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, config metadata.MetricsBuilderConfig, enableAdvancedMetrics bool) (*CoreScraper, error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &CoreScraper{
		client:                c,
		mb:                    mb,
		logger:                logger,
		config:                config,
		enableAdvancedMetrics: enableAdvancedMetrics,
	}, nil
}

// ScrapeCoreMetrics collects Oracle core database metrics
func (s *CoreScraper) ScrapeCoreMetrics(ctx context.Context) []error {
	var errors []error
	s.logger.Debug("Scraping Oracle core database metrics")
	now := pcommon.NewTimestampFromTime(time.Now())

	// Always emit UI-critical metrics (scrapePGAMetrics handles both mandatory and advanced based on flag)
	errors = append(errors, s.scrapePGAMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGAHitRatioMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGASharedPoolLibraryCacheHitRatioMetrics(ctx, now)...)

	// Emit advanced metrics only when flag is enabled
	if s.enableAdvancedMetrics {
		errors = append(errors, s.scrapeAdvancedCoreMetrics(ctx, now)...)
	}

	return errors
}

// scrapeAdvancedCoreMetrics scrapes all advanced core metrics
func (s *CoreScraper) scrapeAdvancedCoreMetrics(ctx context.Context, now pcommon.Timestamp) []error {
	var errors []error

	errors = append(errors, s.scrapeLockedAccountsMetrics(ctx, now)...)
	errors = append(errors, s.scrapeReadWriteMetrics(ctx, now)...)
	errors = append(errors, s.scrapeGlobalNameInstanceMetrics(ctx, now)...)
	errors = append(errors, s.scrapeDBIDInstanceMetrics(ctx, now)...)
	errors = append(errors, s.scrapeLongRunningQueriesMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGAUGATotalMemoryMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGASharedPoolLibraryCacheMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGASharedPoolLibraryCacheUserMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGASharedPoolLibraryCacheReloadRatioMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGASharedPoolDictCacheMissRatioMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGALogBufferSpaceWaitsMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGALogAllocRetriesMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSysstatMetrics(ctx, now)...)
	errors = append(errors, s.scrapeSGAMetrics(ctx, now)...)
	errors = append(errors, s.scrapeRollbackSegmentsMetrics(ctx, now)...)
	errors = append(errors, s.scrapeRedoLogWaitsMetrics(ctx, now)...)

	return errors
}
