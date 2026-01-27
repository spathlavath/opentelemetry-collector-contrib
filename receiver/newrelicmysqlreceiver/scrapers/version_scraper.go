// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/client"
)

// VersionScraper handles MySQL version information collection.
// Note: String metrics are not supported in OpenTelemetry, so we only log the version.
type VersionScraper struct {
	client client.MySQLClient
	logger *zap.Logger
}

// NewVersionScraper creates a new version scraper.
func NewVersionScraper(c client.MySQLClient, logger *zap.Logger) (*VersionScraper, error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &VersionScraper{
		client: c,
		logger: logger,
	}, nil
}

// ScrapeMetrics collects MySQL version information.
func (s *VersionScraper) ScrapeMetrics(_ context.Context, _ pcommon.Timestamp, errs *scrapererror.ScrapeErrors) {
	s.logger.Debug("Scraping MySQL version information")

	version, err := s.client.GetVersion()
	if err != nil {
		s.logger.Error("Failed to fetch version", zap.Error(err))
		errs.AddPartial(1, err)
		return
	}

	// Log version info (string metrics not supported in OpenTelemetry)
	s.logger.Info("MySQL version detected", zap.String("version", version))
}
