// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	internalerrors "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/errors"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

type SessionScraper struct {
	client client.OracleClient
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
	config metadata.MetricsBuilderConfig
}

func NewSessionScraper(c client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, config metadata.MetricsBuilderConfig) *SessionScraper {
	return &SessionScraper{
		client: c,
		mb:     mb,
		logger: logger,
		config: config,
	}
}

func (s *SessionScraper) ScrapeSessionCount(ctx context.Context) []error {
	var errs []error

	if !s.config.Metrics.NewrelicoracledbSessionsCount.Enabled {
		return errs
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	count, err := s.client.QuerySessionCount(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errs
		}

		scraperErr := internalerrors.NewQueryError(
			"session_count_query",
			"SessionCountSQL",
			err,
			map[string]any{
				"retryable": internalerrors.IsRetryableError(err),
				"permanent": internalerrors.IsPermanentError(err),
			},
		)

		errs = append(errs, scraperErr)
		return errs
	}

	if count != nil {
		s.mb.RecordNewrelicoracledbSessionsCountDataPoint(now, count.Count)

		s.logger.Debug("Session count scrape completed")
	}

	return errs
}
