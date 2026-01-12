// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/errors"
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
		if err == sql.ErrNoRows {
			return errs
		}

		scraperErr := errors.NewQueryError(
			"session_count_query",
			"SessionCountSQL",
			err,
			map[string]interface{}{
				"retryable": errors.IsRetryableError(err),
				"permanent": errors.IsPermanentError(err),
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

func (s *SessionScraper) ScrapeUserSessionDetails(ctx context.Context) []error {
	var errs []error

	if !s.config.Metrics.NewrelicoracledbUserSessionDetails.Enabled {
		return errs
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	details, err := s.client.QueryUserSessionDetails(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return errs
		}

		scraperErr := errors.NewQueryError(
			"user_session_details_query",
			"UserSessionDetailsSQL",
			err,
			map[string]interface{}{
				"retryable": errors.IsRetryableError(err),
				"permanent": errors.IsPermanentError(err),
			},
		)

		errs = append(errs, scraperErr)
		return errs
	}

	for _, detail := range details {
		if detail.Username.Valid && detail.SID.Valid && detail.Serial.Valid && detail.Status.Valid {
			username := detail.Username.String
			sessionID := fmt.Sprintf("%d", detail.SID.Int64)
			serialNum := detail.Serial.Int64
			machine := ""
			if detail.Machine.Valid {
				machine = detail.Machine.String
			}
			program := ""
			if detail.Program.Valid {
				program = detail.Program.String
			}
			logonTime := ""
			if detail.LogonTime.Valid {
				logonTime = detail.LogonTime.Time.Format(time.RFC3339)
			}
			status := detail.Status.String

			s.mb.RecordNewrelicoracledbUserSessionDetailsDataPoint(
				now,
				1,         // Value of 1 to indicate session exists
				username,  // username
				sessionID, // session_id
				serialNum, // session_serial
				machine,   // session_machine
				program,   // session_program
				logonTime, // session_logon_time
				status,    // session_status
			)
		}
	}

	s.logger.Debug("User session details scrape completed",
		zap.Int("session_count", len(details)))

	return errs
}
