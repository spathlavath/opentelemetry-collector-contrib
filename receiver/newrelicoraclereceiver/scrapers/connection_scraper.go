// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package scrapers // import "github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/scrapers"

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/client"
	"github.com/newrelic/nrdot-collector-components/receiver/newrelicoraclereceiver/internal/metadata"
)

// ConnectionScraper contains the scraper for Oracle connection statistics
type ConnectionScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewConnectionScraper creates a new Connection Scraper instance
func NewConnectionScraper(
	oracleClient client.OracleClient,
	mb *metadata.MetricsBuilder,
	logger *zap.Logger,
	metricsBuilderConfig metadata.MetricsBuilderConfig,
) (*ConnectionScraper, error) {
	if oracleClient == nil {
		return nil, errors.New("client cannot be nil")
	}
	if mb == nil {
		return nil, errors.New("metrics builder cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &ConnectionScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		metricsBuilderConfig: metricsBuilderConfig,
	}, nil
}

// ScrapeConnectionMetrics collects Oracle connection statistics
func (s *ConnectionScraper) ScrapeConnectionMetrics(ctx context.Context) []error {
	var scrapeErrors []error
	now := pcommon.NewTimestampFromTime(time.Now())

	scrapeErrors = append(scrapeErrors, s.scrapeCoreConnectionCounts(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeSessionBreakdown(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeLogonStats(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeConnectionPoolMetrics(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeSessionLimits(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeConnectionQuality(ctx, now)...)

	return scrapeErrors
}

// scrapeCoreConnectionCounts scrapes basic connection counts
func (s *ConnectionScraper) scrapeCoreConnectionCounts(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	totalSessions, err := s.client.QueryTotalSessions(ctx)
	if err != nil {
		s.logger.Debug("Failed to query total sessions", zap.Error(err))
		errors = append(errors, err)
	} else {
		s.mb.RecordNewrelicoracledbConnectionTotalSessionsDataPoint(timestamp, float64(totalSessions))
	}

	activeSessions, err := s.client.QueryActiveSessions(ctx)
	if err != nil {
		s.logger.Debug("Failed to query active sessions", zap.Error(err))
		errors = append(errors, err)
	} else {
		s.mb.RecordNewrelicoracledbConnectionActiveSessionsDataPoint(timestamp, float64(activeSessions))
	}

	inactiveSessions, err := s.client.QueryInactiveSessions(ctx)
	if err != nil {
		s.logger.Debug("Failed to query inactive sessions", zap.Error(err))
		errors = append(errors, err)
	} else {
		s.mb.RecordNewrelicoracledbConnectionInactiveSessionsDataPoint(timestamp, float64(inactiveSessions))
	}

	return errors
}

// scrapeSessionBreakdown scrapes session breakdown by status and type
func (s *ConnectionScraper) scrapeSessionBreakdown(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	sessionStatuses, err := s.client.QuerySessionStatus(ctx)
	if err != nil {
		s.logger.Debug("Failed to query session status", zap.Error(err))
		errors = append(errors, err)
	} else {
		for _, status := range sessionStatuses {
			if status.Status.Valid && status.Count.Valid {
				s.mb.RecordNewrelicoracledbConnectionSessionsByStatusDataPoint(
					timestamp,
					float64(status.Count.Int64),
					status.Status.String,
				)
			}
		}
	}

	sessionTypes, err := s.client.QuerySessionTypes(ctx)
	if err != nil {
		s.logger.Debug("Failed to query session type", zap.Error(err))
		errors = append(errors, err)
	} else {
		for _, sessionType := range sessionTypes {
			if sessionType.Type.Valid && sessionType.Count.Valid {
				s.mb.RecordNewrelicoracledbConnectionSessionsByTypeDataPoint(
					timestamp,
					float64(sessionType.Count.Int64),
					sessionType.Type.String,
				)
			}
		}
	}

	return errors
}

// scrapeLogonStats scrapes logon statistics
func (s *ConnectionScraper) scrapeLogonStats(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	logonStats, err := s.client.QueryLogonStats(ctx)
	if err != nil {
		s.logger.Debug("Failed to query logon stats", zap.Error(err))
		return []error{err}
	}

	for _, stat := range logonStats {
		if !stat.Name.Valid || !stat.Value.Valid {
			continue
		}

		switch stat.Name.String {
		case "logons cumulative":
			s.mb.RecordNewrelicoracledbConnectionLogonsCumulativeDataPoint(
				timestamp,
				stat.Value.Float64,
			)
		case "logons current":
			s.mb.RecordNewrelicoracledbConnectionLogonsCurrentDataPoint(
				timestamp,
				stat.Value.Float64,
			)
		}
	}

	return errors
}

// scrapeConnectionPoolMetrics scrapes connection pool metrics
func (s *ConnectionScraper) scrapeConnectionPoolMetrics(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	poolMetrics, err := s.client.QueryConnectionPoolMetrics(ctx)
	if err != nil {
		s.logger.Debug("Failed to query connection pool metrics", zap.Error(err))
		return []error{err}
	}

	for _, metric := range poolMetrics {
		if !metric.MetricName.Valid || !metric.Value.Valid {
			continue
		}

		switch metric.MetricName.String {
		case "shared_servers":
			s.mb.RecordNewrelicoracledbConnectionSharedServersDataPoint(
				timestamp,
				float64(metric.Value.Int64),
			)
		case "dispatchers":
			s.mb.RecordNewrelicoracledbConnectionDispatchersDataPoint(
				timestamp,
				float64(metric.Value.Int64),
			)
		case "circuits":
			s.mb.RecordNewrelicoracledbConnectionCircuitsDataPoint(
				timestamp,
				float64(metric.Value.Int64),
			)
		}
	}

	return errors
}

// scrapeSessionLimits scrapes session limits
func (s *ConnectionScraper) scrapeSessionLimits(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	limits, err := s.client.QuerySessionLimits(ctx)
	if err != nil {
		s.logger.Debug("Failed to query session limits", zap.Error(err))
		return []error{err}
	}

	for _, limit := range limits {
		if !limit.ResourceName.Valid || !limit.CurrentUtilization.Valid {
			continue
		}

		resourceStr := limit.ResourceName.String

		s.mb.RecordNewrelicoracledbConnectionResourceCurrentUtilizationDataPoint(
			timestamp,
			float64(limit.CurrentUtilization.Int64),
			resourceStr,
		)

		if limit.MaxUtilization.Valid {
			s.mb.RecordNewrelicoracledbConnectionResourceMaxUtilizationDataPoint(
				timestamp,
				float64(limit.MaxUtilization.Int64),
				resourceStr,
			)
		}

		if limit.LimitValue.Valid && limit.LimitValue.String != "UNLIMITED" {
			if limitVal, err := strconv.ParseInt(limit.LimitValue.String, 10, 64); err == nil {
				s.mb.RecordNewrelicoracledbConnectionResourceLimitDataPoint(
					timestamp,
					float64(limitVal),
					resourceStr,
				)
			}
		}
	}

	return errors
}

// scrapeConnectionQuality scrapes connection quality metrics
func (s *ConnectionScraper) scrapeConnectionQuality(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	qualityMetrics, err := s.client.QueryConnectionQuality(ctx)
	if err != nil {
		s.logger.Debug("Failed to query connection quality", zap.Error(err))
		return []error{err}
	}

	for _, metric := range qualityMetrics {
		if !metric.Name.Valid || !metric.Value.Valid {
			continue
		}

		switch metric.Name.String {
		case "user commits":
			s.mb.RecordNewrelicoracledbConnectionUserCommitsDataPoint(timestamp, metric.Value.Float64)
		case "user rollbacks":
			s.mb.RecordNewrelicoracledbConnectionUserRollbacksDataPoint(timestamp, metric.Value.Float64)
		case "parse count (total)":
			s.mb.RecordNewrelicoracledbConnectionParseCountTotalDataPoint(timestamp, metric.Value.Float64)
		case "parse count (hard)":
			s.mb.RecordNewrelicoracledbConnectionParseCountHardDataPoint(timestamp, metric.Value.Float64)
		case "execute count":
			s.mb.RecordNewrelicoracledbConnectionExecuteCountDataPoint(timestamp, metric.Value.Float64)
		case "SQL*Net roundtrips to/from client":
			s.mb.RecordNewrelicoracledbConnectionSqlnetRoundtripsDataPoint(timestamp, metric.Value.Float64)
		case "bytes sent via SQL*Net to client":
			s.mb.RecordNewrelicoracledbConnectionBytesSentDataPoint(timestamp, metric.Value.Float64)
		case "bytes received via SQL*Net from client":
			s.mb.RecordNewrelicoracledbConnectionBytesReceivedDataPoint(timestamp, metric.Value.Float64)
		}
	}

	return errors
}
