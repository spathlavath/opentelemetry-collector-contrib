// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

type ActiveSessionsScraper struct {
	client               client.OracleClient
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

func NewActiveSessionsScraper(oracleClient client.OracleClient, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *ActiveSessionsScraper {
	return &ActiveSessionsScraper{
		client:               oracleClient,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

func (s *ActiveSessionsScraper) ScrapeActiveSessions(ctx context.Context, sqlIDs []string) []error {
	var errs []error

	if len(sqlIDs) == 0 {
		return errs
	}

	s.logger.Debug("Starting active sessions scrape",
		zap.Int("sql_id_count", len(sqlIDs)),
		zap.Strings("sql_ids", sqlIDs))

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	now := pcommon.NewTimestampFromTime(time.Now())

	// Format SQL IDs for IN clause
	formattedSQLIDs := ""
	for i, sqlID := range sqlIDs {
		if i > 0 {
			formattedSQLIDs += ","
		}
		formattedSQLIDs += "'" + sqlID + "'"
	}

	// Fetch all active sessions for all SQL IDs at once using IN clause
	sessions, err := s.client.QueryActiveSessionDetails(queryCtx, formattedSQLIDs)
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to query active sessions for SQL IDs: %w", err))
		s.logger.Warn("Failed to retrieve active sessions",
			zap.Strings("sql_ids", sqlIDs),
			zap.Error(err))
		return errs
	}

	s.logger.Debug("Retrieved active sessions",
		zap.Int("sql_id_count", len(sqlIDs)),
		zap.Int("sessions_count", len(sessions)))

	// Record metrics for all sessions
	successCount := 0
	for _, session := range sessions {
		if err := s.recordActiveSessionMetric(&session, now); err != nil {
			s.logger.Warn("Failed to record metric for active session",
				zap.Error(err))
			errs = append(errs, err)
		} else {
			successCount++
		}
	}

	s.logger.Debug("Scraped active sessions",
		zap.Int("sql_ids_processed", len(sqlIDs)),
		zap.Int("sessions_recorded", successCount),
		zap.Int("failed", len(errs)))

	return errs
}

// recordActiveSessionMetric records an active session as a metric with all session attributes
func (s *ActiveSessionsScraper) recordActiveSessionMetric(session *models.ActiveSession, now pcommon.Timestamp) error {
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbActiveSessionsSecondsInWait.Enabled {
		return nil
	}

	// Record metric with seconds_in_wait as the value
	// All session details are in the attributes/dimensions
	s.mb.RecordNewrelicoracledbActiveSessionsSecondsInWaitDataPoint(
		now,
		session.GetSecondsInWait(),
		session.GetCollectionTimestamp().Format("2006-01-02 15:04:05"),
		session.GetUsername(),
		fmt.Sprintf("%d", session.GetSID()),
		session.GetSerial(),
		session.GetQueryID(),
		session.GetSQLChildNumber(),
		session.GetSQLExecStart().Format("2006-01-02 15:04:05"),
		session.GetSQLExecID(),
	)

	return nil
}
