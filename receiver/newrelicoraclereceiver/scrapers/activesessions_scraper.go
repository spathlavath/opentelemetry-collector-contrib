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
		zap.Int("sql_id_count", len(sqlIDs)))

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	now := pcommon.NewTimestampFromTime(time.Now())
	successCount := 0

	for _, sqlID := range sqlIDs {
		sessions, err := s.client.QueryActiveSessionsForSQLID(queryCtx, sqlID)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to query active sessions for SQL ID %s: %w", sqlID, err))
			s.logger.Warn("Failed to retrieve active sessions",
				zap.String("sql_id", sqlID),
				zap.Error(err))
			continue
		}

		for _, session := range sessions {
			if err := s.recordActiveSessionMetric(&session, now); err != nil {
				s.logger.Warn("Failed to record metric for active session",
					zap.String("sql_id", sqlID),
					zap.Error(err))
				errs = append(errs, err)
			} else {
				successCount++
			}
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
	if !s.metricsBuilderConfig.Metrics.NewrelicoracledbActiveSessionsInfo.Enabled {
		return nil
	}

	// Extract values with defaults for null fields
	username := ""
	if session.Username.Valid {
		username = session.Username.String
	}

	sid := int64(0)
	if session.SID.Valid {
		sid = session.SID.Int64
	}

	serial := int64(0)
	if session.Serial.Valid {
		serial = session.Serial.Int64
	}

	queryID := ""
	if session.QueryID.Valid {
		queryID = session.QueryID.String
	}

	sqlChildNumber := int64(0)
	if session.SQLChildNumber.Valid {
		sqlChildNumber = session.SQLChildNumber.Int64
	}

	sqlExecStart := ""
	if session.SQLExecStart.Valid {
		sqlExecStart = session.SQLExecStart.String
	}

	sqlExecID := int64(0)
	if session.SQLExecID.Valid {
		sqlExecID = session.SQLExecID.Int64
	}

	// Record metric with value 1 for this active session
	// All session details are in the attributes/dimensions
	s.mb.RecordNewrelicoracledbActiveSessionsInfoDataPoint(
		now,
		1, // Value is always 1, indicating this session is active
		username,
		sid,
		serial,
		queryID,
		sqlChildNumber,
		sqlExecStart,
		sqlExecID,
	)

	return nil
}
