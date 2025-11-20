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
	client            client.OracleClient
	lb                *metadata.LogsBuilder
	logger            *zap.Logger
	instanceName      string
	logsBuilderConfig metadata.LogsBuilderConfig
}

func NewActiveSessionsScraper(oracleClient client.OracleClient, lb *metadata.LogsBuilder, logger *zap.Logger, instanceName string, logsBuilderConfig metadata.LogsBuilderConfig) *ActiveSessionsScraper {
	return &ActiveSessionsScraper{
		client:            oracleClient,
		lb:                lb,
		logger:            logger,
		instanceName:      instanceName,
		logsBuilderConfig: logsBuilderConfig,
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
			if err := s.buildActiveSessionLog(&session); err != nil {
				s.logger.Warn("Failed to build log for active session",
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

// buildActiveSessionLog converts an active session to a log event with individual attributes
func (s *ActiveSessionsScraper) buildActiveSessionLog(session *models.ActiveSession) error {
	if !s.logsBuilderConfig.Events.NewrelicoracledbActiveSession.Enabled {
		return nil
	}

	// Extract values with defaults for null fields
	username := ""
	if session.Username.Valid {
		username = session.Username.String
	}

	sid := int64(-1)
	if session.SID.Valid {
		sid = session.SID.Int64
	}

	serial := int64(-1)
	if session.Serial.Valid {
		serial = session.Serial.Int64
	}

	status := ""
	if session.Status.Valid {
		status = session.Status.String
	}

	queryID := ""
	if session.QueryID.Valid {
		queryID = session.QueryID.String
	}

	sqlChildNumber := int64(-1)
	if session.SQLChildNumber.Valid {
		sqlChildNumber = session.SQLChildNumber.Int64
	}

	sqlExecStart := ""
	if session.SQLExecStart.Valid {
		sqlExecStart = session.SQLExecStart.String
	}

	sqlExecID := int64(-1)
	if session.SQLExecID.Valid {
		sqlExecID = session.SQLExecID.Int64
	}

	// Record the event with all attributes
	s.lb.RecordNewrelicoracledbActiveSessionEvent(
		context.Background(),
		pcommon.NewTimestampFromTime(time.Now()),
		username,
		sid,
		serial,
		status,
		queryID,
		sqlChildNumber,
		sqlExecStart,
		sqlExecID,
	)

	return nil
}
