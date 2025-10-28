// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// ConnectionScraper contains the scraper for Oracle connection statistics
type ConnectionScraper struct {
	db                   *sql.DB
	mb                   *metadata.MetricsBuilder
	logger               *zap.Logger
	instanceName         string
	metricsBuilderConfig metadata.MetricsBuilderConfig
}

// NewConnectionScraper creates a new Connection Scraper instance
func NewConnectionScraper(db *sql.DB, mb *metadata.MetricsBuilder, logger *zap.Logger, instanceName string, metricsBuilderConfig metadata.MetricsBuilderConfig) *ConnectionScraper {
	return &ConnectionScraper{
		db:                   db,
		mb:                   mb,
		logger:               logger,
		instanceName:         instanceName,
		metricsBuilderConfig: metricsBuilderConfig,
	}
}

// ScrapeConnectionMetrics collects Oracle connection statistics
func (s *ConnectionScraper) ScrapeConnectionMetrics(ctx context.Context) []error {
	var scrapeErrors []error
	now := pcommon.NewTimestampFromTime(time.Now())

	scrapeErrors = append(scrapeErrors, s.scrapeCoreConnectionCounts(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeSessionBreakdown(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeLogonStats(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeSessionResourceConsumption(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeWaitEvents(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeBlockingSessions(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeWaitEventSummary(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeConnectionPoolMetrics(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeSessionLimits(ctx, now)...)
	scrapeErrors = append(scrapeErrors, s.scrapeConnectionQuality(ctx, now)...)

	return scrapeErrors
}

// scrapeCoreConnectionCounts scrapes basic connection counts
func (s *ConnectionScraper) scrapeCoreConnectionCounts(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	if err := s.scrapeSingleValue(ctx, queries.TotalSessionsSQL, "total_sessions", timestamp); err != nil {
		errors = append(errors, err)
	}

	if err := s.scrapeSingleValue(ctx, queries.ActiveSessionsSQL, "active_sessions", timestamp); err != nil {
		errors = append(errors, err)
	}

	if err := s.scrapeSingleValue(ctx, queries.InactiveSessionsSQL, "inactive_sessions", timestamp); err != nil {
		errors = append(errors, err)
	}

	return errors
}

// scrapeSessionBreakdown scrapes session breakdown by status and type
func (s *ConnectionScraper) scrapeSessionBreakdown(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SessionStatusSQL)
	if err != nil {
		s.logger.Debug("Failed to query session status", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var status sql.NullString
		var count sql.NullInt64

		if err := rows.Scan(&status, &count); err != nil {
			errors = append(errors, err)
			continue
		}

		if status.Valid && count.Valid {
			s.mb.RecordNewrelicoracledbConnectionSessionsByStatusDataPoint(
				timestamp,
				float64(count.Int64),
				s.instanceName,
				status.String,
			)
		}
	}

	rows, err = s.db.QueryContext(ctx, queries.SessionTypeSQL)
	if err != nil {
		s.logger.Debug("Failed to query session type", zap.Error(err))
		errors = append(errors, err)
		return errors
	}
	defer rows.Close()

	for rows.Next() {
		var sessionType sql.NullString
		var count sql.NullInt64

		if err := rows.Scan(&sessionType, &count); err != nil {
			errors = append(errors, err)
			continue
		}

		if sessionType.Valid && count.Valid {
			s.mb.RecordNewrelicoracledbConnectionSessionsByTypeDataPoint(
				timestamp,
				float64(count.Int64),
				s.instanceName,
				sessionType.String,
			)
		}
	}

	return errors
}

// scrapeLogonStats scrapes logon statistics
func (s *ConnectionScraper) scrapeLogonStats(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.LogonsStatsSQL)
	if err != nil {
		s.logger.Debug("Failed to query logon stats", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var name sql.NullString
		var value sql.NullFloat64

		if err := rows.Scan(&name, &value); err != nil {
			errors = append(errors, err)
			continue
		}

		if !name.Valid || !value.Valid {
			continue
		}

		switch name.String {
		case "logons cumulative":
			s.mb.RecordNewrelicoracledbConnectionLogonsCumulativeDataPoint(
				timestamp,
				value.Float64,
				s.instanceName,
			)
		case "logons current":
			s.mb.RecordNewrelicoracledbConnectionLogonsCurrentDataPoint(
				timestamp,
				value.Float64,
				s.instanceName,
			)
		}
	}

	return errors
}

// scrapeSessionResourceConsumption scrapes top resource consuming sessions
func (s *ConnectionScraper) scrapeSessionResourceConsumption(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SessionResourceConsumptionSQL)
	if err != nil {
		s.logger.Debug("Failed to query session resource consumption", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var sid sql.NullInt64
		var username, status, program, machine, osuser sql.NullString
		var logonTime sql.NullTime
		var lastCallET sql.NullInt64
		var cpuUsageSeconds sql.NullFloat64
		var pgaMemoryBytes, logicalReads sql.NullInt64

		if err := rows.Scan(&sid, &username, &status, &program, &machine, &osuser,
			&logonTime, &lastCallET, &cpuUsageSeconds, &pgaMemoryBytes, &logicalReads); err != nil {
			errors = append(errors, err)
			continue
		}

		sidStr := s.formatInt64(sid)
		userStr := s.formatString(username)
		statusStr := s.formatString(status)
		programStr := s.formatString(program)

		if cpuUsageSeconds.Valid {
			s.mb.RecordNewrelicoracledbConnectionSessionCPUUsageDataPoint(
				timestamp,
				cpuUsageSeconds.Float64,
				s.instanceName,
				sidStr,
				userStr,
				statusStr,
				programStr,
			)
		}

		if pgaMemoryBytes.Valid {
			s.mb.RecordNewrelicoracledbConnectionSessionPgaMemoryDataPoint(
				timestamp,
				float64(pgaMemoryBytes.Int64),
				s.instanceName,
				sidStr,
				userStr,
				statusStr,
				programStr,
			)
		}

		if logicalReads.Valid {
			s.mb.RecordNewrelicoracledbConnectionSessionLogicalReadsDataPoint(
				timestamp,
				float64(logicalReads.Int64),
				s.instanceName,
				sidStr,
				userStr,
				statusStr,
				programStr,
			)
		}

		if lastCallET.Valid {
			s.mb.RecordNewrelicoracledbConnectionSessionIdleTimeDataPoint(
				timestamp,
				float64(lastCallET.Int64),
				s.instanceName,
				sidStr,
				userStr,
				statusStr,
				programStr,
			)
		}
	}

	return errors
}

// scrapeWaitEvents scrapes current wait events
func (s *ConnectionScraper) scrapeWaitEvents(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.CurrentWaitEventsSQL)
	if err != nil {
		s.logger.Debug("Failed to query wait events", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var sid sql.NullInt64
		var username, event, state, waitClass sql.NullString
		var waitTime, secondsInWait sql.NullInt64

		if err := rows.Scan(&sid, &username, &event, &waitTime, &state, &secondsInWait, &waitClass); err != nil {
			errors = append(errors, err)
			continue
		}

		if !sid.Valid || !event.Valid || !secondsInWait.Valid {
			continue
		}

		s.mb.RecordNewrelicoracledbConnectionWaitEventsDataPoint(
			timestamp,
			float64(secondsInWait.Int64),
			s.instanceName,
			s.formatInt64(sid),
			s.formatString(username),
			event.String,
			s.formatString(state),
			s.formatString(waitClass),
		)
	}

	return errors
}

// scrapeBlockingSessions scrapes blocking sessions information
func (s *ConnectionScraper) scrapeBlockingSessions(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.BlockingSessionsSQL)
	if err != nil {
		s.logger.Debug("Failed to query blocking sessions", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var sid, serial, blockingSession, secondsInWait sql.NullInt64
		var event, username, program sql.NullString

		if err := rows.Scan(&sid, &serial, &blockingSession, &event, &username, &program, &secondsInWait); err != nil {
			errors = append(errors, err)
			continue
		}

		if !sid.Valid || !blockingSession.Valid || !secondsInWait.Valid {
			continue
		}

		s.mb.RecordNewrelicoracledbConnectionBlockingSessionsDataPoint(
			timestamp,
			float64(secondsInWait.Int64),
			s.instanceName,
			s.formatInt64(sid),
			s.formatInt64(blockingSession),
			s.formatString(username),
			s.formatString(event),
			s.formatString(program),
		)
	}

	return errors
}

// scrapeWaitEventSummary scrapes wait event summary
func (s *ConnectionScraper) scrapeWaitEventSummary(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.WaitEventSummarySQL)
	if err != nil {
		s.logger.Debug("Failed to query wait event summary", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var event, waitClass sql.NullString
		var totalWaits, timeWaitedMicro sql.NullInt64
		var averageWaitMicro sql.NullFloat64

		if err := rows.Scan(&event, &totalWaits, &timeWaitedMicro, &averageWaitMicro, &waitClass); err != nil {
			errors = append(errors, err)
			continue
		}

		if !event.Valid || !totalWaits.Valid || !timeWaitedMicro.Valid {
			continue
		}

		eventStr := event.String
		waitClassStr := s.formatString(waitClass)

		s.mb.RecordNewrelicoracledbConnectionWaitEventTotalWaitsDataPoint(
			timestamp,
			float64(totalWaits.Int64),
			s.instanceName,
			eventStr,
			waitClassStr,
		)

		s.mb.RecordNewrelicoracledbConnectionWaitEventTimeWaitedDataPoint(
			timestamp,
			float64(timeWaitedMicro.Int64)/1000.0,
			s.instanceName,
			eventStr,
			waitClassStr,
		)

		if averageWaitMicro.Valid {
			s.mb.RecordNewrelicoracledbConnectionWaitEventAvgWaitTimeDataPoint(
				timestamp,
				averageWaitMicro.Float64/1000.0,
				s.instanceName,
				eventStr,
				waitClassStr,
			)
		}
	}

	return errors
}

// scrapeConnectionPoolMetrics scrapes connection pool metrics
func (s *ConnectionScraper) scrapeConnectionPoolMetrics(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.ConnectionPoolMetricsSQL)
	if err != nil {
		s.logger.Debug("Failed to query connection pool metrics", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var metricName sql.NullString
		var value sql.NullInt64

		if err := rows.Scan(&metricName, &value); err != nil {
			errors = append(errors, err)
			continue
		}

		if !metricName.Valid || !value.Valid {
			continue
		}

		switch metricName.String {
		case "shared_servers":
			s.mb.RecordNewrelicoracledbConnectionSharedServersDataPoint(
				timestamp,
				float64(value.Int64),
				s.instanceName,
			)
		case "dispatchers":
			s.mb.RecordNewrelicoracledbConnectionDispatchersDataPoint(
				timestamp,
				float64(value.Int64),
				s.instanceName,
			)
		case "circuits":
			s.mb.RecordNewrelicoracledbConnectionCircuitsDataPoint(
				timestamp,
				float64(value.Int64),
				s.instanceName,
			)
		}
	}

	return errors
}

// scrapeSessionLimits scrapes session limits
func (s *ConnectionScraper) scrapeSessionLimits(ctx context.Context, timestamp pcommon.Timestamp) []error {
	var errors []error

	rows, err := s.db.QueryContext(ctx, queries.SessionLimitsSQL)
	if err != nil {
		s.logger.Debug("Failed to query session limits", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var resourceName, initialAllocation, limitValue sql.NullString
		var currentUtilization, maxUtilization sql.NullInt64

		if err := rows.Scan(&resourceName, &currentUtilization, &maxUtilization, &initialAllocation, &limitValue); err != nil {
			errors = append(errors, err)
			continue
		}

		if !resourceName.Valid || !currentUtilization.Valid {
			continue
		}

		resourceStr := resourceName.String

		s.mb.RecordNewrelicoracledbConnectionResourceCurrentUtilizationDataPoint(
			timestamp,
			float64(currentUtilization.Int64),
			s.instanceName,
			resourceStr,
		)

		if maxUtilization.Valid {
			s.mb.RecordNewrelicoracledbConnectionResourceMaxUtilizationDataPoint(
				timestamp,
				float64(maxUtilization.Int64),
				s.instanceName,
				resourceStr,
			)
		}

		if limitValue.Valid && limitValue.String != "UNLIMITED" {
			if limit, err := strconv.ParseInt(limitValue.String, 10, 64); err == nil {
				s.mb.RecordNewrelicoracledbConnectionResourceLimitDataPoint(
					timestamp,
					float64(limit),
					s.instanceName,
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

	rows, err := s.db.QueryContext(ctx, queries.ConnectionQualitySQL)
	if err != nil {
		s.logger.Debug("Failed to query connection quality", zap.Error(err))
		return []error{err}
	}
	defer rows.Close()

	for rows.Next() {
		var name sql.NullString
		var value sql.NullFloat64

		if err := rows.Scan(&name, &value); err != nil {
			errors = append(errors, err)
			continue
		}

		if !name.Valid || !value.Valid {
			continue
		}

		switch name.String {
		case "user commits":
			s.mb.RecordNewrelicoracledbConnectionUserCommitsDataPoint(timestamp, value.Float64, s.instanceName)
		case "user rollbacks":
			s.mb.RecordNewrelicoracledbConnectionUserRollbacksDataPoint(timestamp, value.Float64, s.instanceName)
		case "parse count (total)":
			s.mb.RecordNewrelicoracledbConnectionParseCountTotalDataPoint(timestamp, value.Float64, s.instanceName)
		case "parse count (hard)":
			s.mb.RecordNewrelicoracledbConnectionParseCountHardDataPoint(timestamp, value.Float64, s.instanceName)
		case "execute count":
			s.mb.RecordNewrelicoracledbConnectionExecuteCountDataPoint(timestamp, value.Float64, s.instanceName)
		case "SQL*Net roundtrips to/from client":
			s.mb.RecordNewrelicoracledbConnectionSqlnetRoundtripsDataPoint(timestamp, value.Float64, s.instanceName)
		case "bytes sent via SQL*Net to client":
			s.mb.RecordNewrelicoracledbConnectionBytesSentDataPoint(timestamp, value.Float64, s.instanceName)
		case "bytes received via SQL*Net from client":
			s.mb.RecordNewrelicoracledbConnectionBytesReceivedDataPoint(timestamp, value.Float64, s.instanceName)
		}
	}

	return errors
}

// scrapeSingleValue is a helper function to scrape a single numeric value
func (s *ConnectionScraper) scrapeSingleValue(ctx context.Context, query string, metricType string, timestamp pcommon.Timestamp) error {
	var value sql.NullFloat64

	if err := s.db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		s.logger.Debug("Failed to query single value", zap.String("metric_type", metricType), zap.Error(err))
		return err
	}

	if !value.Valid {
		return nil
	}

	switch metricType {
	case "total_sessions":
		s.mb.RecordNewrelicoracledbConnectionTotalSessionsDataPoint(timestamp, value.Float64, s.instanceName)
	case "active_sessions":
		s.mb.RecordNewrelicoracledbConnectionActiveSessionsDataPoint(timestamp, value.Float64, s.instanceName)
	case "inactive_sessions":
		s.mb.RecordNewrelicoracledbConnectionInactiveSessionsDataPoint(timestamp, value.Float64, s.instanceName)
	}

	return nil
}

// formatInt64 converts sql.NullInt64 to string
func (s *ConnectionScraper) formatInt64(val sql.NullInt64) string {
	if val.Valid {
		return strconv.FormatInt(val.Int64, 10)
	}
	return ""
}

// formatString converts sql.NullString to string
func (s *ConnectionScraper) formatString(val sql.NullString) string {
	if val.Valid {
		return val.String
	}
	return ""
}
