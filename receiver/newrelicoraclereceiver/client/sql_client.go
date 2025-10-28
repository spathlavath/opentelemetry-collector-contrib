// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/queries"
)

// SQLClient is the production implementation that executes real SQL queries.
type SQLClient struct {
	db *sql.DB
}

// NewSQLClient creates a new production Oracle client.
func NewSQLClient(db *sql.DB) OracleClient {
	return &SQLClient{db: db}
}

func (c *SQLClient) Connect() error {
	return c.db.Ping()
}

func (c *SQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *SQLClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// QueryExecutionPlans executes the execution plan query for a given SQL ID.
func (c *SQLClient) QueryExecutionPlans(ctx context.Context, sqlID string) ([]models.ExecutionPlan, error) {
	query := queries.GetExecutionPlanQuery(sqlID)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.ExecutionPlan
	var currentPlan *models.ExecutionPlan
	var planLines []string

	for rows.Next() {
		var databaseName, queryID, planLine sql.NullString
		var planHashValue sql.NullInt64

		err := rows.Scan(&databaseName, &queryID, &planHashValue, &planLine)
		if err != nil {
			return nil, err
		}

		if currentPlan == nil {
			currentPlan = &models.ExecutionPlan{
				DatabaseName:  databaseName,
				QueryID:       queryID,
				PlanHashValue: planHashValue,
			}
		}

		if planLine.Valid && planLine.String != "" {
			planLines = append(planLines, planLine.String)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if currentPlan != nil && len(planLines) > 0 {
		currentPlan.ExecutionPlanText = sql.NullString{
			String: strings.Join(planLines, "\n"),
			Valid:  true,
		}
		results = append(results, *currentPlan)
	}

	return results, nil
}

// QuerySlowQueries executes the slow queries query.
func (c *SQLClient) QuerySlowQueries(ctx context.Context, responseTimeThreshold, countThreshold int) ([]models.SlowQuery, error) {
	query := queries.GetSlowQueriesSQL(responseTimeThreshold, countThreshold)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SlowQuery

	for rows.Next() {
		var slowQuery models.SlowQuery

		err := rows.Scan(
			&slowQuery.DatabaseName,
			&slowQuery.QueryID,
			&slowQuery.SchemaName,
			&slowQuery.UserName,
			&slowQuery.LastLoadTime,
			&slowQuery.SharableMemoryBytes,
			&slowQuery.PersistentMemoryBytes,
			&slowQuery.RuntimeMemoryBytes,
			&slowQuery.StatementType,
			&slowQuery.ExecutionCount,
			&slowQuery.QueryText,
			&slowQuery.AvgCPUTimeMs,
			&slowQuery.AvgDiskReads,
			&slowQuery.AvgDiskWrites,
			&slowQuery.AvgElapsedTimeMs,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, slowQuery)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryBlockingQueries executes the blocking queries query.
func (c *SQLClient) QueryBlockingQueries(ctx context.Context, countThreshold int) ([]models.BlockingQuery, error) {
	query := queries.GetBlockingQueriesSQL(countThreshold)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.BlockingQuery

	for rows.Next() {
		var blockingQuery models.BlockingQuery

		err := rows.Scan(
			&blockingQuery.BlockedSID,
			&blockingQuery.BlockedSerial,
			&blockingQuery.BlockedUser,
			&blockingQuery.BlockedWaitSec,
			&blockingQuery.BlockedSQLID,
			&blockingQuery.BlockedQueryText,
			&blockingQuery.BlockingSID,
			&blockingQuery.BlockingSerial,
			&blockingQuery.BlockingUser,
			&blockingQuery.DatabaseName,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, blockingQuery)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryWaitEvents executes the wait events query.
func (c *SQLClient) QueryWaitEvents(ctx context.Context, countThreshold int) ([]models.WaitEvent, error) {
	query := queries.GetWaitEventQueriesSQL(countThreshold)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.WaitEvent

	for rows.Next() {
		var waitEvent models.WaitEvent

		err := rows.Scan(
			&waitEvent.DatabaseName,
			&waitEvent.QueryID,
			&waitEvent.WaitCategory,
			&waitEvent.WaitEventName,
			&waitEvent.CollectionTimestamp,
			&waitEvent.WaitingTasksCount,
			&waitEvent.TotalWaitTimeMs,
			&waitEvent.AvgWaitTimeMs,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, waitEvent)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryTotalSessions returns the total session count
func (c *SQLClient) QueryTotalSessions(ctx context.Context) (int64, error) {
	var count sql.NullInt64
	err := c.db.QueryRowContext(ctx, queries.TotalSessionsSQL).Scan(&count)
	if err != nil {
		return 0, err
	}
	if !count.Valid {
		return 0, nil
	}
	return count.Int64, nil
}

// QueryActiveSessions returns the active session count
func (c *SQLClient) QueryActiveSessions(ctx context.Context) (int64, error) {
	var count sql.NullInt64
	err := c.db.QueryRowContext(ctx, queries.ActiveSessionsSQL).Scan(&count)
	if err != nil {
		return 0, err
	}
	if !count.Valid {
		return 0, nil
	}
	return count.Int64, nil
}

// QueryInactiveSessions returns the inactive session count
func (c *SQLClient) QueryInactiveSessions(ctx context.Context) (int64, error) {
	var count sql.NullInt64
	err := c.db.QueryRowContext(ctx, queries.InactiveSessionsSQL).Scan(&count)
	if err != nil {
		return 0, err
	}
	if !count.Valid {
		return 0, nil
	}
	return count.Int64, nil
}

// QuerySessionStatus returns session counts by status
func (c *SQLClient) QuerySessionStatus(ctx context.Context) ([]models.SessionStatus, error) {
	rows, err := c.db.QueryContext(ctx, queries.SessionStatusSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SessionStatus
	for rows.Next() {
		var status models.SessionStatus
		err := rows.Scan(&status.Status, &status.Count)
		if err != nil {
			return nil, err
		}
		results = append(results, status)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QuerySessionTypes returns session counts by type
func (c *SQLClient) QuerySessionTypes(ctx context.Context) ([]models.SessionType, error) {
	rows, err := c.db.QueryContext(ctx, queries.SessionTypeSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SessionType
	for rows.Next() {
		var sessionType models.SessionType
		err := rows.Scan(&sessionType.Type, &sessionType.Count)
		if err != nil {
			return nil, err
		}
		results = append(results, sessionType)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QueryLogonStats returns logon statistics
func (c *SQLClient) QueryLogonStats(ctx context.Context) ([]models.LogonStat, error) {
	rows, err := c.db.QueryContext(ctx, queries.LogonsStatsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.LogonStat
	for rows.Next() {
		var stat models.LogonStat
		err := rows.Scan(&stat.Name, &stat.Value)
		if err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QuerySessionResources returns session resource consumption
func (c *SQLClient) QuerySessionResources(ctx context.Context) ([]models.SessionResource, error) {
	rows, err := c.db.QueryContext(ctx, queries.SessionResourceConsumptionSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SessionResource
	for rows.Next() {
		var resource models.SessionResource
		err := rows.Scan(
			&resource.SID,
			&resource.Username,
			&resource.Status,
			&resource.Program,
			&resource.Machine,
			&resource.OSUser,
			&resource.LogonTime,
			&resource.LastCallET,
			&resource.CPUUsageSeconds,
			&resource.PGAMemoryBytes,
			&resource.LogicalReads,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, resource)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QueryCurrentWaitEvents returns current wait events
func (c *SQLClient) QueryCurrentWaitEvents(ctx context.Context) ([]models.CurrentWaitEvent, error) {
	rows, err := c.db.QueryContext(ctx, queries.CurrentWaitEventsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.CurrentWaitEvent
	for rows.Next() {
		var event models.CurrentWaitEvent
		err := rows.Scan(
			&event.SID,
			&event.Username,
			&event.Event,
			&event.WaitTime,
			&event.State,
			&event.SecondsInWait,
			&event.WaitClass,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, event)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QueryBlockingSessions returns blocking sessions
func (c *SQLClient) QueryBlockingSessions(ctx context.Context) ([]models.BlockingSession, error) {
	rows, err := c.db.QueryContext(ctx, queries.BlockingSessionsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.BlockingSession
	for rows.Next() {
		var session models.BlockingSession
		err := rows.Scan(
			&session.SID,
			&session.Serial,
			&session.BlockingSession,
			&session.Event,
			&session.Username,
			&session.Program,
			&session.SecondsInWait,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, session)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QueryWaitEventSummary returns wait event summary
func (c *SQLClient) QueryWaitEventSummary(ctx context.Context) ([]models.WaitEventSummary, error) {
	rows, err := c.db.QueryContext(ctx, queries.WaitEventSummarySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.WaitEventSummary
	for rows.Next() {
		var summary models.WaitEventSummary
		err := rows.Scan(
			&summary.Event,
			&summary.TotalWaits,
			&summary.TimeWaitedMicro,
			&summary.AverageWaitMicro,
			&summary.WaitClass,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, summary)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QueryConnectionPoolMetrics returns connection pool metrics
func (c *SQLClient) QueryConnectionPoolMetrics(ctx context.Context) ([]models.ConnectionPoolMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.ConnectionPoolMetricsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.ConnectionPoolMetric
	for rows.Next() {
		var metric models.ConnectionPoolMetric
		err := rows.Scan(&metric.MetricName, &metric.Value)
		if err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QuerySessionLimits returns session limits
func (c *SQLClient) QuerySessionLimits(ctx context.Context) ([]models.SessionLimit, error) {
	rows, err := c.db.QueryContext(ctx, queries.SessionLimitsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SessionLimit
	for rows.Next() {
		var limit models.SessionLimit
		err := rows.Scan(
			&limit.ResourceName,
			&limit.CurrentUtilization,
			&limit.MaxUtilization,
			&limit.InitialAllocation,
			&limit.LimitValue,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, limit)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// QueryConnectionQuality returns connection quality metrics
func (c *SQLClient) QueryConnectionQuality(ctx context.Context) ([]models.ConnectionQualityMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.ConnectionQualitySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.ConnectionQualityMetric
	for rows.Next() {
		var metric models.ConnectionQualityMetric
		err := rows.Scan(&metric.Name, &metric.Value)
		if err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
