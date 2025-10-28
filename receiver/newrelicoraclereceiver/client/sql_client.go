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
