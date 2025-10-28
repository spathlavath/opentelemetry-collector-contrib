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

// CheckCDBFeature checks if database is a Container Database
func (c *SQLClient) CheckCDBFeature(ctx context.Context) (int64, error) {
	var isCDB int64
	err := c.db.QueryRowContext(ctx, queries.CheckCDBFeatureSQL).Scan(&isCDB)
	if err != nil {
		return 0, err
	}
	return isCDB, nil
}

// CheckPDBCapability checks if PDB functionality is available
func (c *SQLClient) CheckPDBCapability(ctx context.Context) (int64, error) {
	var pdbCount int64
	err := c.db.QueryRowContext(ctx, queries.CheckPDBCapabilitySQL).Scan(&pdbCount)
	if err != nil {
		return 0, err
	}
	return pdbCount, nil
}

// CheckCurrentContainer returns current container context information
func (c *SQLClient) CheckCurrentContainer(ctx context.Context) (models.ContainerContext, error) {
	var result models.ContainerContext
	err := c.db.QueryRowContext(ctx, queries.CheckCurrentContainerSQL).Scan(&result.ContainerName, &result.ContainerID)
	if err != nil {
		return models.ContainerContext{}, err
	}
	return result, nil
}

// QueryContainerStatus queries container status from GV$CONTAINERS
func (c *SQLClient) QueryContainerStatus(ctx context.Context) ([]models.ContainerStatus, error) {
	rows, err := c.db.QueryContext(ctx, queries.ContainerStatusSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.ContainerStatus
	for rows.Next() {
		var cs models.ContainerStatus
		if err := rows.Scan(&cs.ConID, &cs.ContainerName, &cs.OpenMode, &cs.Restricted, &cs.OpenTime); err != nil {
			return nil, err
		}
		results = append(results, cs)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryPDBStatus queries PDB status from GV$PDBS
func (c *SQLClient) QueryPDBStatus(ctx context.Context) ([]models.PDBStatus, error) {
	rows, err := c.db.QueryContext(ctx, queries.PDBStatusSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.PDBStatus
	for rows.Next() {
		var ps models.PDBStatus
		if err := rows.Scan(&ps.ConID, &ps.PDBName, &ps.Status, &ps.CreationSCN, &ps.OpenMode, &ps.Restricted, &ps.OpenTime, &ps.TotalSize); err != nil {
			return nil, err
		}
		results = append(results, ps)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryCDBTablespaceUsage queries tablespace usage across containers
func (c *SQLClient) QueryCDBTablespaceUsage(ctx context.Context) ([]models.CDBTablespaceUsage, error) {
	rows, err := c.db.QueryContext(ctx, queries.CDBTablespaceUsageSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.CDBTablespaceUsage
	for rows.Next() {
		var tu models.CDBTablespaceUsage
		if err := rows.Scan(&tu.ConID, &tu.TablespaceName, &tu.UsedBytes, &tu.TotalBytes, &tu.UsedPercent); err != nil {
			return nil, err
		}
		results = append(results, tu)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryCDBDataFiles queries data file information across containers
func (c *SQLClient) QueryCDBDataFiles(ctx context.Context) ([]models.CDBDataFile, error) {
	rows, err := c.db.QueryContext(ctx, queries.CDBDataFilesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.CDBDataFile
	for rows.Next() {
		var df models.CDBDataFile
		if err := rows.Scan(&df.ConID, &df.FileName, &df.TablespaceName, &df.Bytes, &df.Status, &df.Autoextensible, &df.MaxBytes, &df.UserBytes); err != nil {
			return nil, err
		}
		results = append(results, df)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryCDBServices queries service information across containers
func (c *SQLClient) QueryCDBServices(ctx context.Context) ([]models.CDBService, error) {
	rows, err := c.db.QueryContext(ctx, queries.CDBServicesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.CDBService
	for rows.Next() {
		var svc models.CDBService
		if err := rows.Scan(&svc.ConID, &svc.ServiceName, &svc.NetworkName, &svc.CreationDate, &svc.PDB, &svc.Enabled); err != nil {
			return nil, err
		}
		results = append(results, svc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryDiskIOMetrics executes the disk I/O metrics query.
func (c *SQLClient) QueryDiskIOMetrics(ctx context.Context) ([]models.DiskIOMetrics, error) {
	rows, err := c.db.QueryContext(ctx, queries.ReadWriteMetricsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.DiskIOMetrics
	for rows.Next() {
		var metric models.DiskIOMetrics
		if err := rows.Scan(&metric.InstID, &metric.PhysicalReads, &metric.PhysicalWrites,
			&metric.PhysicalBlockReads, &metric.PhysicalBlockWrites,
			&metric.ReadTime, &metric.WriteTime); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryLockedAccounts executes the locked accounts query.
func (c *SQLClient) QueryLockedAccounts(ctx context.Context) ([]models.LockedAccountsMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.LockedAccountsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.LockedAccountsMetric
	for rows.Next() {
		var metric models.LockedAccountsMetric
		if err := rows.Scan(&metric.InstID, &metric.LockedAccounts); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryGlobalName executes the global name query.
func (c *SQLClient) QueryGlobalName(ctx context.Context) ([]models.GlobalNameMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.GlobalNameInstanceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.GlobalNameMetric
	for rows.Next() {
		var metric models.GlobalNameMetric
		if err := rows.Scan(&metric.InstID, &metric.GlobalName); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryDBID executes the database ID query.
func (c *SQLClient) QueryDBID(ctx context.Context) ([]models.DBIDMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.DBIDInstanceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.DBIDMetric
	for rows.Next() {
		var metric models.DBIDMetric
		if err := rows.Scan(&metric.InstID, &metric.DBID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryLongRunningQueries executes the long-running queries query.
func (c *SQLClient) QueryLongRunningQueries(ctx context.Context) ([]models.LongRunningQueriesMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.LongRunningQueriesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.LongRunningQueriesMetric
	for rows.Next() {
		var metric models.LongRunningQueriesMetric
		if err := rows.Scan(&metric.InstID, &metric.Total); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryPGAMetrics executes the PGA metrics query.
func (c *SQLClient) QueryPGAMetrics(ctx context.Context) ([]models.PGAMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PGAMetricsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.PGAMetric
	for rows.Next() {
		var metric models.PGAMetric
		if err := rows.Scan(&metric.InstID, &metric.Name, &metric.Value); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGAUGATotalMemory executes the SGA UGA total memory query.
func (c *SQLClient) QuerySGAUGATotalMemory(ctx context.Context) ([]models.SGAUGATotalMemoryMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGAUGATotalMemorySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGAUGATotalMemoryMetric
	for rows.Next() {
		var metric models.SGAUGATotalMemoryMetric
		if err := rows.Scan(&metric.Sum, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGASharedPoolLibraryCache executes the SGA shared pool library cache query.
func (c *SQLClient) QuerySGASharedPoolLibraryCache(ctx context.Context) ([]models.SGASharedPoolLibraryCacheMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheShareableStatementSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGASharedPoolLibraryCacheMetric
	for rows.Next() {
		var metric models.SGASharedPoolLibraryCacheMetric
		if err := rows.Scan(&metric.Sum, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGASharedPoolLibraryCacheUser executes the SGA shared pool library cache user query.
func (c *SQLClient) QuerySGASharedPoolLibraryCacheUser(ctx context.Context) ([]models.SGASharedPoolLibraryCacheUserMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheShareableUserSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGASharedPoolLibraryCacheUserMetric
	for rows.Next() {
		var metric models.SGASharedPoolLibraryCacheUserMetric
		if err := rows.Scan(&metric.Sum, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGAMetrics executes the SGA metrics query.
func (c *SQLClient) QuerySGAMetrics(ctx context.Context) ([]models.SGAMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGASQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGAMetric
	for rows.Next() {
		var metric models.SGAMetric
		if err := rows.Scan(&metric.InstID, &metric.Name, &metric.Value); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySysstatMetrics executes the sysstat metrics query.
func (c *SQLClient) QuerySysstatMetrics(ctx context.Context) ([]models.SysstatMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SysstatSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SysstatMetric
	for rows.Next() {
		var metric models.SysstatMetric
		if err := rows.Scan(&metric.InstID, &metric.Name, &metric.Value); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryRollbackSegmentsMetrics executes the rollback segments metrics query.
func (c *SQLClient) QueryRollbackSegmentsMetrics(ctx context.Context) ([]models.RollbackSegmentsMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.RollbackSegmentsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.RollbackSegmentsMetric
	for rows.Next() {
		var metric models.RollbackSegmentsMetric
		if err := rows.Scan(&metric.Gets, &metric.Waits, &metric.Ratio, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryRedoLogWaitsMetrics executes the redo log waits metrics query.
func (c *SQLClient) QueryRedoLogWaitsMetrics(ctx context.Context) ([]models.RedoLogWaitsMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.RedoLogWaitsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.RedoLogWaitsMetric
	for rows.Next() {
		var metric models.RedoLogWaitsMetric
		if err := rows.Scan(&metric.TotalWaits, &metric.InstID, &metric.Event); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGASharedPoolLibraryCacheReloadRatio executes the SGA shared pool library cache reload ratio query.
func (c *SQLClient) QuerySGASharedPoolLibraryCacheReloadRatio(ctx context.Context) ([]models.SGASharedPoolLibraryCacheReloadRatioMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheReloadRatioSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGASharedPoolLibraryCacheReloadRatioMetric
	for rows.Next() {
		var metric models.SGASharedPoolLibraryCacheReloadRatioMetric
		if err := rows.Scan(&metric.Ratio, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGASharedPoolLibraryCacheHitRatio executes the SGA shared pool library cache hit ratio query.
func (c *SQLClient) QuerySGASharedPoolLibraryCacheHitRatio(ctx context.Context) ([]models.SGASharedPoolLibraryCacheHitRatioMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGASharedPoolLibraryCacheHitRatioSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGASharedPoolLibraryCacheHitRatioMetric
	for rows.Next() {
		var metric models.SGASharedPoolLibraryCacheHitRatioMetric
		if err := rows.Scan(&metric.Ratio, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGASharedPoolDictCacheMissRatio executes the SGA shared pool dictionary cache miss ratio query.
func (c *SQLClient) QuerySGASharedPoolDictCacheMissRatio(ctx context.Context) ([]models.SGASharedPoolDictCacheMissRatioMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGASharedPoolDictCacheMissRatioSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGASharedPoolDictCacheMissRatioMetric
	for rows.Next() {
		var metric models.SGASharedPoolDictCacheMissRatioMetric
		if err := rows.Scan(&metric.Ratio, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGALogBufferSpaceWaits executes the SGA log buffer space waits query.
func (c *SQLClient) QuerySGALogBufferSpaceWaits(ctx context.Context) ([]models.SGALogBufferSpaceWaitsMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGALogBufferSpaceWaitsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGALogBufferSpaceWaitsMetric
	for rows.Next() {
		var metric models.SGALogBufferSpaceWaitsMetric
		if err := rows.Scan(&metric.Count, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGALogAllocRetries executes the SGA log allocation retries query.
func (c *SQLClient) QuerySGALogAllocRetries(ctx context.Context) ([]models.SGALogAllocRetriesMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGALogAllocRetriesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGALogAllocRetriesMetric
	for rows.Next() {
		var metric models.SGALogAllocRetriesMetric
		if err := rows.Scan(&metric.Ratio, &metric.InstID); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySGAHitRatio executes the SGA hit ratio query.
func (c *SQLClient) QuerySGAHitRatio(ctx context.Context) ([]models.SGAHitRatioMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SGAHitRatioSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.SGAHitRatioMetric
	for rows.Next() {
		var metric models.SGAHitRatioMetric
		if err := rows.Scan(&metric.InstID, &metric.Ratio); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryDatabaseInfo retrieves database version and platform information
func (c *SQLClient) QueryDatabaseInfo(ctx context.Context) ([]models.DatabaseInfoMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.OptimizedDatabaseInfoSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.DatabaseInfoMetric
	for rows.Next() {
		var metric models.DatabaseInfoMetric
		if err := rows.Scan(&metric.InstID, &metric.VersionFull, &metric.HostName, &metric.DatabaseName, &metric.PlatformName); err != nil {
			return nil, err
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryPDBSysMetrics executes the PDB system metrics query
func (c *SQLClient) QueryPDBSysMetrics(ctx context.Context) ([]models.PDBSysMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.PDBSysMetricsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.PDBSysMetric
	for rows.Next() {
		var metric models.PDBSysMetric
		if err := rows.Scan(&metric.InstID, &metric.MetricName, &metric.Value); err != nil {
			continue
		}
		results = append(results, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryCDBCapability checks if the database is a Container Database
func (c *SQLClient) QueryCDBCapability(ctx context.Context) (*models.CDBCapability, error) {
	var capability models.CDBCapability
	err := c.db.QueryRowContext(ctx, queries.CheckCDBFeatureSQL).Scan(&capability.IsCDB)
	if err != nil {
		if strings.Contains(err.Error(), "ORA-00942") || strings.Contains(err.Error(), "ORA-01722") {
			// Table doesn't exist or invalid number - not a CDB
			capability.IsCDB = 0
			return &capability, nil
		}
		return nil, err
	}
	return &capability, nil
}

// QueryRACDetection checks if Oracle is running in RAC mode
func (c *SQLClient) QueryRACDetection(ctx context.Context) (*models.RACDetection, error) {
	var detection models.RACDetection
	rows, err := c.db.QueryContext(ctx, queries.RACDetectionSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&detection.ClusterDB); err != nil {
			return nil, err
		}
	}

	return &detection, nil
}

// QueryASMDetection checks if ASM instance is available
func (c *SQLClient) QueryASMDetection(ctx context.Context) (*models.ASMDetection, error) {
	var detection models.ASMDetection
	rows, err := c.db.QueryContext(ctx, queries.ASMDetectionSQL)
	if err != nil {
		return &detection, nil // ASM not configured, return zero count
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&detection.ASMCount); err != nil {
			return nil, err
		}
	}

	return &detection, nil
}

// QueryASMDiskGroups retrieves ASM disk group information
func (c *SQLClient) QueryASMDiskGroups(ctx context.Context) ([]models.ASMDiskGroup, error) {
	rows, err := c.db.QueryContext(ctx, queries.ASMDiskGroupSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.ASMDiskGroup
	for rows.Next() {
		var diskGroup models.ASMDiskGroup
		if err := rows.Scan(&diskGroup.Name, &diskGroup.TotalMB, &diskGroup.FreeMB, &diskGroup.OfflineDisks); err != nil {
			continue
		}
		results = append(results, diskGroup)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryClusterWaitEvents retrieves cluster wait event metrics
func (c *SQLClient) QueryClusterWaitEvents(ctx context.Context) ([]models.ClusterWaitEvent, error) {
	rows, err := c.db.QueryContext(ctx, queries.ClusterWaitEventsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.ClusterWaitEvent
	for rows.Next() {
		var event models.ClusterWaitEvent
		if err := rows.Scan(&event.InstID, &event.Event, &event.TotalWaits, &event.TimeWaitedMicro); err != nil {
			continue
		}
		results = append(results, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryRACInstanceStatus retrieves RAC instance status information
func (c *SQLClient) QueryRACInstanceStatus(ctx context.Context) ([]models.RACInstanceStatus, error) {
	rows, err := c.db.QueryContext(ctx, queries.RACInstanceStatusSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.RACInstanceStatus
	for rows.Next() {
		var status models.RACInstanceStatus
		if err := rows.Scan(&status.InstID, &status.InstanceName, &status.HostName, &status.Status,
			&status.StartupTime, &status.DatabaseStatus, &status.ActiveState, &status.Logins,
			&status.Archiver, &status.Version); err != nil {
			continue
		}
		results = append(results, status)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QueryRACActiveServices retrieves RAC active service information
func (c *SQLClient) QueryRACActiveServices(ctx context.Context) ([]models.RACActiveService, error) {
	rows, err := c.db.QueryContext(ctx, queries.RACActiveServicesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.RACActiveService
	for rows.Next() {
		var service models.RACActiveService
		if err := rows.Scan(&service.ServiceName, &service.InstID, &service.FailoverMethod,
			&service.FailoverType, &service.Goal, &service.NetworkName, &service.CreationDate,
			&service.FailoverRetries, &service.FailoverDelay, &service.ClbGoal); err != nil {
			continue
		}
		results = append(results, service)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// QuerySessionCount retrieves the count of user sessions
func (c *SQLClient) QuerySessionCount(ctx context.Context) (*models.SessionCount, error) {
	var count models.SessionCount
	err := c.db.QueryRowContext(ctx, queries.SessionCountSQL).Scan(&count.Count)
	if err != nil {
		if err == sql.ErrNoRows {
			count.Count = 0
			return &count, nil
		}
		return nil, err
	}
	return &count, nil
}

func (c *SQLClient) QuerySystemMetrics(ctx context.Context) ([]models.SystemMetric, error) {
	rows, err := c.db.QueryContext(ctx, queries.SystemSysMetricsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []models.SystemMetric
	for rows.Next() {
		var instID sql.NullString
		var metricName sql.NullString
		var value sql.NullFloat64

		if err := rows.Scan(&instID, &metricName, &value); err != nil {
			return nil, err
		}

		if !metricName.Valid || !value.Valid {
			continue
		}

		metric := models.SystemMetric{
			MetricName: metricName.String,
			Value:      value.Float64,
		}

		if instID.Valid {
			metric.InstanceID = instID.String
		}

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return metrics, nil
}

func (c *SQLClient) QueryTablespaceUsage(ctx context.Context) ([]models.TablespaceUsage, error) {
	rows, err := c.db.QueryContext(ctx, queries.TablespaceMetricsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespaceUsage
	for rows.Next() {
		var ts models.TablespaceUsage
		if err := rows.Scan(&ts.TablespaceName, &ts.UsedPercent, &ts.Used, &ts.Size, &ts.Offline); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespaceGlobalName(ctx context.Context) ([]models.TablespaceGlobalName, error) {
	rows, err := c.db.QueryContext(ctx, queries.GlobalNameTablespaceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespaceGlobalName
	for rows.Next() {
		var ts models.TablespaceGlobalName
		if err := rows.Scan(&ts.TablespaceName, &ts.GlobalName); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespaceDBID(ctx context.Context) ([]models.TablespaceDBID, error) {
	rows, err := c.db.QueryContext(ctx, queries.DBIDTablespaceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespaceDBID
	for rows.Next() {
		var ts models.TablespaceDBID
		if err := rows.Scan(&ts.TablespaceName, &ts.DBID); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespaceCDBDatafilesOffline(ctx context.Context) ([]models.TablespaceCDBDatafilesOffline, error) {
	rows, err := c.db.QueryContext(ctx, queries.CDBDatafilesOfflineTablespaceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespaceCDBDatafilesOffline
	for rows.Next() {
		var ts models.TablespaceCDBDatafilesOffline
		if err := rows.Scan(&ts.OfflineCount, &ts.TablespaceName); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespacePDBDatafilesOffline(ctx context.Context) ([]models.TablespacePDBDatafilesOffline, error) {
	rows, err := c.db.QueryContext(ctx, queries.PDBDatafilesOfflineTablespaceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespacePDBDatafilesOffline
	for rows.Next() {
		var ts models.TablespacePDBDatafilesOffline
		if err := rows.Scan(&ts.OfflineCount, &ts.TablespaceName); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespacePDBDatafilesOfflineCurrentContainer(ctx context.Context) ([]models.TablespacePDBDatafilesOffline, error) {
	rows, err := c.db.QueryContext(ctx, queries.PDBDatafilesOfflineCurrentContainerSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespacePDBDatafilesOffline
	for rows.Next() {
		var ts models.TablespacePDBDatafilesOffline
		if err := rows.Scan(&ts.OfflineCount, &ts.TablespaceName); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespacePDBNonWrite(ctx context.Context) ([]models.TablespacePDBNonWrite, error) {
	rows, err := c.db.QueryContext(ctx, queries.PDBNonWriteTablespaceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespacePDBNonWrite
	for rows.Next() {
		var ts models.TablespacePDBNonWrite
		if err := rows.Scan(&ts.TablespaceName, &ts.NonWriteCount); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}

func (c *SQLClient) QueryTablespacePDBNonWriteCurrentContainer(ctx context.Context) ([]models.TablespacePDBNonWrite, error) {
	rows, err := c.db.QueryContext(ctx, queries.PDBNonWriteCurrentContainerSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tablespaces []models.TablespacePDBNonWrite
	for rows.Next() {
		var ts models.TablespacePDBNonWrite
		if err := rows.Scan(&ts.TablespaceName, &ts.NonWriteCount); err != nil {
			return nil, err
		}
		tablespaces = append(tablespaces, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablespaces, nil
}
