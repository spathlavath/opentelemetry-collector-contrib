// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// SQLClient implements the SQLServerClient interface for SQL Server database operations
type SQLClient struct {
	db     *sqlx.DB
	logger *zap.Logger
}

// NewSQLClient creates a new SQL client from a database connection
func NewSQLClient(db *sqlx.DB, logger *zap.Logger) *SQLClient {
	return &SQLClient{
		db:     db,
		logger: logger,
	}
}

// Close closes the database connection
func (c *SQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Ping tests the connection to the database
func (c *SQLClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Query runs a query and loads results into dest
func (c *SQLClient) Query(ctx context.Context, dest interface{}, query string) error {
	c.logger.Debug("Running query", zap.String("query", query))
	return c.db.SelectContext(ctx, dest, query)
}

// QueryRow runs a query that returns a single row
func (c *SQLClient) QueryRow(ctx context.Context, query string) *sql.Row {
	c.logger.Debug("Running single row query", zap.String("query", query))
	return c.db.QueryRowContext(ctx, query)
}

// Stats returns database connection statistics
func (c *SQLClient) Stats() sql.DBStats {
	return c.db.Stats()
}

// Wait time metrics methods

func (c *SQLClient) QueryWaitTimeMetrics(ctx context.Context, engineEdition int) ([]models.WaitTimeMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.WaitTimeQueries, "sqlserver.wait_stats.wait_time_metrics", engineEdition)
	if !found {
		return nil, fmt.Errorf("no wait time metrics query available for engine edition %d", engineEdition)
	}
	var results []models.WaitTimeMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryLatchWaitTimeMetrics(ctx context.Context, engineEdition int) ([]models.LatchWaitTimeMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.WaitTimeQueries, "sqlserver.wait_stats.latch.wait_time_metrics", engineEdition)
	if !found {
		return nil, fmt.Errorf("no latch wait time metrics query available for engine edition %d", engineEdition)
	}
	var results []models.LatchWaitTimeMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

// Instance metrics methods

func (c *SQLClient) QueryInstanceMemory(ctx context.Context, engineEdition int) ([]models.InstanceMemoryDefinitionsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.memory", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance memory query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceMemoryDefinitionsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceComprehensiveStats(ctx context.Context, engineEdition int) ([]models.InstanceStatsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.comprehensive_stats", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance comprehensive stats query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceStatsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceProcessCounts(ctx context.Context, engineEdition int) ([]models.InstanceProcessCountsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.process_counts", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance process counts query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceProcessCountsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceRunnableTasks(ctx context.Context, engineEdition int) ([]models.RunnableTasksMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.runnable_tasks", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance runnable tasks query available for engine edition %d", engineEdition)
	}
	var results []models.RunnableTasksMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceActiveConnections(ctx context.Context, engineEdition int) ([]models.InstanceActiveConnectionsMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.active_connections", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance active connections query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceActiveConnectionsMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceBufferPoolHitPercent(ctx context.Context, engineEdition int) ([]models.BufferPoolHitPercentMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.buffer_pool_hit_percent", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance buffer pool hit percent query available for engine edition %d", engineEdition)
	}
	var results []models.BufferPoolHitPercentMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceDiskMetrics(ctx context.Context, engineEdition int) ([]models.InstanceDiskMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.disk_metrics", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance disk metrics query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceDiskMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceBufferPoolSize(ctx context.Context, engineEdition int) ([]models.InstanceBufferMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.buffer_pool_size", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance buffer pool size query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceBufferMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceTargetMemory(ctx context.Context, engineEdition int) ([]models.InstanceTargetMemoryModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.target_memory", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance target memory query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceTargetMemoryModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstancePerformanceRatios(ctx context.Context, engineEdition int) ([]models.InstancePerformanceRatiosModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.performance_ratios", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance performance ratios query available for engine edition %d", engineEdition)
	}
	var results []models.InstancePerformanceRatiosModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceIndexMetrics(ctx context.Context, engineEdition int) ([]models.InstanceIndexMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.index_metrics", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance index metrics query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceIndexMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryInstanceLockMetrics(ctx context.Context, engineEdition int) ([]models.InstanceLockMetricsModel, error) {
	query, found := queries.GetQueryForMetric(queries.InstanceQueries, "sqlserver.instance.lock_metrics", engineEdition)
	if !found {
		return nil, fmt.Errorf("no instance lock metrics query available for engine edition %d", engineEdition)
	}
	var results []models.InstanceLockMetricsModel
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

// Database metrics methods

func (c *SQLClient) QueryDatabaseBufferMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseBufferMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.bufferpool.size", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database buffer metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseBufferMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseDiskMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseDiskMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.disk.max_size", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database disk metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseDiskMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseIOMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseIOMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.io.stall_time", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database IO metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseIOMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseLogGrowthMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseLogGrowthMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.log.transaction_growth", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database log growth metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseLogGrowthMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabasePageFileMetrics(ctx context.Context, engineEdition int) ([]models.DatabasePageFileMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.pagefile.available", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database page file metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabasePageFileMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabasePageFileTotalMetrics(ctx context.Context, engineEdition int) ([]models.DatabasePageFileTotalMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.pagefile.total", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database page file total metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabasePageFileTotalMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseMemoryMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseMemoryMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.instance.memory", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database memory metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseMemoryMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseSizeMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseSizeMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.size", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database size metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseSizeMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseTransactionLogMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseTransactionLogMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "sqlserver.database.log.transaction", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database transaction log metrics query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseTransactionLogMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

// Database principals and roles methods

func (c *SQLClient) QueryDatabasePrincipals(ctx context.Context, engineEdition int) ([]models.DatabasePrincipalsMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "database_principals", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database principals query available for engine edition %d", engineEdition)
	}
	var results []models.DatabasePrincipalsMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabasePrincipalsSummary(ctx context.Context, engineEdition int) ([]models.DatabasePrincipalsSummary, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "database_principals_summary", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database principals summary query available for engine edition %d", engineEdition)
	}
	var results []models.DatabasePrincipalsSummary
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseRoleMembership(ctx context.Context, engineEdition int) ([]models.DatabaseRoleMembershipMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "database_role_membership", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database role membership query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseRoleMembershipMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseRoleMembershipSummary(ctx context.Context, engineEdition int) ([]models.DatabaseRoleMembershipSummary, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "database_role_membership_summary", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database role membership summary query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseRoleMembershipSummary
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryDatabaseRoleHierarchy(ctx context.Context, engineEdition int) ([]models.DatabaseRoleHierarchy, error) {
	query, found := queries.GetQueryForMetric(queries.DatabaseQueries, "database_role_hierarchy", engineEdition)
	if !found {
		return nil, fmt.Errorf("no database role hierarchy query available for engine edition %d", engineEdition)
	}
	var results []models.DatabaseRoleHierarchy
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

// Security metrics methods

func (c *SQLClient) QuerySecurityPrincipals(ctx context.Context, engineEdition int) ([]models.SecurityPrincipalsModel, error) {
	var results []models.SecurityPrincipalsModel
	err := c.db.SelectContext(ctx, &results, queries.SecurityPrincipalsQuery)
	return results, err
}

func (c *SQLClient) QuerySecurityRoleMembers(ctx context.Context, engineEdition int) ([]models.SecurityRoleMembersModel, error) {
	var results []models.SecurityRoleMembersModel
	err := c.db.SelectContext(ctx, &results, queries.SecurityRoleMembersQuery)
	return results, err
}

// Lock metrics methods

func (c *SQLClient) QueryLockResourceSummary(ctx context.Context, engineEdition int) ([]models.LockResourceSummary, error) {
	var results []models.LockResourceSummary
	err := c.db.SelectContext(ctx, &results, queries.LockResourceQuery)
	return results, err
}

func (c *SQLClient) QueryLockModeSummary(ctx context.Context, engineEdition int) ([]models.LockModeSummary, error) {
	var results []models.LockModeSummary
	err := c.db.SelectContext(ctx, &results, queries.LockModeQuery)
	return results, err
}

// TempDB contention methods

func (c *SQLClient) QueryTempDBContention(ctx context.Context, engineEdition int) ([]models.TempDBContention, error) {
	var results []models.TempDBContention
	err := c.db.SelectContext(ctx, &results, queries.TempDBContentionQuery)
	return results, err
}

// Thread pool health methods

func (c *SQLClient) QueryThreadPoolHealth(ctx context.Context, engineEdition int) ([]models.ThreadPoolHealth, error) {
	var results []models.ThreadPoolHealth
	err := c.db.SelectContext(ctx, &results, queries.ThreadPoolHealthQuery)
	return results, err
}

// User connection metrics methods

func (c *SQLClient) QueryUserConnectionStatus(ctx context.Context, engineEdition int) ([]models.UserConnectionStatusMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.UserConnectionQueries, "user_connection_status", engineEdition)
	if !found {
		return nil, fmt.Errorf("no user connection status query available for engine edition %d", engineEdition)
	}
	var results []models.UserConnectionStatusMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryUserConnectionStatusSummary(ctx context.Context, engineEdition int) ([]models.UserConnectionStatusSummary, error) {
	query, found := queries.GetQueryForMetric(queries.UserConnectionQueries, "user_connection_status_summary", engineEdition)
	if !found {
		return nil, fmt.Errorf("no user connection status summary query available for engine edition %d", engineEdition)
	}
	var results []models.UserConnectionStatusSummary
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryUserConnectionByClient(ctx context.Context, engineEdition int) ([]models.UserConnectionByClientMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.UserConnectionQueries, "user_connection_by_client", engineEdition)
	if !found {
		return nil, fmt.Errorf("no user connection by client query available for engine edition %d", engineEdition)
	}
	var results []models.UserConnectionByClientMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

// Failover cluster metrics methods

func (c *SQLClient) QueryFailoverClusterReplica(ctx context.Context, engineEdition int) ([]models.FailoverClusterReplicaMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.FailoverClusterQueries, "failover_cluster_replica", engineEdition)
	if !found {
		return nil, fmt.Errorf("no failover cluster replica query available for engine edition %d", engineEdition)
	}
	var results []models.FailoverClusterReplicaMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryFailoverClusterReplicaState(ctx context.Context, engineEdition int) ([]models.FailoverClusterReplicaStateMetrics, error) {
	query, found := queries.GetQueryForMetric(queries.FailoverClusterQueries, "failover_cluster_replica_state", engineEdition)
	if !found {
		return nil, fmt.Errorf("no failover cluster replica state query available for engine edition %d", engineEdition)
	}
	var results []models.FailoverClusterReplicaStateMetrics
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

// Query performance monitoring methods

func (c *SQLClient) QuerySlowQueries(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold int, engineEdition int) ([]models.SlowQuery, error) {
	query := fmt.Sprintf(`
		SELECT TOP %d
			qs.sql_handle as query_id,
			qs.plan_handle,
			qs.execution_count,
			qs.total_worker_time / 1000 as total_cpu_time_ms,
			qs.total_elapsed_time / 1000 as total_elapsed_time_ms,
			qs.total_logical_reads,
			qs.total_physical_reads,
			qs.total_logical_writes,
			SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
				((CASE qs.statement_end_offset
					WHEN -1 THEN DATALENGTH(st.text)
					ELSE qs.statement_end_offset
				END - qs.statement_start_offset)/2) + 1) AS query_text
		FROM sys.dm_exec_query_stats qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
		WHERE qs.total_elapsed_time / 1000 > %d
		ORDER BY qs.total_elapsed_time DESC
	`, topN, elapsedTimeThreshold)
	
	var results []models.SlowQuery
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryActiveQueries(ctx context.Context, engineEdition int) ([]models.ActiveRunningQuery, error) {
	query := `
		SELECT 
			r.session_id as current_session_id,
			r.request_id,
			r.command as request_command,
			r.status as request_status,
			r.wait_type,
			r.wait_time / 1000.0 as wait_time_s,
			r.wait_resource,
			r.last_wait_type,
			DB_NAME(r.database_id) as database_name,
			s.login_name,
			s.host_name,
			s.program_name
		FROM sys.dm_exec_requests r
		LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
		WHERE r.session_id != @@SPID
	`
	
	var results []models.ActiveRunningQuery
	err := c.db.SelectContext(ctx, &results, query)
	return results, err
}

func (c *SQLClient) QueryExecutionPlanForQuery(ctx context.Context, planHandle string) (string, error) {
	query := fmt.Sprintf(`
		SELECT query_plan
		FROM sys.dm_exec_query_plan(%s)
	`, planHandle)
	
	var planXML string
	err := c.db.QueryRowContext(ctx, query).Scan(&planXML)
	return planXML, err
}
