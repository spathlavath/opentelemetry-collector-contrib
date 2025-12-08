// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"database/sql"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
)

// SQLServerClient defines the interface for SQL Server database operations.
// This abstraction allows for easy testing by injecting mock implementations.
// Pattern follows newrelicoraclereceiver with domain-specific methods for each query type.
type SQLServerClient interface {
	// Connection management
	Close() error
	Ping(ctx context.Context) error
	Stats() sql.DBStats

	// Generic query execution (for backwards compatibility and special cases)
	Query(ctx context.Context, dest interface{}, query string) error
	QueryRow(ctx context.Context, query string) *sql.Row

	// Wait time metrics
	QueryWaitTimeMetrics(ctx context.Context, engineEdition int) ([]models.WaitTimeMetricsModel, error)
	QueryLatchWaitTimeMetrics(ctx context.Context, engineEdition int) ([]models.LatchWaitTimeMetricsModel, error)

	// Instance metrics
	QueryInstanceMemory(ctx context.Context, engineEdition int) ([]models.InstanceMemoryDefinitionsModel, error)
	QueryInstanceComprehensiveStats(ctx context.Context, engineEdition int) ([]models.InstanceStatsModel, error)
	QueryInstanceProcessCounts(ctx context.Context, engineEdition int) ([]models.InstanceProcessCountsModel, error)
	QueryInstanceRunnableTasks(ctx context.Context, engineEdition int) ([]models.RunnableTasksMetricsModel, error)
	QueryInstanceActiveConnections(ctx context.Context, engineEdition int) ([]models.InstanceActiveConnectionsMetricsModel, error)
	QueryInstanceBufferPoolHitPercent(ctx context.Context, engineEdition int) ([]models.BufferPoolHitPercentMetricsModel, error)
	QueryInstanceDiskMetrics(ctx context.Context, engineEdition int) ([]models.InstanceDiskMetricsModel, error)
	QueryInstanceBufferPoolSize(ctx context.Context, engineEdition int) ([]models.InstanceBufferMetricsModel, error)
	QueryInstanceTargetMemory(ctx context.Context, engineEdition int) ([]models.InstanceTargetMemoryModel, error)
	QueryInstancePerformanceRatios(ctx context.Context, engineEdition int) ([]models.InstancePerformanceRatiosModel, error)
	QueryInstanceIndexMetrics(ctx context.Context, engineEdition int) ([]models.InstanceIndexMetricsModel, error)
	QueryInstanceLockMetrics(ctx context.Context, engineEdition int) ([]models.InstanceLockMetricsModel, error)

	// Database metrics
	QueryDatabaseBufferMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseBufferMetrics, error)
	QueryDatabaseDiskMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseDiskMetrics, error)
	QueryDatabaseIOMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseIOMetrics, error)
	QueryDatabaseLogGrowthMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseLogGrowthMetrics, error)
	QueryDatabasePageFileMetrics(ctx context.Context, engineEdition int) ([]models.DatabasePageFileMetrics, error)
	QueryDatabasePageFileTotalMetrics(ctx context.Context, engineEdition int) ([]models.DatabasePageFileTotalMetrics, error)
	QueryDatabaseMemoryMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseMemoryMetrics, error)
	QueryDatabaseSizeMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseSizeMetrics, error)
	QueryDatabaseTransactionLogMetrics(ctx context.Context, engineEdition int) ([]models.DatabaseTransactionLogMetrics, error)

	// Database principals and roles
	QueryDatabasePrincipals(ctx context.Context, engineEdition int) ([]models.DatabasePrincipalsMetrics, error)
	QueryDatabasePrincipalsSummary(ctx context.Context, engineEdition int) ([]models.DatabasePrincipalsSummary, error)
	QueryDatabaseRoleMembership(ctx context.Context, engineEdition int) ([]models.DatabaseRoleMembershipMetrics, error)
	QueryDatabaseRoleMembershipSummary(ctx context.Context, engineEdition int) ([]models.DatabaseRoleMembershipSummary, error)
	QueryDatabaseRoleHierarchy(ctx context.Context, engineEdition int) ([]models.DatabaseRoleHierarchy, error)

	// Security metrics
	QuerySecurityPrincipals(ctx context.Context, engineEdition int) ([]models.SecurityPrincipalsModel, error)
	QuerySecurityRoleMembers(ctx context.Context, engineEdition int) ([]models.SecurityRoleMembersModel, error)

	// Lock metrics
	QueryLockResourceSummary(ctx context.Context, engineEdition int) ([]models.LockResourceSummary, error)
	QueryLockModeSummary(ctx context.Context, engineEdition int) ([]models.LockModeSummary, error)

	// TempDB contention metrics
	QueryTempDBContention(ctx context.Context, engineEdition int) ([]models.TempDBContention, error)

	// Thread pool health metrics
	QueryThreadPoolHealth(ctx context.Context, engineEdition int) ([]models.ThreadPoolHealth, error)

	// User connection metrics
	QueryUserConnectionStatus(ctx context.Context, engineEdition int) ([]models.UserConnectionStatusMetrics, error)
	QueryUserConnectionStatusSummary(ctx context.Context, engineEdition int) ([]models.UserConnectionStatusSummary, error)
	QueryUserConnectionByClient(ctx context.Context, engineEdition int) ([]models.UserConnectionByClientMetrics, error)

	// Failover cluster metrics
	QueryFailoverClusterReplica(ctx context.Context, engineEdition int) ([]models.FailoverClusterReplicaMetrics, error)
	QueryFailoverClusterReplicaState(ctx context.Context, engineEdition int) ([]models.FailoverClusterReplicaStateMetrics, error)

	// Query performance monitoring
	QuerySlowQueries(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold, engineEdition int) ([]models.SlowQuery, error)
	QueryActiveQueries(ctx context.Context, engineEdition int) ([]models.ActiveRunningQuery, error)
	QueryExecutionPlanForQuery(ctx context.Context, planHandle string) (string, error)
}
