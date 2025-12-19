// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// showing how user connections are distributed across different states.
//
// User Connection Status Metrics Overview:
//
// User connection status represents the current state of active user sessions in SQL Server,
// providing insights into connection behavior and potential performance issues. This is
// critical for:
//
// 1. Connection Pool Monitoring: Understanding how connections are being utilized
// 2. Performance Analysis: Identifying bottlenecks through connection status patterns
// 3. Capacity Planning: Monitoring connection usage patterns and limits
// 4. Issue Detection: High numbers of specific statuses can indicate problems
//
// Connection Status Types:
//
// In SQL Server, user sessions can be in various states:
// - Running: Currently executing a request
// - Sleeping: Idle connection waiting for next request
// - Suspended: Waiting for a resource (I/O, lock, etc.)
// - Runnable: Ready to run but waiting for CPU time
// - Background: System background processes (excluded from user connections)
// - Dormant: Connection exists but no current activity
//
// Key Monitoring Use Cases:
// - High "sleeping" count: May indicate connection pooling issues or leaked connections
// - High "suspended" count: May indicate resource contention (locks, I/O waits)
// - High "blocked" count: May indicate blocking chain issues
// - Trend analysis: Changes in connection patterns over time
//
// Query Source:
// Based on sys.dm_exec_sessions with filtering for user processes:
// ```sql
// SELECT status, COUNT(session_id) AS session_count
// FROM sys.dm_exec_sessions
// WHERE is_user_process = 1
// GROUP BY status;
// ```
//
// Monitoring Benefits:
// - Real-time connection health visibility
// - Early detection of connection pool issues
// - Performance bottleneck identification
// - Capacity planning support
// - Application behavior insights
//
// Engine Compatibility:
// - Standard SQL Server: Full access to session status information
// - Azure SQL Database: Full session status visibility within database scope
// - Azure SQL Managed Instance: Complete functionality with all session states
package models

// UserConnectionStatusSummary represents aggregated statistics about user connection statuses
// This model provides summary metrics for monitoring and alerting on connection patterns
type UserConnectionStatusSummary struct {
	// TotalUserConnections is the total count of active user connections
	// Sum of all user sessions regardless of status
	TotalUserConnections *int64 `db:"total_user_connections" metric_name:"sqlserver.user_connections.total" source_type:"gauge"`

	// SleepingConnections is the count of connections in sleeping state
	// High numbers may indicate connection pool issues or connection leaks
	SleepingConnections *int64 `db:"sleeping_connections" metric_name:"sqlserver.user_connections.sleeping" source_type:"gauge"`

	// RunningConnections is the count of connections actively executing requests
	// Indicates current workload and system activity
	RunningConnections *int64 `db:"running_connections" metric_name:"sqlserver.user_connections.running" source_type:"gauge"`

	// SuspendedConnections is the count of connections waiting for resources
	// High numbers may indicate resource contention (locks, I/O, memory)
	SuspendedConnections *int64 `db:"suspended_connections" metric_name:"sqlserver.user_connections.suspended" source_type:"gauge"`

	// RunnableConnections is the count of connections ready to run but waiting for CPU
	// High numbers may indicate CPU pressure or scheduler contention
	RunnableConnections *int64 `db:"runnable_connections" metric_name:"sqlserver.user_connections.runnable" source_type:"gauge"`
}

// UserConnectionUtilization represents connection utilization metrics
// This model provides insights into connection efficiency and usage patterns
type UserConnectionUtilization struct {
	// ActiveConnectionRatio is the percentage of connections actively doing work
	// (running + runnable) / total_connections * 100
	ActiveConnectionRatio *float64 `db:"active_connection_ratio" metric_name:"sqlserver.user_connections.utilization.active_ratio" source_type:"gauge"`

	// IdleConnectionRatio is the percentage of connections that are idle
	// (sleeping + dormant) / total_connections * 100
	IdleConnectionRatio *float64 `db:"idle_connection_ratio" metric_name:"sqlserver.user_connections.utilization.idle_ratio" source_type:"gauge"`
}

// UserConnectionByClientMetrics represents user connections grouped by client host and program
// This model captures connection distribution by source client and application
// as defined by the sys.dm_exec_sessions system view
type UserConnectionByClientMetrics struct {
	// HostName represents the name of the client host/machine making the connection
	// This corresponds to the 'host_name' column in sys.dm_exec_sessions
	// Examples: "WEB-SERVER-01", "APP-CLIENT-05", "DESKTOP-ABC123"
	HostName string `db:"host_name" source_type:"attribute"`

	// ProgramName represents the name of the client application/program
	// This corresponds to the 'program_name' column in sys.dm_exec_sessions
	// Examples: "Microsoft SQL Server Management Studio", "MyApp.exe", ".Net SqlClient Data Provider"
	ProgramName string `db:"program_name" source_type:"attribute"`

	// ConnectionCount is the number of connections from this host/program combination
	// This corresponds to COUNT(session_id) grouped by host_name and program_name
	// Useful for identifying connection patterns and potential issues
	ConnectionCount *int64 `db:"connection_count" metric_name:"sqlserver.user_connections.client.count" source_type:"gauge"`
}

// UserConnectionClientSummary represents aggregated statistics about client connections
// This model provides summary metrics for monitoring connection sources and applications
type UserConnectionClientSummary struct {
	// UniqueHosts is the count of distinct client hosts with active connections
	// Helps understand the distribution of connection sources
	UniqueHosts *int64 `db:"unique_hosts" metric_name:"sqlserver.user_connections.client.unique_hosts" source_type:"gauge"`

	// UniquePrograms is the count of distinct programs/applications with active connections
	// Helps understand application diversity and usage patterns
	UniquePrograms *int64 `db:"unique_programs" metric_name:"sqlserver.user_connections.client.unique_programs" source_type:"gauge"`
}

// LoginLogoutSummary represents aggregated login/logout statistics
// This provides summary metrics for authentication activity analysis
type LoginLogoutSummary struct {
	// LoginsPerSec is the current login rate per second
	// Indicates new connection establishment rate
	LoginsPerSec *int64 `db:"logins_per_sec" metric_name:"sqlserver.user_connections.authentication.logins_per_sec" source_type:"gauge"`

	// ConnectionChurnRate is the logout/login ratio as percentage
	// High values indicate excessive connection turnover
	ConnectionChurnRate *float64 `db:"connection_churn_rate" metric_name:"sqlserver.user_connections.authentication.churn_rate" source_type:"gauge"`

	// Username represents the user for grouped authentication statistics
	// Allows for per-user authentication analysis
	Username *string `db:"username" source_type:"attribute"`

	// SourceIP represents the client IP address for grouped statistics
	// Allows for per-IP authentication analysis
	SourceIP *string `db:"source_ip" source_type:"attribute"`
}

// FailedLoginSummary represents aggregated failed login statistics
// This provides summary metrics for security monitoring and analysis
type FailedLoginSummary struct {
	// TotalFailedLogins is the total count of failed login attempts in the current error log
	TotalFailedLogins *int64 `db:"total_failed_logins" metric_name:"sqlserver.user_connections.authentication.total_failed_logins" source_type:"gauge"`

	// RecentFailedLogins is the count of failed logins in the last hour
	RecentFailedLogins *int64 `db:"recent_failed_logins" metric_name:"sqlserver.user_connections.authentication.recent_failed_logins" source_type:"gauge"`

	// UniqueFailedUsers is the count of distinct usernames with failed logins
	UniqueFailedUsers *int64 `db:"unique_failed_users" metric_name:"sqlserver.user_connections.authentication.unique_failed_users" source_type:"gauge"`

	// UniqueFailedSources is the count of distinct source IPs with failed logins
	UniqueFailedSources *int64 `db:"unique_failed_sources" metric_name:"sqlserver.user_connections.authentication.unique_failed_sources" source_type:"gauge"`

	// Username for per-user failed login aggregation
	// Enables grouping of failed login metrics by specific user
	Username *string `db:"username" source_type:"attribute"`

	// SourceIP for per-IP failed login aggregation
	// Enables grouping of failed login metrics by source location
	SourceIP *string `db:"source_ip" source_type:"attribute"`
}
