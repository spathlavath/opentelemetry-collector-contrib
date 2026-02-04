// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

// Oracle SQL queries for connection statistics and session monitoring

// Session Count Queries
const (
	// SessionCountSQL returns the count of user sessions
	SessionCountSQL = "SELECT COUNT(*) AS SESSION_COUNT FROM v$session WHERE type = 'USER'"

	// TotalSessionsSQL returns the total number of sessions
	TotalSessionsSQL = "SELECT COUNT(*) as TOTAL_SESSIONS FROM V$SESSION"

	// ActiveSessionsSQL returns the count of active sessions
	ActiveSessionsSQL = "SELECT COUNT(*) as ACTIVE_SESSIONS FROM V$SESSION WHERE STATUS = 'ACTIVE'"

	// InactiveSessionsSQL returns the count of inactive sessions
	InactiveSessionsSQL = "SELECT COUNT(*) as INACTIVE_SESSIONS FROM V$SESSION WHERE STATUS = 'INACTIVE'"

	// SessionStatusSQL returns session count breakdown by status
	SessionStatusSQL = `
		SELECT 
			STATUS,
			COUNT(*) AS SESSION_COUNT
		FROM V$SESSION 
		GROUP BY STATUS`

	// SessionTypeSQL returns session count breakdown by type
	SessionTypeSQL = `
		SELECT 
			TYPE,
			COUNT(*) AS SESSION_COUNT
		FROM V$SESSION 
		GROUP BY TYPE`

	// LogonsStatsSQL returns logon statistics
	LogonsStatsSQL = `
		SELECT 
			NAME, 
			VALUE 
		FROM V$SYSSTAT 
		WHERE NAME IN ('logons cumulative', 'logons current')`
)

// Connection Pool and Resource Queries
const (

	// ConnectionPoolMetricsSQL returns connection pool related metrics
	ConnectionPoolMetricsSQL = `
		SELECT 
			'shared_servers' AS METRIC_NAME,
			COUNT(*) AS VALUE
		FROM V$SHARED_SERVER
		UNION ALL
		SELECT 
			'dispatchers' AS METRIC_NAME,
			COUNT(*) AS VALUE
		FROM V$DISPATCHER
		UNION ALL
		SELECT 
			'circuits' AS METRIC_NAME,
			COUNT(*) AS VALUE
		FROM V$CIRCUIT
		WHERE STATUS = 'NORMAL'`

	// SessionLimitsSQL returns session and resource limits
	SessionLimitsSQL = `
		SELECT 
			RESOURCE_NAME,
			CURRENT_UTILIZATION,
			MAX_UTILIZATION,
			INITIAL_ALLOCATION,
			LIMIT_VALUE
		FROM V$RESOURCE_LIMIT 
		WHERE RESOURCE_NAME IN ('sessions', 'processes', 'enqueue_locks', 'enqueue_resources')`

	// ConnectionQualitySQL returns connection quality and performance metrics
	ConnectionQualitySQL = `
		SELECT 
			NAME,
			VALUE
		FROM V$SYSSTAT 
		WHERE NAME IN (
			'user commits',
			'user rollbacks', 
			'parse count (total)',
			'parse count (hard)',
			'execute count',
			'SQL*Net roundtrips to/from client',
			'bytes sent via SQL*Net to client',
			'bytes received via SQL*Net from client'
		)`
)
