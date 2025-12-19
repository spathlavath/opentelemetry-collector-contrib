// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// User Connection Metrics Queries:
//
// 1. Connection Status Distribution:
//   - Groups user connections by their current status
//   - Provides insight into connection behavior and potential issues
//   - Source: sys.dm_exec_sessions with user process filtering
//
// 2. Connection Summary Statistics:
//   - Aggregated counts for different connection states
//   - Pre-calculated metrics for common monitoring scenarios
//   - Optimized for dashboard and alerting use cases
//
// 3. Connection Utilization Analysis:
//   - Calculates ratios and efficiency metrics
//   - Helps identify connection pool optimization opportunities
//   - Provides percentage-based metrics for trend analysis
//
// Query Compatibility:
// - Standard SQL Server: Full access to all session information
// - Azure SQL Database: Complete session visibility within database scope
// - Azure SQL Managed Instance: Full functionality with all connection states
//
// Performance Considerations:
// - Uses sys.dm_exec_sessions which is lightweight and fast
// - Filters to user processes only (is_user_process = 1)
// - Minimal resource impact suitable for frequent collection
// - No locks or blocking operations
package queries

// The query returns:
// - status: Current status of the user session (running, sleeping, suspended, etc.)
// - session_count: Number of user sessions in each status
//
// This is particularly useful for:
// - Identifying connection pooling issues (high sleeping count)
// - Detecting resource contention (high suspended count)
// - Monitoring active workload (running/runnable counts)
const UserConnectionStatusQuery = `SELECT 
    status,
    COUNT(session_id) AS session_count
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1
GROUP BY status`

// Azure SQL Database supports full session monitoring within the database scope
const UserConnectionStatusQueryAzureSQL = `SELECT 
    status,
    COUNT(session_id) AS session_count
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
GROUP BY status`

// Azure SQL Managed Instance has full session monitoring capabilities
const UserConnectionStatusQueryAzureMI = `SELECT 
    status,
    COUNT(session_id) AS session_count
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1
GROUP BY status`

// UserConnectionSummaryQuery returns aggregated user connection statistics
//
// The query returns:
// - total_user_connections: Total count of user connections
// - sleeping_connections: Count of idle connections
// - running_connections: Count of actively executing connections
// - suspended_connections: Count of connections waiting for resources
// - runnable_connections: Count of connections ready to run
const UserConnectionSummaryQuery = `SELECT
    COUNT(*) AS total_user_connections,
    SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS sleeping_connections,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_connections,
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended_connections,
    SUM(CASE WHEN status = 'runnable' THEN 1 ELSE 0 END) AS runnable_connections
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1`

const UserConnectionSummaryQueryAzureSQL = `SELECT
    COUNT(*) AS total_user_connections,
    SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS sleeping_connections,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_connections,
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended_connections,
    SUM(CASE WHEN status = 'runnable' THEN 1 ELSE 0 END) AS runnable_connections
FROM sys.dm_exec_sessions
WHERE is_user_process = 1`

const UserConnectionSummaryQueryAzureMI = `SELECT
    COUNT(*) AS total_user_connections,
    SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) AS sleeping_connections,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_connections,
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended_connections,
    SUM(CASE WHEN status = 'runnable' THEN 1 ELSE 0 END) AS runnable_connections
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1`

// UserConnectionUtilizationQuery returns connection utilization metrics
//
// The query returns:
// - active_connection_ratio: Percentage of connections actively working
// - idle_connection_ratio: Percentage of connections that are idle
const UserConnectionUtilizationQuery = `WITH ConnectionStats AS (
    SELECT
        COUNT(*) AS total_connections,
        SUM(CASE WHEN status IN ('running', 'runnable') THEN 1 ELSE 0 END) AS active_connections,
        SUM(CASE WHEN status IN ('sleeping', 'dormant') THEN 1 ELSE 0 END) AS idle_connections
    FROM sys.dm_exec_sessions WITH (NOLOCK)
    WHERE is_user_process = 1
)
SELECT
    CASE WHEN total_connections > 0
        THEN (active_connections * 100.0) / total_connections
        ELSE 0
    END AS active_connection_ratio,
    CASE WHEN total_connections > 0
        THEN (idle_connections * 100.0) / total_connections
        ELSE 0
    END AS idle_connection_ratio
FROM ConnectionStats`

const UserConnectionUtilizationQueryAzureSQL = `WITH ConnectionStats AS (
    SELECT
        COUNT(*) AS total_connections,
        SUM(CASE WHEN status IN ('running', 'runnable') THEN 1 ELSE 0 END) AS active_connections,
        SUM(CASE WHEN status IN ('sleeping', 'dormant') THEN 1 ELSE 0 END) AS idle_connections
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
)
SELECT
    CASE WHEN total_connections > 0
        THEN (active_connections * 100.0) / total_connections
        ELSE 0
    END AS active_connection_ratio,
    CASE WHEN total_connections > 0
        THEN (idle_connections * 100.0) / total_connections
        ELSE 0
    END AS idle_connection_ratio
FROM ConnectionStats`

const UserConnectionUtilizationQueryAzureMI = `WITH ConnectionStats AS (
    SELECT
        COUNT(*) AS total_connections,
        SUM(CASE WHEN status IN ('running', 'runnable') THEN 1 ELSE 0 END) AS active_connections,
        SUM(CASE WHEN status IN ('sleeping', 'dormant') THEN 1 ELSE 0 END) AS idle_connections
    FROM sys.dm_exec_sessions WITH (NOLOCK)
    WHERE is_user_process = 1
)
SELECT
    CASE WHEN total_connections > 0
        THEN (active_connections * 100.0) / total_connections
        ELSE 0
    END AS active_connection_ratio,
    CASE WHEN total_connections > 0
        THEN (idle_connections * 100.0) / total_connections
        ELSE 0
    END AS idle_connection_ratio
FROM ConnectionStats`

// The query returns:
// - host_name: Name of the client host/machine making the connection
// - program_name: Name of the client application/program
// - connection_count: Number of connections from this host/program combination
//
// This is particularly useful for:
// - Identifying which applications generate the most connections
// - Understanding connection source distribution
// - Detecting potential connection pooling issues by application
// - Monitoring client-side connection patterns
const UserConnectionByClientQuery = `SELECT 
    ISNULL(host_name, 'Unknown') AS host_name,
    ISNULL(program_name, 'Unknown') AS program_name,
    COUNT(session_id) AS connection_count
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1
GROUP BY host_name, program_name
ORDER BY connection_count DESC`

const UserConnectionByClientQueryAzureSQL = `SELECT 
    ISNULL(host_name, 'Unknown') AS host_name,
    ISNULL(program_name, 'Unknown') AS program_name,
    COUNT(session_id) AS connection_count
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
GROUP BY host_name, program_name
ORDER BY connection_count DESC`

const UserConnectionByClientQueryAzureMI = `SELECT 
    ISNULL(host_name, 'Unknown') AS host_name,
    ISNULL(program_name, 'Unknown') AS program_name,
    COUNT(session_id) AS connection_count
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1
GROUP BY host_name, program_name
ORDER BY connection_count DESC`

// UserConnectionClientSummaryQuery returns aggregated statistics about client connections
//
// The query returns:
// - unique_hosts: Count of distinct client hosts
// - unique_programs: Count of distinct programs/applications
const UserConnectionClientSummaryQuery = `SELECT
    COUNT(DISTINCT host_name) AS unique_hosts,
    COUNT(DISTINCT program_name) AS unique_programs
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1
  AND host_name IS NOT NULL
  AND program_name IS NOT NULL`

const UserConnectionClientSummaryQueryAzureSQL = `SELECT
    COUNT(DISTINCT host_name) AS unique_hosts,
    COUNT(DISTINCT program_name) AS unique_programs
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
  AND host_name IS NOT NULL
  AND program_name IS NOT NULL`

const UserConnectionClientSummaryQueryAzureMI = `SELECT
    COUNT(DISTINCT host_name) AS unique_hosts,
    COUNT(DISTINCT program_name) AS unique_programs
FROM sys.dm_exec_sessions WITH (NOLOCK)
WHERE is_user_process = 1
  AND host_name IS NOT NULL
  AND program_name IS NOT NULL`

// The query returns:
// - counter_name: Name of the performance counter (Logins/sec, Logouts/sec)
// - cntr_value: Current counter value representing rate per second
// - username: Username from active sessions for context
// - source_ip: Client IP address from session host information
//
// These metrics are useful for:
// - Monitoring authentication activity and connection patterns per user
// - Detecting abnormal login/logout spikes for specific users or IPs
// - Understanding connection churn by user and location
// - Identifying potential security events or authentication problems by source
const LoginLogoutQuery = `WITH auth_counters AS (
    SELECT
        RTRIM(counter_name) AS counter_name,
        cntr_value
    FROM sys.dm_os_performance_counters WITH (NOLOCK)
    WHERE object_name LIKE '%General Statistics%'
        AND counter_name IN ('Logins/sec', 'Logouts/sec')
),
user_sessions AS (
    SELECT DISTINCT
        ISNULL(login_name, 'unknown') AS username,
        ISNULL(host_name, 'unknown') AS source_ip
    FROM sys.dm_exec_sessions WITH (NOLOCK)
    WHERE is_user_process = 1
        AND session_id != @@SPID
)
SELECT 
    ac.counter_name,
    ac.cntr_value,
    us.username,
    us.source_ip
FROM auth_counters ac
CROSS JOIN user_sessions us`

const LoginLogoutQueryAzureSQL = `WITH auth_counters AS (
    SELECT
        RTRIM(counter_name) AS counter_name,
        cntr_value
    FROM sys.dm_os_performance_counters
    WHERE object_name LIKE '%General Statistics%'
        AND counter_name IN ('Logins/sec', 'Logouts/sec')
),
user_sessions AS (
    SELECT DISTINCT
        ISNULL(login_name, 'unknown') AS username,
        ISNULL(host_name, 'unknown') AS source_ip
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
        AND session_id != @@SPID
)
SELECT 
    ac.counter_name,
    ac.cntr_value,
    us.username,
    us.source_ip
FROM auth_counters ac
CROSS JOIN user_sessions us`

const LoginLogoutQueryAzureMI = `WITH auth_counters AS (
    SELECT
        RTRIM(counter_name) AS counter_name,
        cntr_value
    FROM sys.dm_os_performance_counters WITH (NOLOCK)
    WHERE object_name LIKE '%General Statistics%'
        AND counter_name IN ('Logins/sec', 'Logouts/sec')
),
user_sessions AS (
    SELECT DISTINCT
        ISNULL(login_name, 'unknown') AS username,
        ISNULL(host_name, 'unknown') AS source_ip
    FROM sys.dm_exec_sessions WITH (NOLOCK)
    WHERE is_user_process = 1
        AND session_id != @@SPID
)
SELECT 
    ac.counter_name,
    ac.cntr_value,
    us.username,
    us.source_ip
FROM auth_counters ac
CROSS JOIN user_sessions us`

// LoginLogoutSummaryQuery returns aggregated login/logout statistics with user grouping
//
// The query returns:
// - logins_per_sec: Current login rate per second
// - connection_churn_rate: Percentage of connections that are being churned (logout/login ratio)
// - username: Username for grouping statistics
// - source_ip: Source IP for grouping statistics
const LoginLogoutSummaryQuery = `WITH AuthStats AS (
    SELECT
        CASE WHEN counter_name = 'Logins/sec' THEN cntr_value ELSE 0 END AS logins_per_sec,
        CASE WHEN counter_name = 'Logouts/sec' THEN cntr_value ELSE 0 END AS logouts_per_sec
    FROM sys.dm_os_performance_counters WITH (NOLOCK)
    WHERE object_name LIKE '%General Statistics%'
        AND counter_name IN ('Logins/sec', 'Logouts/sec')
),
user_sessions AS (
    SELECT DISTINCT
        ISNULL(login_name, 'unknown') AS username,
        ISNULL(host_name, 'unknown') AS source_ip
    FROM sys.dm_exec_sessions WITH (NOLOCK)
    WHERE is_user_process = 1
        AND session_id != @@SPID
)
SELECT
    MAX(logins_per_sec) AS logins_per_sec,
    CASE
        WHEN MAX(logins_per_sec) > 0
        THEN (MAX(logouts_per_sec) * 100.0) / MAX(logins_per_sec)
        ELSE 0
    END AS connection_churn_rate,
    us.username,
    us.source_ip
FROM AuthStats
CROSS JOIN user_sessions us
GROUP BY us.username, us.source_ip`

const LoginLogoutSummaryQueryAzureSQL = `WITH AuthStats AS (
    SELECT
        CASE WHEN counter_name = 'Logins/sec' THEN cntr_value ELSE 0 END AS logins_per_sec,
        CASE WHEN counter_name = 'Logouts/sec' THEN cntr_value ELSE 0 END AS logouts_per_sec
    FROM sys.dm_os_performance_counters
    WHERE object_name LIKE '%General Statistics%'
        AND counter_name IN ('Logins/sec', 'Logouts/sec')
),
user_sessions AS (
    SELECT DISTINCT
        ISNULL(login_name, 'unknown') AS username,
        ISNULL(host_name, 'unknown') AS source_ip
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
        AND session_id != @@SPID
)
SELECT
    MAX(logins_per_sec) AS logins_per_sec,
    CASE
        WHEN MAX(logins_per_sec) > 0
        THEN (MAX(logouts_per_sec) * 100.0) / MAX(logins_per_sec)
        ELSE 0
    END AS connection_churn_rate,
    us.username,
    us.source_ip
FROM AuthStats
CROSS JOIN user_sessions us
GROUP BY us.username, us.source_ip`

const LoginLogoutSummaryQueryAzureMI = `WITH AuthStats AS (
    SELECT
        CASE WHEN counter_name = 'Logins/sec' THEN cntr_value ELSE 0 END AS logins_per_sec,
        CASE WHEN counter_name = 'Logouts/sec' THEN cntr_value ELSE 0 END AS logouts_per_sec
    FROM sys.dm_os_performance_counters WITH (NOLOCK)
    WHERE object_name LIKE '%General Statistics%'
        AND counter_name IN ('Logins/sec', 'Logouts/sec')
),
user_sessions AS (
    SELECT DISTINCT
        ISNULL(login_name, 'unknown') AS username,
        ISNULL(host_name, 'unknown') AS source_ip
    FROM sys.dm_exec_sessions WITH (NOLOCK)
    WHERE is_user_process = 1
        AND session_id != @@SPID
)
SELECT
    MAX(logins_per_sec) AS logins_per_sec,
    CASE
        WHEN MAX(logins_per_sec) > 0
        THEN (MAX(logouts_per_sec) * 100.0) / MAX(logins_per_sec)
        ELSE 0
    END AS connection_churn_rate,
    us.username,
    us.source_ip
FROM AuthStats
CROSS JOIN user_sessions us
GROUP BY us.username, us.source_ip`

const FailedLoginQuery = `
DECLARE @FailedLogins TABLE (
    LogDate DATETIME,
    ProcessInfo NVARCHAR(100),
    Text NVARCHAR(MAX)
);

INSERT INTO @FailedLogins (LogDate, ProcessInfo, Text)
EXEC sp_readerrorlog 0, 1, 'Login failed';

SELECT 
    LogDate,
    ProcessInfo,
    Text,
    -- Extract username from the failed login message
    CASE 
        WHEN Text LIKE '%for user ''%''%' THEN 
            SUBSTRING(Text, CHARINDEX('for user ''', Text) + 10, 
                      CHARINDEX('''', Text, CHARINDEX('for user ''', Text) + 10) - CHARINDEX('for user ''', Text) - 10)
        ELSE 'unknown'
    END AS username,
    -- Extract source IP from the failed login message  
    CASE 
        WHEN Text LIKE '%Client: %' THEN 
            RTRIM(LTRIM(SUBSTRING(Text, CHARINDEX('Client: ', Text) + 8, 50)))
        WHEN Text LIKE '%[CLIENT: %]%' THEN 
            SUBSTRING(Text, CHARINDEX('[CLIENT: ', Text) + 9, 
                      CHARINDEX(']', Text, CHARINDEX('[CLIENT: ', Text)) - CHARINDEX('[CLIENT: ', Text) - 9)
        ELSE 'unknown'
    END AS source_ip
FROM @FailedLogins`

// Azure SQL Database uses sys.event_log to track connection failures and authentication events
const FailedLoginQueryAzureSQL = `SELECT
    event_type,
    event_subtype_desc AS description,
    start_time,
    JSON_VALUE(CAST(additional_data AS NVARCHAR(MAX)), '$.client_ip') AS client_ip,
    -- Extract username from additional_data if available
    ISNULL(JSON_VALUE(CAST(additional_data AS NVARCHAR(MAX)), '$.user_name'), 'unknown') AS username,
    -- Use client_ip as source_ip for consistency
    ISNULL(JSON_VALUE(CAST(additional_data AS NVARCHAR(MAX)), '$.client_ip'), 'unknown') AS source_ip
FROM
    sys.event_log
WHERE
    event_type IN ('connection_failed')
    AND start_time >= DATEADD(HOUR, -24, GETUTCDATE())
ORDER BY
    start_time DESC`

const FailedLoginQueryAzureMI = `
DECLARE @FailedLogins TABLE (
    LogDate DATETIME,
    ProcessInfo NVARCHAR(100),
    Text NVARCHAR(MAX)
);

INSERT INTO @FailedLogins (LogDate, ProcessInfo, Text)
EXEC sp_readerrorlog 0, 1, 'Login failed';

SELECT 
    LogDate,
    ProcessInfo,
    Text,
    -- Extract username from the failed login message
    CASE 
        WHEN Text LIKE '%for user ''%''%' THEN 
            SUBSTRING(Text, CHARINDEX('for user ''', Text) + 10, 
                      CHARINDEX('''', Text, CHARINDEX('for user ''', Text) + 10) - CHARINDEX('for user ''', Text) - 10)
        ELSE 'unknown'
    END AS username,
    -- Extract source IP from the failed login message  
    CASE 
        WHEN Text LIKE '%Client: %' THEN 
            RTRIM(LTRIM(SUBSTRING(Text, CHARINDEX('Client: ', Text) + 8, 50)))
        WHEN Text LIKE '%[CLIENT: %]%' THEN 
            SUBSTRING(Text, CHARINDEX('[CLIENT: ', Text) + 9, 
                      CHARINDEX(']', Text, CHARINDEX('[CLIENT: ', Text)) - CHARINDEX('[CLIENT: ', Text) - 9)
        ELSE 'unknown'
    END AS source_ip
FROM @FailedLogins`

// FailedLoginSummaryQuery returns aggregated statistics about failed login attempts with user grouping
// Always returns at least one row with zeros if no failed logins exist
const FailedLoginSummaryQuery = `
DECLARE @FailedLogins TABLE (
    LogDate DATETIME,
    ProcessInfo NVARCHAR(100),
    Text NVARCHAR(MAX)
);

-- Insert data from the current and previous error logs
-- This captures "Login failed" messages
INSERT INTO @FailedLogins (LogDate, ProcessInfo, Text)
EXEC sp_readerrorlog 0, 1, 'Login failed';

INSERT INTO @FailedLogins (LogDate, ProcessInfo, Text)
EXEC sp_readerrorlog 1, 1, 'Login failed';

-- Use a Common Table Expression (CTE) to filter and parse the log text
WITH FilteredLogins AS (
    SELECT
        LogDate,
        -- Parse the username from the log text
        CASE
            WHEN Text LIKE '%for user ''%''%' THEN
                SUBSTRING(Text, CHARINDEX('for user ''', Text) + 10,
                          CHARINDEX('''', Text, CHARINDEX('for user ''', Text) + 10) - CHARINDEX('for user ''', Text) - 10)
            ELSE 'unknown'
        END AS failed_user,
        -- Parse the client IP address from the log text
        CASE
            WHEN Text LIKE '%Client: %' THEN
                RTRIM(LTRIM(SUBSTRING(Text, CHARINDEX('Client: ', Text) + 8, 50)))
            WHEN Text LIKE '%[CLIENT: %]%' THEN
                SUBSTRING(Text, CHARINDEX('[CLIENT: ', Text) + 9,
                          CHARINDEX(']', Text, CHARINDEX('[CLIENT: ', Text)) - CHARINDEX('[CLIENT: ', Text) - 9)
            ELSE 'unknown'
        END AS source_ip
    FROM @FailedLogins
    WHERE Text LIKE '%Login failed for user%' -- Ensure we only process relevant rows
)
-- Aggregate the final metrics
-- Use COALESCE to ensure we always return at least one row with zeros
SELECT
    COALESCE(SUM(1), 0) AS total_failed_logins,
    COALESCE(SUM(CASE WHEN LogDate >= DATEADD(HOUR, -1, GETDATE()) THEN 1 ELSE 0 END), 0) AS recent_failed_logins,
    COALESCE(COUNT(DISTINCT CASE WHEN failed_user IS NOT NULL THEN failed_user END), 0) AS unique_failed_users,
    COALESCE(COUNT(DISTINCT CASE WHEN source_ip IS NOT NULL THEN source_ip END), 0) AS unique_failed_sources,
    'none' AS username,
    'none' AS source_ip
FROM FilteredLogins`

const FailedLoginSummaryQueryAzureSQL = `WITH failed_events AS (
    SELECT
        start_time,
        ISNULL(JSON_VALUE(CAST(additional_data AS NVARCHAR(MAX)), '$.client_ip'), 'unknown') AS source_ip,
        ISNULL(JSON_VALUE(CAST(additional_data AS NVARCHAR(MAX)), '$.user_name'), 'unknown') AS username
    FROM sys.event_log
    WHERE event_type IN ('connection_failed')
        AND start_time >= DATEADD(HOUR, -24, GETUTCDATE())
)
SELECT
    COALESCE(COUNT(*), 0) AS total_failed_logins,
    COALESCE(SUM(CASE WHEN start_time >= DATEADD(HOUR, -1, GETUTCDATE()) THEN 1 ELSE 0 END), 0) AS recent_failed_logins,
    COALESCE(COUNT(DISTINCT CASE WHEN source_ip IS NOT NULL THEN source_ip END), 0) AS unique_failed_sources,
    COALESCE(COUNT(DISTINCT CASE WHEN username IS NOT NULL THEN username END), 0) AS unique_failed_users,
    'none' AS username,
    'none' AS source_ip
FROM failed_events`

// Azure SQL Managed Instance supports sp_readerrorlog with full functionality
// Always returns at least one row with zeros if no failed logins exist
const FailedLoginSummaryQueryAzureMI = `
DECLARE @FailedLogins TABLE (
    LogDate DATETIME,
    ProcessInfo NVARCHAR(100),
    Text NVARCHAR(MAX)
);

-- Insert data from the current and previous error logs
-- This captures "Login failed" messages
INSERT INTO @FailedLogins (LogDate, ProcessInfo, Text)
EXEC sp_readerrorlog 0, 1, 'Login failed';

INSERT INTO @FailedLogins (LogDate, ProcessInfo, Text)
EXEC sp_readerrorlog 1, 1, 'Login failed';

-- Use a Common Table Expression (CTE) to filter and parse the log text
WITH FilteredLogins AS (
    SELECT
        LogDate,
        -- Parse the username from the log text
        CASE
            WHEN Text LIKE '%for user ''%''%' THEN
                SUBSTRING(Text, CHARINDEX('for user ''', Text) + 10,
                          CHARINDEX('''', Text, CHARINDEX('for user ''', Text) + 10) - CHARINDEX('for user ''', Text) - 10)
            ELSE 'unknown'
        END AS failed_user,
        -- Parse the client IP address from the log text
        CASE
            WHEN Text LIKE '%Client: %' THEN
                RTRIM(LTRIM(SUBSTRING(Text, CHARINDEX('Client: ', Text) + 8, 50)))
            WHEN Text LIKE '%[CLIENT: %]%' THEN
                SUBSTRING(Text, CHARINDEX('[CLIENT: ', Text) + 9,
                          CHARINDEX(']', Text, CHARINDEX('[CLIENT: ', Text)) - CHARINDEX('[CLIENT: ', Text) - 9)
            ELSE 'unknown'
        END AS source_ip
    FROM @FailedLogins
    WHERE Text LIKE '%Login failed for user%' -- Ensure we only process relevant rows
)
-- Aggregate the final metrics
-- Use COALESCE to ensure we always return at least one row with zeros
SELECT
    COALESCE(SUM(1), 0) AS total_failed_logins,
    COALESCE(SUM(CASE WHEN LogDate >= DATEADD(HOUR, -1, GETDATE()) THEN 1 ELSE 0 END), 0) AS recent_failed_logins,
    COALESCE(COUNT(DISTINCT CASE WHEN failed_user IS NOT NULL THEN failed_user END), 0) AS unique_failed_users,
    COALESCE(COUNT(DISTINCT CASE WHEN source_ip IS NOT NULL THEN source_ip END), 0) AS unique_failed_sources,
    'none' AS username,
    'none' AS source_ip
FROM FilteredLogins`
