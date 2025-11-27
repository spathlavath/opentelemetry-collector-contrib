-- Test the exact queries used by the collector
-- Run this on your SQL Server to see what data the collector sees

PRINT '=== Testing AlwaysOn Queries for OpenTelemetry Collector ==='
PRINT ''

-- Test 1: Check if AlwaysOn is enabled
PRINT '1. AlwaysOn Availability Groups Status:'
SELECT 
    SERVERPROPERTY('IsHadrEnabled') AS IsAlwaysOnEnabled,
    SERVERPROPERTY('HadrManagerStatus') AS HadrManagerStatus
GO

-- Test 2: Check Availability Groups
PRINT ''
PRINT '2. Availability Groups:'
SELECT 
    name AS availability_group_name,
    automated_backup_preference_desc,
    failure_condition_level,
    health_check_timeout,
    cluster_type_desc
FROM sys.availability_groups
GO

-- Test 3: Check Availability Replicas  
PRINT ''
PRINT '3. Availability Replicas:'
SELECT 
    ar.replica_server_name,
    ar.availability_mode_desc,
    ar.failover_mode_desc,
    ar.backup_priority,
    ar.endpoint_url
FROM sys.availability_replicas ar
GO

-- Test 4: Test the EXACT redo queue metrics query from the collector
PRINT ''
PRINT '4. Redo Queue Metrics Query (EXACT from collector):'
SELECT
    ar.replica_server_name,
    d.name AS database_name,
    drs.log_send_queue_size AS log_send_queue_kb,
    drs.redo_queue_size AS redo_queue_kb,
    drs.redo_rate AS redo_rate_kb_sec
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id;
GO

-- Test 5: Test the replica state query
PRINT ''
PRINT '5. Replica State Metrics Query (EXACT from collector):'
SELECT
    ar.replica_server_name,
    d.name AS database_name,
    drs.log_send_queue_size AS log_send_queue_kb,
    drs.redo_queue_size AS redo_queue_kb,
    drs.redo_rate AS redo_rate_kb_sec,
    ISNULL(drs.database_state_desc, 'UNKNOWN') AS database_state_desc,
    ISNULL(drs.synchronization_state_desc, 'UNKNOWN') AS synchronization_state_desc,
    CAST(drs.is_local AS INT) AS is_local,
    CAST(drs.is_primary_replica AS INT) AS is_primary_replica,
    ISNULL(CONVERT(VARCHAR(23), drs.last_commit_time, 121), '') AS last_commit_time,
    ISNULL(CONVERT(VARCHAR(23), drs.last_sent_time, 121), '') AS last_sent_time,
    ISNULL(CONVERT(VARCHAR(23), drs.last_received_time, 121), '') AS last_received_time,
    ISNULL(CONVERT(VARCHAR(23), drs.last_hardened_time, 121), '') AS last_hardened_time,
    ISNULL(CONVERT(VARCHAR(25), drs.last_redone_lsn), '') AS last_redone_lsn,
    ISNULL(drs.suspend_reason_desc, 'NONE') AS suspend_reason_desc
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id;
GO

-- Test 6: Check performance counters
PRINT ''
PRINT '6. Performance Counters for Always On:'
SELECT
    instance_name,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log Bytes Received/sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction Delay],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow Control Time (ms/sec)]
FROM
    sys.dm_os_performance_counters
WHERE
    object_name LIKE '%Database Replica%'
    AND counter_name IN (
        'Log Bytes Received/sec',
        'Transaction Delay',
        'Flow Control Time (ms/sec)'
    )
GROUP BY
    instance_name;
GO

PRINT ''
PRINT 'Test completed. If any queries return empty results, that explains why collector shows no metrics.'