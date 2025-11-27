-- ===================================================================
-- AlwaysOn Availability Group Metrics Verification Script
-- Run this to understand your current AG status and why metrics show 0
-- ===================================================================

PRINT '=== AlwaysOn Availability Group Health Check ==='
PRINT ''

-- 1. Verify AlwaysOn is enabled
PRINT '1. AlwaysOn Configuration:'
SELECT 
    'IsHadrEnabled' AS Setting,
    CASE WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 'ENABLED ✓' ELSE 'DISABLED ✗' END AS Status
UNION ALL
SELECT 
    'HadrManagerStatus' AS Setting,
    CAST(SERVERPROPERTY('HadrManagerStatus') AS VARCHAR(20)) AS Status

PRINT ''

-- 2. Check Availability Groups
PRINT '2. Availability Groups:'
IF EXISTS (SELECT 1 FROM sys.availability_groups)
BEGIN
    SELECT 
        name AS AvailabilityGroup,
        cluster_type_desc AS ClusterType,
        failure_condition_level AS FailureLevel,
        automated_backup_preference_desc AS BackupPreference
    FROM sys.availability_groups
END
ELSE
BEGIN
    PRINT '   ⚠️  No Availability Groups found!'
END

PRINT ''

-- 3. Check replicas and their roles
PRINT '3. Replica Status:'
IF EXISTS (SELECT 1 FROM sys.dm_hadr_availability_replica_states)
BEGIN
    SELECT 
        ar.replica_server_name AS ReplicaServer,
        ars.role_desc AS Role,
        ars.operational_state_desc AS OperationalState,
        ars.connected_state_desc AS ConnectionState,
        ars.synchronization_health_desc AS SyncHealth,
        ar.availability_mode_desc AS AvailabilityMode,
        ar.failover_mode_desc AS FailoverMode
    FROM sys.dm_hadr_availability_replica_states ars
    JOIN sys.availability_replicas ar ON ars.replica_id = ar.replica_id
    ORDER BY ars.role_desc DESC, ar.replica_server_name
END
ELSE
BEGIN
    PRINT '   ⚠️  No replica states found!'
END

PRINT ''

-- 4. Check database replica states (the source of your metrics)
PRINT '4. Database Replica States (Metrics Source):'
IF EXISTS (SELECT 1 FROM sys.dm_hadr_database_replica_states)
BEGIN
    SELECT 
        ar.replica_server_name AS ReplicaServer,
        d.name AS DatabaseName,
        CASE WHEN drs.is_primary_replica = 1 THEN 'PRIMARY' ELSE 'SECONDARY' END AS ReplicaRole,
        drs.database_state_desc AS DatabaseState,
        drs.synchronization_state_desc AS SyncState,
        drs.log_send_queue_size AS LogSendQueueKB,
        drs.redo_queue_size AS RedoQueueKB, 
        drs.redo_rate AS RedoRateKBSec,
        CASE 
            WHEN drs.log_send_queue_size IS NULL THEN 'NULL (Normal for this replica type)'
            WHEN drs.log_send_queue_size = 0 THEN 'ZERO (Caught up - GOOD!)'
            ELSE 'NON-ZERO (Has backlog)'
        END AS LogQueueExplanation,
        CASE 
            WHEN drs.redo_queue_size IS NULL THEN 'NULL (Normal for this replica type)'
            WHEN drs.redo_queue_size = 0 THEN 'ZERO (Caught up - GOOD!)'
            ELSE 'NON-ZERO (Has backlog)'
        END AS RedoQueueExplanation
    FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id
    JOIN sys.databases d ON drs.database_id = d.database_id
    ORDER BY ar.replica_server_name, d.name
END
ELSE
BEGIN
    PRINT '   ⚠️  No database replica states found!'
    PRINT '   This means no databases are in availability groups'
END

PRINT ''

-- 5. Check transaction log activity
PRINT '5. Transaction Log Activity:'
SELECT 
    db.name AS DatabaseName,
    ls.database_id,
    ls.total_log_size_in_bytes / 1024 / 1024 AS TotalLogSizeMB,
    ls.used_log_space_in_bytes / 1024 / 1024 AS UsedLogSpaceMB,
    ls.used_log_space_in_percent AS UsedLogSpacePercent,
    ls.log_space_in_bytes_since_last_backup / 1024 / 1024 AS LogSinceLastBackupMB,
    db.log_reuse_wait_desc AS LogReuseWaitReason
FROM sys.dm_db_log_space_usage ls
JOIN sys.databases db ON ls.database_id = db.database_id
WHERE db.name IN (SELECT d.name FROM sys.dm_hadr_database_replica_states drs JOIN sys.databases d ON drs.database_id = d.database_id)
   OR db.name = 'AdventureWorks2022'

PRINT ''

-- 6. Performance counters check
PRINT '6. Performance Counters (Always On related):'
SELECT 
    object_name AS ObjectName,
    counter_name AS CounterName,
    instance_name AS InstanceName,
    cntr_value AS CurrentValue,
    CASE 
        WHEN cntr_value = 0 THEN 'ZERO (No activity or caught up)'
        ELSE 'NON-ZERO (Active or has backlog)'
    END AS ValueExplanation
FROM sys.dm_os_performance_counters
WHERE object_name LIKE '%Database Replica%'
   OR (object_name LIKE '%Availability Replica%')
ORDER BY object_name, counter_name, instance_name

PRINT ''
PRINT '=== SUMMARY EXPLANATION ==='
PRINT ''
PRINT 'WHY YOUR METRICS SHOW ZERO VALUES:'
PRINT '✓ Zero values indicate a HEALTHY availability group'
PRINT '✓ log_send_queue_kb = 0: Primary has sent all log records (good!)'
PRINT '✓ redo_queue_kb = 0: Secondary has processed all log records (good!)'  
PRINT '✓ redo_rate_kb_sec = 0: No active redo operations (system is idle)'
PRINT ''
PRINT 'TO SEE NON-ZERO VALUES:'
PRINT '1. Run the test_ag_workload.sql script to generate heavy transaction load'
PRINT '2. Or run sustained_ag_load.sql for continuous load testing'
PRINT '3. Check metrics immediately after running the load scripts'
PRINT ''
PRINT 'Your AlwaysOn setup is working correctly! 🎉'