-- =====================================================
-- AlwaysOn Availability Group Status Check
-- Run this script to verify your AG configuration
-- =====================================================

PRINT '=== AlwaysOn Availability Group Configuration Check ===';
PRINT '';

-- 1. Check if AlwaysOn is enabled
PRINT '1. AlwaysOn Availability Groups Status:';
SELECT 
    CASE 
        WHEN SERVERPROPERTY('IsHadrEnabled') = 1 THEN 'ENABLED' 
        ELSE 'DISABLED' 
    END AS AlwaysOn_Status;
PRINT '';

-- 2. Check if any Availability Groups exist
PRINT '2. Available Availability Groups:';
IF EXISTS (SELECT * FROM sys.availability_groups)
BEGIN
    SELECT 
        ag.name AS AvailabilityGroupName,
        ag.cluster_type_desc AS ClusterType,
        ag.automated_backup_preference_desc AS BackupPreference
    FROM sys.availability_groups ag;
END
ELSE
BEGIN
    PRINT 'No Availability Groups found.';
END
PRINT '';

-- 3. Check Availability Replicas
PRINT '3. Availability Group Replicas:';
IF EXISTS (SELECT * FROM sys.availability_replicas)
BEGIN
    SELECT 
        ag.name AS AvailabilityGroupName,
        ar.replica_server_name AS ReplicaServer,
        ars.role_desc AS CurrentRole,
        ar.availability_mode_desc AS AvailabilityMode,
        ar.failover_mode_desc AS FailoverMode,
        ars.connected_state_desc AS ConnectionState,
        ars.synchronization_health_desc AS SyncHealth
    FROM sys.availability_groups ag
    JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
    JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id;
END
ELSE
BEGIN
    PRINT 'No Availability Group replicas found.';
END
PRINT '';

-- 4. Check databases in Availability Groups
PRINT '4. Databases in Availability Groups:';
IF EXISTS (SELECT * FROM sys.dm_hadr_database_replica_states)
BEGIN
    SELECT 
        ag.name AS AvailabilityGroupName,
        ar.replica_server_name AS ReplicaServer,
        d.name AS DatabaseName,
        drs.database_state_desc AS DatabaseState,
        drs.synchronization_state_desc AS SyncState,
        drs.log_send_queue_size AS LogSendQueue_KB,
        drs.redo_queue_size AS RedoQueue_KB,
        drs.redo_rate AS RedoRate_KB_per_sec,
        drs.is_primary_replica AS IsPrimary
    FROM sys.dm_hadr_database_replica_states drs
    JOIN sys.availability_replicas ar ON drs.replica_id = ar.replica_id
    JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
    JOIN sys.databases d ON drs.database_id = d.database_id
    ORDER BY ag.name, ar.replica_server_name, d.name;
END
ELSE
BEGIN
    PRINT 'No databases found in Availability Groups.';
END
PRINT '';

-- 5. Test the exact query that the collector uses
PRINT '5. Testing Collector Query (FailoverClusterRedoQueueQuery):';
BEGIN TRY
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
    
    PRINT 'SUCCESS: Collector query executed successfully.';
END TRY
BEGIN CATCH
    PRINT 'ERROR: Collector query failed with error:';
    PRINT ERROR_MESSAGE();
END CATCH
PRINT '';

-- 6. Check SQL Server version and edition
PRINT '6. SQL Server Version and Edition:';
SELECT 
    SERVERPROPERTY('ProductVersion') AS ProductVersion,
    SERVERPROPERTY('ProductLevel') AS ProductLevel,
    SERVERPROPERTY('Edition') AS Edition,
    SERVERPROPERTY('EngineEdition') AS EngineEdition,
    CASE SERVERPROPERTY('EngineEdition')
        WHEN 1 THEN 'Personal/Desktop'
        WHEN 2 THEN 'Standard'
        WHEN 3 THEN 'Enterprise'
        WHEN 4 THEN 'Express'
        WHEN 5 THEN 'SQL Database'
        WHEN 6 THEN 'SQL Data Warehouse'
        WHEN 8 THEN 'Azure SQL Managed Instance'
        WHEN 9 THEN 'Azure SQL Edge'
        ELSE 'Unknown'
    END AS EngineEditionDescription;
PRINT '';

-- 7. Performance counter availability check
PRINT '7. Performance Counter Availability:';
SELECT 
    object_name,
    counter_name,
    instance_name,
    cntr_value
FROM sys.dm_os_performance_counters
WHERE object_name LIKE '%Database Replica%'
  AND counter_name IN (
      'Log Bytes Received/sec',
      'Transaction Delay',
      'Flow Control Time (ms/sec)'
  )
ORDER BY object_name, counter_name, instance_name;

PRINT '';
PRINT '=== AlwaysOn Configuration Check Complete ===';
PRINT '';
PRINT 'TROUBLESHOOTING:';
PRINT '- If AlwaysOn Status is DISABLED: Enable AlwaysOn in SQL Server Configuration Manager';
PRINT '- If no Availability Groups found: Create an Availability Group first';
PRINT '- If no databases in AGs: Add your test database to the Availability Group';
PRINT '- If collector query fails: Check permissions for the collector service account';
PRINT '- If no performance counters: Restart SQL Server service after enabling AlwaysOn';