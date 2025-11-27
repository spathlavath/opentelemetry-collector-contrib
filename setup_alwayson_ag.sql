-- =====================================================
-- Complete Always On Availability Group Setup
-- This script will create an AG and generate the metrics you need
-- =====================================================

-- Step 1: Check prerequisites
PRINT '=== Always On Availability Group Setup ===';
PRINT '';

-- Check if AlwaysOn is enabled
DECLARE @IsHadrEnabled BIT = CONVERT(BIT, SERVERPROPERTY('IsHadrEnabled'));
IF @IsHadrEnabled = 0
BEGIN
    PRINT 'ERROR: AlwaysOn Availability Groups is not enabled.';
    PRINT 'Please enable AlwaysOn in SQL Server Configuration Manager and restart the service.';
    RETURN;
END
ELSE
BEGIN
    PRINT '✓ AlwaysOn Availability Groups is enabled.';
END
PRINT '';

-- Step 2: Create test databases if they don't exist
PRINT '=== Creating Test Databases ===';

-- Create primary test database
IF DB_ID('AlwaysOnTestDB') IS NULL
BEGIN
    CREATE DATABASE AlwaysOnTestDB;
    PRINT '✓ Created AlwaysOnTestDB database';
END
ELSE
BEGIN
    PRINT '✓ AlwaysOnTestDB already exists';
END

-- Ensure database is in FULL recovery model (required for AG)
ALTER DATABASE AlwaysOnTestDB SET RECOVERY FULL;
PRINT '✓ Set AlwaysOnTestDB to FULL recovery model';

-- Create a backup (required before adding to AG)
DECLARE @BackupPath NVARCHAR(500);
SET @BackupPath = N'C:\Temp\AlwaysOnTestDB_Full.bak';

-- Create backup directory if it doesn't exist (you may need to adjust the path)
PRINT '✓ Creating backup of AlwaysOnTestDB...';
BACKUP DATABASE AlwaysOnTestDB 
TO DISK = @BackupPath
WITH INIT, SKIP, NOREWIND, NOUNLOAD, STATS = 10;
PRINT '✓ Backup completed: ' + @BackupPath;

PRINT '';

-- Step 3: Create database mirroring endpoint (if it doesn't exist)
PRINT '=== Setting up Database Mirroring Endpoint ===';

IF NOT EXISTS (SELECT * FROM sys.endpoints WHERE name = 'AlwaysOn_Endpoint')
BEGIN
    CREATE ENDPOINT AlwaysOn_Endpoint
    STATE = STARTED
    AS TCP (LISTENER_PORT = 5022, LISTENER_IP = ALL)
    FOR DATABASE_MIRRORING (
        ROLE = ALL,
        AUTHENTICATION = WINDOWS NEGOTIATE,
        ENCRYPTION = REQUIRED ALGORITHM AES
    );
    PRINT '✓ Created AlwaysOn_Endpoint on port 5022';
END
ELSE
BEGIN
    PRINT '✓ AlwaysOn_Endpoint already exists';
END
PRINT '';

-- Step 4: Create Availability Group
PRINT '=== Creating Availability Group ===';

IF NOT EXISTS (SELECT * FROM sys.availability_groups WHERE name = 'TestAG')
BEGIN
    -- Create the Availability Group (single replica for testing)
    CREATE AVAILABILITY GROUP TestAG
    WITH (
        AUTOMATED_BACKUP_PREFERENCE = SECONDARY,
        FAILURE_CONDITION_LEVEL = 3,
        HEALTH_CHECK_TIMEOUT = 30000
    )
    FOR DATABASE AlwaysOnTestDB
    REPLICA ON 
        N'@@SERVERNAME' WITH (
            ENDPOINT_URL = N'TCP://@@SERVERNAME:5022',
            AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
            FAILOVER_MODE = MANUAL,
            BACKUP_PRIORITY = 50,
            SECONDARY_ROLE(ALLOW_CONNECTIONS = YES)
        );
    
    PRINT '✓ Created Availability Group: TestAG';
    PRINT '✓ Added AlwaysOnTestDB to TestAG';
END
ELSE
BEGIN
    PRINT '✓ Availability Group TestAG already exists';
    
    -- Try to add database if it's not already in the group
    IF NOT EXISTS (
        SELECT * FROM sys.availability_databases_cluster 
        WHERE database_name = 'AlwaysOnTestDB' 
        AND group_id = (SELECT group_id FROM sys.availability_groups WHERE name = 'TestAG')
    )
    BEGIN
        ALTER AVAILABILITY GROUP TestAG ADD DATABASE AlwaysOnTestDB;
        PRINT '✓ Added AlwaysOnTestDB to existing TestAG';
    END
    ELSE
    BEGIN
        PRINT '✓ AlwaysOnTestDB is already in TestAG';
    END
END
PRINT '';

-- Step 5: Generate some activity to populate metrics
PRINT '=== Generating Database Activity ===';

USE AlwaysOnTestDB;

-- Create test table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TestMetricsTable')
BEGIN
    CREATE TABLE TestMetricsTable (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        TestData NVARCHAR(100),
        CreatedDate DATETIME2 DEFAULT GETDATE()
    );
    PRINT '✓ Created TestMetricsTable';
END

-- Insert some test data to generate log activity
INSERT INTO TestMetricsTable (TestData)
VALUES 
    ('Test data for AG metrics - ' + CONVERT(VARCHAR(23), GETDATE(), 121)),
    ('Log activity generation - ' + CONVERT(VARCHAR(23), GETDATE(), 121)),
    ('Always On testing data - ' + CONVERT(VARCHAR(23), GETDATE(), 121));

PRINT '✓ Inserted test data to generate log activity';

-- Force a log backup to generate more activity
BACKUP LOG AlwaysOnTestDB TO DISK = N'C:\Temp\AlwaysOnTestDB_Log.trn' WITH INIT;
PRINT '✓ Created log backup to generate more activity';

PRINT '';

-- Step 6: Verify the setup and test our collector query
PRINT '=== Verification ===';

-- Test the exact query the collector uses
PRINT 'Testing collector queries...';
PRINT '';

-- Query 1: FailoverClusterRedoQueueQuery
PRINT '1. Redo Queue Metrics (FailoverClusterRedoQueueQuery):';
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
PRINT '';

-- Query 2: FailoverClusterReplicaQuery (Performance Counters)
PRINT '2. Performance Counter Metrics (FailoverClusterReplicaQuery):';
SELECT
    instance_name,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log_Bytes_Received_Per_Sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction_Delay_Ms],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow_Control_Time_Ms]
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
PRINT '';

-- Query 3: Show AG status
PRINT '3. Availability Group Status:';
SELECT 
    ag.name AS AvailabilityGroup,
    ar.replica_server_name AS ReplicaServer,
    ars.role_desc AS Role,
    ars.synchronization_health_desc AS SyncHealth,
    ars.connected_state_desc AS ConnectionState
FROM sys.availability_groups ag
JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id;

PRINT '';
PRINT '=== Setup Complete! ===';
PRINT '';
PRINT 'Your Always On Availability Group is now configured.';
PRINT 'The OpenTelemetry collector should now be able to collect:';
PRINT '- sqlserver.failover_cluster.log_send_queue_kb';
PRINT '- sqlserver.failover_cluster.redo_queue_kb'; 
PRINT '- sqlserver.failover_cluster.redo_rate_kb_sec';
PRINT '';
PRINT 'Note: Since this is a single-replica AG, some metrics may be zero.';
PRINT 'For full metrics, you would need a secondary replica on another server.';

-- Continue generating activity in a loop for testing
PRINT '';
PRINT 'Generating ongoing activity for 2 minutes...';

DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, 2, GETDATE());
DECLARE @Counter INT = 1;

WHILE GETDATE() < @EndTime
BEGIN
    INSERT INTO TestMetricsTable (TestData)
    VALUES ('Activity batch ' + CAST(@Counter AS VARCHAR(10)) + ' - ' + CONVERT(VARCHAR(23), GETDATE(), 121));
    
    -- Update some records to generate more log activity
    UPDATE TestMetricsTable 
    SET TestData = TestData + ' [Updated]'
    WHERE Id % 5 = 0;
    
    -- Small delay
    WAITFOR DELAY '00:00:05'; -- 5 second delay
    
    SET @Counter = @Counter + 1;
    
    IF @Counter % 5 = 0
        PRINT 'Activity batch ' + CAST(@Counter AS VARCHAR(10)) + ' completed...';
END

PRINT '✓ Activity generation completed!';
PRINT 'Check your OpenTelemetry collector output now.';