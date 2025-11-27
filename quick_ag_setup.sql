-- =====================================================
-- Quick Always On AG Setup - Simplified Version
-- Run this to create a basic AG setup for testing metrics
-- =====================================================

-- Get the current server name
DECLARE @ServerName NVARCHAR(128) = CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128));
DECLARE @BackupPath NVARCHAR(500) = N'C:\Temp\';
DECLARE @EndpointURL NVARCHAR(200) = N'TCP://' + @ServerName + N':5022';

PRINT '=== Quick Always On AG Setup ===';
PRINT 'Server: ' + @ServerName;
PRINT '';

-- 1. Check AlwaysOn is enabled
IF CONVERT(BIT, SERVERPROPERTY('IsHadrEnabled')) = 0
BEGIN
    PRINT 'ERROR: AlwaysOn is not enabled. Enable it in SQL Server Configuration Manager and restart.';
    RETURN;
END
PRINT '✓ AlwaysOn is enabled';

-- 2. Create test database
IF DB_ID('AGTestDB') IS NULL
BEGIN
    CREATE DATABASE AGTestDB;
    ALTER DATABASE AGTestDB SET RECOVERY FULL;
    PRINT '✓ Created AGTestDB database';
    
    -- Create backup
    BACKUP DATABASE AGTestDB TO DISK = N'C:\Temp\AGTestDB.bak' WITH INIT;
    PRINT '✓ Created backup';
END
ELSE
    PRINT '✓ AGTestDB already exists';

-- 3. Create endpoint
IF NOT EXISTS (SELECT * FROM sys.endpoints WHERE name = 'AG_Endpoint')
BEGIN
    EXEC('CREATE ENDPOINT AG_Endpoint
        STATE = STARTED
        AS TCP (LISTENER_PORT = 5022)
        FOR DATABASE_MIRRORING (ROLE = ALL)');
    PRINT '✓ Created endpoint on port 5022';
END
ELSE
    PRINT '✓ Endpoint already exists';

-- 4. Create Availability Group
IF NOT EXISTS (SELECT * FROM sys.availability_groups WHERE name = 'TestAG')
BEGIN
    DECLARE @SQL NVARCHAR(MAX) = N'
    CREATE AVAILABILITY GROUP TestAG
    WITH (AUTOMATED_BACKUP_PREFERENCE = PRIMARY)
    FOR DATABASE AGTestDB
    REPLICA ON N''' + @ServerName + N''' WITH (
        ENDPOINT_URL = N''' + @EndpointURL + N''',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        FAILOVER_MODE = MANUAL,
        BACKUP_PRIORITY = 50
    )';
    
    EXEC sp_executesql @SQL;
    PRINT '✓ Created Availability Group: TestAG';
END
ELSE
    PRINT '✓ TestAG already exists';

-- 5. Generate test data
USE AGTestDB;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MetricsTest')
BEGIN
    CREATE TABLE MetricsTest (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        Data NVARCHAR(500),
        Created DATETIME2 DEFAULT GETDATE()
    );
END

-- Insert data to generate log activity
INSERT INTO MetricsTest (Data)
SELECT 'Test data ' + CAST(number AS VARCHAR(10)) + ' - ' + CONVERT(VARCHAR(23), GETDATE(), 121)
FROM master.dbo.spt_values 
WHERE type = 'P' AND number BETWEEN 1 AND 100;

PRINT '✓ Generated test data';

-- 6. Test the collector queries
PRINT '';
PRINT '=== Testing Collector Queries ===';

-- Test query 1: Redo queue metrics
PRINT 'Redo Queue Metrics:';
SELECT
    ar.replica_server_name,
    d.name AS database_name,
    drs.log_send_queue_size AS log_send_queue_kb,
    drs.redo_queue_size AS redo_queue_kb,
    drs.redo_rate AS redo_rate_kb_sec
FROM sys.dm_hadr_database_replica_states AS drs
JOIN sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN sys.databases AS d ON drs.database_id = d.database_id;

PRINT '';
PRINT 'Setup complete! Your collector should now see metrics.';
PRINT 'Wait 1-2 minutes for metrics to appear in the collector output.';