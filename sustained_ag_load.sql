-- ===================================================================
-- Sustained Load Generator for AlwaysOn Metrics Testing
-- Run this on the PRIMARY replica to create continuous queue activity
-- ===================================================================

USE AdventureWorks2022
GO

PRINT '=== Starting Sustained Load for AlwaysOn Metrics ==='

-- Create a larger test table for sustained load
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SustainedLoadTest')
BEGIN
    CREATE TABLE SustainedLoadTest (
        ID BIGINT IDENTITY(1,1) PRIMARY KEY,
        LoadData NVARCHAR(MAX),  -- Use MAX to create large log entries
        BinaryData VARBINARY(MAX),
        CreatedAt DATETIME2 DEFAULT GETDATE(),
        UpdatedAt DATETIME2,
        SessionID INT,
        Counter BIGINT
    )
    
    -- Add some indexes to create more log activity during updates
    CREATE INDEX IX_SustainedLoadTest_SessionID ON SustainedLoadTest(SessionID)
    CREATE INDEX IX_SustainedLoadTest_CreatedAt ON SustainedLoadTest(CreatedAt)
    
    PRINT '✓ Created SustainedLoadTest table with indexes'
END

-- Function to check current AG queue metrics
CREATE OR ALTER PROCEDURE CheckAGQueues
AS
BEGIN
    SELECT 
        ar.replica_server_name,
        d.name AS database_name,
        drs.log_send_queue_size AS log_send_queue_kb,
        drs.redo_queue_size AS redo_queue_kb,
        drs.redo_rate AS redo_rate_kb_sec,
        drs.database_state_desc,
        drs.synchronization_state_desc,
        CASE WHEN drs.is_primary_replica = 1 THEN 'PRIMARY' ELSE 'SECONDARY' END AS replica_role,
        drs.last_sent_time,
        drs.last_received_time,
        drs.last_hardened_time
    FROM sys.dm_hadr_database_replica_states AS drs
    JOIN sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
    JOIN sys.databases AS d ON drs.database_id = d.database_id
    WHERE d.name = 'AdventureWorks2022'
    ORDER BY ar.replica_server_name
END
GO

-- Start the sustained load
PRINT 'Starting sustained load generation...'
PRINT 'This will run for 10 minutes or until you stop it manually'

DECLARE @startTime DATETIME2 = GETDATE()
DECLARE @endTime DATETIME2 = DATEADD(MINUTE, 10, GETDATE()) -- Run for 10 minutes
DECLARE @loopCounter BIGINT = 1
DECLARE @checkInterval INT = 100 -- Check metrics every 100 operations

WHILE GETDATE() < @endTime
BEGIN
    -- Large batch insert with substantial data
    INSERT INTO SustainedLoadTest (LoadData, BinaryData, SessionID, Counter)
    SELECT TOP 50
        'SUSTAINED LOAD TEST - Loop: ' + CAST(@loopCounter AS VARCHAR(20)) + 
        ' - Timestamp: ' + CONVERT(VARCHAR(30), GETDATE(), 121) + 
        ' - Large data payload: ' + REPLICATE('SUSTAINED_LOAD_DATA_ABCDEFGHIJKLMNOPQRSTUVWXYZ_', 50) +
        ' - Random: ' + CAST(NEWID() AS VARCHAR(36)) +
        ' - More padding: ' + REPLICATE('PADDING_', 100),
        CRYPT_GEN_RANDOM(8000), -- 8KB of random data per row
        (@loopCounter % 10) + 1, -- Session ID 1-10
        @loopCounter
    FROM sys.objects o1
    CROSS JOIN sys.objects o2
    
    -- Heavy update operations
    UPDATE SustainedLoadTest 
    SET 
        LoadData = LoadData + ' - UPDATED Loop ' + CAST(@loopCounter AS VARCHAR(20)) + ' at ' + CONVERT(VARCHAR(30), GETDATE(), 121),
        BinaryData = CRYPT_GEN_RANDOM(4000),
        UpdatedAt = GETDATE()
    WHERE ID % 7 = (@loopCounter % 7) -- Update different sets of rows
    
    -- Simulate some deletes and re-inserts
    IF @loopCounter % 50 = 0
    BEGIN
        DELETE FROM SustainedLoadTest 
        WHERE ID % 20 = 0 AND CreatedAt < DATEADD(MINUTE, -5, GETDATE())
    END
    
    -- Check AG queue metrics periodically
    IF @loopCounter % @checkInterval = 0
    BEGIN
        PRINT 'Loop ' + CAST(@loopCounter AS VARCHAR(20)) + ' completed at ' + CONVERT(VARCHAR(30), GETDATE(), 121)
        PRINT 'Checking AG queue metrics...'
        EXEC CheckAGQueues
        PRINT '------------------------------------------'
        
        -- Brief pause to allow queue buildup observation
        WAITFOR DELAY '00:00:03'
    END
    ELSE
    BEGIN
        -- Smaller pause between operations
        WAITFOR DELAY '00:00:01'
    END
    
    SET @loopCounter = @loopCounter + 1
END

PRINT 'Sustained load test completed!'
PRINT 'Final AG queue metrics:'
EXEC CheckAGQueues

-- Show final table statistics
SELECT 
    'SustainedLoadTest' AS TableName,
    COUNT(*) AS TotalRows,
    MAX(ID) AS MaxID,
    MIN(CreatedAt) AS FirstRecord,
    MAX(CreatedAt) AS LastRecord
FROM SustainedLoadTest

PRINT ''
PRINT 'Check your New Relic dashboard now - you should see non-zero values!'
PRINT 'If you want more load, run this script again or run multiple instances simultaneously.'