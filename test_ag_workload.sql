-- ===================================================================
-- AlwaysOn Availability Group Workload Generator
-- Run this on the PRIMARY replica to generate log send queue activity
-- ===================================================================

USE AdventureWorks2022
GO

PRINT '=== Starting AlwaysOn Workload Generation ==='

-- Create test table if not exists  
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AG_Test_Load')
BEGIN
    CREATE TABLE AG_Test_Load (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        Data NVARCHAR(2000),  -- Larger data to generate more log activity
        CreateDate DATETIME2 DEFAULT GETDATE(),
        UpdateCount INT DEFAULT 0,
        RandomData VARBINARY(1000)  -- Additional data to increase log size
    )
    PRINT '✓ Created AG_Test_Load table'
END
ELSE
    PRINT '✓ AG_Test_Load table already exists'
GO

-- Generate HEAVY transaction load to create queue buildup
PRINT 'Generating heavy transaction load...'

DECLARE @counter INT = 1
DECLARE @batchSize INT = 50  -- Process in batches
DECLARE @totalBatches INT = 100  -- Total batches to create substantial load

WHILE @counter <= @totalBatches
BEGIN
    BEGIN TRANSACTION -- Use explicit transactions for more log activity
    
    -- Insert multiple records in each batch
    DECLARE @innerCounter INT = 1
    WHILE @innerCounter <= @batchSize
    BEGIN
        INSERT INTO AG_Test_Load (Data, RandomData)
        SELECT 
            'HEAVY LOAD BATCH ' + CAST(@counter AS VARCHAR(10)) + 
            ' RECORD ' + CAST(@innerCounter AS VARCHAR(10)) + 
            ' - timestamp: ' + CONVERT(VARCHAR(25), GETDATE(), 121) + 
            ' - random data: ' + CAST(NEWID() AS VARCHAR(36)) +
            ' - additional padding data to increase log size: ' + REPLICATE('X', 500),
            CRYPT_GEN_RANDOM(800)  -- Random binary data to increase log volume
        
        SET @innerCounter = @innerCounter + 1
    END
    
    -- Heavy updates to generate more log activity
    UPDATE AG_Test_Load 
    SET 
        Data = Data + ' - HEAVY UPDATE BATCH ' + CAST(@counter AS VARCHAR(10)) + ' at ' + CONVERT(VARCHAR(25), GETDATE(), 121),
        UpdateCount = UpdateCount + 1,
        RandomData = CRYPT_GEN_RANDOM(800)
    WHERE ID % 5 = 0  -- Update more records
    
    COMMIT TRANSACTION
    
    -- Create intentional delays to allow queue buildup
    IF @counter % 10 = 0
    BEGIN
        PRINT 'Processed batch ' + CAST(@counter AS VARCHAR(10)) + ' of ' + CAST(@totalBatches AS VARCHAR(10))
        
        -- Don't checkpoint immediately - let log build up
        WAITFOR DELAY '00:00:02'  -- 2 second delay
    END
    
    -- Periodic large batch operations
    IF @counter % 20 = 0
    BEGIN
        -- Large update operation
        UPDATE AG_Test_Load 
        SET Data = REPLICATE('LARGE_UPDATE_', 50) + CAST(GETDATE() AS VARCHAR(50))
        WHERE ID % 3 = 0
        
        PRINT 'Created large update operation at batch ' + CAST(@counter AS VARCHAR(10))
    END
    
    SET @counter = @counter + 1
END

-- Create multiple concurrent sessions for maximum load
PRINT 'Creating additional concurrent load...'

-- Simulate multiple applications hitting the database
DECLARE @sessionCounter INT = 1
WHILE @sessionCounter <= 5  -- 5 concurrent "sessions"
BEGIN
    INSERT INTO AG_Test_Load (Data, RandomData)
    SELECT TOP 100  -- Bulk insert
        'CONCURRENT SESSION ' + CAST(@sessionCounter AS VARCHAR(5)) + 
        ' - BULK INSERT - ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10)) +
        ' - ' + CONVERT(VARCHAR(25), GETDATE(), 121) + 
        ' - ' + CAST(NEWID() AS VARCHAR(36)) +
        ' - ' + REPLICATE('BULK_DATA_', 20),
        CRYPT_GEN_RANDOM(500)
    FROM sys.objects o1 
    CROSS JOIN sys.objects o2
    
    SET @sessionCounter = @sessionCounter + 1
END

PRINT 'Heavy workload generation completed!'

-- Check current queue metrics IMMEDIATELY after load
PRINT '=== IMMEDIATE AlwaysOn Metrics Check ==='
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
    drs.last_received_time
FROM sys.dm_hadr_database_replica_states AS drs
JOIN sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN sys.databases AS d ON drs.database_id = d.database_id
ORDER BY ar.replica_server_name

-- Check transaction log usage
PRINT ''
PRINT '=== Transaction Log Usage ==='
SELECT 
    name AS database_name,
    log_reuse_wait_desc,
    (total_log_size_in_bytes / 1024 / 1024) AS total_log_size_mb,
    (used_log_space_in_bytes / 1024 / 1024) AS used_log_space_mb,
    (used_log_space_in_bytes * 100.0 / total_log_size_in_bytes) AS log_space_used_percent
FROM sys.dm_db_log_space_usage

-- Check performance counters
PRINT ''
PRINT '=== Performance Counters (Should show activity now) ==='
SELECT
    pc.object_name,
    pc.counter_name,
    pc.instance_name,
    pc.cntr_value,
    pc.cntr_type
FROM sys.dm_os_performance_counters pc
WHERE pc.object_name LIKE '%Database Replica%'
   OR (pc.object_name LIKE '%Databases%' AND pc.instance_name = 'AdventureWorks2022')
ORDER BY pc.object_name, pc.counter_name

PRINT ''
PRINT '=== Instructions ==='
PRINT 'Workload completed! You should now see non-zero values for:'
PRINT '1. log_send_queue_kb - Log records waiting to be sent'
PRINT '2. redo_queue_kb - Log records waiting to be processed'  
PRINT '3. redo_rate_kb_sec - Active redo operations'
PRINT ''
PRINT 'Check your collector metrics and New Relic dashboard now!'
PRINT 'If values are still 0, your replication is extremely fast (which is good!)'
PRINT ''
PRINT 'To create sustained load, run this script multiple times or'
PRINT 'run it simultaneously from multiple query windows.'