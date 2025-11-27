-- =====================================================
-- SQL Server AlwaysOn Availability Group Load Generation Scripts
-- Generate data for metrics: Log bytes received/sec, Log send queue, 
-- Redo queue, Redo rate, Transaction delay ms/sec, Flow control time ms/sec
-- =====================================================

-- 1. Create test database and enable it for AlwaysOn AG
USE master;
GO

-- Drop existing database if it exists
IF DB_ID('AlwaysOnLoadTest') IS NOT NULL
BEGIN
    DROP DATABASE AlwaysOnLoadTest;
END
GO

-- Create a new database for load testing
CREATE DATABASE AlwaysOnLoadTest;
GO

-- Switch to the test database
USE AlwaysOnLoadTest;
GO

-- =====================================================
-- 2. Create test tables for workload generation
-- =====================================================

-- Large transaction table to generate log volume
CREATE TABLE LargeTransactionTable (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    TransactionData NVARCHAR(4000),
    TransactionValue DECIMAL(18,2),
    TransactionDate DATETIME2 DEFAULT GETDATE(),
    StatusCode INT DEFAULT 1,
    ProcessingFlag BIT DEFAULT 0,
    LargeTextField NVARCHAR(MAX)
);
GO

-- Frequent update table to create redo queue pressure
CREATE TABLE FrequentUpdateTable (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Counter INT DEFAULT 0,
    LastUpdated DATETIME2 DEFAULT GETDATE(),
    RandomValue UNIQUEIDENTIFIER DEFAULT NEWID(),
    UpdateCount BIGINT DEFAULT 0
);
GO

-- Bulk insert table for high volume log generation
CREATE TABLE BulkInsertTable (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    BulkData NVARCHAR(2000),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    SequenceNumber BIGINT,
    ChecksumValue BIGINT
);
GO

-- Create indexes for performance
CREATE NONCLUSTERED INDEX IX_LargeTransactionTable_Date 
    ON LargeTransactionTable (TransactionDate) INCLUDE (TransactionValue, StatusCode);

CREATE NONCLUSTERED INDEX IX_FrequentUpdateTable_LastUpdated 
    ON FrequentUpdateTable (LastUpdated) INCLUDE (Counter, UpdateCount);

CREATE NONCLUSTERED INDEX IX_BulkInsertTable_Sequence 
    ON BulkInsertTable (SequenceNumber) INCLUDE (CreatedDate);
GO

-- =====================================================
-- 3. Initialize data for load testing
-- =====================================================

-- Insert initial data
INSERT INTO FrequentUpdateTable (Counter, RandomValue, UpdateCount)
SELECT 
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    NEWID(),
    0
FROM sys.objects s1 CROSS JOIN sys.objects s2
WHERE ROWCOUNT_BIG() < 1000;
GO

-- =====================================================
-- 4. WORKLOAD SCRIPT 1: High Volume Transaction Load
-- Generate large amounts of log data to stress log transport
-- =====================================================

-- Run this in a separate session/window for continuous load
/*
DECLARE @Counter INT = 0;
DECLARE @BatchSize INT = 100;
DECLARE @MaxIterations INT = 1000; -- Adjust based on desired load duration

WHILE @Counter < @MaxIterations
BEGIN
    -- Large batch insert to generate significant log volume
    INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
    SELECT 
        'Transaction batch ' + CAST(@Counter AS VARCHAR(10)) + ' - ' + 
        REPLICATE('LoadTestData', 50), -- Creates ~500 char string
        RAND() * 10000,
        REPLICATE('Large text field data for testing log volume generation. ', 100) -- ~6KB per row
    FROM sys.objects s1 CROSS JOIN sys.objects s2
    WHERE ROWCOUNT_BIG() < @BatchSize;
    
    -- Create some transaction delay with explicit transactions
    BEGIN TRANSACTION;
        UPDATE LargeTransactionTable 
        SET ProcessingFlag = 1,
            TransactionValue = TransactionValue * 1.1
        WHERE Id % 10 = 0 AND ProcessingFlag = 0;
        
        -- Add artificial delay to create transaction pressure
        WAITFOR DELAY '00:00:01';
    COMMIT TRANSACTION;
    
    SET @Counter = @Counter + 1;
    
    -- Brief pause between batches
    WAITFOR DELAY '00:00:02';
END;
*/

-- =====================================================
-- 5. WORKLOAD SCRIPT 2: Frequent Updates for Redo Queue Pressure
-- Generate continuous updates to stress redo processing on secondary
-- =====================================================

-- Run this in a separate session/window
/*
DECLARE @IterationCount INT = 0;
DECLARE @MaxIterations INT = 2000;

WHILE @IterationCount < @MaxIterations
BEGIN
    -- Rapid fire updates to create redo queue pressure
    UPDATE FrequentUpdateTable 
    SET Counter = Counter + 1,
        LastUpdated = GETDATE(),
        UpdateCount = UpdateCount + 1,
        RandomValue = NEWID()
    WHERE Id <= 100; -- Update first 100 rows repeatedly
    
    -- Create some longer transactions to increase transaction delay
    BEGIN TRANSACTION;
        UPDATE FrequentUpdateTable 
        SET Counter = Counter + 10
        WHERE Id BETWEEN 101 AND 200;
        
        -- Insert some new data during transaction
        INSERT INTO BulkInsertTable (BulkData, SequenceNumber, ChecksumValue)
        VALUES 
        ('Bulk data iteration ' + CAST(@IterationCount AS VARCHAR(10)), @IterationCount, CHECKSUM(@IterationCount));
        
        -- Small delay while transaction is open
        WAITFOR DELAY '00:00:00.500'; -- 500ms delay
    COMMIT TRANSACTION;
    
    SET @IterationCount = @IterationCount + 1;
    
    -- Very brief pause
    WAITFOR DELAY '00:00:00.100'; -- 100ms pause
END;
*/

-- =====================================================
-- 6. WORKLOAD SCRIPT 3: Bulk Operations for Log Send Queue Stress
-- Generate large bulk operations to stress log send mechanisms
-- =====================================================

-- Run this in a separate session/window
/*
DECLARE @BulkCounter INT = 0;
DECLARE @BulkMaxIterations INT = 500;

WHILE @BulkCounter < @BulkMaxIterations
BEGIN
    -- Large bulk insert to generate significant log send queue activity
    DECLARE @StartSequence BIGINT = @BulkCounter * 1000;
    
    INSERT INTO BulkInsertTable (BulkData, SequenceNumber, ChecksumValue)
    SELECT 
        'Bulk operation ' + CAST(@BulkCounter AS VARCHAR(10)) + ' - Row ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10)) + 
        ' - ' + REPLICATE('DataLoad', 25), -- ~200 chars per row
        @StartSequence + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        CHECKSUM(@StartSequence + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
    FROM sys.objects s1 CROSS JOIN sys.objects s2
    WHERE ROWCOUNT_BIG() < 500; -- 500 rows per batch
    
    -- Create a large transaction that spans multiple operations
    BEGIN TRANSACTION;
        -- Mass update operation
        UPDATE BulkInsertTable 
        SET BulkData = BulkData + ' - Updated in batch ' + CAST(@BulkCounter AS VARCHAR(10))
        WHERE SequenceNumber BETWEEN @StartSequence - 500 AND @StartSequence;
        
        -- Delete some old data
        DELETE FROM BulkInsertTable 
        WHERE SequenceNumber < (@StartSequence - 2000) 
        AND SequenceNumber % 3 = 0;
        
        -- Another insert within the same transaction
        INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
        SELECT 
            'Cross-table transaction ' + CAST(@BulkCounter AS VARCHAR(10)),
            RAND() * 5000,
            REPLICATE('Cross-transaction data. ', 150) -- ~3KB per row
        FROM (SELECT TOP 50 * FROM sys.objects) AS sub;
        
        -- Hold transaction open briefly to create pressure
        WAITFOR DELAY '00:00:01'; -- 1 second delay with open transaction
    COMMIT TRANSACTION;
    
    SET @BulkCounter = @BulkCounter + 1;
    
    -- Moderate pause between bulk operations
    WAITFOR DELAY '00:00:03'; -- 3 second pause
END;
*/

-- =====================================================
-- 7. WORKLOAD SCRIPT 4: Mixed Workload for Flow Control
-- Create mixed read/write workload to trigger flow control
-- =====================================================

-- Run this in a separate session/window
/*
DECLARE @MixedCounter INT = 0;
DECLARE @MixedMaxIterations INT = 1000;

WHILE @MixedCounter < @MixedMaxIterations
BEGIN
    -- Heavy SELECT operations to create read pressure
    SELECT COUNT(*), AVG(TransactionValue), MAX(LEN(LargeTextField))
    FROM LargeTransactionTable 
    WHERE TransactionDate >= DATEADD(MINUTE, -10, GETDATE());
    
    -- Concurrent write operations
    BEGIN TRANSACTION;
        INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
        VALUES 
        ('Mixed workload entry ' + CAST(@MixedCounter AS VARCHAR(10)), 
         RAND() * 1000,
         REPLICATE('Mixed workload data for flow control testing. ', 80)); -- ~4KB
         
        UPDATE FrequentUpdateTable 
        SET Counter = Counter + (@MixedCounter % 100),
            UpdateCount = UpdateCount + 1
        WHERE Id = ((@MixedCounter % 1000) + 1);
        
        -- Create deliberate lock contention
        UPDATE LargeTransactionTable 
        SET StatusCode = 2
        WHERE Id = ((@MixedCounter % 10000) + 1);
        
        -- Hold locks briefly
        WAITFOR DELAY '00:00:00.200'; -- 200ms delay
    COMMIT TRANSACTION;
    
    -- More read operations
    SELECT TOP 100 * 
    FROM BulkInsertTable 
    ORDER BY SequenceNumber DESC;
    
    SET @MixedCounter = @MixedCounter + 1;
    
    -- Brief pause
    WAITFOR DELAY '00:00:00.500'; -- 500ms pause
END;
*/

-- =====================================================
-- 8. MONITORING QUERIES: Check AlwaysOn metrics while load is running
-- =====================================================

-- Query to monitor Log Bytes Received/sec, Transaction Delay, Flow Control Time
-- (Run this periodically while load scripts are running)
/*
SELECT
    instance_name,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log_Bytes_Received_Per_Sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction_Delay_Ms],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow_Control_Time_Ms_Per_Sec],
    GETDATE() AS [Sample_Time]
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
*/

-- Query to monitor Log Send Queue, Redo Queue, and Redo Rate
/*
SELECT
    ar.replica_server_name,
    d.name AS database_name,
    drs.log_send_queue_size AS log_send_queue_kb,
    drs.redo_queue_size AS redo_queue_kb,
    drs.redo_rate AS redo_rate_kb_sec,
    drs.database_state_desc,
    drs.synchronization_state_desc,
    GETDATE() AS [Sample_Time]
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id
WHERE
    d.name = 'AlwaysOnLoadTest';  -- Monitor our test database
*/

-- Query to check overall Always On Availability Group health
/*
SELECT
    ar.replica_server_name,
    ars.role_desc,
    ars.synchronization_health_desc,
    ar.availability_mode_desc,
    ars.connected_state_desc,
    ars.operational_state_desc,
    GETDATE() AS [Sample_Time]
FROM
    sys.dm_hadr_availability_replica_states AS ars
INNER JOIN
    sys.availability_replicas AS ar ON ars.replica_id = ar.replica_id;
*/

-- =====================================================
-- 9. CLEANUP SCRIPT (Run when testing is complete)
-- =====================================================

-- Use this to clean up test data when done
/*
USE master;
GO

-- Remove database from availability group (if added)
-- ALTER AVAILABILITY GROUP [YourAGName] REMOVE DATABASE [AlwaysOnLoadTest];
-- GO

-- Drop the test database
-- DROP DATABASE AlwaysOnLoadTest;
-- GO
*/

-- =====================================================
-- 10. USAGE INSTRUCTIONS
-- =====================================================

/*
SETUP INSTRUCTIONS:

1. Ensure you have an AlwaysOn Availability Group configured
2. Run the database creation and table setup sections (1-3) on the primary replica
3. Add the AlwaysOnLoadTest database to your availability group:
   
   -- On Primary Replica:
   ALTER AVAILABILITY GROUP [YourAGName] ADD DATABASE [AlwaysOnLoadTest];
   
   -- On Secondary Replica(s):
   RESTORE DATABASE [AlwaysOnLoadTest] FROM DISK = 'backup_location'
   WITH NORECOVERY;
   RESTORE LOG [AlwaysOnLoadTest] FROM DISK = 'log_backup_location'
   WITH NORECOVERY;
   ALTER DATABASE [AlwaysOnLoadTest] SET HADR AVAILABILITY GROUP = [YourAGName];

4. Open multiple query windows and run the workload scripts (4-7) simultaneously:
   - Window 1: High Volume Transaction Load (Script 4)
   - Window 2: Frequent Updates (Script 5)  
   - Window 3: Bulk Operations (Script 6)
   - Window 4: Mixed Workload (Script 7)

5. In another window, run the monitoring queries (Script 8) every 30-60 seconds

6. Monitor your OpenTelemetry collector output for the metrics:
   - sqlserver.failover_cluster.log_bytes_received_per_sec
   - sqlserver.failover_cluster.transaction_delay_ms
   - sqlserver.failover_cluster.flow_control_time_ms
   - sqlserver.failover_cluster.log_send_queue_kb
   - sqlserver.failover_cluster.redo_queue_kb
   - sqlserver.failover_cluster.redo_rate_kb_sec

EXPECTED RESULTS:
- Log bytes received/sec: Should show increased values as log data flows from primary to secondary
- Log send queue: Should show varying queue sizes as large transactions are processed
- Redo queue: Should show backlog on secondary replica during heavy write loads
- Redo rate: Should show increased processing rate on secondary
- Transaction delay: Should show latency increases during heavy workloads
- Flow control time: Should show flow control activation during peak loads

TROUBLESHOOTING:
- If metrics show zero values, ensure AlwaysOn AG is properly configured and synchronized
- Verify the database is actually part of the availability group
- Check that the secondary replica is online and accessible
- Monitor sys.dm_hadr_database_replica_states for replication status
*/