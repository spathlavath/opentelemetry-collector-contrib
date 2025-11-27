-- =====================================================
-- WORKLOAD 4: Mixed Workload for Flow Control
-- Creates mixed read/write workload to trigger flow control
-- Run this in a separate query window
-- =====================================================

USE AlwaysOnLoadTest;
GO

DECLARE @MixedCounter INT = 0;
DECLARE @MixedMaxIterations INT = 1000;

PRINT 'Starting Mixed Workload...';
PRINT 'This will trigger flow control mechanisms';

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
    
    -- Progress indicator
    IF @MixedCounter % 100 = 0
        PRINT 'Completed ' + CAST(@MixedCounter AS VARCHAR(10)) + ' mixed operations...';
    
    -- Brief pause
    WAITFOR DELAY '00:00:00.500'; -- 500ms pause
END;

PRINT 'Mixed Workload completed!';
GO