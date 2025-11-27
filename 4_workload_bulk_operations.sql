-- =====================================================
-- WORKLOAD 3: Bulk Operations for Log Send Queue Stress
-- Generates large bulk operations to stress log send mechanisms
-- Run this in a separate query window
-- =====================================================

USE AlwaysOnLoadTest;
GO

DECLARE @BulkCounter INT = 0;
DECLARE @BulkMaxIterations INT = 500;

PRINT 'Starting Bulk Operations Workload...';
PRINT 'This will stress log send queue mechanisms';

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
    
    -- Progress indicator
    IF @BulkCounter % 25 = 0
        PRINT 'Completed ' + CAST(@BulkCounter AS VARCHAR(10)) + ' bulk operations...';
    
    -- Moderate pause between bulk operations
    WAITFOR DELAY '00:00:03'; -- 3 second pause
END;

PRINT 'Bulk Operations Workload completed!';
GO