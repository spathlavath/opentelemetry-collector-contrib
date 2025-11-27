-- =====================================================
-- WORKLOAD 1: High Volume Transaction Load
-- Generates large amounts of log data to stress log transport
-- Run this in a separate query window
-- =====================================================

USE AlwaysOnLoadTest;
GO

DECLARE @Counter INT = 0;
DECLARE @BatchSize INT = 100;
DECLARE @MaxIterations INT = 1000; -- Adjust based on desired load duration

PRINT 'Starting High Volume Transaction Load...';
PRINT 'This will generate significant log volume for AlwaysOn replication';

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
    
    -- Progress indicator
    IF @Counter % 50 = 0
        PRINT 'Completed ' + CAST(@Counter AS VARCHAR(10)) + ' iterations...';
    
    -- Brief pause between batches
    WAITFOR DELAY '00:00:02';
END;

PRINT 'High Volume Transaction Load completed!';
GO