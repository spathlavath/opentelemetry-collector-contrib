-- =====================================================
-- WORKLOAD 2: Frequent Updates for Redo Queue Pressure
-- Generates continuous updates to stress redo processing on secondary
-- Run this in a separate query window
-- =====================================================

USE AlwaysOnLoadTest;
GO

DECLARE @IterationCount INT = 0;
DECLARE @MaxIterations INT = 2000;

PRINT 'Starting Frequent Updates Workload...';
PRINT 'This will create redo queue pressure on secondary replicas';

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
    
    -- Progress indicator
    IF @IterationCount % 100 = 0
        PRINT 'Completed ' + CAST(@IterationCount AS VARCHAR(10)) + ' update iterations...';
    
    -- Very brief pause
    WAITFOR DELAY '00:00:00.100'; -- 100ms pause
END;

PRINT 'Frequent Updates Workload completed!';
GO