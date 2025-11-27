-- SQL Server Load Generation Scripts for Testing New Relic Dashboard
-- These scripts will generate various types of database activity to populate metrics

-- =============================================================================
-- 1. DATABASE SIZE AND STORAGE ACTIVITY
-- =============================================================================

-- Create a test database for generating metrics
USE master;
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TestLoadDB')
BEGIN
    CREATE DATABASE TestLoadDB;
END
GO

USE TestLoadDB;
GO

-- Create tables to generate storage metrics
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'LoadTestTable1')
BEGIN
    CREATE TABLE LoadTestTable1 (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        Data NVARCHAR(1000),
        CreatedDate DATETIME2 DEFAULT GETDATE(),
        LargeData VARBINARY(MAX)
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'LoadTestTable2')
BEGIN
    CREATE TABLE LoadTestTable2 (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerName NVARCHAR(100),
        OrderAmount DECIMAL(18,2),
        OrderDate DATETIME2 DEFAULT GETDATE(),
        Notes TEXT
    );
END
GO

-- =============================================================================
-- 2. GENERATE DATABASE SIZE GROWTH (for sqlserver.database.size.* metrics)
-- =============================================================================

-- Insert data to increase database size
DECLARE @Counter INT = 1;
WHILE @Counter <= 1000
BEGIN
    INSERT INTO LoadTestTable1 (Data, LargeData)
    VALUES (
        REPLICATE('Sample data for testing purposes ', 20),
        CAST(REPLICATE('Binary data for storage testing', 50) AS VARBINARY(MAX))
    );
    
    INSERT INTO LoadTestTable2 (CustomerName, OrderAmount, Notes)
    VALUES (
        'Customer ' + CAST(@Counter AS NVARCHAR(10)),
        RAND() * 10000,
        REPLICATE('Order notes and details for testing dashboard metrics ', 10)
    );
    
    SET @Counter = @Counter + 1;
    
    -- Small delay to spread the load
    IF @Counter % 100 = 0
        WAITFOR DELAY '00:00:01';
END
GO

-- =============================================================================
-- 3. GENERATE TRANSACTION ACTIVITY (for sqlserver.database.transactions.active)
-- =============================================================================

-- Create a procedure that runs long transactions
CREATE OR ALTER PROCEDURE GenerateTransactionLoad
AS
BEGIN
    BEGIN TRANSACTION;
    
    -- Simulate some work
    UPDATE LoadTestTable1 SET Data = Data + ' Updated at ' + CAST(GETDATE() AS NVARCHAR(30))
    WHERE ID % 10 = 1;
    
    -- Hold transaction for a bit to show active transactions
    WAITFOR DELAY '00:00:05';
    
    COMMIT TRANSACTION;
END
GO

-- =============================================================================
-- 4. GENERATE CONNECTION ACTIVITY (for sqlserver.stats.connections)
-- =============================================================================

-- This will show user connections - run multiple sessions
-- Instructions: Open multiple SSMS query windows and run this simultaneously

SELECT 
    SESSION_ID = @@SPID,
    CONNECTION_TIME = GETDATE(),
    USER_NAME = SUSER_NAME(),
    DATABASE_NAME = DB_NAME()

-- Simulate some connection activity
DECLARE @i INT = 1;
WHILE @i <= 50
BEGIN
    SELECT COUNT(*) FROM LoadTestTable1;
    SELECT COUNT(*) FROM LoadTestTable2;
    WAITFOR DELAY '00:00:02';
    SET @i = @i + 1;
END

-- =============================================================================
-- 5. GENERATE LOCK ACTIVITY (for sqlserver.lock.resource.* metrics)
-- =============================================================================

-- Create procedure to generate lock contention
CREATE OR ALTER PROCEDURE GenerateLockContention
AS
BEGIN
    BEGIN TRANSACTION;
    
    -- This will create table locks
    UPDATE LoadTestTable1 SET Data = 'Locking test data'
    WHERE ID BETWEEN 1 AND 100;
    
    -- Hold locks for demonstration
    WAITFOR DELAY '00:00:10';
    
    COMMIT TRANSACTION;
END
GO

-- =============================================================================
-- 6. GENERATE QUERY PERFORMANCE DATA (for slow query metrics)
-- =============================================================================

-- Create indexes to simulate different access patterns
CREATE NONCLUSTERED INDEX IX_LoadTestTable1_Data ON LoadTestTable1(Data);
CREATE NONCLUSTERED INDEX IX_LoadTestTable2_CustomerName ON LoadTestTable2(CustomerName);
GO

-- Generate slow queries
CREATE OR ALTER PROCEDURE GenerateSlowQueries
AS
BEGIN
    -- Slow query 1: Full table scan
    SELECT COUNT(*) 
    FROM LoadTestTable1 l1
    CROSS JOIN LoadTestTable2 l2
    WHERE l1.Data LIKE '%test%';
    
    -- Slow query 2: Complex aggregation
    SELECT 
        YEAR(l2.OrderDate) as OrderYear,
        COUNT(*) as OrderCount,
        SUM(l2.OrderAmount) as TotalAmount,
        AVG(l2.OrderAmount) as AvgAmount
    FROM LoadTestTable2 l2
    GROUP BY YEAR(l2.OrderDate)
    ORDER BY OrderYear;
    
    -- Slow query 3: Nested subqueries
    SELECT * FROM LoadTestTable1 
    WHERE ID IN (
        SELECT TOP 100 l2.ID 
        FROM LoadTestTable2 l2
        WHERE l2.OrderAmount > (
            SELECT AVG(OrderAmount) FROM LoadTestTable2
        )
    );
END
GO

-- =============================================================================
-- 7. GENERATE WAIT STATISTICS (for sqlserver.wait_stats.* metrics)
-- =============================================================================

-- Generate I/O waits
CREATE OR ALTER PROCEDURE GenerateIOWaits
AS
BEGIN
    -- Force physical reads by dropping buffer pool for this database
    DECLARE @sql NVARCHAR(1000);
    SET @sql = 'DBCC DROPCLEANBUFFERS';
    EXEC sp_executesql @sql;
    
    -- Now read data to generate I/O waits
    SELECT * FROM LoadTestTable1 ORDER BY Data;
    SELECT * FROM LoadTestTable2 ORDER BY CustomerName;
END
GO

-- =============================================================================
-- 8. GENERATE BUFFER POOL ACTIVITY (for sqlserver.bufferPoolHitPercent)
-- =============================================================================

CREATE OR ALTER PROCEDURE GenerateBufferPoolActivity
AS
BEGIN
    -- Read data multiple times to improve buffer pool hit ratio
    DECLARE @i INT = 1;
    WHILE @i <= 20
    BEGIN
        SELECT COUNT(*) FROM LoadTestTable1;
        SELECT COUNT(*) FROM LoadTestTable2;
        SELECT TOP 100 * FROM LoadTestTable1 ORDER BY ID;
        SELECT TOP 100 * FROM LoadTestTable2 ORDER BY OrderDate;
        SET @i = @i + 1;
    END
END
GO

-- =============================================================================
-- 9. GENERATE DEADLOCKS (for sqlserver.stats.deadlocks_per_sec)
-- =============================================================================

-- Warning: This will intentionally create deadlocks for testing
CREATE OR ALTER PROCEDURE GenerateDeadlock_Session1
AS
BEGIN
    BEGIN TRANSACTION;
    
    UPDATE LoadTestTable1 SET Data = 'Session1 Update' WHERE ID = 1;
    WAITFOR DELAY '00:00:05';
    UPDATE LoadTestTable2 SET CustomerName = 'Session1 Update' WHERE ID = 1;
    
    COMMIT TRANSACTION;
END
GO

CREATE OR ALTER PROCEDURE GenerateDeadlock_Session2  
AS
BEGIN
    BEGIN TRANSACTION;
    
    UPDATE LoadTestTable2 SET CustomerName = 'Session2 Update' WHERE ID = 1;
    WAITFOR DELAY '00:00:05';
    UPDATE LoadTestTable1 SET Data = 'Session2 Update' WHERE ID = 1;
    
    COMMIT TRANSACTION;
END
GO

-- =============================================================================
-- 10. MASTER LOAD GENERATION SCRIPT
-- =============================================================================

CREATE OR ALTER PROCEDURE RunAllLoadTests
AS
BEGIN
    PRINT 'Starting comprehensive load generation for dashboard testing...';
    
    -- Generate transaction activity
    EXEC GenerateTransactionLoad;
    PRINT 'Transaction load completed';
    
    -- Generate slow queries
    EXEC GenerateSlowQueries;
    PRINT 'Slow query generation completed';
    
    -- Generate I/O waits
    EXEC GenerateIOWaits;
    PRINT 'I/O wait generation completed';
    
    -- Generate buffer pool activity
    EXEC GenerateBufferPoolActivity;
    PRINT 'Buffer pool activity completed';
    
    PRINT 'Load generation completed successfully!';
END
GO

-- =============================================================================
-- EXECUTION INSTRUCTIONS
-- =============================================================================

PRINT '=============================================================================';
PRINT 'SQL SERVER LOAD GENERATION FOR NEW RELIC DASHBOARD TESTING';
PRINT '=============================================================================';
PRINT '';
PRINT 'To generate comprehensive load and populate dashboard metrics:';
PRINT '';
PRINT '1. Run the main load test:';
PRINT '   EXEC RunAllLoadTests;';
PRINT '';
PRINT '2. For ongoing activity, run in separate sessions simultaneously:';
PRINT '   -- Session 1: EXEC GenerateTransactionLoad;';
PRINT '   -- Session 2: EXEC GenerateSlowQueries;';  
PRINT '   -- Session 3: EXEC GenerateBufferPoolActivity;';
PRINT '';
PRINT '3. To generate deadlocks (run in 2 separate sessions at same time):';
PRINT '   -- Session A: EXEC GenerateDeadlock_Session1;';
PRINT '   -- Session B: EXEC GenerateDeadlock_Session2;';
PRINT '';
PRINT '4. Check your New Relic dashboard after 2-3 minutes for populated metrics';
PRINT '';
PRINT '=============================================================================';

-- Start the load generation
PRINT 'Starting initial load generation...';
EXEC RunAllLoadTests;