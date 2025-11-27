-- =====================================================
-- EXECUTABLE SQL Server AlwaysOn AG Load Generation Scripts
-- Ready-to-run workloads for generating AlwaysOn metrics
-- =====================================================

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
-- CREATE TABLES AND INITIAL DATA
-- =====================================================

-- Large transaction table
CREATE TABLE LargeTransactionTable (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    TransactionData NVARCHAR(4000),
    TransactionValue DECIMAL(18,2),
    TransactionDate DATETIME2 DEFAULT GETDATE(),
    StatusCode INT DEFAULT 1,
    ProcessingFlag BIT DEFAULT 0,
    LargeTextField NVARCHAR(MAX)
);

-- Frequent update table
CREATE TABLE FrequentUpdateTable (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Counter INT DEFAULT 0,
    LastUpdated DATETIME2 DEFAULT GETDATE(),
    RandomValue UNIQUEIDENTIFIER DEFAULT NEWID(),
    UpdateCount BIGINT DEFAULT 0
);

-- Bulk insert table
CREATE TABLE BulkInsertTable (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    BulkData NVARCHAR(2000),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    SequenceNumber BIGINT,
    ChecksumValue BIGINT
);

-- Create indexes
CREATE NONCLUSTERED INDEX IX_LargeTransactionTable_Date 
    ON LargeTransactionTable (TransactionDate) INCLUDE (TransactionValue, StatusCode);

CREATE NONCLUSTERED INDEX IX_FrequentUpdateTable_LastUpdated 
    ON FrequentUpdateTable (LastUpdated) INCLUDE (Counter, UpdateCount);

CREATE NONCLUSTERED INDEX IX_BulkInsertTable_Sequence 
    ON BulkInsertTable (SequenceNumber) INCLUDE (CreatedDate);

-- Insert initial data
INSERT INTO FrequentUpdateTable (Counter, RandomValue, UpdateCount)
SELECT 
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    NEWID(),
    0
FROM sys.objects s1 CROSS JOIN sys.objects s2
WHERE ROWCOUNT_BIG() < 1000;

PRINT 'Database setup completed. Add this database to your Always On Availability Group before running workloads.';
GO