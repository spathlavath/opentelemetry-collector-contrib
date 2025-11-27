# =====================================================
# PowerShell Script to Run AlwaysOn AG Load Generation
# This script runs multiple SQL workloads concurrently to generate
# AlwaysOn Availability Group metrics
# =====================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$ServerInstance,
    
    [Parameter(Mandatory=$true)]
    [string]$DatabaseName = "AlwaysOnLoadTest",
    
    [Parameter(Mandatory=$false)]
    [int]$DurationMinutes = 30,
    
    [Parameter(Mandatory=$false)]
    [System.Management.Automation.PSCredential]$Credential,
    
    [Parameter(Mandatory=$false)]
    [switch]$UseTrustedConnection = $true
)

# Import SQL Server module
Import-Module SqlServer -ErrorAction SilentlyContinue
if (-not (Get-Module SqlServer)) {
    Write-Error "SQL Server PowerShell module not found. Please install: Install-Module SqlServer"
    exit 1
}

# Function to execute SQL in background
function Start-SqlWorkload {
    param(
        [string]$Query,
        [string]$WorkloadName,
        [string]$ServerInstance,
        [string]$DatabaseName,
        [System.Management.Automation.PSCredential]$Credential,
        [bool]$UseTrustedConnection
    )
    
    $scriptBlock = {
        param($Query, $WorkloadName, $ServerInstance, $DatabaseName, $Credential, $UseTrustedConnection)
        
        try {
            Import-Module SqlServer -ErrorAction SilentlyContinue
            
            $connectionParams = @{
                ServerInstance = $ServerInstance
                Database = $DatabaseName
                Query = $Query
                QueryTimeout = 0  # No timeout for background workloads
            }
            
            if ($UseTrustedConnection) {
                $connectionParams.Add('TrustServerCertificate', $true)
            } else {
                $connectionParams.Add('Credential', $Credential)
            }
            
            Write-Host "[$WorkloadName] Starting workload..." -ForegroundColor Green
            Invoke-Sqlcmd @connectionParams
            Write-Host "[$WorkloadName] Workload completed." -ForegroundColor Green
            
        } catch {
            Write-Error "[$WorkloadName] Error: $($_.Exception.Message)"
        }
    }
    
    return Start-Job -ScriptBlock $scriptBlock -ArgumentList $Query, $WorkloadName, $ServerInstance, $DatabaseName, $Credential, $UseTrustedConnection
}

# Function to monitor metrics
function Start-MetricsMonitoring {
    param(
        [string]$ServerInstance,
        [string]$DatabaseName,
        [System.Management.Automation.PSCredential]$Credential,
        [bool]$UseTrustedConnection,
        [int]$DurationMinutes
    )
    
    $monitoringQuery = @"
-- Monitor AlwaysOn performance counters
SELECT
    'Performance Counters' AS MetricType,
    instance_name AS InstanceName,
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
    instance_name

UNION ALL

-- Monitor redo queue metrics
SELECT
    'Redo Queue Metrics' AS MetricType,
    ar.replica_server_name + '.' + d.name AS InstanceName,
    drs.log_send_queue_size AS [Log_Send_Queue_KB],
    drs.redo_queue_size AS [Redo_Queue_KB],
    drs.redo_rate AS [Redo_Rate_KB_Sec],
    GETDATE() AS [Sample_Time]
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id
WHERE
    d.name = '$DatabaseName';
"@
    
    $scriptBlock = {
        param($Query, $ServerInstance, $DatabaseName, $Credential, $UseTrustedConnection, $DurationMinutes)
        
        try {
            Import-Module SqlServer -ErrorAction SilentlyContinue
            
            $connectionParams = @{
                ServerInstance = $ServerInstance
                Database = 'master'  # Use master for DMV queries
                QueryTimeout = 30
            }
            
            if ($UseTrustedConnection) {
                $connectionParams.Add('TrustServerCertificate', $true)
            } else {
                $connectionParams.Add('Credential', $Credential)
            }
            
            $endTime = (Get-Date).AddMinutes($DurationMinutes)
            $iteration = 1
            
            Write-Host "[MONITOR] Starting metrics monitoring for $DurationMinutes minutes..." -ForegroundColor Cyan
            
            while ((Get-Date) -lt $endTime) {
                try {
                    $connectionParams.Query = $Query
                    $results = Invoke-Sqlcmd @connectionParams
                    
                    Write-Host "[MONITOR] Iteration $iteration - $(Get-Date)" -ForegroundColor Cyan
                    $results | Format-Table -AutoSize
                    
                    $iteration++
                    Start-Sleep -Seconds 30  # Sample every 30 seconds
                    
                } catch {
                    Write-Warning "[MONITOR] Error collecting metrics: $($_.Exception.Message)"
                    Start-Sleep -Seconds 10
                }
            }
            
            Write-Host "[MONITOR] Monitoring completed." -ForegroundColor Cyan
            
        } catch {
            Write-Error "[MONITOR] Fatal error: $($_.Exception.Message)"
        }
    }
    
    return Start-Job -ScriptBlock $scriptBlock -ArgumentList $monitoringQuery, $ServerInstance, $DatabaseName, $Credential, $UseTrustedConnection, $DurationMinutes
}

# Workload queries
$workload1 = @"
-- High Volume Transaction Load
DECLARE @Counter INT = 0;
DECLARE @BatchSize INT = 100;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $DurationMinutes, GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    -- Large batch insert to generate significant log volume
    INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
    SELECT 
        'Transaction batch ' + CAST(@Counter AS VARCHAR(10)) + ' - ' + 
        REPLICATE('LoadTestData', 50),
        RAND() * 10000,
        REPLICATE('Large text field data for testing log volume generation. ', 100)
    FROM sys.objects s1 CROSS JOIN sys.objects s2
    WHERE ROWCOUNT_BIG() < @BatchSize;
    
    -- Create transaction delay
    BEGIN TRANSACTION;
        UPDATE LargeTransactionTable 
        SET ProcessingFlag = 1,
            TransactionValue = TransactionValue * 1.1
        WHERE Id % 10 = 0 AND ProcessingFlag = 0;
        
        WAITFOR DELAY '00:00:01';
    COMMIT TRANSACTION;
    
    SET @Counter = @Counter + 1;
    WAITFOR DELAY '00:00:02';
END;
"@

$workload2 = @"
-- Frequent Updates for Redo Queue Pressure
DECLARE @IterationCount INT = 0;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $DurationMinutes, GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    -- Rapid fire updates
    UPDATE FrequentUpdateTable 
    SET Counter = Counter + 1,
        LastUpdated = GETDATE(),
        UpdateCount = UpdateCount + 1,
        RandomValue = NEWID()
    WHERE Id <= 100;
    
    -- Longer transactions
    BEGIN TRANSACTION;
        UPDATE FrequentUpdateTable 
        SET Counter = Counter + 10
        WHERE Id BETWEEN 101 AND 200;
        
        INSERT INTO BulkInsertTable (BulkData, SequenceNumber, ChecksumValue)
        VALUES 
        ('Bulk data iteration ' + CAST(@IterationCount AS VARCHAR(10)), @IterationCount, CHECKSUM(@IterationCount));
        
        WAITFOR DELAY '00:00:00.500';
    COMMIT TRANSACTION;
    
    SET @IterationCount = @IterationCount + 1;
    WAITFOR DELAY '00:00:00.100';
END;
"@

$workload3 = @"
-- Bulk Operations for Log Send Queue Stress
DECLARE @BulkCounter INT = 0;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $DurationMinutes, GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    DECLARE @StartSequence BIGINT = @BulkCounter * 1000;
    
    -- Large bulk insert
    INSERT INTO BulkInsertTable (BulkData, SequenceNumber, ChecksumValue)
    SELECT 
        'Bulk operation ' + CAST(@BulkCounter AS VARCHAR(10)) + ' - Row ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10)) + 
        ' - ' + REPLICATE('DataLoad', 25),
        @StartSequence + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        CHECKSUM(@StartSequence + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
    FROM sys.objects s1 CROSS JOIN sys.objects s2
    WHERE ROWCOUNT_BIG() < 500;
    
    -- Large transaction
    BEGIN TRANSACTION;
        UPDATE BulkInsertTable 
        SET BulkData = BulkData + ' - Updated in batch ' + CAST(@BulkCounter AS VARCHAR(10))
        WHERE SequenceNumber BETWEEN @StartSequence - 500 AND @StartSequence;
        
        DELETE FROM BulkInsertTable 
        WHERE SequenceNumber < (@StartSequence - 2000) 
        AND SequenceNumber % 3 = 0;
        
        INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
        SELECT 
            'Cross-table transaction ' + CAST(@BulkCounter AS VARCHAR(10)),
            RAND() * 5000,
            REPLICATE('Cross-transaction data. ', 150)
        FROM (SELECT TOP 50 * FROM sys.objects) AS sub;
        
        WAITFOR DELAY '00:00:01';
    COMMIT TRANSACTION;
    
    SET @BulkCounter = @BulkCounter + 1;
    WAITFOR DELAY '00:00:03';
END;
"@

$workload4 = @"
-- Mixed Workload for Flow Control
DECLARE @MixedCounter INT = 0;
DECLARE @EndTime DATETIME2 = DATEADD(MINUTE, $DurationMinutes, GETDATE());

WHILE GETDATE() < @EndTime
BEGIN
    -- Heavy SELECT operations
    SELECT COUNT(*), AVG(TransactionValue), MAX(LEN(LargeTextField))
    FROM LargeTransactionTable 
    WHERE TransactionDate >= DATEADD(MINUTE, -10, GETDATE());
    
    -- Concurrent write operations
    BEGIN TRANSACTION;
        INSERT INTO LargeTransactionTable (TransactionData, TransactionValue, LargeTextField)
        VALUES 
        ('Mixed workload entry ' + CAST(@MixedCounter AS VARCHAR(10)), 
         RAND() * 1000,
         REPLICATE('Mixed workload data for flow control testing. ', 80));
         
        UPDATE FrequentUpdateTable 
        SET Counter = Counter + (@MixedCounter % 100),
            UpdateCount = UpdateCount + 1
        WHERE Id = ((@MixedCounter % 1000) + 1);
        
        UPDATE LargeTransactionTable 
        SET StatusCode = 2
        WHERE Id = ((@MixedCounter % 10000) + 1);
        
        WAITFOR DELAY '00:00:00.200';
    COMMIT TRANSACTION;
    
    SELECT TOP 100 * 
    FROM BulkInsertTable 
    ORDER BY SequenceNumber DESC;
    
    SET @MixedCounter = @MixedCounter + 1;
    WAITFOR DELAY '00:00:00.500';
END;
"@

# Main execution
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "AlwaysOn AG Load Generation Script" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Server: $ServerInstance" -ForegroundColor White
Write-Host "Database: $DatabaseName" -ForegroundColor White
Write-Host "Duration: $DurationMinutes minutes" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Yellow

# Test connection first
try {
    $testParams = @{
        ServerInstance = $ServerInstance
        Database = $DatabaseName
        Query = "SELECT 1 AS TestConnection"
    }
    
    if ($UseTrustedConnection) {
        $testParams.Add('TrustServerCertificate', $true)
    } else {
        $testParams.Add('Credential', $Credential)
    }
    
    Invoke-Sqlcmd @testParams | Out-Null
    Write-Host "✓ Connection to $ServerInstance.$DatabaseName successful" -ForegroundColor Green
    
} catch {
    Write-Error "✗ Failed to connect to $ServerInstance.$DatabaseName - $($_.Exception.Message)"
    exit 1
}

# Start workloads
Write-Host "`nStarting workloads..." -ForegroundColor Yellow
$jobs = @()

$jobs += Start-SqlWorkload -Query $workload1 -WorkloadName "HIGH_VOLUME_TRANSACTIONS" -ServerInstance $ServerInstance -DatabaseName $DatabaseName -Credential $Credential -UseTrustedConnection $UseTrustedConnection
$jobs += Start-SqlWorkload -Query $workload2 -WorkloadName "FREQUENT_UPDATES" -ServerInstance $ServerInstance -DatabaseName $DatabaseName -Credential $Credential -UseTrustedConnection $UseTrustedConnection  
$jobs += Start-SqlWorkload -Query $workload3 -WorkloadName "BULK_OPERATIONS" -ServerInstance $ServerInstance -DatabaseName $DatabaseName -Credential $Credential -UseTrustedConnection $UseTrustedConnection
$jobs += Start-SqlWorkload -Query $workload4 -WorkloadName "MIXED_WORKLOAD" -ServerInstance $ServerInstance -DatabaseName $DatabaseName -Credential $Credential -UseTrustedConnection $UseTrustedConnection

# Start monitoring
$monitorJob = Start-MetricsMonitoring -ServerInstance $ServerInstance -DatabaseName $DatabaseName -Credential $Credential -UseTrustedConnection $UseTrustedConnection -DurationMinutes $DurationMinutes

Write-Host "✓ Started $($jobs.Count) workload jobs and 1 monitoring job" -ForegroundColor Green
Write-Host "✓ Load generation will run for $DurationMinutes minutes" -ForegroundColor Green
Write-Host "`nMonitoring jobs... (Press Ctrl+C to stop early)" -ForegroundColor Cyan

# Wait for jobs to complete
try {
    $endTime = (Get-Date).AddMinutes($DurationMinutes + 2)  # Extra 2 minutes buffer
    
    while ((Get-Date) -lt $endTime) {
        $runningJobs = $jobs + $monitorJob | Where-Object { $_.State -eq 'Running' }
        
        if ($runningJobs.Count -eq 0) {
            Write-Host "All jobs completed." -ForegroundColor Green
            break
        }
        
        Write-Host "Jobs running: $($runningJobs.Count)" -ForegroundColor Cyan
        Start-Sleep -Seconds 10
    }
    
} catch {
    Write-Warning "Interrupted by user or error: $($_.Exception.Message)"
}

# Stop all jobs and get results
Write-Host "`nStopping jobs..." -ForegroundColor Yellow
$allJobs = $jobs + $monitorJob

foreach ($job in $allJobs) {
    if ($job.State -eq 'Running') {
        Stop-Job -Job $job
    }
    
    $output = Receive-Job -Job $job
    if ($output) {
        Write-Host "Job $($job.Name) output:" -ForegroundColor Gray
        $output
    }
    
    Remove-Job -Job $job -Force
}

Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "Load generation completed!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Check your OpenTelemetry collector output for metrics:" -ForegroundColor White
Write-Host "- sqlserver.failover_cluster.log_bytes_received_per_sec" -ForegroundColor Gray
Write-Host "- sqlserver.failover_cluster.transaction_delay_ms" -ForegroundColor Gray  
Write-Host "- sqlserver.failover_cluster.flow_control_time_ms" -ForegroundColor Gray
Write-Host "- sqlserver.failover_cluster.log_send_queue_kb" -ForegroundColor Gray
Write-Host "- sqlserver.failover_cluster.redo_queue_kb" -ForegroundColor Gray
Write-Host "- sqlserver.failover_cluster.redo_rate_kb_sec" -ForegroundColor Gray