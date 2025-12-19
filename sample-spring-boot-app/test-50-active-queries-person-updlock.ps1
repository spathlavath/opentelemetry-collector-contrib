# 50 Active Person UPDLOCK Queries Test - PowerShell Version

$baseUrl = "http://localhost:8080"

Write-Host "=========================================="
Write-Host "50 Active Person UPDLOCK Queries Test"
Write-Host "=========================================="
Write-Host ""
Write-Host "This script will:"
Write-Host "  1. Run 1 SLOW complex Person UPDLOCK query (1-minute delay)"
Write-Host "  2. Run 50 ACTIVE complex Person UPDLOCK queries (90-second delay each)"
Write-Host ""
Write-Host "Query Pattern:"
Write-Host "  SELECT * FROM Person WITH(UPDLOCK) WHERE BusinessEntityID=@"
Write-Host ""
Write-Host "Features:"
Write-Host "  - Complex subqueries (Email, Phone, Address, Sales)"
Write-Host "  - Large execution plan (8+ subqueries)"
Write-Host "  - UPDLOCK hint (creates LCK_M_U wait type)"
Write-Host "  - Matches your real application pattern"
Write-Host ""
Write-Host "=========================================="
Write-Host ""

Read-Host "Press ENTER to start"

Write-Host ""
$startTime = Get-Date
Write-Host "[1/51] Starting 1-minute SLOW complex Person UPDLOCK query at $($startTime.ToString('HH:mm:ss'))..."

# Start slow query in background
Start-Job -ScriptBlock {
    param($url)
    try {
        Invoke-RestMethod -Uri "$url/api/users/slow-complex-person-updlock-query" -Method Get -TimeoutSec 120
    } catch {
        Write-Host "Slow query completed or timed out"
    }
} -ArgumentList $baseUrl | Out-Null

Write-Host "Slow Person UPDLOCK query (1 min) started"
Write-Host ""

Start-Sleep -Seconds 2

Write-Host "Starting 50 ACTIVE complex Person UPDLOCK queries (90 seconds each)..."
Write-Host ""

# Start 50 active Person UPDLOCK queries
for ($i = 1; $i -le 50; $i++) {
    Start-Job -ScriptBlock {
        param($url)
        try {
            Invoke-RestMethod -Uri "$url/api/users/active-complex-person-updlock-query" -Method Get -TimeoutSec 180
        } catch {
            # Silently handle completion
        }
    } -ArgumentList $baseUrl | Out-Null

    Write-Host "[$i/50] Active Person UPDLOCK query $i started"
}

Write-Host ""
Write-Host "=========================================="
Write-Host "All queries started!"
Write-Host "=========================================="
Write-Host ""
Write-Host "Status:"
Write-Host "  - 1 slow Person UPDLOCK query (1 minute) running"
Write-Host "  - 50 active Person UPDLOCK queries (90 seconds each) running"
Write-Host "  - Total: 51 concurrent queries"
Write-Host ""
Write-Host "Query Pattern: SELECT * FROM Person WITH(UPDLOCK) WHERE BusinessEntityID=@"
Write-Host ""
Write-Host "Complex Execution Plan includes:"
Write-Host "  1. Person table scan with UPDLOCK"
Write-Host "  2. EmailAddress subquery"
Write-Host "  3. PersonPhone subquery"
Write-Host "  4. BusinessEntityAddress + Address + StateProvince JOIN"
Write-Host "  5. Customer existence check"
Write-Host "  6. Employee existence check"
Write-Host "  7. Sales order count aggregation"
Write-Host "  8. Total sales amount aggregation"
Write-Host ""
Write-Host "=========================================="
Write-Host "Monitoring Commands"
Write-Host "=========================================="
Write-Host ""
Write-Host "1. SQL Server - Count active Person UPDLOCK queries:"
Write-Host ""
Write-Host "SELECT COUNT(*) AS active_count"
Write-Host "FROM sys.dm_exec_requests"
Write-Host "WHERE text LIKE '%COMPLEX_PERSON_UPDLOCK%';"
Write-Host ""
Write-Host "2. SQL Server - View active Person queries:"
Write-Host ""
Write-Host "SELECT session_id, start_time, status, wait_type, wait_time,"
Write-Host "       total_elapsed_time / 1000 AS elapsed_sec,"
Write-Host "       SUBSTRING(qt.text, 1, 150) AS query_text"
Write-Host "FROM sys.dm_exec_requests er"
Write-Host "CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
Write-Host "WHERE qt.text LIKE '%Person.Person%UPDLOCK%';"
Write-Host ""
Write-Host "3. New Relic - After 2 minutes:"
Write-Host ""
Write-Host "SELECT count(*), latest(query_text)"
Write-Host "FROM Metric"
Write-Host "WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'"
Write-Host "  AND query_text LIKE '%COMPLEX_PERSON_UPDLOCK%'"
Write-Host "FACET query_id"
Write-Host "SINCE 5 minutes ago"
Write-Host ""
Write-Host "=========================================="
Write-Host "Waiting for queries to complete..."
Write-Host "=========================================="
Write-Host ""
Write-Host "Timeline:"
Write-Host "  - 60 seconds:  Slow query (1 min) completes"
Write-Host "  - 90 seconds:  Active queries (90 sec) complete"
Write-Host ""

# Wait for queries to complete
Start-Sleep -Seconds 95

Write-Host ""
Write-Host "=========================================="
Write-Host "Test Complete!"
Write-Host "=========================================="
Write-Host ""
Write-Host "Background jobs status:"
Get-Job | Select-Object Id, State, Name | Format-Table

Write-Host ""
Write-Host "To view job results:"
Write-Host "  Get-Job | Receive-Job"
Write-Host ""
Write-Host "To clean up completed jobs:"
Write-Host "  Get-Job | Remove-Job"
Write-Host ""
Write-Host "=========================================="
Write-Host ""

Read-Host "Press ENTER to exit and clean up jobs"

# Clean up jobs
Get-Job | Remove-Job -Force
