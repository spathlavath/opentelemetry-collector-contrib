# Simple Person UPDLOCK Test - ONE Query Pattern
# This creates blocking scenario with same query_hash

$baseUrl = "http://localhost:8080"

Write-Host "=========================================="
Write-Host "Person UPDLOCK Test - Simple Scenario"
Write-Host "=========================================="
Write-Host ""
Write-Host "This test will:"
Write-Host "  1. Fire 10 concurrent Person UPDLOCK queries"
Write-Host "  2. Each query holds UPDLOCK for 90 seconds"
Write-Host "  3. Creates blocking (first query blocks others)"
Write-Host "  4. Collector (15s interval) will capture:"
Write-Host "     - Active queries in sys.dm_exec_requests"
Write-Host "     - Blocking session IDs"
Write-Host "     - Wait events (LCK_M_U)"
Write-Host "     - Execution plan"
Write-Host ""
Write-Host "Query Pattern:"
Write-Host "  SELECT * FROM Person.Person WITH(UPDLOCK)"
Write-Host "  WHERE BusinessEntityID BETWEEN 1 AND 50"
Write-Host ""
Write-Host "=========================================="
Write-Host ""

Read-Host "Press ENTER to start"

Write-Host ""
$startTime = Get-Date
Write-Host "Starting 10 concurrent Person UPDLOCK queries at $($startTime.ToString('HH:mm:ss'))..."
Write-Host ""

# Start 10 queries in background
for ($i = 1; $i -le 10; $i++) {
    Start-Job -ScriptBlock {
        param($url)
        try {
            Invoke-RestMethod -Uri "$url/api/users/person-updlock-query" -Method Get -TimeoutSec 120
        } catch {
            # Silently handle completion
        }
    } -ArgumentList $baseUrl | Out-Null

    Write-Host "[$i/10] Query $i started"
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "=========================================="
Write-Host "All 10 queries started!"
Write-Host "=========================================="
Write-Host ""
Write-Host "Status:"
Write-Host "  - 10 concurrent queries running"
Write-Host "  - First query holds UPDLOCK"
Write-Host "  - Other 9 queries blocked (waiting for lock)"
Write-Host "  - Each query runs for 90 seconds"
Write-Host ""
Write-Host "=========================================="
Write-Host "SQL Server Monitoring"
Write-Host "=========================================="
Write-Host ""
Write-Host "1. Check active queries (run NOW in SQL Server):"
Write-Host ""
Write-Host "SELECT"
Write-Host "    session_id,"
Write-Host "    status,"
Write-Host "    wait_type,"
Write-Host "    wait_time,"
Write-Host "    blocking_session_id,"
Write-Host "    total_elapsed_time / 1000 AS elapsed_sec,"
Write-Host "    SUBSTRING(qt.text, 1, 200) AS query_text"
Write-Host "FROM sys.dm_exec_requests er"
Write-Host "CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
Write-Host "WHERE qt.text LIKE '%PERSON_UPDLOCK_WITH_PLAN%';"
Write-Host ""
Write-Host "Expected: 10 rows"
Write-Host "  - 1st query: status='running', wait_type='WAITFOR', blocking_session_id=NULL"
Write-Host "  - Other 9: status='suspended', wait_type='LCK_M_U', blocking_session_id=<first session>"
Write-Host ""
Write-Host "2. Check execution plan:"
Write-Host ""
Write-Host "SELECT"
Write-Host "    qs.query_hash,"
Write-Host "    qs.query_plan_hash,"
Write-Host "    CAST(qp.query_plan AS XML) AS execution_plan"
Write-Host "FROM sys.dm_exec_query_stats qs"
Write-Host "CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp"
Write-Host "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st"
Write-Host "WHERE st.text LIKE '%PERSON_UPDLOCK_WITH_PLAN%';"
Write-Host ""
Write-Host "=========================================="
Write-Host "Waiting for queries to complete (95 seconds)..."
Write-Host "=========================================="
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
Write-Host "Verification:"
Write-Host ""
Write-Host "1. Check New Relic for active query metrics:"
Write-Host ""
Write-Host "SELECT"
Write-Host "    count(*) AS total_captures,"
Write-Host "    latest(query_text),"
Write-Host "    latest(blocking_session_id),"
Write-Host "    latest(wait_type)"
Write-Host "FROM Metric"
Write-Host "WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'"
Write-Host "  AND query_text LIKE '%PERSON_UPDLOCK_WITH_PLAN%'"
Write-Host "SINCE 5 minutes ago"
Write-Host ""
Write-Host "Expected: Multiple captures showing blocking relationships"
Write-Host ""
Write-Host "=========================================="
Write-Host ""

Read-Host "Press ENTER to clean up jobs"

# Clean up jobs
Get-Job | Remove-Job -Force
