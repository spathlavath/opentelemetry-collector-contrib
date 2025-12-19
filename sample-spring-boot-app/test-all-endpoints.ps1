# Test All Endpoints - Verify API is Working

$baseUrl = "http://localhost:8080"

Write-Host "=========================================="
Write-Host "Testing All Person UPDLOCK Endpoints"
Write-Host "=========================================="
Write-Host ""

# Test health first
Write-Host "1. Testing Health Endpoint..."
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/api/health" -Method Get -TimeoutSec 5
    Write-Host "   ✅ Health: $($health.status)" -ForegroundColor Green
} catch {
    Write-Host "   ❌ Health endpoint failed!" -ForegroundColor Red
    Write-Host "   Error: $_"
    Write-Host ""
    Write-Host "Please start the application first:"
    Write-Host "  start-with-apm.bat"
    exit 1
}

Write-Host ""
Write-Host "2. Testing Person UPDLOCK Endpoints (quick test - will start but not wait)..."
Write-Host ""

$endpoints = @(
    @{Name="Simple Person UPDLOCK"; Path="/api/users/active-person-updlock-query"},
    @{Name="Complex Person UPDLOCK (Active)"; Path="/api/users/active-complex-person-updlock-query"},
    @{Name="Complex Person UPDLOCK (Slow)"; Path="/api/users/slow-complex-person-updlock-query"}
)

foreach ($endpoint in $endpoints) {
    Write-Host "   Testing: $($endpoint.Name)"
    Write-Host "   URL: $baseUrl$($endpoint.Path)"

    try {
        # Start in background job to avoid waiting
        $job = Start-Job -ScriptBlock {
            param($url)
            try {
                Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 180
                return "SUCCESS"
            } catch {
                return "ERROR: $_"
            }
        } -ArgumentList "$baseUrl$($endpoint.Path)"

        # Wait 2 seconds to see if it starts
        Start-Sleep -Seconds 2

        $jobState = (Get-Job -Id $job.Id).State
        if ($jobState -eq "Running") {
            Write-Host "   ✅ Endpoint is accessible and query started" -ForegroundColor Green
            # Kill the job since we don't need to wait
            Stop-Job -Id $job.Id
            Remove-Job -Id $job.Id -Force
        } elseif ($jobState -eq "Completed") {
            $result = Receive-Job -Id $job.Id
            Write-Host "   ✅ Endpoint completed: $result" -ForegroundColor Green
            Remove-Job -Id $job.Id
        } else {
            $result = Receive-Job -Id $job.Id
            Write-Host "   ❌ Endpoint failed: $result" -ForegroundColor Red
            Remove-Job -Id $job.Id
        }
    } catch {
        Write-Host "   ❌ Error: $_" -ForegroundColor Red
    }
    Write-Host ""
}

Write-Host "=========================================="
Write-Host "SQL Query Patterns Used"
Write-Host "=========================================="
Write-Host ""
Write-Host "These endpoints execute queries on these tables:"
Write-Host ""
Write-Host "✅ 1. Person.Person (with UPDLOCK)"
Write-Host "✅ 2. Person.EmailAddress (subquery)"
Write-Host "✅ 3. Person.PersonPhone (subquery)"
Write-Host "✅ 4. Person.BusinessEntityAddress (JOIN)"
Write-Host "✅ 5. Person.Address (JOIN)"
Write-Host "✅ 6. Person.StateProvince (JOIN)"
Write-Host "✅ 7. Sales.Customer (subquery)"
Write-Host "✅ 8. HumanResources.Employee (subquery)"
Write-Host "✅ 9. Sales.SalesOrderHeader (subquery aggregation)"
Write-Host ""
Write-Host "All queries include WITH(UPDLOCK) on Person.Person table"
Write-Host ""
Write-Host "=========================================="
Write-Host "Verify in SQL Server"
Write-Host "=========================================="
Write-Host ""
Write-Host "Run this query to see if any Person UPDLOCK queries are active:"
Write-Host ""
Write-Host "SELECT"
Write-Host "    session_id,"
Write-Host "    start_time,"
Write-Host "    total_elapsed_time / 1000 AS elapsed_sec,"
Write-Host "    wait_type,"
Write-Host "    SUBSTRING(qt.text, 1, 200) AS query_text"
Write-Host "FROM sys.dm_exec_requests er"
Write-Host "CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
Write-Host "WHERE qt.text LIKE '%Person.Person%UPDLOCK%';"
Write-Host ""
Write-Host "=========================================="
Write-Host "Next Steps"
Write-Host "=========================================="
Write-Host ""
Write-Host "If all endpoints are working, run the full test:"
Write-Host ""
Write-Host "  .\test-50-active-queries-person-updlock.ps1"
Write-Host ""
Write-Host "Or batch version:"
Write-Host ""
Write-Host "  test-50-active-queries-person-updlock.bat"
Write-Host ""
Write-Host "=========================================="
