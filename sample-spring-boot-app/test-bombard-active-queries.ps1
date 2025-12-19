# PowerShell Bombard Script - Most Reliable for Windows
# This script continuously fires queries for 5 minutes

Write-Host "======================================================================"
Write-Host "💣 BOMBARD TEST - Continuous Active Queries for 5 Minutes"
Write-Host "======================================================================"
Write-Host ""
Write-Host "This script will continuously start new queries for 5 minutes."
Write-Host "Each query runs for 90 seconds, creating overlapping active queries."
Write-Host ""
Write-Host "Strategy:"
Write-Host "  - Fire a new query every 10 seconds"
Write-Host "  - Each query runs for 90 seconds"
Write-Host "  - Result: ~9 queries running concurrently at any time"
Write-Host "  - Duration: 5 minutes (300 seconds)"
Write-Host ""
Write-Host "With 60-second collector scrapes, you'll get 5 scrapes during test."
Write-Host "======================================================================"
Write-Host ""

$baseUrl = "http://localhost:8080/api/users"

Write-Host "📋 SELECT QUERY PATTERN TO BOMBARD:"
Write-Host ""
Write-Host "1. Active Sales Query (90s)        - Complex JOIN with WAITFOR"
Write-Host "2. Active Aggregation (120s)       - Time-series aggregation"
Write-Host "3. Active Blocking (90s)           - WITH (UPDLOCK, HOLDLOCK)"
Write-Host "4. Active CPU (90s)                - CPU-intensive computation"
Write-Host "5. Active I/O (90s)                - I/O-intensive query"
Write-Host "6. Person UPDLOCK (90s)            - SELECT * FROM Person WITH(UPDLOCK) ⭐ NEW"
Write-Host "7. ALL PATTERNS (mixed)            - Rotate through all patterns"
Write-Host ""

$choice = Read-Host "Enter choice (1-7)"

switch ($choice) {
    "1" {
        $endpoint = "active-sales-query"
        $pattern = "Active Sales Query"
    }
    "2" {
        $endpoint = "active-aggregation-query"
        $pattern = "Active Aggregation Query"
    }
    "3" {
        $endpoint = "active-blocking-query"
        $pattern = "Active Blocking Query"
    }
    "4" {
        $endpoint = "active-cpu-query"
        $pattern = "Active CPU Query"
    }
    "5" {
        $endpoint = "active-io-query"
        $pattern = "Active I/O Query"
    }
    "6" {
        $endpoint = "active-person-updlock-query"
        $pattern = "Person UPDLOCK Query"
    }
    "7" {
        $endpoint = "ALL"
        $pattern = "Mixed Pattern Rotation"
    }
    default {
        Write-Host "Invalid choice. Defaulting to Active Sales Query."
        $endpoint = "active-sales-query"
        $pattern = "Active Sales Query"
    }
}

Write-Host ""
Write-Host "======================================================================"
Write-Host "🎯 BOMBARD CONFIGURATION"
Write-Host "======================================================================"
Write-Host ""
Write-Host "Pattern: $pattern"
Write-Host "Endpoint: $endpoint"
Write-Host "Duration: 5 minutes (300 seconds)"
Write-Host "Query Interval: Every 10 seconds"
Write-Host "Expected Concurrent Queries: 8-9"
Write-Host "Total Queries to Fire: 30"
Write-Host ""
Write-Host "Press ENTER to start bombardment..."
Read-Host

Write-Host ""
Write-Host "======================================================================"
Write-Host "💣 STARTING BOMBARDMENT"
Write-Host "======================================================================"
Write-Host ""
$startTime = Get-Date
Write-Host "Start Time: $($startTime.ToString('HH:mm:ss'))"
Write-Host ""

# Define pattern endpoints for rotation
$patterns = @(
    "active-sales-query",
    "active-aggregation-query",
    "active-blocking-query",
    "active-cpu-query",
    "active-io-query",
    "active-person-updlock-query"
)

# Fire 30 queries over 5 minutes
for ($i = 1; $i -le 30; $i++) {
    $currentTime = Get-Date
    $elapsed = ($currentTime - $startTime).TotalSeconds
    $remaining = 300 - $elapsed

    # Select endpoint
    if ($endpoint -eq "ALL") {
        $currentEndpoint = $patterns[($i - 1) % 6]
    } else {
        $currentEndpoint = $endpoint
    }

    # Start query in background job
    Write-Host "[$($currentTime.ToString('HH:mm:ss'))] 🚀 Starting Query #$i - Endpoint: $currentEndpoint"

    Start-Job -ScriptBlock {
        param($url)
        try {
            Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 180
        } catch {
            # Silently ignore errors (expected for long-running queries)
        }
    } -ArgumentList "$baseUrl/$currentEndpoint" | Out-Null

    # Show progress
    Write-Host "    ⏱️  Elapsed: $([math]::Round($elapsed))s / 300s | Remaining: $([math]::Round($remaining))s | Total Fired: $i"
    Write-Host ""

    # Wait 10 seconds before next query (except for last)
    if ($i -lt 30) {
        Start-Sleep -Seconds 10
    }
}

$endTime = Get-Date
$totalDuration = ($endTime - $startTime).TotalSeconds

Write-Host ""
Write-Host "======================================================================"
Write-Host "✅ BOMBARDMENT COMPLETE"
Write-Host "======================================================================"
Write-Host ""
Write-Host "Total Queries Fired: 30"
Write-Host "Duration: $([math]::Round($totalDuration)) seconds"
Write-Host "End Time: $($endTime.ToString('HH:mm:ss'))"
Write-Host ""
Write-Host "======================================================================"
Write-Host "📊 MONITORING COMMANDS"
Write-Host "======================================================================"
Write-Host ""
Write-Host "1. Check active query count in SQL Server:"
Write-Host ""
Write-Host "SELECT COUNT(*) AS active_count"
Write-Host "FROM sys.dm_exec_requests"
Write-Host "WHERE session_id > 50;"
Write-Host ""
Write-Host "Expected: 6-9 active queries"
Write-Host ""
Write-Host "2. View all active queries:"
Write-Host ""
Write-Host "SELECT"
Write-Host "    session_id,"
Write-Host "    start_time,"
Write-Host "    status,"
Write-Host "    wait_type,"
Write-Host "    wait_time,"
Write-Host "    total_elapsed_time / 1000 AS elapsed_sec,"
Write-Host "    SUBSTRING(qt.text, 1, 150) AS query_text"
Write-Host "FROM sys.dm_exec_requests er"
Write-Host "CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt"
Write-Host "WHERE session_id > 50"
Write-Host "ORDER BY total_elapsed_time DESC;"
Write-Host ""
Write-Host "======================================================================"
Write-Host "🔍 NEW RELIC VERIFICATION"
Write-Host "======================================================================"
Write-Host ""
Write-Host "Check active queries captured by collector:"
Write-Host ""
Write-Host "SELECT"
Write-Host "    latest(sqlserver.activequery.elapsed_time_ms) / 1000 AS elapsed_sec,"
Write-Host "    latest(query_text),"
Write-Host "    latest(wait_type),"
Write-Host "    count(*) AS capture_count"
Write-Host "FROM Metric"
Write-Host "WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'"
Write-Host "  AND elapsed_time_ms > 5000"
Write-Host "FACET query_id"
Write-Host "SINCE 10 minutes ago"
Write-Host ""
Write-Host "Expected Results:"
Write-Host "  - 1-5 unique query_id values (depending on pattern choice)"
Write-Host "  - capture_count: 40-45 (5 scrapes × 8-9 queries)"
Write-Host "  - elapsed_sec ranging from 5 to 90+ seconds"
Write-Host ""
Write-Host "======================================================================"
Write-Host "⏳ WAIT TIME"
Write-Host "======================================================================"
Write-Host ""
Write-Host "Queries will continue running for up to 120 more seconds."
Write-Host "Monitor SQL Server or New Relic to see them complete."
Write-Host ""
Write-Host "Background jobs are still running. To view them:"
Write-Host "  Get-Job"
Write-Host ""
Write-Host "To stop all background jobs:"
Write-Host "  Get-Job | Stop-Job"
Write-Host "  Get-Job | Remove-Job"
Write-Host ""
Write-Host "======================================================================"
Write-Host ""
Write-Host "Press ENTER to exit (queries will continue in background)..."
Read-Host

# Optional: Clean up completed jobs
Get-Job | Where-Object { $_.State -eq "Completed" } | Remove-Job
