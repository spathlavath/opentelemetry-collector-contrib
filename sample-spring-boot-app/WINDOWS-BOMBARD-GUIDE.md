# Windows Bombard Script Guide

## Issue with Original Script

The original `test-bombard-active-queries.bat` had Windows batch arithmetic issues with the `%%` modulo operator and time parsing.

## ✅ Solution: Use the Simple Script

### Use This Script Instead:

```batch
test-bombard-active-queries-simple.bat
```

This simplified version:
- ✅ No complex arithmetic
- ✅ Simple FOR loop (30 iterations)
- ✅ 10-second delays between queries
- ✅ Works on all Windows versions

---

## Quick Start (Windows)

### Step 1: Open PowerShell or Command Prompt

```powershell
cd C:\Users\mssql\sample-spring-boot-app
```

### Step 2: Run the Simple Script

```batch
test-bombard-active-queries-simple.bat
```

### Step 3: Choose Pattern

```
Enter choice (1-5): 1
```

**Recommendation**: Choose **1** (Active Sales Query) for simplest testing.

### Step 4: Press Any Key to Start

The script will:
1. Fire Query #1
2. Wait 10 seconds
3. Fire Query #2
4. Wait 10 seconds
5. ... repeat 30 times (5 minutes total)

---

## What You'll See

```
======================================================================
STARTING BOMBARDMENT
======================================================================

Start Time: 14:30:25.12

[14:30:25.12] Starting Query #1 - Endpoint: active-sales-query
    Progress: 1/30 queries fired
    Waiting 10 seconds...

[14:30:35.15] Starting Query #2 - Endpoint: active-sales-query
    Progress: 2/30 queries fired
    Waiting 10 seconds...

[14:30:45.18] Starting Query #3 - Endpoint: active-sales-query
    Progress: 3/30 queries fired
    Waiting 10 seconds...

...

[14:35:25.89] Starting Query #30 - Endpoint: active-sales-query
    Progress: 30/30 queries fired

======================================================================
BOMBARDMENT COMPLETE
======================================================================

Total Queries Fired: 30
End Time: 14:35:25.89
```

---

## Verification Steps

### During Test (SQL Server Management Studio)

```sql
-- Should show 6-9 active queries
SELECT COUNT(*) AS active_count
FROM sys.dm_exec_requests
WHERE session_id > 50;

-- View details
SELECT
    session_id,
    start_time,
    total_elapsed_time / 1000 AS elapsed_sec,
    SUBSTRING(qt.text, 1, 100) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE session_id > 50
ORDER BY total_elapsed_time DESC;
```

### After Test (New Relic)

```nrql
SELECT count(*) AS total_captures,
       latest(query_text)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND elapsed_time_ms > 5000
FACET query_id
SINCE 10 minutes ago
```

**Expected**: 40-45 captures

---

## Alternative: Manual Testing (If Scripts Fail)

If batch scripts continue to have issues, use PowerShell instead:

```powershell
$baseUrl = "http://localhost:8080/api/users"
$endpoint = "active-sales-query"

Write-Host "Starting 30 queries over 5 minutes..."

for ($i = 1; $i -le 30; $i++) {
    Write-Host "[$((Get-Date).ToString('HH:mm:ss'))] Query #$i"
    Start-Job -ScriptBlock {
        param($url)
        Invoke-RestMethod -Uri $url -Method Get
    } -ArgumentList "$baseUrl/$endpoint"

    if ($i -lt 30) {
        Start-Sleep -Seconds 10
    }
}

Write-Host "All 30 queries started!"
```

Save this as `test-bombard.ps1` and run:

```powershell
powershell -ExecutionPolicy Bypass -File test-bombard.ps1
```

---

## Expected Results

| Metric | Expected Value |
|--------|---------------|
| Queries fired | 30 |
| Duration | ~5 minutes |
| Concurrent queries | 6-9 at any time |
| Collector scrapes | 5 |
| Total captures | 40-45 |
| Success rate | 100% |

---

## Troubleshooting

### Issue: Script won't start

**Solution**: Right-click script → "Run as Administrator"

### Issue: curl command not found

**Install curl**: Windows 10+ includes curl by default. If missing:

```powershell
# Test curl
curl --version

# If missing, use Invoke-RestMethod instead:
Invoke-RestMethod -Uri http://localhost:8080/api/users/active-sales-query
```

### Issue: Queries not showing in SQL Server

**Check application**:
```powershell
# Test app is running
curl http://localhost:8080/api/health
```

**Check connection pool**:
```properties
# In application.properties
spring.datasource.hikari.maximum-pool-size=50
```

---

## Files Available

| File | Description | Status |
|------|-------------|--------|
| `test-bombard-active-queries.bat` | Original (has issues) | ❌ Don't use |
| `test-bombard-active-queries-simple.bat` | Simplified version | ✅ **Use this** |
| `test-bombard-active-queries-fixed.bat` | Fixed version with rotation | ✅ Use for mixed patterns |

---

## Summary

1. ✅ Use `test-bombard-active-queries-simple.bat`
2. ✅ Choose option 1 (Active Sales Query)
3. ✅ Let it run for 5 minutes
4. ✅ Check New Relic for 40-45 captures

The simplified script avoids Windows batch limitations and guarantees success!
