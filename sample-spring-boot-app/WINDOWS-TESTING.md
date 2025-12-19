# Windows Testing Guide

## Quick Start on Windows

### 1. Start the Application

```batch
start-with-apm.bat
```

Wait for the application to start (you'll see "Started DemoApplication" in the console).

---

## 2. Test Slow Queries

You have several options:

### Option A: Interactive Menu (Recommended)

```batch
test-slow-query.bat
```

This will show you a menu:
```
1) All queries (parallel)
2) Simple users query only
3) Sales query only
4) Product query only
```

### Option B: Run All Queries at Once

```batch
test-all-slow-queries.bat
```

This automatically starts all 3 slow queries in parallel.

### Option C: Individual curl Commands

```batch
REM Simple users query
curl http://localhost:8080/api/users/slow-query

REM Complex sales query
curl http://localhost:8080/api/users/slow-sales-query

REM Complex product query
curl http://localhost:8080/api/users/slow-product-query
```

### Option D: Run in Background

```batch
REM Start all queries in background
start /B curl http://localhost:8080/api/users/slow-query > %TEMP%\query1.log 2>&1
start /B curl http://localhost:8080/api/users/slow-sales-query > %TEMP%\query2.log 2>&1
start /B curl http://localhost:8080/api/users/slow-product-query > %TEMP%\query3.log 2>&1

echo Queries running! Check logs at %TEMP%\query*.log
```

---

## 3. Monitor Active Queries

### Check SQL Server for Active Queries

Open SQL Server Management Studio (SSMS) or use sqlcmd:

```sql
-- View active slow queries
SELECT
    er.session_id,
    er.start_time,
    er.status,
    er.command,
    er.wait_type,
    er.wait_time,
    er.total_elapsed_time / 1000 AS elapsed_seconds,
    SUBSTRING(qt.text, 1, 500) AS query_text
FROM sys.dm_exec_requests er
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
WHERE qt.text LIKE '%WAITFOR DELAY%'
AND er.session_id > 50
ORDER BY er.start_time;
```

### Check Running Processes

```batch
REM Find curl processes
tasklist | findstr curl

REM Check which queries are active
netstat -ano | findstr 8080
```

---

## 4. View Logs

Logs are saved to your temp directory:

```batch
REM Open temp folder
explorer %TEMP%

REM View specific log
type %TEMP%\slow-query-users.log
type %TEMP%\slow-query-sales.log
type %TEMP%\slow-query-product.log
```

---

## 5. Stop Running Queries

### Stop from Windows

```batch
REM Kill all curl processes
taskkill /F /IM curl.exe

REM Or kill specific process by PID
taskkill /F /PID <process_id>
```

### Stop from SQL Server

```sql
-- Find session ID
SELECT session_id
FROM sys.dm_exec_requests
WHERE text LIKE '%WAITFOR DELAY%';

-- Kill the session (replace 123 with actual session_id)
KILL 123;
```

---

## Available Scripts

| Script | Purpose |
|--------|---------|
| `start-with-apm.bat` | Start the Spring Boot app with New Relic APM |
| `test-slow-query.bat` | Interactive menu to select which queries to run |
| `test-all-slow-queries.bat` | Run all 3 queries in parallel (quick test) |
| `test-api.bat` | Full API test suite (includes slow query) |
| `set-env-windows.bat` | Set environment variables interactively |
| `set-env-example.bat` | Example environment variable template |

---

## Expected Behavior

### During Query Execution

1. **Application Console:** Shows Hibernate SQL execution logs
2. **SQL Server:** Query appears in `sys.dm_exec_requests`
3. **OTEL Collector:** Should capture active running query
4. **New Relic APM:** Transaction duration increases to ~300 seconds

### Each Query Takes

- **Duration:** Exactly 5 minutes (300 seconds)
- **Status in SQL:** SUSPENDED (waiting)
- **Wait Type:** WAITFOR

---

## Troubleshooting

### Application Won't Start

```batch
REM Check if port 8080 is already in use
netstat -ano | findstr 8080

REM Kill process using port 8080
FOR /F "tokens=5" %P IN ('netstat -ano ^| findstr :8080') DO taskkill /F /PID %P
```

### Can't Connect to SQL Server

```batch
REM Test SQL Server connection
sqlcmd -S localhost -U sa -P AbAnTaPassword@123 -d AdventureWorks2022 -Q "SELECT @@VERSION"

REM Check SQL Server is running
sc query MSSQLSERVER

REM Check TCP/IP is enabled
REM Open SQL Server Configuration Manager
REM Enable TCP/IP in Protocols for MSSQLSERVER
```

### Queries Not Appearing in OTEL Collector

1. Verify collector is running
2. Check collector config for SQL Server connection
3. Ensure scrape interval allows query to be captured
4. Check collector logs for errors

### Curl Not Found

```batch
REM Curl is included in Windows 10+ by default
REM If not available, download from: https://curl.se/windows/

REM Or use PowerShell alternative:
powershell -Command "Invoke-WebRequest -Uri http://localhost:8080/api/users/slow-query"
```

---

## Testing Checklist

- [ ] SQL Server is running
- [ ] AdventureWorks2022 database exists
- [ ] Application starts without errors
- [ ] Can access http://localhost:8080/api/health
- [ ] Slow query endpoint responds (takes 5 minutes)
- [ ] Query appears in `sys.dm_exec_requests`
- [ ] OTEL collector captures the query
- [ ] New Relic APM shows high-duration transaction

---

## Quick Commands Reference

```batch
REM Start everything
start-with-apm.bat

REM In a new terminal, test slow queries
test-all-slow-queries.bat

REM Check SQL Server for active queries
sqlcmd -S localhost -U sa -P AbAnTaPassword@123 -d AdventureWorks2022 -Q ^
  "SELECT session_id, wait_type, wait_time, SUBSTRING(qt.text, 1, 100) ^
   FROM sys.dm_exec_requests er ^
   CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt ^
   WHERE qt.text LIKE '%WAITFOR%'"

REM Stop all queries
taskkill /F /IM curl.exe
```

---

## Notes

- All queries use `WAITFOR DELAY '00:05:00'` for 5-minute delay
- Queries use real AdventureWorks2022 tables and data
- Safe to run in test environments
- Each query consumes one database connection during execution
- Multiple queries can run simultaneously
