@echo off
SET BASE_URL=http://localhost:8080

echo ==========================================
echo 50 Active Running Queries Test
echo ==========================================
echo.
echo This script will:
echo   1. Run 1 slow sales query with 1-minute delay
echo   2. Run 50 active sales queries with 5-second delay (concurrently)
echo.
echo This simulates:
echo   - 1 slow query captured in query stats
echo   - 50 active running queries visible in sys.dm_exec_requests
echo.
echo ==========================================
echo.

pause

echo.
echo [1/51] Starting 1-minute slow sales query at %date% %time%...
start /B curl -s "%BASE_URL%/api/users/slow-sales-query" > %TEMP%\slow-sales-1min.log 2>&1
echo Slow query (1 min) started
echo.

REM Wait 2 seconds for slow query to start
timeout /t 2 /nobreak > nul

echo Starting 50 active queries (5-second delay each)...
echo.

REM Start 50 active queries
FOR /L %%i IN (1,1,50) DO (
    start /B curl -s "%BASE_URL%/api/users/active-sales-query" > %TEMP%\active-sales-%%i.log 2>&1
    echo [%%i/50] Active query %%i started
    timeout /t 0 /nobreak > nul
)

echo.
echo ==========================================
echo All queries started!
echo ==========================================
echo.
echo Status:
echo   - 1 slow query (1 minute) running
echo   - 50 active queries (5 seconds each) running
echo   - Total: 51 concurrent queries
echo.
echo While queries are running, check:
echo.
echo 1. SQL Server - Count active queries:
echo    SELECT COUNT(*) AS active_count
echo    FROM sys.dm_exec_requests
echo    WHERE text LIKE '%%WAITFOR DELAY%%';
echo.
echo 2. SQL Server - View all active queries:
echo    SELECT session_id, start_time, wait_type, wait_time,
echo           total_elapsed_time / 1000 AS elapsed_seconds,
echo           SUBSTRING(qt.text, 1, 100) AS query_text
echo    FROM sys.dm_exec_requests er
echo    CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo    WHERE qt.text LIKE '%%WAITFOR DELAY%%';
echo.
echo 3. OTEL Collector - Should capture all active running queries
echo.
echo Logs saved to %TEMP%\slow-sales-1min.log and %TEMP%\active-sales-*.log
echo.
echo ==========================================
echo Waiting for queries to complete...
echo ==========================================
echo.
echo The 50 active queries will complete in ~5 seconds
echo The 1 slow query will complete in ~60 seconds
echo.

REM Wait for queries to complete
timeout /t 65 /nobreak

echo.
echo ==========================================
echo Test Complete!
echo ==========================================
echo All queries should have finished.
echo.
echo To verify completion, check:
echo   - Logs in %TEMP%\slow-sales-*.log and %TEMP%\active-sales-*.log
echo   - SQL Server for any remaining active queries
echo.
pause
