@echo off
REM Simplified Bombard Active Queries - Windows Version

echo ======================================================================
echo BOMBARD TEST - Continuous Active Queries for 5 Minutes
echo ======================================================================
echo.
echo This script will continuously start new queries for 5 minutes.
echo Each query runs for 90 seconds, creating overlapping active queries.
echo.
echo Strategy:
echo   - Fire a new query every 10 seconds
echo   - Each query runs for 90 seconds
echo   - Result: ~9 queries running concurrently at any time
echo   - Duration: 5 minutes (300 seconds = 30 queries)
echo.
echo With 60-second collector scrapes, you'll get 5 scrapes during test.
echo Each scrape will see 8-9 active queries!
echo ======================================================================
echo.

set BASE_URL=http://localhost:8080/api/users

echo SELECT QUERY PATTERN TO BOMBARD:
echo.
echo 1. Active Sales Query (90s)        - Complex JOIN with WAITFOR
echo 2. Active Aggregation (120s)       - Time-series aggregation
echo 3. Active Blocking (90s)           - WITH (UPDLOCK, HOLDLOCK)
echo 4. Active CPU (variable)           - CPU-intensive computation
echo 5. Active I/O (variable)           - I/O-intensive cross join
echo.
set /p CHOICE="Enter choice (1-5): "

if "%CHOICE%"=="1" (
    set ENDPOINT=active-sales-query
    set PATTERN=Active Sales Query
) else if "%CHOICE%"=="2" (
    set ENDPOINT=active-aggregation-query
    set PATTERN=Active Aggregation Query
) else if "%CHOICE%"=="3" (
    set ENDPOINT=active-blocking-query
    set PATTERN=Active Blocking Query
) else if "%CHOICE%"=="4" (
    set ENDPOINT=active-cpu-query
    set PATTERN=Active CPU Query
) else if "%CHOICE%"=="5" (
    set ENDPOINT=active-io-query
    set PATTERN=Active I/O Query
) else (
    echo Invalid choice. Defaulting to Active Sales Query.
    set ENDPOINT=active-sales-query
    set PATTERN=Active Sales Query
)

echo.
echo ======================================================================
echo BOMBARD CONFIGURATION
echo ======================================================================
echo.
echo Pattern: %PATTERN%
echo Endpoint: %ENDPOINT%
echo Duration: 5 minutes (300 seconds)
echo Query Interval: Every 10 seconds
echo Expected Concurrent Queries: 8-9
echo Total Queries to Fire: 30
echo.
echo Press any key to start bombardment...
pause > nul

echo.
echo ======================================================================
echo STARTING BOMBARDMENT
echo ======================================================================
echo.
echo Start Time: %TIME%
echo.

REM Fire 30 queries (one every 10 seconds = 300 seconds total)
FOR /L %%i IN (1,1,30) DO (
    echo [%TIME%] Starting Query #%%i - Endpoint: %ENDPOINT%
    start /B curl -s %BASE_URL%/%ENDPOINT% > nul 2>&1

    REM Show progress
    echo     Progress: %%i/30 queries fired

    REM Wait 10 seconds before next query (except for the last one)
    if %%i LSS 30 (
        echo     Waiting 10 seconds...
        timeout /t 10 /nobreak > nul
    )
)

echo.
echo ======================================================================
echo BOMBARDMENT COMPLETE
echo ======================================================================
echo.
echo Total Queries Fired: 30
echo End Time: %TIME%
echo.
echo ======================================================================
echo MONITORING COMMANDS
echo ======================================================================
echo.
echo 1. Check active query count in SQL Server:
echo.
echo SELECT COUNT(*) AS active_count
echo FROM sys.dm_exec_requests
echo WHERE session_id ^> 50;
echo.
echo Expected: 6-9 active queries
echo.
echo ======================================================================
echo.
echo 2. View all active queries:
echo.
echo SELECT session_id, start_time, status, wait_type,
echo        total_elapsed_time / 1000 AS elapsed_sec,
echo        SUBSTRING(qt.text, 1, 100) AS query_text
echo FROM sys.dm_exec_requests er
echo CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo WHERE session_id ^> 50
echo ORDER BY total_elapsed_time DESC;
echo.
echo ======================================================================
echo.
echo 3. Check New Relic after test (wait 2 minutes):
echo.
echo SELECT count(*) AS total_captures,
echo        latest(query_text)
echo FROM Metric
echo WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
echo   AND elapsed_time_ms ^> 5000
echo FACET query_id
echo SINCE 10 minutes ago
echo.
echo Expected: 40-45 captures
echo.
echo ======================================================================
echo TIMELINE
echo ======================================================================
echo.
echo Collector scrapes every 60 seconds:
echo   - Scrape 1 at ~60s:  Should see ~6 queries
echo   - Scrape 2 at ~120s: Should see ~9 queries
echo   - Scrape 3 at ~180s: Should see ~9 queries
echo   - Scrape 4 at ~240s: Should see ~9 queries
echo   - Scrape 5 at ~300s: Should see ~9 queries
echo.
echo Queries will continue running for up to 90 more seconds.
echo Monitor SQL Server or New Relic to see them complete.
echo.
echo ======================================================================
echo.
echo Press any key to exit (queries will continue in background)...
pause > nul
