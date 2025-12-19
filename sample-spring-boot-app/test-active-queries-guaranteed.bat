@echo off
REM Test Active Queries - Guaranteed Capture Script (Windows)
REM This script runs queries that will DEFINITELY be captured as active queries

echo ======================================================================
echo 🎯 Active Query Test - GUARANTEED CAPTURE
echo ======================================================================
echo.
echo This script will execute long-running queries that will be visible
echo in sys.dm_exec_requests when the OTEL collector scrapes (every 60s).
echo.
echo Query Patterns:
echo   1. Active Sales Query      - 90 seconds  (1.5x scrape interval)
echo   2. Active Aggregation      - 120 seconds (2x scrape interval)
echo   3. Active Blocking Query   - 90 seconds  (with LCK_M_X waits)
echo   4. Active CPU Query        - Variable    (CPU-intensive)
echo   5. Active I/O Query        - Variable    (I/O-intensive)
echo.
echo Each query runs long enough to guarantee capture!
echo ======================================================================
echo.

set BASE_URL=http://localhost:8080/api/users

echo 📋 INSTRUCTIONS:
echo 1. Make sure the Spring Boot application is running
echo 2. Make sure the OTEL Collector is running with 60-second scrape interval
echo 3. This script will start all active queries in parallel
echo 4. Queries will run for 90-120 seconds
echo 5. Monitor SQL Server DMVs or New Relic for results
echo.
echo Press any key to start the test...
pause > nul

echo ======================================================================
echo 🏁 STARTING ACTIVE QUERY TEST
echo ======================================================================
echo.

REM Pattern 1: Active Sales Query (90 seconds) - 10 concurrent
echo 🚀 Starting: Active Sales Query (90s)
echo    Endpoint: active-sales-query
echo    Count: 10 concurrent queries
echo.

FOR /L %%i IN (1,1,10) DO (
    echo    [%%i/10] Starting query...
    start /B curl -s %BASE_URL%/active-sales-query > nul 2>&1
    timeout /t 1 /nobreak > nul
)

echo ✅ All 10 queries started for: Active Sales Query
echo.

REM Pattern 2: Active Aggregation Query (120 seconds) - 5 concurrent
echo 🚀 Starting: Active Aggregation Query (120s)
echo    Endpoint: active-aggregation-query
echo    Count: 5 concurrent queries
echo.

FOR /L %%i IN (1,1,5) DO (
    echo    [%%i/5] Starting query...
    start /B curl -s %BASE_URL%/active-aggregation-query > nul 2>&1
    timeout /t 1 /nobreak > nul
)

echo ✅ All 5 queries started for: Active Aggregation Query
echo.

REM Pattern 3: Active Blocking Query (90 seconds) - 3 concurrent
echo 🚀 Starting: Active Blocking Query (90s with locks)
echo    Endpoint: active-blocking-query
echo    Count: 3 concurrent queries
echo    WARNING: These will create blocking scenarios
echo.

FOR /L %%i IN (1,1,3) DO (
    echo    [%%i/3] Starting query...
    start /B curl -s %BASE_URL%/active-blocking-query > nul 2>&1
    timeout /t 1 /nobreak > nul
)

echo ✅ All 3 queries started for: Active Blocking Query
echo.

REM Pattern 4: Active CPU Query - 2 concurrent
echo 🚀 Starting: Active CPU-Intensive Query
echo    Endpoint: active-cpu-query
echo    Count: 2 concurrent queries
echo.

FOR /L %%i IN (1,1,2) DO (
    echo    [%%i/2] Starting query...
    start /B curl -s %BASE_URL%/active-cpu-query > nul 2>&1
    timeout /t 1 /nobreak > nul
)

echo ✅ All 2 queries started for: Active CPU Query
echo.

REM Pattern 5: Active I/O Query - 5 concurrent
echo 🚀 Starting: Active I/O-Intensive Query
echo    Endpoint: active-io-query
echo    Count: 5 concurrent queries
echo.

FOR /L %%i IN (1,1,5) DO (
    echo    [%%i/5] Starting query...
    start /B curl -s %BASE_URL%/active-io-query > nul 2>&1
    timeout /t 1 /nobreak > nul
)

echo ✅ All 5 queries started for: Active I/O Query
echo.

echo ======================================================================
echo ✅ ALL QUERIES STARTED SUCCESSFULLY
echo ======================================================================
echo.
echo Total queries running: 25
echo   - Active Sales: 10
echo   - Active Aggregation: 5
echo   - Active Blocking: 3
echo   - Active CPU: 2
echo   - Active I/O: 5
echo.
echo ======================================================================
echo ⏰ TIMING INFORMATION
echo ======================================================================
echo.
echo Start Time: %TIME%
echo.
echo Expected completion timeline:
echo   - 90 seconds:  Active Sales, Blocking queries complete
echo   - 120 seconds: Active Aggregation queries complete
echo   - Variable:    CPU and I/O queries (check progress)
echo.
echo ======================================================================
echo 📊 MONITORING QUERIES
echo ======================================================================
echo.
echo Run these SQL queries to monitor progress:
echo.
echo 1. Count active queries:
echo    SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id ^> 50;
echo.
echo 2. View active queries with details:
echo    SELECT session_id, start_time, status, command, wait_type, wait_time,
echo           total_elapsed_time / 1000 AS elapsed_seconds,
echo           SUBSTRING(qt.text, 1, 100) AS query_text
echo    FROM sys.dm_exec_requests er
echo    CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo    WHERE session_id ^> 50
echo    ORDER BY total_elapsed_time DESC;
echo.
echo 3. Find blocking queries:
echo    SELECT blocking_session_id, session_id, wait_type, wait_time, wait_resource
echo    FROM sys.dm_exec_requests
echo    WHERE blocking_session_id ^> 0;
echo.
echo ======================================================================
echo 🔍 NEW RELIC VERIFICATION
echo ======================================================================
echo.
echo After queries run for 60+ seconds, check New Relic with:
echo.
echo SELECT latest(sqlserver.activequery.wait_time_seconds)
echo FROM Metric
echo WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
echo FACET query_id, session_id, wait_type
echo SINCE 5 minutes ago
echo.
echo Look for queries with:
echo   - query_text containing 'APM_ACTIVE_'
echo   - elapsed_time ^> 5000ms
echo   - Various wait_types (WAITFOR, LCK_M_X, etc.)
echo.
echo ======================================================================
echo ⏳ Queries are running in background...
echo ======================================================================
echo.
echo Queries will complete in approximately 120 seconds.
echo You can close this window. Queries will continue in background.
echo.
echo Press any key to exit...
pause > nul
