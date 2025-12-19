@echo off
REM Bombard Active Queries - Continuous 5-Minute Test (Windows)

echo ======================================================================
echo 💣 BOMBARD TEST - Continuous Active Queries for 5 Minutes
echo ======================================================================
echo.
echo This script will continuously start new queries for 5 minutes.
echo Each query runs for 90 seconds, creating overlapping active queries.
echo.
echo Strategy:
echo   - Fire a new query every 10 seconds
echo   - Each query runs for 90 seconds
echo   - Result: ~9 queries running concurrently at any time
echo   - Duration: 5 minutes (300 seconds)
echo.
echo With 60-second collector scrapes, you'll get 5 scrapes during test.
echo Each scrape will see 8-9 active queries!
echo ======================================================================
echo.

set BASE_URL=http://localhost:8080/api/users
set DURATION=300
set INTERVAL=10
set QUERY_COUNT=0

echo 📋 SELECT QUERY PATTERN TO BOMBARD:
echo.
echo 1. Active Sales Query (90s)        - Complex JOIN with WAITFOR
echo 2. Active Aggregation (120s)       - Time-series aggregation
echo 3. Active Blocking (90s)           - WITH (UPDLOCK, HOLDLOCK)
echo 4. Active CPU (variable)           - CPU-intensive computation
echo 5. Active I/O (variable)           - I/O-intensive cross join
echo 6. ALL PATTERNS (mixed)            - Rotate through all patterns
echo.
set /p CHOICE="Enter choice (1-6): "

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
) else if "%CHOICE%"=="6" (
    set ENDPOINT=ALL
    set PATTERN=Mixed Pattern Rotation
) else (
    echo Invalid choice. Defaulting to Active Sales Query.
    set ENDPOINT=active-sales-query
    set PATTERN=Active Sales Query
)

echo.
echo ======================================================================
echo 🎯 BOMBARD CONFIGURATION
echo ======================================================================
echo.
echo Pattern: %PATTERN%
echo Endpoint: %ENDPOINT%
echo Duration: 5 minutes (300 seconds)
echo Query Interval: Every %INTERVAL% seconds
echo Expected Concurrent Queries: 8-9
echo Total Queries to Fire: ~30
echo.
echo Press any key to start bombardment...
pause > nul

echo ======================================================================
echo 💣 STARTING BOMBARDMENT
echo ======================================================================
echo.

REM Get start time
set START_TIME=%TIME%
for /f "tokens=1-4 delims=:.," %%a in ("%START_TIME%") do (
    set /a START_SEC=(((%%a*60)+1%%b %% 100)*60+1%%c %% 100)
)

REM Main bombardment loop
set ELAPSED=0

:LOOP
if %ELAPSED% GEQ %DURATION% goto END_LOOP

REM Calculate current endpoint for mixed pattern
set /a PATTERN_INDEX=%QUERY_COUNT% %% 5
if "%ENDPOINT%"=="ALL" (
    if %PATTERN_INDEX%==0 set CURRENT_ENDPOINT=active-sales-query
    if %PATTERN_INDEX%==1 set CURRENT_ENDPOINT=active-aggregation-query
    if %PATTERN_INDEX%==2 set CURRENT_ENDPOINT=active-blocking-query
    if %PATTERN_INDEX%==3 set CURRENT_ENDPOINT=active-cpu-query
    if %PATTERN_INDEX%==4 set CURRENT_ENDPOINT=active-io-query
) else (
    set CURRENT_ENDPOINT=%ENDPOINT%
)

REM Start new query
set /a QUERY_COUNT+=1
echo [%TIME%] 🚀 Starting Query #%QUERY_COUNT% - Endpoint: %CURRENT_ENDPOINT%
start /B curl -s %BASE_URL%/%CURRENT_ENDPOINT% > nul 2>&1

REM Calculate elapsed and remaining time
for /f "tokens=1-4 delims=:.," %%a in ("%TIME%") do (
    set /a CURRENT_SEC=(((%%a*60)+1%%b %% 100)*60+1%%c %% 100)
)
set /a ELAPSED=%CURRENT_SEC%-%START_SEC%
if %ELAPSED% LSS 0 set /a ELAPSED+=86400
set /a REMAINING=%DURATION%-%ELAPSED%

echo     ⏱️  Elapsed: %ELAPSED%s / %DURATION%s ^| Remaining: %REMAINING%s ^| Total Fired: %QUERY_COUNT%

REM Wait before next query
timeout /t %INTERVAL% /nobreak > nul

goto LOOP

:END_LOOP

echo.
echo ======================================================================
echo ✅ BOMBARDMENT COMPLETE
echo ======================================================================
echo.
echo Total Queries Fired: %QUERY_COUNT%
echo End Time: %TIME%
echo.
echo ======================================================================
echo 📊 MONITORING COMMANDS
echo ======================================================================
echo.
echo 1. Check active query count in SQL Server:
echo.
echo SELECT COUNT(*) AS active_count
echo FROM sys.dm_exec_requests
echo WHERE session_id ^> 50;
echo.
echo 2. View all active queries:
echo.
echo SELECT session_id, start_time, status, wait_type, wait_time,
echo        total_elapsed_time / 1000 AS elapsed_sec, blocking_session_id,
echo        SUBSTRING(qt.text, 1, 150) AS query_text
echo FROM sys.dm_exec_requests er
echo CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo WHERE session_id ^> 50
echo ORDER BY total_elapsed_time DESC;
echo.
echo 3. Group by query pattern:
echo.
echo SELECT
echo     CASE
echo         WHEN qt.text LIKE '%%APM_ACTIVE_AGGREGATION%%' THEN 'Aggregation'
echo         WHEN qt.text LIKE '%%APM_BLOCKING_PATTERN%%' THEN 'Blocking'
echo         WHEN qt.text LIKE '%%APM_CPU_INTENSIVE%%' THEN 'CPU'
echo         WHEN qt.text LIKE '%%APM_IO_INTENSIVE%%' THEN 'I/O'
echo         ELSE 'Sales'
echo     END AS pattern_type,
echo     COUNT(*) AS query_count,
echo     AVG(total_elapsed_time / 1000) AS avg_elapsed_sec,
echo     MAX(total_elapsed_time / 1000) AS max_elapsed_sec
echo FROM sys.dm_exec_requests er
echo CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt
echo WHERE session_id ^> 50
echo GROUP BY CASE
echo     WHEN qt.text LIKE '%%APM_ACTIVE_AGGREGATION%%' THEN 'Aggregation'
echo     WHEN qt.text LIKE '%%APM_BLOCKING_PATTERN%%' THEN 'Blocking'
echo     WHEN qt.text LIKE '%%APM_CPU_INTENSIVE%%' THEN 'CPU'
echo     WHEN qt.text LIKE '%%APM_IO_INTENSIVE%%' THEN 'I/O'
echo     ELSE 'Sales'
echo END;
echo.
echo ======================================================================
echo 🔍 NEW RELIC VERIFICATION
echo ======================================================================
echo.
echo Check active queries captured by collector:
echo.
echo SELECT
echo     latest(sqlserver.activequery.elapsed_time_ms) / 1000 AS elapsed_sec,
echo     latest(query_text),
echo     latest(wait_type),
echo     latest(session_id),
echo     count(*) AS capture_count
echo FROM Metric
echo WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
echo   AND elapsed_time_ms ^> 5000
echo FACET query_id
echo SINCE 10 minutes ago
echo.
echo Expected Results:
echo   - 1-5 unique query_id values (depending on pattern choice)
echo   - Multiple capture_count per query_id (5+ scrapes)
echo   - elapsed_sec ranging from 5 to 90+ seconds
echo   - query_text containing 'APM_ACTIVE_' or similar
echo.
echo ======================================================================
echo ⏳ WAIT TIME
echo ======================================================================
echo.
echo Queries will continue running for up to 120 more seconds.
echo Monitor SQL Server or New Relic to see them complete.
echo.
echo Collector scrapes:
echo   - Scrape 1: ~60s  (should see ~6 queries)
echo   - Scrape 2: ~120s (should see ~9 queries)
echo   - Scrape 3: ~180s (should see ~9 queries)
echo   - Scrape 4: ~240s (should see ~9 queries)
echo   - Scrape 5: ~300s (should see ~9 queries)
echo.
echo Total unique active query captures expected: 5 scrapes × 8-9 queries = 40-45 captures!
echo.
echo ======================================================================
echo.
echo Press any key to exit (queries will continue in background)...
pause > nul
