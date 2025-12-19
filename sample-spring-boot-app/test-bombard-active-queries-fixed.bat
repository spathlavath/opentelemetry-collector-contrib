@echo off
REM Bombard Active Queries - Fixed Windows Version

echo ======================================================================
echo BOMBARD TEST - Continuous Active Queries for 5 Minutes
echo ======================================================================
echo.
echo This script will fire queries continuously for 5 minutes
echo Each query runs for 90-120 seconds, creating overlapping queries
echo.
echo With 60-second collector scrapes, you'll get 5+ scrapes during test.
echo ======================================================================
echo.

set BASE_URL=http://localhost:8080/api/users

echo SELECT QUERY PATTERN TO BOMBARD:
echo.
echo 1. Active Sales Query (90s)
echo 2. Active Aggregation (120s)
echo 3. Active Blocking (90s)
echo 4. Active CPU (variable)
echo 5. Active I/O (variable)
echo 6. ALL PATTERNS (rotates through all 5)
echo.
set /p CHOICE="Enter choice (1-6): "

if "%CHOICE%"=="6" (
    set USE_ROTATION=1
    set PATTERN=Mixed Pattern Rotation
) else (
    set USE_ROTATION=0
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
)

echo.
echo ======================================================================
echo BOMBARD CONFIGURATION
echo ======================================================================
echo.
echo Pattern: %PATTERN%
echo Duration: 5 minutes (30 queries)
echo Query Interval: Every 10 seconds
echo Expected Concurrent: 8-9 queries
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

REM Fire 30 queries over 5 minutes
FOR /L %%i IN (1,1,30) DO (
    REM Determine which endpoint to use
    if "%USE_ROTATION%"=="1" (
        REM Rotate through patterns
        set /a PATTERN_NUM=%%i %% 5
        call :SELECT_ENDPOINT !PATTERN_NUM!
    )

    echo [%TIME%] Query #%%i - Endpoint: !ENDPOINT!
    start /B curl -s %BASE_URL%/!ENDPOINT! > nul 2>&1
    echo     Progress: %%i/30 queries fired

    REM Wait 10 seconds (except for last query)
    if %%i LSS 30 (
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
echo Queries will continue running for up to 120 more seconds.
echo.
echo ======================================================================
echo VERIFICATION
echo ======================================================================
echo.
echo 1. Check SQL Server (run during test):
echo    SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id ^> 50;
echo    Expected: 6-9 queries
echo.
echo 2. Check New Relic (after test completes):
echo    SELECT count(*) FROM Metric
echo    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
echo    SINCE 10 minutes ago
echo    Expected: 40-45 captures
echo.
echo ======================================================================
echo.
echo Press any key to exit...
pause > nul
goto :eof

REM Subroutine to select endpoint based on rotation
:SELECT_ENDPOINT
if "%~1"=="0" set ENDPOINT=active-sales-query
if "%~1"=="1" set ENDPOINT=active-aggregation-query
if "%~1"=="2" set ENDPOINT=active-blocking-query
if "%~1"=="3" set ENDPOINT=active-cpu-query
if "%~1"=="4" set ENDPOINT=active-io-query
goto :eof
